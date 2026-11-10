package proxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

type fakeTopologyReconciler struct {
	calls atomic.Int32
	ready chan struct{}
	block chan struct{}
}

func (r *fakeTopologyReconciler) Reconcile(context.Context) (primarySnapshot, error) {
	r.calls.Add(1)
	if r.ready != nil {
		r.ready <- struct{}{}
	}
	if r.block != nil {
		<-r.block
	}
	return primarySnapshot{}, nil
}

type fakeSwitchMasterSubscriber struct {
	watch      func(context.Context, func()) error
	closeCalls atomic.Int32
}

func (s *fakeSwitchMasterSubscriber) Watch(ctx context.Context, onEvent func()) error {
	return s.watch(ctx, onEvent)
}

func (s *fakeSwitchMasterSubscriber) Close() {
	s.closeCalls.Add(1)
}

func waitForSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestValkeySubscriberFactoryUnreachableDoesNotPanic(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	subscriber, err := newValkeySubscriberFactory(addr, valkey.ClientOption{})()
	if err == nil {
		subscriber.Close()
		t.Fatal("subscriber factory error = nil, want connection error")
	}
}

func TestValkeySubscriberReconnectUsesOuterFactory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var subscriptions atomic.Int32
	firstSubscribed := make(chan struct{})
	var serverMu sync.Mutex
	var serverConnections []net.Conn
	option := valkey.ClientOption{
		PipelineMultiplex: -1,
		DialFn: func(_ string, _ *net.Dialer, _ *tls.Config) (net.Conn, error) {
			client, server := net.Pipe()
			serverMu.Lock()
			serverConnections = append(serverConnections, server)
			serverMu.Unlock()
			go func() {
				defer server.Close()
				reader := bufio.NewReader(server)
				for {
					command, err := readRESPCommand(reader)
					if err != nil {
						return
					}
					if len(command) != 2 || command[0] != "SUBSCRIBE" || command[1] != switchMasterChannel {
						_, _ = io.WriteString(server, "-ERR unexpected command\r\n")
						continue
					}
					number := subscriptions.Add(1)
					_, _ = io.WriteString(server, "*3\r\n$9\r\nsubscribe\r\n$14\r\n+switch-master\r\n:1\r\n")
					if number == 1 {
						close(firstSubscribed)
						return
					}
					<-ctx.Done()
					return
				}
			}()
			return client, nil
		},
	}
	t.Cleanup(func() {
		serverMu.Lock()
		defer serverMu.Unlock()
		for _, conn := range serverConnections {
			_ = conn.Close()
		}
	})

	realFactory := newValkeySubscriberFactory("unused", option)
	var outerFactories atomic.Int32
	factory := func() (switchMasterSubscriber, error) {
		outerFactories.Add(1)
		return realFactory()
	}
	watcher := newTopologyWatcher(&fakeTopologyReconciler{}, []subscriberFactory{factory}, time.Hour)
	watcher.Start(ctx)
	defer watcher.Close()
	waitForSignal(t, firstSubscribed, "first subscription")
	waitForCondition(t, "outer subscription reconnect", func() bool {
		return outerFactories.Load() == 2 && subscriptions.Load() == 2
	})
}

func TestTopologyWatcherCoalescesEvents(t *testing.T) {
	reconciler := &fakeTopologyReconciler{ready: make(chan struct{}, 8), block: make(chan struct{})}
	ready := make(chan struct{})
	var callbackMu sync.Mutex
	var callback func()
	subscriber := &fakeSwitchMasterSubscriber{watch: func(ctx context.Context, onEvent func()) error {
		callbackMu.Lock()
		callback = onEvent
		callbackMu.Unlock()
		close(ready)
		<-ctx.Done()
		return ctx.Err()
	}}
	watcher := newTopologyWatcher(reconciler, []subscriberFactory{func() (switchMasterSubscriber, error) {
		return subscriber, nil
	}}, time.Hour)
	watcher.Start(context.Background())
	defer watcher.Close()

	waitForSignal(t, reconciler.ready, "initial reconcile")
	waitForSignal(t, ready, "subscriber")
	for i := 0; i < 8; i++ {
		callbackMu.Lock()
		cb := callback
		callbackMu.Unlock()
		cb()
	}
	close(reconciler.block)
	waitForSignal(t, reconciler.ready, "coalesced reconcile")

	select {
	case <-reconciler.ready:
		t.Fatalf("event burst caused more than two reconciles: %d", reconciler.calls.Load())
	case <-time.After(50 * time.Millisecond):
	}
	if got := reconciler.calls.Load(); got != 2 {
		t.Fatalf("Reconcile calls = %d, want 2", got)
	}
}

func TestTopologyWatcherReconnectsAndReconciles(t *testing.T) {
	reconciler := &fakeTopologyReconciler{ready: make(chan struct{}, 8), block: make(chan struct{})}
	var factories atomic.Int32
	secondStarted := make(chan struct{})
	second := &fakeSwitchMasterSubscriber{watch: func(ctx context.Context, _ func()) error {
		close(secondStarted)
		<-ctx.Done()
		return ctx.Err()
	}}
	watcher := newTopologyWatcher(reconciler, []subscriberFactory{func() (switchMasterSubscriber, error) {
		if factories.Add(1) == 1 {
			return nil, errors.New("subscription failed")
		}
		return second, nil
	}}, time.Hour)
	watcher.Start(context.Background())
	defer watcher.Close()

	waitForSignal(t, reconciler.ready, "initial reconcile")
	waitForSignal(t, secondStarted, "subscription reconnect")
	close(reconciler.block)
	waitForSignal(t, reconciler.ready, "reconnect reconcile")
	if got := reconciler.calls.Load(); got != 2 {
		t.Fatalf("Reconcile calls = %d, want 2", got)
	}
	if got := factories.Load(); got != 2 {
		t.Fatalf("factory calls = %d, want 2", got)
	}
}

func TestTopologyWatcherPeriodicReconcile(t *testing.T) {
	reconciler := &fakeTopologyReconciler{ready: make(chan struct{}, 8)}
	watcher := newTopologyWatcher(reconciler, nil, 20*time.Millisecond)
	watcher.Start(context.Background())
	defer watcher.Close()

	waitForSignal(t, reconciler.ready, "initial reconcile")
	waitForSignal(t, reconciler.ready, "periodic reconcile")
	if got := reconciler.calls.Load(); got < 2 {
		t.Fatalf("Reconcile calls = %d, want at least 2", got)
	}
}

func TestTopologyWatcherConcurrentStartClose(t *testing.T) {
	for i := 0; i < 1000; i++ {
		subscriber := &fakeSwitchMasterSubscriber{watch: func(ctx context.Context, _ func()) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		watcher := newTopologyWatcher(&fakeTopologyReconciler{}, []subscriberFactory{func() (switchMasterSubscriber, error) {
			return subscriber, nil
		}}, time.Hour)
		var calls sync.WaitGroup
		calls.Add(2)
		go func() {
			defer calls.Done()
			watcher.Start(context.Background())
		}()
		go func() {
			defer calls.Done()
			watcher.Close()
		}()
		done := make(chan struct{})
		go func() {
			calls.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("Start/Close deadlock at iteration %d", i)
		}
	}
}

func TestTopologyWatcherClose(t *testing.T) {
	watchStarted := make(chan struct{})
	watchStopped := make(chan struct{})
	subscriber := &fakeSwitchMasterSubscriber{watch: func(ctx context.Context, _ func()) error {
		close(watchStarted)
		<-ctx.Done()
		close(watchStopped)
		return ctx.Err()
	}}
	reconciler := &fakeTopologyReconciler{}
	watcher := newTopologyWatcher(reconciler, []subscriberFactory{func() (switchMasterSubscriber, error) {
		return subscriber, nil
	}}, time.Hour)
	watcher.Start(context.Background())
	waitForSignal(t, watchStarted, "Watch start")

	closed := make(chan struct{})
	go func() {
		watcher.Close()
		close(closed)
	}()
	waitForSignal(t, watchStopped, "Watch cancellation")
	waitForSignal(t, closed, "watcher Close")
	if got := subscriber.closeCalls.Load(); got != 1 {
		t.Fatalf("subscriber close calls = %d, want 1", got)
	}
}
