package proxy

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

const switchMasterChannel = "+switch-master"

type switchMasterSubscriber interface {
	Watch(ctx context.Context, onEvent func()) error
	Close()
}

type subscriberFactory func() (switchMasterSubscriber, error)

type valkeySwitchMasterSubscriber struct {
	client valkey.Client
}

func newValkeySubscriberFactory(addr string, option valkey.ClientOption) subscriberFactory {
	return func() (switchMasterSubscriber, error) {
		option.DisableRetry = true
		client, err := valkey.NewClient(directValkeyClientOption(addr, option))
		if err != nil {
			return nil, err
		}
		return &valkeySwitchMasterSubscriber{client: client}, nil
	}
}

func (s *valkeySwitchMasterSubscriber) Watch(ctx context.Context, onEvent func()) error {
	return s.client.Receive(ctx, s.client.B().Subscribe().Channel(switchMasterChannel).Build(), func(message valkey.PubSubMessage) {
		if message.Channel == switchMasterChannel {
			onEvent()
		}
	})
}

func (s *valkeySwitchMasterSubscriber) Close() {
	s.client.Close()
}

type topologyWatcher struct {
	reconciler topologyReconciler
	factories  []subscriberFactory
	interval   time.Duration
	trigger    chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup
	mu        sync.Mutex
	cancel    context.CancelFunc
	closed    bool
}

func newTopologyWatcher(reconciler topologyReconciler, factories []subscriberFactory, interval time.Duration) *topologyWatcher {
	return &topologyWatcher{
		reconciler: reconciler,
		factories:  factories,
		interval:   interval,
		trigger:    make(chan struct{}, 1),
	}
}

func (w *topologyWatcher) Start(ctx context.Context) {
	w.startOnce.Do(func() {
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return
		}
		watchCtx, cancel := context.WithCancel(ctx)
		w.cancel = cancel
		w.wg.Add(1 + len(w.factories))
		w.mu.Unlock()

		go w.reconcileLoop(watchCtx)
		for _, factory := range w.factories {
			go w.subscriptionLoop(watchCtx, factory)
		}
		w.requestReconcile()
	})
}

func (w *topologyWatcher) Close() {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		if w.cancel != nil {
			w.cancel()
		}
		w.mu.Unlock()
		w.wg.Wait()
	})
}

func (w *topologyWatcher) requestReconcile() {
	select {
	case w.trigger <- struct{}{}:
	default:
	}
}

func (w *topologyWatcher) reconcileLoop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-w.trigger:
		}
		if _, err := w.reconciler.Reconcile(ctx); err != nil && ctx.Err() == nil {
			log.Printf("Sentinel topology reconciliation failed: %v", err)
		}
	}
}

func (w *topologyWatcher) subscriptionLoop(ctx context.Context, factory subscriberFactory) {
	defer w.wg.Done()
	backoff := 100 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		subscriber, err := factory()
		if err == nil {
			w.requestReconcile()
			err = subscriber.Watch(ctx, w.requestReconcile)
			subscriber.Close()
		}
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			backoff = 100 * time.Millisecond
		} else {
			log.Printf("Sentinel subscription failed: %v", err)
		}
		if !waitForReconnect(ctx, backoff) {
			return
		}
		if backoff *= 2; backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}

func waitForReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var _ switchMasterSubscriber = (*valkeySwitchMasterSubscriber)(nil)
