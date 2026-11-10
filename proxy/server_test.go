package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

type reconcilerFunc func(context.Context) (primarySnapshot, error)

func (f reconcilerFunc) Reconcile(ctx context.Context) (primarySnapshot, error) {
	return f(ctx)
}

func newTestProxyServer(reconciler topologyReconciler) *valkeyProxyServer {
	ctx, cancel := context.WithCancel(context.Background())
	dialer := &net.Dialer{Timeout: time.Second}
	return &valkeyProxyServer{
		listener:    &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0},
		reconciler:  reconciler,
		sessions:    newSessionManager(),
		dialContext: dialer.DialContext,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func waitForCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func startTestBackend(t *testing.T, handle func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handle(conn)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	return listener.Addr().String()
}

func newTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("ListenTCP() error = %v", err)
	}
	defer listener.Close()
	accepted := make(chan *net.TCPConn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatalf("DialTCP() error = %v", err)
	}
	select {
	case server := <-accepted:
		t.Cleanup(func() {
			_ = client.Close()
			_ = server.Close()
		})
		return client, server
	case err := <-acceptErr:
		t.Fatalf("AcceptTCP() error = %v", err)
		return nil, nil
	case <-time.After(time.Second):
		t.Fatal("timed out accepting TCP pair")
		return nil, nil
	}
}

func TestNewValkeyProxyServerOptions(t *testing.T) {
	listenAddr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
	clientOption := valkey.ClientOption{
		InitAddress: []string{"sentinel-1:26379", "sentinel-2:26379"},
		Username:    "sentinel-user",
		Password:    "sentinel-pass",
	}
	server, err := NewValkeyProxyServer(listenAddr, clientOption, "mymaster", WithNodeCredentials("node-user", "node-pass"), WithTopologyRefreshInterval(time.Second))
	if err != nil {
		t.Fatalf("NewValkeyProxyServer() error = %v", err)
	}
	defer server.Close()
	if got := len(server.controls); got != 2 {
		t.Fatalf("Sentinel controls = %d, want 2", got)
	}
	if server.options.nodeUsername != "node-user" || server.options.nodePassword != "node-pass" {
		t.Fatal("data-node credentials were not applied")
	}
	if server.options.refreshInterval != time.Second {
		t.Fatalf("refresh interval = %s, want 1s", server.options.refreshInterval)
	}

	withoutNodeCredentials, err := NewValkeyProxyServer(listenAddr, clientOption, "mymaster")
	if err != nil {
		t.Fatalf("NewValkeyProxyServer() without node credentials error = %v", err)
	}
	defer withoutNodeCredentials.Close()
	if withoutNodeCredentials.options.nodeUsername != "" || withoutNodeCredentials.options.nodePassword != "" {
		t.Fatal("Sentinel credentials were implicitly reused for data nodes")
	}

	for name, mutate := range map[string]func(*net.TCPAddr, *valkey.ClientOption, *string, *[]ServerOption){
		"empty Sentinel address": func(_ *net.TCPAddr, option *valkey.ClientOption, _ *string, _ *[]ServerOption) {
			option.InitAddress = []string{}
		},
		"blank Sentinel address": func(_ *net.TCPAddr, option *valkey.ClientOption, _ *string, _ *[]ServerOption) {
			option.InitAddress = []string{" "}
		},
		"empty master": func(_ *net.TCPAddr, _ *valkey.ClientOption, master *string, _ *[]ServerOption) {
			*master = ""
		},
		"zero refresh interval": func(_ *net.TCPAddr, _ *valkey.ClientOption, _ *string, options *[]ServerOption) {
			*options = append(*options, WithTopologyRefreshInterval(0))
		},
	} {
		t.Run(name, func(t *testing.T) {
			option := clientOption
			master := "mymaster"
			var options []ServerOption
			mutate(listenAddr, &option, &master, &options)
			if server, err := NewValkeyProxyServer(listenAddr, option, master, options...); err == nil {
				_ = server.Close()
				t.Fatal("NewValkeyProxyServer() error = nil, want configuration error")
			}
		})
	}
}

func TestProxyHalfClose(t *testing.T) {
	backendAddr := startTestBackend(t, func(conn net.Conn) {
		request, err := io.ReadAll(conn)
		if err != nil {
			return
		}
		_, _ = conn.Write(append([]byte("echo:"), request...))
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	})
	var sessions *sessionManager
	serverReconciler := reconcilerFunc(func(context.Context) (primarySnapshot, error) {
		return sessions.PublishPrimary(backendAddr, 1)
	})
	server := newTestProxyServer(serverReconciler)
	sessions = server.sessions
	defer server.Close()

	client, downstream := newTCPPair(t)
	handlerDone := make(chan struct{})
	go func() {
		server.handleConnection(downstream)
		close(handlerDone)
	}()
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatalf("client Write() error = %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite() error = %v", err)
	}
	response, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("client ReadAll() error = %v", err)
	}
	if string(response) != "echo:ping" {
		t.Fatalf("response = %q, want echo:ping", response)
	}
	waitForSignal(t, handlerDone, "proxy handler completion")
}

type failingReadConn struct {
	net.Conn
}

func (c *failingReadConn) Read([]byte) (int, error) {
	return 0, errors.New("injected downstream read failure")
}

func TestProxyCopyErrorClosesSession(t *testing.T) {
	upstream, _ := newTCPPair(t)
	downstream, downstreamPeer := net.Pipe()
	defer downstreamPeer.Close()
	current := newSession(&failingReadConn{Conn: downstream}, upstream, testPrimaryA)
	defer current.Close()
	server := &valkeyProxyServer{}
	proxyDone := make(chan struct{})
	go func() {
		server.proxy(current)
		close(proxyDone)
	}()
	select {
	case <-proxyDone:
	case <-time.After(time.Second):
		t.Fatal("proxy retained a session after a non-EOF copy error")
	}
}

func TestServerClosesStaleSession(t *testing.T) {
	backendAddr := startTestBackend(t, func(conn net.Conn) {
		_, _ = io.Copy(io.Discard, conn)
	})
	var sessions *sessionManager
	serverReconciler := reconcilerFunc(func(context.Context) (primarySnapshot, error) {
		return sessions.PublishPrimary(backendAddr, 1)
	})
	server := newTestProxyServer(serverReconciler)
	sessions = server.sessions
	defer server.Close()

	client, downstream := newTCPPair(t)
	handlerDone := make(chan struct{})
	go func() {
		server.handleConnection(downstream)
		close(handlerDone)
	}()
	waitForCondition(t, "session registration", func() bool {
		sessions.mu.Lock()
		defer sessions.mu.Unlock()
		return len(sessions.sessions) == 1
	})
	if _, err := sessions.PublishPrimary(testPrimaryB, 2); err != nil {
		t.Fatalf("PublishPrimary(B) error = %v", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("stale client connection remained open")
	}
	waitForSignal(t, handlerDone, "stale session shutdown")
}

func TestServerRejectsRegistrationRace(t *testing.T) {
	serverReconciler := reconcilerFunc(func(context.Context) (primarySnapshot, error) {
		return primarySnapshot{addr: testPrimaryA, generation: 1}, nil
	})
	server := newTestProxyServer(serverReconciler)
	if _, err := server.sessions.PublishPrimary(testPrimaryA, 1); err != nil {
		t.Fatalf("PublishPrimary(A) error = %v", err)
	}
	defer server.Close()

	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	upstream, upstreamPeer := net.Pipe()
	trackedUpstream := &trackedConn{Conn: upstream}
	server.dialContext = func(context.Context, string, string) (net.Conn, error) {
		close(dialStarted)
		<-releaseDial
		return trackedUpstream, nil
	}
	downstream, downstreamPeer := net.Pipe()
	trackedDownstream := &trackedConn{Conn: downstream}
	defer downstreamPeer.Close()
	defer upstreamPeer.Close()
	handlerDone := make(chan struct{})
	go func() {
		server.handleConnection(trackedDownstream)
		close(handlerDone)
	}()
	waitForSignal(t, dialStarted, "upstream dial")
	if _, err := server.sessions.PublishPrimary(testPrimaryB, 2); err != nil {
		t.Fatalf("PublishPrimary(B) error = %v", err)
	}
	close(releaseDial)
	waitForSignal(t, handlerDone, "racing handler")
	if trackedDownstream.closeCount() != 1 || trackedUpstream.closeCount() != 1 {
		t.Fatal("racing handler did not close both connections")
	}
}

func TestServerCloseUnblocksHandler(t *testing.T) {
	reconcileStarted := make(chan struct{})
	serverReconciler := reconcilerFunc(func(ctx context.Context) (primarySnapshot, error) {
		close(reconcileStarted)
		<-ctx.Done()
		return primarySnapshot{}, ctx.Err()
	})
	server := newTestProxyServer(serverReconciler)

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.serve()
	}()
	waitForCondition(t, "TCP listener", func() bool {
		server.lifecycleMu.Lock()
		defer server.lifecycleMu.Unlock()
		return server.tcpListener != nil
	})
	server.lifecycleMu.Lock()
	addr := server.tcpListener.Addr().String()
	server.lifecycleMu.Unlock()
	client, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	defer client.Close()
	waitForSignal(t, reconcileStarted, "handler reconciliation")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- server.Close()
	}()
	select {
	case err := <-closeDone:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not unblock the handler")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serve() did not exit")
	}
}

func TestServerCloseIdempotent(t *testing.T) {
	server := newTestProxyServer(reconcilerFunc(func(context.Context) (primarySnapshot, error) {
		return primarySnapshot{}, errors.New("unused")
	}))
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			_ = server.Close()
		}()
	}
	wg.Wait()
}
