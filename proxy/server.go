package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

const defaultTopologyRefreshInterval = 5 * time.Second

type serverOptions struct {
	nodeUsername    string
	nodePassword    string
	refreshInterval time.Duration
}

type ServerOption func(*serverOptions) error

func WithNodeCredentials(username, password string) ServerOption {
	return func(options *serverOptions) error {
		options.nodeUsername = username
		options.nodePassword = password
		return nil
	}
}

func WithTopologyRefreshInterval(interval time.Duration) ServerOption {
	return func(options *serverOptions) error {
		if interval <= 0 {
			return fmt.Errorf("topology refresh interval must be positive")
		}
		options.refreshInterval = interval
		return nil
	}
}

type valkeyProxyServer struct {
	listener    *net.TCPAddr
	masterName  string
	controls    []sentinelControl
	options     serverOptions
	reconciler  topologyReconciler
	sessions    *sessionManager
	watcher     *topologyWatcher
	dialContext func(context.Context, string, string) (net.Conn, error)

	ctx    context.Context
	cancel context.CancelFunc

	lifecycleMu sync.Mutex
	tcpListener *net.TCPListener
	closed      bool
	serveWG     sync.WaitGroup
	handlerWG   sync.WaitGroup
	closeOnce   sync.Once
	closeErr    error
}

func NewValkeyProxyServer(listener *net.TCPAddr, clientOption valkey.ClientOption, masterName string, options ...ServerOption) (*valkeyProxyServer, error) {
	if listener == nil {
		return nil, fmt.Errorf("listen address is required")
	}
	masterName = strings.TrimSpace(masterName)
	if masterName == "" {
		return nil, fmt.Errorf("master name is required")
	}
	if len(clientOption.InitAddress) == 0 {
		return nil, fmt.Errorf("at least one Sentinel address is required")
	}
	for _, addr := range clientOption.InitAddress {
		if strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("Sentinel address cannot be empty")
		}
	}

	configuration := serverOptions{refreshInterval: defaultTopologyRefreshInterval}
	for _, apply := range options {
		if err := apply(&configuration); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	sessions := newSessionManager()
	controls := make([]sentinelControl, 0, len(clientOption.InitAddress))
	factories := make([]subscriberFactory, 0, len(clientOption.InitAddress))
	for _, configuredAddr := range clientOption.InitAddress {
		addr := strings.TrimSpace(configuredAddr)
		controls = append(controls, newValkeySentinelControl(addr, clientOption))
		factories = append(factories, newValkeySubscriberFactory(addr, clientOption))
	}
	validator := newPrimaryValidator(valkey.ClientOption{
		Username: configuration.nodeUsername,
		Password: configuration.nodePassword,
	})
	resolver := newTopologyResolver(masterName, controls, validator, sessions)
	reconciler := newTopologyController(resolver, sessions)
	dialer := &net.Dialer{Timeout: topologyOperationTimeout}
	return &valkeyProxyServer{
		listener:    listener,
		masterName:  masterName,
		controls:    controls,
		options:     configuration,
		reconciler:  reconciler,
		sessions:    sessions,
		watcher:     newTopologyWatcher(reconciler, factories, configuration.refreshInterval),
		dialContext: dialer.DialContext,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

func (r *valkeyProxyServer) Serve() {
	if err := r.serve(); err != nil {
		log.Fatal(err)
	}
}

func (r *valkeyProxyServer) serve() error {
	r.lifecycleMu.Lock()
	if r.closed {
		r.lifecycleMu.Unlock()
		return errServerClosed
	}
	r.serveWG.Add(1)
	listener, err := net.ListenTCP("tcp", r.listener)
	if err != nil {
		r.serveWG.Done()
		r.lifecycleMu.Unlock()
		return err
	}
	r.tcpListener = listener
	r.lifecycleMu.Unlock()
	defer r.serveWG.Done()

	if r.watcher != nil {
		r.watcher.Start(r.ctx)
	}
	for {
		downstream, err := listener.AcceptTCP()
		if err != nil {
			if r.isClosed() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				log.Printf("temporary AcceptTCP error: %v", err)
				continue
			}
			return err
		}

		r.lifecycleMu.Lock()
		if r.closed {
			r.lifecycleMu.Unlock()
			_ = downstream.Close()
			return nil
		}
		r.handlerWG.Add(1)
		r.lifecycleMu.Unlock()
		go func() {
			defer r.handlerWG.Done()
			r.handleConnection(downstream)
		}()
	}
}

func (r *valkeyProxyServer) handleConnection(downstream net.Conn) {
	var current *session
	defer func() {
		if current == nil {
			_ = downstream.Close()
			return
		}
		r.sessions.RemoveSession(current)
		_ = current.Close()
	}()

	snapshot, err := r.reconciler.Reconcile(r.ctx)
	if err != nil {
		if r.ctx.Err() == nil {
			log.Printf("resolve primary for %q: %v", r.masterName, err)
		}
		return
	}
	upstream, err := r.dialContext(r.ctx, "tcp", snapshot.addr)
	if err != nil {
		if r.ctx.Err() == nil {
			log.Printf("connect to primary %s: %v", snapshot.addr, err)
		}
		return
	}
	current = newSession(downstream, upstream, snapshot.addr)
	if err := r.sessions.RegisterSession(current, snapshot.addr, snapshot.generation); err != nil {
		return
	}
	r.proxy(current)
}

func (r *valkeyProxyServer) proxy(current *session) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := io.Copy(current.upstream, current.downstream)
		if err != nil {
			_ = current.Close()
			return
		}
		if tcp, ok := current.upstream.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		} else {
			_ = current.upstream.Close()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(current.downstream, current.upstream)
		_ = current.Close()
	}()
	wg.Wait()
}

func (r *valkeyProxyServer) isClosed() bool {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	return r.closed
}

func (r *valkeyProxyServer) Close() error {
	r.closeOnce.Do(func() {
		r.lifecycleMu.Lock()
		r.closed = true
		if r.cancel != nil {
			r.cancel()
		}
		if r.tcpListener != nil {
			if err := r.tcpListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				r.closeErr = errors.Join(r.closeErr, err)
			}
		}
		r.lifecycleMu.Unlock()

		if r.watcher != nil {
			r.watcher.Close()
		}
		r.serveWG.Wait()
		if r.sessions != nil {
			r.sessions.CloseAll()
		}
		r.handlerWG.Wait()
		for _, control := range r.controls {
			control.Close()
		}
	})
	return r.closeErr
}
