package proxy

import (
	"errors"
	"net"
	"sync"
)

var (
	errStaleResolution = errors.New("stale topology resolution")
	errTopologyChanged = errors.New("topology changed while establishing session")
	errServerClosed    = errors.New("proxy server is closed")
)

type primarySnapshot struct {
	addr       string
	generation uint64
}

type resolution struct {
	addr string
	id   uint64
}

type topologyState interface {
	PublishPrimary(addr string, resolutionID uint64) (primarySnapshot, error)
	InvalidatePrimary(addr string, resolutionID uint64) error
}

type session struct {
	downstream   net.Conn
	upstream     net.Conn
	upstreamAddr string
	closeOnce    sync.Once
	closeErr     error
}

func newSession(downstream, upstream net.Conn, upstreamAddr string) *session {
	return &session{
		downstream:   downstream,
		upstream:     upstream,
		upstreamAddr: upstreamAddr,
	}
}

func (s *session) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = errors.Join(s.downstream.Close(), s.upstream.Close())
	})
	return s.closeErr
}

type sessionManager struct {
	mu               sync.Mutex
	primary          string
	valid            bool
	generation       uint64
	lastResolutionID uint64
	sessions         map[*session]struct{}
	closed           bool
}

func newSessionManager() *sessionManager {
	return &sessionManager{sessions: make(map[*session]struct{})}
}

func (m *sessionManager) PublishPrimary(addr string, resolutionID uint64) (primarySnapshot, error) {
	var stale []*session

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return primarySnapshot{}, errServerClosed
	}
	if resolutionID < m.lastResolutionID {
		m.mu.Unlock()
		return primarySnapshot{}, errStaleResolution
	}
	m.lastResolutionID = resolutionID
	if !m.valid || m.primary != addr {
		m.generation++
	}
	m.primary = addr
	m.valid = true
	snapshot := primarySnapshot{addr: addr, generation: m.generation}
	for s := range m.sessions {
		if s.upstreamAddr != addr {
			stale = append(stale, s)
			delete(m.sessions, s)
		}
	}
	m.mu.Unlock()

	closeSessions(stale)
	return snapshot, nil
}

func (m *sessionManager) InvalidatePrimary(addr string, resolutionID uint64) error {
	var stale []*session

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errServerClosed
	}
	if resolutionID < m.lastResolutionID {
		m.mu.Unlock()
		return errStaleResolution
	}
	if !m.valid || m.primary != addr {
		m.mu.Unlock()
		return nil
	}
	m.lastResolutionID = resolutionID
	m.valid = false
	m.generation++
	for s := range m.sessions {
		if s.upstreamAddr == addr {
			stale = append(stale, s)
			delete(m.sessions, s)
		}
	}
	m.mu.Unlock()

	closeSessions(stale)
	return nil
}

func (m *sessionManager) RegisterSession(s *session, addr string, generation uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errServerClosed
	}
	if !m.valid || m.primary != addr || m.generation != generation {
		return errTopologyChanged
	}
	m.sessions[s] = struct{}{}
	return nil
}

func (m *sessionManager) RemoveSession(s *session) {
	m.mu.Lock()
	delete(m.sessions, s)
	m.mu.Unlock()
}

func (m *sessionManager) CloseAll() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.valid = false
	sessions := make([]*session, 0, len(m.sessions))
	for s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.sessions = make(map[*session]struct{})
	m.mu.Unlock()

	closeSessions(sessions)
}

func closeSessions(sessions []*session) {
	for _, s := range sessions {
		_ = s.Close()
	}
}

var _ topologyState = (*sessionManager)(nil)
