package proxy

import (
	"errors"
	"net"
	"sync"
	"testing"
)

const (
	testPrimaryA = "127.0.0.1:6379"
	testPrimaryB = "127.0.0.1:6380"
)

type trackedConn struct {
	net.Conn
	mu         sync.Mutex
	closeCalls int
}

func (c *trackedConn) Close() error {
	c.mu.Lock()
	c.closeCalls++
	c.mu.Unlock()
	return c.Conn.Close()
}

func (c *trackedConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCalls
}

func newTestSession(t *testing.T, upstreamAddr string) (*session, *trackedConn, *trackedConn) {
	t.Helper()
	downstream, downstreamPeer := net.Pipe()
	upstream, upstreamPeer := net.Pipe()
	d := &trackedConn{Conn: downstream}
	u := &trackedConn{Conn: upstream}
	s := newSession(d, u, upstreamAddr)
	t.Cleanup(func() {
		_ = s.Close()
		_ = downstreamPeer.Close()
		_ = upstreamPeer.Close()
	})
	return s, d, u
}

func TestSessionCloseIdempotent(t *testing.T) {
	s, downstream, upstream := newTestSession(t, testPrimaryA)
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if got := downstream.closeCount(); got != 1 {
		t.Fatalf("downstream close count = %d, want 1", got)
	}
	if got := upstream.closeCount(); got != 1 {
		t.Fatalf("upstream close count = %d, want 1", got)
	}
}

func TestSessionManagerPublishPrimary(t *testing.T) {
	m := newSessionManager()
	snapshotA, err := m.PublishPrimary(testPrimaryA, 1)
	if err != nil {
		t.Fatalf("PublishPrimary(A) error = %v", err)
	}
	if snapshotA.generation != 1 {
		t.Fatalf("generation = %d, want 1", snapshotA.generation)
	}

	sessionA1, downstreamA1, upstreamA1 := newTestSession(t, testPrimaryA)
	sessionA2, downstreamA2, upstreamA2 := newTestSession(t, testPrimaryA)
	for _, s := range []*session{sessionA1, sessionA2} {
		if err := m.RegisterSession(s, testPrimaryA, snapshotA.generation); err != nil {
			t.Fatalf("RegisterSession(A) error = %v", err)
		}
	}

	repeatedA, err := m.PublishPrimary(testPrimaryA, 2)
	if err != nil {
		t.Fatalf("repeated PublishPrimary(A) error = %v", err)
	}
	if repeatedA.generation != snapshotA.generation {
		t.Fatalf("repeated generation = %d, want %d", repeatedA.generation, snapshotA.generation)
	}
	for name, conn := range map[string]*trackedConn{"downstream A1": downstreamA1, "upstream A1": upstreamA1, "downstream A2": downstreamA2, "upstream A2": upstreamA2} {
		if got := conn.closeCount(); got != 0 {
			t.Fatalf("%s close count after repeated event = %d, want 0", name, got)
		}
	}

	snapshotB, err := m.PublishPrimary(testPrimaryB, 3)
	if err != nil {
		t.Fatalf("PublishPrimary(B) error = %v", err)
	}
	if snapshotB.generation != 2 {
		t.Fatalf("generation after A to B = %d, want 2", snapshotB.generation)
	}
	for name, conn := range map[string]*trackedConn{"downstream A1": downstreamA1, "upstream A1": upstreamA1, "downstream A2": downstreamA2, "upstream A2": upstreamA2} {
		if got := conn.closeCount(); got != 1 {
			t.Fatalf("%s close count after failover = %d, want 1", name, got)
		}
	}

	sessionB, downstreamB, upstreamB := newTestSession(t, testPrimaryB)
	if err := m.RegisterSession(sessionB, testPrimaryB, snapshotB.generation); err != nil {
		t.Fatalf("RegisterSession(B) error = %v", err)
	}
	if _, err := m.PublishPrimary(testPrimaryB, 4); err != nil {
		t.Fatalf("repeated PublishPrimary(B) error = %v", err)
	}
	if downstreamB.closeCount() != 0 || upstreamB.closeCount() != 0 {
		t.Fatal("repeated primary B closed a healthy B session")
	}

	if _, err := m.PublishPrimary(testPrimaryA, 5); err != nil {
		t.Fatalf("PublishPrimary(A again) error = %v", err)
	}
	if downstreamB.closeCount() != 1 || upstreamB.closeCount() != 1 {
		t.Fatal("publishing A did not close only the stale B session")
	}
}

func TestSessionManagerInvalidatePrimary(t *testing.T) {
	m := newSessionManager()
	snapshotA, err := m.PublishPrimary(testPrimaryA, 1)
	if err != nil {
		t.Fatalf("PublishPrimary(A) error = %v", err)
	}
	s, downstream, upstream := newTestSession(t, testPrimaryA)
	if err := m.RegisterSession(s, testPrimaryA, snapshotA.generation); err != nil {
		t.Fatalf("RegisterSession(A) error = %v", err)
	}

	if err := m.InvalidatePrimary(testPrimaryA, 2); err != nil {
		t.Fatalf("InvalidatePrimary(A) error = %v", err)
	}
	if downstream.closeCount() != 1 || upstream.closeCount() != 1 {
		t.Fatal("invalidating A did not close the A session")
	}
	newSession, _, _ := newTestSession(t, testPrimaryA)
	if err := m.RegisterSession(newSession, testPrimaryA, snapshotA.generation); !errors.Is(err, errTopologyChanged) {
		t.Fatalf("RegisterSession after invalidation error = %v, want errTopologyChanged", err)
	}

	snapshotB, err := m.PublishPrimary(testPrimaryB, 3)
	if err != nil {
		t.Fatalf("PublishPrimary(B) error = %v", err)
	}
	if snapshotB.generation != 3 {
		t.Fatalf("generation after invalidation and new primary = %d, want 3", snapshotB.generation)
	}
}

func TestSessionManagerRejectsStaleRegistration(t *testing.T) {
	m := newSessionManager()
	snapshotA, err := m.PublishPrimary(testPrimaryA, 1)
	if err != nil {
		t.Fatalf("PublishPrimary(A) error = %v", err)
	}
	if _, err := m.PublishPrimary(testPrimaryB, 2); err != nil {
		t.Fatalf("PublishPrimary(B) error = %v", err)
	}

	wrongAddress, _, _ := newTestSession(t, testPrimaryA)
	if err := m.RegisterSession(wrongAddress, testPrimaryA, snapshotA.generation); !errors.Is(err, errTopologyChanged) {
		t.Fatalf("wrong-address registration error = %v, want errTopologyChanged", err)
	}
	wrongGeneration, _, _ := newTestSession(t, testPrimaryB)
	if err := m.RegisterSession(wrongGeneration, testPrimaryB, snapshotA.generation); !errors.Is(err, errTopologyChanged) {
		t.Fatalf("wrong-generation registration error = %v, want errTopologyChanged", err)
	}
}

func TestSessionManagerIgnoresLateResolution(t *testing.T) {
	m := newSessionManager()
	snapshotB, err := m.PublishPrimary(testPrimaryB, 2)
	if err != nil {
		t.Fatalf("PublishPrimary(B) error = %v", err)
	}
	if _, err := m.PublishPrimary(testPrimaryA, 1); !errors.Is(err, errStaleResolution) {
		t.Fatalf("late PublishPrimary error = %v, want errStaleResolution", err)
	}
	if err := m.InvalidatePrimary(testPrimaryA, 1); !errors.Is(err, errStaleResolution) {
		t.Fatalf("late InvalidatePrimary error = %v, want errStaleResolution", err)
	}

	s, downstream, upstream := newTestSession(t, testPrimaryB)
	if err := m.RegisterSession(s, testPrimaryB, snapshotB.generation); err != nil {
		t.Fatalf("RegisterSession(B) error = %v", err)
	}
	repeated, err := m.PublishPrimary(testPrimaryB, 2)
	if err != nil {
		t.Fatalf("same-resolution PublishPrimary error = %v", err)
	}
	if repeated.generation != snapshotB.generation {
		t.Fatalf("generation = %d, want %d", repeated.generation, snapshotB.generation)
	}
	if downstream.closeCount() != 0 || upstream.closeCount() != 0 {
		t.Fatal("late or duplicate resolutions changed the active session")
	}
}

func TestSessionManagerCloseAll(t *testing.T) {
	m := newSessionManager()
	snapshot, err := m.PublishPrimary(testPrimaryA, 1)
	if err != nil {
		t.Fatalf("PublishPrimary(A) error = %v", err)
	}
	s, downstream, upstream := newTestSession(t, testPrimaryA)
	if err := m.RegisterSession(s, testPrimaryA, snapshot.generation); err != nil {
		t.Fatalf("RegisterSession(A) error = %v", err)
	}

	m.CloseAll()
	m.CloseAll()
	if downstream.closeCount() != 1 || upstream.closeCount() != 1 {
		t.Fatal("CloseAll did not close each connection exactly once")
	}
	another, _, _ := newTestSession(t, testPrimaryA)
	if err := m.RegisterSession(another, testPrimaryA, snapshot.generation); !errors.Is(err, errServerClosed) {
		t.Fatalf("RegisterSession after CloseAll error = %v, want errServerClosed", err)
	}
	if _, err := m.PublishPrimary(testPrimaryB, 2); !errors.Is(err, errServerClosed) {
		t.Fatalf("PublishPrimary after CloseAll error = %v, want errServerClosed", err)
	}
}
