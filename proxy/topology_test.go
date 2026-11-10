package proxy

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeSentinelControl struct {
	name   string
	addr   string
	err    error
	calls  int
	closed bool
}

func (c *fakeSentinelControl) MasterAddr(context.Context, string) (string, error) {
	c.calls++
	return c.addr, c.err
}

func (c *fakeSentinelControl) Close() {
	c.closed = true
}

type fakeRoleValidator struct {
	roles map[string]string
	errs  map[string]error
}

func (v *fakeRoleValidator) Validate(_ context.Context, addr string) (string, error) {
	if err := v.errs[addr]; err != nil {
		return "", err
	}
	return v.roles[addr], nil
}

type fakeTopologyState struct {
	publishedAddr string
	publishedID   uint64
	publishResult primarySnapshot
	publishErr    error
	invalidated   []string
	invalidateIDs []uint64
	invalidateErr error
}

func (s *fakeTopologyState) PublishPrimary(addr string, resolutionID uint64) (primarySnapshot, error) {
	s.publishedAddr = addr
	s.publishedID = resolutionID
	if s.publishErr != nil {
		return primarySnapshot{}, s.publishErr
	}
	if s.publishResult.addr == "" {
		s.publishResult = primarySnapshot{addr: addr, generation: 1}
	}
	return s.publishResult, nil
}

func (s *fakeTopologyState) InvalidatePrimary(addr string, resolutionID uint64) error {
	s.invalidated = append(s.invalidated, addr)
	s.invalidateIDs = append(s.invalidateIDs, resolutionID)
	return s.invalidateErr
}

func TestTopologyResolverSkipsReplica(t *testing.T) {
	s1 := &fakeSentinelControl{name: "s1", addr: testPrimaryA}
	s2 := &fakeSentinelControl{name: "s2", addr: testPrimaryB}
	validator := &fakeRoleValidator{roles: map[string]string{testPrimaryA: "slave", testPrimaryB: "master"}}
	state := &fakeTopologyState{}
	resolver := newTopologyResolver("mymaster", []sentinelControl{s1, s2}, validator, state)

	got, err := resolver.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got.addr != testPrimaryB {
		t.Fatalf("Resolve().addr = %q, want %q", got.addr, testPrimaryB)
	}
	if got.id != 1 {
		t.Fatalf("Resolve().id = %d, want 1", got.id)
	}
	if len(state.invalidated) != 1 || state.invalidated[0] != testPrimaryA {
		t.Fatalf("invalidated = %v, want [%s]", state.invalidated, testPrimaryA)
	}
	if len(state.invalidateIDs) != 1 || state.invalidateIDs[0] != got.id {
		t.Fatalf("invalidate IDs = %v, want [%d]", state.invalidateIDs, got.id)
	}
}

func TestTopologyResolverInconclusiveDoesNotInvalidate(t *testing.T) {
	s1 := &fakeSentinelControl{name: "s1", addr: testPrimaryA}
	s2 := &fakeSentinelControl{name: "s2", addr: testPrimaryB}
	validator := &fakeRoleValidator{
		roles: map[string]string{testPrimaryB: "master"},
		errs:  map[string]error{testPrimaryA: errors.New("NOAUTH Authentication required")},
	}
	state := &fakeTopologyState{}
	resolver := newTopologyResolver("mymaster", []sentinelControl{s1, s2}, validator, state)

	got, err := resolver.Resolve(context.Background())
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got.addr != testPrimaryB {
		t.Fatalf("Resolve().addr = %q, want %q", got.addr, testPrimaryB)
	}
	if len(state.invalidated) != 0 {
		t.Fatalf("inconclusive validation invalidated %v, want none", state.invalidated)
	}
}

func TestTopologyResolverNoMaster(t *testing.T) {
	s1 := &fakeSentinelControl{name: "s1", addr: testPrimaryA}
	s2 := &fakeSentinelControl{name: "s2", err: errors.New("sentinel unavailable")}
	validator := &fakeRoleValidator{roles: map[string]string{testPrimaryA: "slave"}}
	state := &fakeTopologyState{}
	resolver := newTopologyResolver("mymaster", []sentinelControl{s1, s2}, validator, state)

	if _, err := resolver.Resolve(context.Background()); err == nil || !strings.Contains(err.Error(), "mymaster") {
		t.Fatalf("Resolve() error = %v, want error containing master name", err)
	}
}

func TestTopologyResolverPrefersLastSuccessfulSentinel(t *testing.T) {
	s1 := &fakeSentinelControl{name: "s1", err: errors.New("unavailable")}
	s2 := &fakeSentinelControl{name: "s2", addr: testPrimaryB}
	validator := &fakeRoleValidator{roles: map[string]string{testPrimaryA: "master", testPrimaryB: "master"}}
	state := &fakeTopologyState{}
	resolver := newTopologyResolver("mymaster", []sentinelControl{s1, s2}, validator, state)

	if _, err := resolver.Resolve(context.Background()); err != nil {
		t.Fatalf("first Resolve() error = %v", err)
	}
	s1.err = nil
	s1.addr = testPrimaryA
	if _, err := resolver.Resolve(context.Background()); err != nil {
		t.Fatalf("second Resolve() error = %v", err)
	}
	if s1.calls != 1 {
		t.Fatalf("s1 calls = %d, want only the initial failed call", s1.calls)
	}
	if s2.calls != 2 {
		t.Fatalf("s2 calls = %d, want 2", s2.calls)
	}
}

func TestTopologyControllerPublishesResolution(t *testing.T) {
	control := &fakeSentinelControl{addr: testPrimaryB}
	validator := &fakeRoleValidator{roles: map[string]string{testPrimaryB: "master"}}
	state := &fakeTopologyState{publishResult: primarySnapshot{addr: testPrimaryB, generation: 7}}
	resolver := newTopologyResolver("mymaster", []sentinelControl{control}, validator, state)
	controller := newTopologyController(resolver, state)

	snapshot, err := controller.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if snapshot != state.publishResult {
		t.Fatalf("Reconcile() = %+v, want %+v", snapshot, state.publishResult)
	}
	if state.publishedAddr != testPrimaryB || state.publishedID != 1 {
		t.Fatalf("published = %q/%d, want %q/1", state.publishedAddr, state.publishedID, testPrimaryB)
	}
	if _, err := controller.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	if state.publishedID != 2 {
		t.Fatalf("second published resolution ID = %d, want 2", state.publishedID)
	}
}
