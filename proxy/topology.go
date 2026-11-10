package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type topologyResolver struct {
	masterName string
	controls   []sentinelControl
	validator  roleValidator
	state      topologyState

	nextResolutionID atomic.Uint64
	preferredMu      sync.Mutex
	preferred        int
}

func newTopologyResolver(masterName string, controls []sentinelControl, validator roleValidator, state topologyState) *topologyResolver {
	return &topologyResolver{
		masterName: masterName,
		controls:   controls,
		validator:  validator,
		state:      state,
	}
}

func (r *topologyResolver) Resolve(ctx context.Context) (resolution, error) {
	id := r.nextResolutionID.Add(1)
	if len(r.controls) == 0 {
		return resolution{}, fmt.Errorf("resolve primary for %q: no Sentinel controls", r.masterName)
	}

	r.preferredMu.Lock()
	preferred := r.preferred
	r.preferredMu.Unlock()

	var errs []error
	for offset := 0; offset < len(r.controls); offset++ {
		index := (preferred + offset) % len(r.controls)
		candidate, err := r.controls[index].MasterAddr(ctx, r.masterName)
		if err != nil {
			errs = append(errs, fmt.Errorf("Sentinel %d: %w", index, err))
			continue
		}
		role, err := r.validator.Validate(ctx, candidate)
		if err != nil {
			errs = append(errs, fmt.Errorf("validate candidate %s: %w", candidate, err))
			continue
		}
		if role != "master" {
			errs = append(errs, fmt.Errorf("candidate %s reported role %q", candidate, role))
			if err := r.state.InvalidatePrimary(candidate, id); err != nil {
				if errors.Is(err, errStaleResolution) {
					return resolution{}, err
				}
				errs = append(errs, fmt.Errorf("invalidate candidate %s: %w", candidate, err))
			}
			continue
		}

		r.preferredMu.Lock()
		r.preferred = index
		r.preferredMu.Unlock()
		return resolution{addr: candidate, id: id}, nil
	}
	return resolution{}, fmt.Errorf("resolve primary for %q: %w", r.masterName, errors.Join(errs...))
}

type topologyReconciler interface {
	Reconcile(context.Context) (primarySnapshot, error)
}

type topologyController struct {
	resolver *topologyResolver
	state    topologyState
}

func newTopologyController(resolver *topologyResolver, state topologyState) *topologyController {
	return &topologyController{resolver: resolver, state: state}
}

func (c *topologyController) Reconcile(ctx context.Context) (primarySnapshot, error) {
	resolved, err := c.resolver.Resolve(ctx)
	if err != nil {
		return primarySnapshot{}, err
	}
	return c.state.PublishPrimary(resolved.addr, resolved.id)
}

var _ topologyReconciler = (*topologyController)(nil)
