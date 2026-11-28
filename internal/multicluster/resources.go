package multicluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// Resources records the containers and cleanup actions a cross-cluster
// fixture owns. Cleanup runs in reverse construction order. Successful actions are retained
// as completed so partial failures can be retried without double termination.
type Resources struct {
	mu      sync.Mutex
	actions []cleanupAction
}

type cleanupAction struct {
	name    string
	cleanup func(context.Context, ...testcontainers.TerminateOption) error
	done    bool
}

// AddContainer records ctr for termination.
func (r *Resources) AddContainer(ctr testcontainers.Container) {
	if ctr == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, cleanupAction{name: "terminate container", cleanup: func(ctx context.Context, opts ...testcontainers.TerminateOption) error {
		return IgnoreMissing(ctr.Terminate(ctx, opts...))
	}})
}

// AddOwned records ctr, a setup client or daemon that runs native commands
// for owner, for termination, and makes Run track the commands run through
// ctr or one of its other handles, such as the inner container of a daemon
// type that embeds it, as owner's until the termination ran.
func (r *Resources) AddOwned(ctr testcontainers.Container, owner *ceph.Container, handles ...testcontainers.Container) {
	if ctr == nil {
		return
	}
	handles = append([]testcontainers.Container{ctr}, handles...)
	own(owner, handles)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, cleanupAction{name: "terminate container", cleanup: func(ctx context.Context, opts ...testcontainers.TerminateOption) error {
		err := IgnoreMissing(ctr.Terminate(ctx, opts...))
		if err == nil {
			forget(handles)
		}
		return err
	}})
}

// IgnoreMissing treats missing resources as already cleaned up: err is
// harmless only when every non-nil error joined in it is a not-found error
// (servicehook.OnlyMissingHostResource), so a not-found error never hides a
// real failure joined next to it.
func IgnoreMissing(err error) error {
	if servicehook.OnlyMissingHostResource(err) {
		return nil
	}
	return err
}

// Len returns the number of recorded cleanup actions, completed or not.
func (r *Resources) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.actions)
}

// AddCleanup records a named cleanup action.
func (r *Resources) AddCleanup(name string, cleanup func(context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, cleanupAction{name: name, cleanup: func(ctx context.Context, _ ...testcontainers.TerminateOption) error { return cleanup(ctx) }})
}

// Terminate runs the recorded actions in reverse order. Completed actions are
// kept so a partial failure can be retried.
func (r *Resources) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if err := clusterutil.LockMutex(ctx, &r.mu); err != nil {
		return err
	}
	defer r.mu.Unlock()
	var errs []error
	for i := len(r.actions) - 1; i >= 0; i-- {
		a := &r.actions[i]
		if a.done {
			continue
		}
		if err := a.cleanup(ctx, opts...); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", a.name, err))
		} else {
			a.done = true
		}
	}
	return errors.Join(errs...)
}

// ValidatePair checks that two clusters are distinct, running and on the same
// network mode before a cross-cluster fixture uses them.
func ValidatePair(ctx context.Context, image string, source, destination *ceph.Container) error {
	if strings.TrimSpace(image) == "" {
		return invalid(errors.New("multicluster image must not be empty"))
	}
	if source == nil || destination == nil || source.Container == nil || destination.Container == nil {
		return invalid(errors.New("multicluster requires two initialized Ceph clusters"))
	}
	sourceFacts, err := ceph.FactsOf(source)
	if err != nil {
		return err
	}
	destinationFacts, err := ceph.FactsOf(destination)
	if err != nil {
		return err
	}
	if sourceFacts.UsesHostNetwork != destinationFacts.UsesHostNetwork {
		return invalid(errors.New("multicluster requires both Ceph clusters to use the same network mode"))
	}
	if source == destination || source.GetContainerID() == destination.GetContainerID() || (!sourceFacts.UsesHostNetwork && sourceFacts.NetworkName == destinationFacts.NetworkName) {
		return invalid(errors.New("multicluster requires independent Ceph clusters"))
	}
	// A multi-MON fixture can remain live after its embedded primary MON was
	// stopped or removed. Its independent CLI is the control handle; checking
	// only the original container would reject a healthy surviving quorum.
	sourceControl, err := source.ControlContainer(ctx)
	if err != nil {
		return fmt.Errorf("inspect source Ceph control handle: %w", err)
	}
	destinationControl, err := destination.ControlContainer(ctx)
	if err != nil {
		return fmt.Errorf("inspect destination Ceph control handle: %w", err)
	}
	if sourceFacts.NetworkName == "" || destinationFacts.NetworkName == "" ||
		sourceControl == nil || destinationControl == nil {
		return invalid(errors.New("multicluster requires two running Ceph clusters"))
	}
	for _, control := range []struct {
		name      string
		container testcontainers.Container
	}{{"source", sourceControl}, {"destination", destinationControl}} {
		if err := inspectControlRunning(ctx, control.name, control.container); err != nil {
			return err
		}
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if _, err := cluster.ConnectionConfig(ctx); err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("multicluster requires initialized, unterminated Ceph clusters: %w", err)
			}
			return invalid(fmt.Errorf("multicluster requires initialized, unterminated Ceph clusters: %w", err))
		}
	}
	return nil
}

func inspectControlRunning(ctx context.Context, name string, control testcontainers.Container) error {
	inspection, err := control.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect %s Ceph control container: %w", name, err)
	}
	if inspection == nil || inspection.State == nil || !inspection.State.Running {
		return invalid(fmt.Errorf("multicluster requires a running %s Ceph control container", name))
	}
	return nil
}

// invalid marks a refused request.
func invalid(err error) error { return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err} }

// RunClient starts an idle CLI client attached to cluster and, on bridge
// networks, to peerNetwork. owned records it for cleanup.
func RunClient(ctx context.Context, image string, cluster *ceph.Container, peerNetwork string, owned *Resources) (testcontainers.Container, error) {
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		return nil, err
	}
	opts := []testcontainers.ContainerCustomizer{cluster.WithClient(), ceph.WithIdleEntrypoint()}
	if peerNetwork != "" && !facts.UsesHostNetwork {
		opts = append(opts, network.WithNetworkName(nil, peerNetwork))
	}
	created, err := testcontainers.Run(ctx, image, opts...)
	var ctr testcontainers.Container
	if created != nil {
		ctr = created
		owned.AddOwned(ctr, cluster)
	}
	if ctr == nil && err == nil {
		return nil, errors.New("run multicluster CLI client returned no container")
	}
	if err != nil {
		return ctr, fmt.Errorf("run multicluster CLI client: %w", err)
	}
	return ctr, nil
}

// CephBefore reports whether cluster's recorded release is older than major.
// An unknown version keeps the default image's command forms.
func CephBefore(cluster *ceph.Container, major int) bool {
	return cluster != nil && servicehook.CephBefore(cluster, major)
}
