package multicluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/containerd/errdefs"
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

// IgnoreMissing treats missing resources as already cleaned up. A joined error is harmless only
// when every non-nil child is missing: errors.Is alone could hide a real failure
// alongside a not-found error. Inspect wrappers for joins before classifying
// the original error, retaining SDK error classifiers carried by wrappers.
func IgnoreMissing(err error) error {
	if err == nil || EntirelyMissing(err) {
		return nil
	}
	return err
}

// EntirelyMissing reports that every non-nil error in err is a not-found error.
func EntirelyMissing(err error) bool {
	if err == nil {
		return true
	}
	for current := err; current != nil; {
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			hasChild := false
			for _, child := range joined.Unwrap() {
				if child == nil {
					continue
				}
				hasChild = true
				if !EntirelyMissing(child) {
					return false
				}
			}
			return hasChild
		}
		wrapped, ok := current.(interface{ Unwrap() error })
		if !ok {
			break
		}
		current = wrapped.Unwrap()
	}
	return errdefs.IsNotFound(err)
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
	if err := LockObservation(ctx, &r.mu); err != nil {
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
	if source.UsesHostNetwork() != destination.UsesHostNetwork() {
		return invalid(errors.New("multicluster requires both Ceph clusters to use the same network mode"))
	}
	if source == destination || source.GetContainerID() == destination.GetContainerID() || (!source.UsesHostNetwork() && source.NetworkName() == destination.NetworkName()) {
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
	if source.NetworkName() == "" || destination.NetworkName() == "" ||
		sourceControl == nil || destinationControl == nil ||
		!sourceControl.IsRunning() || !destinationControl.IsRunning() {
		return invalid(errors.New("multicluster requires two running Ceph clusters"))
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

// invalid marks a refused request.
func invalid(err error) error { return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err} }

// RunClient starts an idle CLI client attached to cluster and, on bridge
// networks, to peerNetwork. owned records it for cleanup.
func RunClient(ctx context.Context, image string, cluster *ceph.Container, peerNetwork string, owned *Resources) (testcontainers.Container, error) {
	opts := []testcontainers.ContainerCustomizer{cluster.WithClient(), ceph.WithIdleEntrypoint()}
	if peerNetwork != "" && !cluster.UsesHostNetwork() {
		opts = append(opts, network.WithNetworkName(nil, peerNetwork))
	}
	ctr, err := testcontainers.Run(ctx, image, opts...)
	owned.AddContainer(ctr)
	if err != nil {
		return ctr, fmt.Errorf("run multicluster CLI client: %w", err)
	}
	return ctr, nil
}

// Exec runs args in ctr and returns stdout. It classifies failures as the
// module's own commands do: a nonzero exit is a *ceph.NativeFailure.
func Exec(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, invalid(errors.New("empty multicluster command"))
	}
	out, err := servicehook.Command(ctx, ctr, args...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CephBefore reports whether cluster's recorded release is older than major.
// An unknown version keeps the default image's command forms.
func CephBefore(cluster *ceph.Container, major int) bool {
	recorded, _, _ := strings.Cut(cluster.CephVersion(), ".")
	value, err := strconv.Atoi(recorded)
	return err == nil && value < major
}
