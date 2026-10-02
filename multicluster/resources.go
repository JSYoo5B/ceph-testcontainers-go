package multicluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/containerd/errdefs"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// Cleanup runs in reverse construction order. Successful actions are retained
// as completed so partial failures can be retried without double termination.
type resources struct {
	mu      sync.Mutex
	actions []cleanupAction
}

type cleanupAction struct {
	name    string
	cleanup func(context.Context, ...testcontainers.TerminateOption) error
	done    bool
}

func (r *resources) addContainer(ctr testcontainers.Container) {
	if ctr == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, cleanupAction{name: "terminate container", cleanup: func(ctx context.Context, opts ...testcontainers.TerminateOption) error {
		return ignoreMissing(ctr.Terminate(ctx, opts...))
	}})
}

// Missing resources are already cleaned up. A joined error is harmless only
// when every non-nil child is missing: errors.Is alone could hide a real failure
// alongside a not-found error. Inspect wrappers for joins before classifying
// the original error, retaining SDK error classifiers carried by wrappers.
func ignoreMissing(err error) error {
	if err == nil || entirelyMissing(err) {
		return nil
	}
	return err
}

func entirelyMissing(err error) bool {
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
				if !entirelyMissing(child) {
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

func (r *resources) addCleanup(name string, cleanup func(context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, cleanupAction{name: name, cleanup: func(ctx context.Context, _ ...testcontainers.TerminateOption) error { return cleanup(ctx) }})
}

func (r *resources) terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	r.mu.Lock()
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

func validatePair(image string, source, destination *ceph.Container) error {
	if strings.TrimSpace(image) == "" {
		return errors.New("multicluster image must not be empty")
	}
	if source == nil || destination == nil || source.Container == nil || destination.Container == nil {
		return errors.New("multicluster requires two initialized Ceph clusters")
	}
	if source == destination || source.GetContainerID() == destination.GetContainerID() || source.NetworkName() == destination.NetworkName() {
		return errors.New("multicluster requires independent Ceph clusters")
	}
	if source.NetworkName() == "" || destination.NetworkName() == "" || !source.IsRunning() || !destination.IsRunning() {
		return errors.New("multicluster requires two running Ceph clusters")
	}
	return nil
}

func runClient(ctx context.Context, image string, cluster *ceph.Container, peerNetwork string, owned *resources) (testcontainers.Container, error) {
	opts := []testcontainers.ContainerCustomizer{cluster.WithClient(), testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity")}
	if peerNetwork != "" {
		opts = append(opts, network.WithNetworkName(nil, peerNetwork))
	}
	ctr, err := testcontainers.Run(ctx, image, opts...)
	owned.addContainer(ctr)
	if err != nil {
		return ctr, fmt.Errorf("run multicluster CLI client: %w", err)
	}
	return ctr, nil
}

func exec(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, errors.New("empty multicluster command")
	}
	code, reader, err := ctr.Exec(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("execute %s: %w", args[0], err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, fmt.Errorf("read %s output: %w", args[0], err)
	}
	if code != 0 {
		return nil, fmt.Errorf("%s exited %d: %s%s", args[0], code, stdout.String(), stderr.String())
	}
	return stdout.Bytes(), nil
}
