package ceph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
)

// cephConn is the command transport to one cluster: it selects the container
// that runs native commands and runs them there. The control container is
// preferred; until it is published, and after it is removed, commands run in
// the first MON container, which is set once while Run starts the cluster and
// never changes afterwards.
//
// The transport holds no cluster state of its own and takes no lock other than
// the control-plane read lock inside controlPlaneState, so it may be used with
// or without Container.mu held (the order is mu, then the control lock). It
// does not check the cluster lifetime: callers decide whether a terminated
// cluster refuses the operation, as they did before the transport existed.
// Domain queries and their decoding belong to the types that hold a cephConn,
// not here.
type cephConn struct {
	control  *controlPlaneState
	fallback testcontainers.Container
}

// conn returns the cluster's transport. It is a view built on each call, so a
// Container composed in a test reaches the same control plane and first MON as
// one started by Run.
func (c *Container) conn() cephConn {
	if c == nil {
		return cephConn{}
	}
	return cephConn{control: &c.controlPlane, fallback: c.Container}
}

// cliContainer returns the command container without waiting: the control
// container when one is published, the first MON otherwise.
func (n cephConn) cliContainer() testcontainers.Container {
	if n.control == nil {
		return n.fallback
	}
	if control := n.control.current(); control != nil {
		return control
	}
	return n.fallback
}

// controlContainer is cliContainer that gives up when ctx ends while it waits
// for a concurrent control-plane publication or termination.
func (n cephConn) controlContainer(ctx context.Context) (testcontainers.Container, error) {
	if n.control == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	control, err := n.control.currentContext(ctx)
	if err != nil {
		return nil, err
	}
	if control != nil {
		return control, nil
	}
	return n.fallback, nil
}

// ceph runs the Ceph CLI for the module's own operations.
func (n cephConn) ceph(ctx context.Context, args ...string) ([]byte, error) {
	control, err := n.controlContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("select Ceph CLI container: %w", err)
	}
	return command(ctx, control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
}

// rotationCommand runs one native command of a rotation in the control
// container under the supervisor, which stops it inside the container before
// returning when ctx ends first, or reports a supervised.NotStoppedError. A
// nonzero exit is a *NativeFailure, which reads only standard error: the
// standard output of a rotation can hold keys. A ceph CLI that no MON
// answered connects again, as in command.
func (n cephConn) rotationCommand(ctx context.Context, argv ...string) ([]byte, error) {
	return reconnectCeph(ctx, argv, func() ([]byte, error) {
		result, err := n.rotationResult(ctx, 1<<20, 64<<10, argv...)
		if err != nil {
			return nil, err
		}
		if result.ExitCode != 0 {
			return result.Stdout, nativeFailure(argv[0], result.ExitCode, nil, result.Stderr)
		}
		return result.Stdout, nil
	})
}

// rotationResult runs argv like rotationCommand and returns the bounded
// result, whatever the exit code.
func (n cephConn) rotationResult(ctx context.Context, stdoutLimit, stderrLimit int, argv ...string) (supervised.Result, error) {
	control, err := n.controlContainer(ctx)
	if err != nil {
		return supervised.Result{}, fmt.Errorf("select Ceph CLI container: %w", err)
	}
	return supervised.Command(ctx, control, "key rotation", keyRotationCommandTimeout, stdoutLimit, stderrLimit, argv...)
}

// supervisedLookup binds a supervised lookup to this transport. Each call
// selects the control container again.
func (n cephConn) supervisedLookup() supervisedLookup {
	return func(ctx context.Context) (supervisedRun, error) {
		control, err := n.controlContainer(ctx)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (supervised.Report, error) {
			return supervised.Run(ctx, control, label, processLimit, stdoutLimit, stderrLimit, argv...)
		}, nil
	}
}
