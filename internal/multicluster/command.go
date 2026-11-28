package multicluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// ExecStdoutLimit bounds the standard output that Exec keeps.
	ExecStdoutLimit = 16 << 20
	// DeadlineSignal is the signal that stops a command at the supervisor's
	// own limit, SIGABRT: Ceph's handler of it writes the fatal signal report
	// of the thread that took it, the main thread of a Ceph command, whose
	// other threads block signals, and so says where the command was stuck.
	DeadlineSignal = 6
)

// owners maps each handle of a setup client or daemon that a fixture
// recorded with Resources.AddOwned to the cluster its commands run for. A
// lookup compares handles and calls no method of the container.
var owners = struct {
	sync.Mutex
	byHandle map[testcontainers.Container]*ceph.Container
}{byHandle: map[testcontainers.Container]*ceph.Container{}}

func own(owner *ceph.Container, handles []testcontainers.Container) {
	owners.Lock()
	defer owners.Unlock()
	for _, handle := range handles {
		if handle != nil && owner != nil {
			owners.byHandle[handle] = owner
		}
	}
}

func forget(handles []testcontainers.Container) {
	owners.Lock()
	defer owners.Unlock()
	for _, handle := range handles {
		delete(owners.byHandle, handle)
	}
}

// ownerOf returns the cluster that ctr was recorded for, or nil.
func ownerOf(ctr testcontainers.Container) *ceph.Container {
	owners.Lock()
	defer owners.Unlock()
	return owners.byHandle[ctr]
}

// Command is one native command of a fixture: Argv runs in Target, a setup
// client, daemon or gateway of Owner. A nil Owner is the cluster that Target
// was recorded for with Resources.AddOwned; a Target recorded for none, such
// as a control container or a client that the caller supplied, runs its
// command untracked by any cluster.
type Command struct {
	Owner  *ceph.Container
	Target testcontainers.Container
	// Label names the command in errors; empty is Argv[0].
	Label string
	// StdoutLimit is the standard output kept, 0 for all of it.
	StdoutLimit int
	// Limit, when positive, replaces Limit(ctx) as the seconds after which
	// the supervisor stops the command.
	Limit int
	Argv  []string
}

// Run runs cmd under the process supervisor, tracked by its owner as
// servicehook.RunOwnedRoleCommand does: the owner's Terminate stops it. The supervisor stops the command
// with the deadline signal at Limit(ctx), and when ctx ends first the command
// is stopped before Run returns. A stop that cannot be confirmed returns a
// *ceph.NativeCommandUnstoppedError, whose Stop retries it. An ended ctx
// otherwise returns an error that wraps ctx's error.
//
// The report is returned when the supervisor reported: the caller classifies
// its exit code, timeout and truncation. The other failures are classified:
// a missing target or command is ceph.ErrInvalidRequest, a target without
// python3 or the program is ceph.ErrImageCapability, and a Docker failure
// after dispatch is classified as servicehook.ClassifyExec does, since the
// command may have run.
func Run(ctx context.Context, cmd Command) (supervised.Report, error) {
	if len(cmd.Argv) == 0 {
		return supervised.Report{}, invalid(errors.New("empty fixture command"))
	}
	label := cmd.Label
	if label == "" {
		label = cmd.Argv[0]
	}
	if servicehook.DiagnosticHandle(cmd.Target) == nil {
		return supervised.Report{}, invalid(fmt.Errorf("%s: the container is unavailable", label))
	}
	if err := ctx.Err(); err != nil {
		return supervised.Report{}, fmt.Errorf("%s: %w", label, err)
	}
	limit := cmd.Limit
	if limit <= 0 {
		limit = Limit(ctx)
	}
	owner := cmd.Owner
	if owner == nil {
		owner = ownerOf(cmd.Target)
	}
	report, err := servicehook.RunOwnedRoleCommand(supervised.WithDeadlineSignal(ctx, DeadlineSignal), owner, cmd.Target, label, limit, cmd.StdoutLimit, cmd.Argv...)
	var unstopped *ceph.NativeCommandUnstoppedError
	switch {
	case err == nil:
		return report, nil
	case errors.As(err, &unstopped):
		// Its Stop, or the owner's Terminate, confirms the stop later.
		return supervised.Report{}, err
	case ctx.Err() != nil:
		return supervised.Report{}, fmt.Errorf("%s: the call ended before the command, which was stopped: %w", label, ctx.Err())
	case errors.Is(err, ceph.ErrImageCapability):
		return supervised.Report{}, err
	}
	var exit *supervised.ExitError
	if errors.As(err, &exit) && exit.Code == supervised.LaunchFailedExit {
		return supervised.Report{}, &ceph.OperationError{Kind: ceph.ErrImageCapability, Op: label, Capability: cmd.Argv[0],
			Err: fmt.Errorf("the container cannot run %s", cmd.Argv[0])}
	}
	return supervised.Report{}, servicehook.ClassifyExec(ctx, err)
}

// Limit is how long a command may run before its supervisor stops it on its
// own: ctx's bound less a margin for stopping it and returning, so that a
// command that runs too long is reported as such, with where it was, within
// the bound. Without a bound it is 0, and an ended ctx stops the command.
func Limit(ctx context.Context) int {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	limit := remaining - 15*time.Second
	if limit < remaining/2 {
		limit = remaining / 2
	}
	return max(1, int(limit/time.Second))
}

// Exec runs args in ctr as Run does, keeping at most
// ExecStdoutLimit bytes of standard output, and returns that output. It
// classifies failures as the module's own commands do: a nonzero exit is a
// *ceph.NativeFailure, and a command that the supervisor stopped at its limit
// or that wrote more than the limit is an error that wraps
// ceph.ErrNativeCommandIncomplete.
func Exec(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	report, err := Run(ctx, Command{Target: ctr, StdoutLimit: ExecStdoutLimit, Argv: args})
	if err != nil {
		return nil, err
	}
	switch {
	case report.TimedOut:
		return nil, fmt.Errorf("%s still ran at its limit and was stopped: %w", args[0], ceph.ErrNativeCommandIncomplete)
	case report.ExitCode != 0:
		return nil, servicehook.NativeFailure(args[0], report.ExitCode, report.Stdout, report.Stderr)
	case report.StdoutTruncated:
		return nil, fmt.Errorf("%s wrote more than %d bytes of standard output: %w", args[0], ExecStdoutLimit, ceph.ErrNativeCommandIncomplete)
	}
	return report.Stdout, nil
}
