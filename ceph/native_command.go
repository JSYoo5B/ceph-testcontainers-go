package ceph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/testcontainers/testcontainers-go"
)

// Limits of a NativeCommand.
const (
	MaxNativeCommandTimeout = time.Hour
	MaxNativeCommandOutput  = 64 << 20
	MaxNativeCommandInput   = 64 << 20
	// NativeCommandStopTimeout bounds one attempt to stop a command inside its
	// container: the canceler and, when it fails, one inspection of the
	// container. The attempt has this budget of its own, independent of the
	// caller's context, which has usually ended.
	NativeCommandStopTimeout = supervised.CancelTimeout + nativeInspectTimeout
	nativeInspectTimeout     = 5 * time.Second
	// Native timeouts need room for exec startup and the supervisor's final
	// report. Core CLI reads retain their independent short retry allowance.
	nativeExecCushion = 2 * time.Second
)

// ErrNativeCommandIncomplete marks a NativeCommandResult that timed out or
// whose output was truncated: its bytes are not the command's complete output.
var ErrNativeCommandIncomplete = errors.New("native command did not complete within its limits")

// errNativeTerminated is the cause with which Terminate cancels a running
// command.
var errNativeTerminated = errors.New("ceph cluster is terminated")

var errNativeTargetRemoved = errors.New("native command target is being removed")

// errClusterTerminated refuses a request to a terminated cluster.
var errClusterTerminated error = &categoryError{text: "ceph cluster is terminated", kind: ErrInvalidRequest}

// NativeCommand is one native command that RunNativeCommand runs under a
// supervisor inside a container.
type NativeCommand struct {
	// Argv is the program and its arguments. It runs without a shell.
	Argv []string
	// Timeout bounds the command's run time, at most
	// MaxNativeCommandTimeout. The caller's context can shorten it.
	Timeout time.Duration
	// MaxStdout and MaxStderr bound each output stream inside the container,
	// each at most MaxNativeCommandOutput bytes.
	MaxStdout, MaxStderr int
	// Label names the command in errors, which never contain its arguments
	// or output. It defaults to "native command".
	Label string
	// Input, when set, is the command's standard input, and MaxInput, at
	// most MaxNativeCommandInput, bounds the bytes written to it in all.
	// Without Input the command's standard input is empty, and MaxInput must
	// be 0. An Input serves one command.
	Input    *NativeInput
	MaxInput int
}

// ErrNativeInputClosed reports a write to a NativeInput that was closed, or
// whose command no longer takes input.
var ErrNativeInputClosed = errors.New("native command input is closed")

// NativeInput is the standard input of one NativeCommand while it runs.
// Write sends data in order, and Close ends the input. The command gets
// exactly the bytes written, without a shell, an encoding or a terminal in
// between. Nothing that is written appears in arguments, the environment,
// files, errors, labels or diagnostics; the command's own output is returned
// as it wrote it, so a command that must not reveal its input must not echo
// it.
//
// Inside the container the supervisor reads what arrives at once, up to
// MaxInput, and passes it on as the command reads, so a command that reads
// slowly or not at all does not hold up Write. Its methods are safe for
// concurrent use.
type NativeInput struct {
	writes   chan supervised.InputWrite
	closed   chan struct{}
	finished chan struct{}
	close    sync.Once
	claimed  atomic.Bool
}

// NewNativeInput returns an open input for one NativeCommand.
//
//purpose:native
func NewNativeInput() *NativeInput {
	return &NativeInput{writes: make(chan supervised.InputWrite), closed: make(chan struct{}), finished: make(chan struct{})}
}

// Write sends data to the command's standard input after the data of earlier
// writes, and returns once the input stream to the container took all of it.
// A write before RunNativeCommand starts the command waits for it. ctx bounds
// that wait; when ctx ends after the stream took the data, Write returns ctx's
// error, and the command may still get the data, as
// NativeCommandResult.InputWritten tells. Write copies data before it returns.
//
// A write that would take the input past MaxInput is refused whole with an
// error that matches ErrInvalidRequest, and the input stays open. Once the
// input is closed, or the command ended or its input stream failed, Write
// returns an error that matches ErrNativeInputClosed. An empty write sends
// nothing.
//
//purpose:native
func (in *NativeInput) Write(ctx context.Context, data []byte) error {
	select {
	case <-in.closed:
		return ErrNativeInputClosed
	case <-in.finished:
		return ErrNativeInputClosed
	default:
	}
	if len(data) == 0 {
		return nil
	}
	done := make(chan error, 1)
	select {
	case in.writes <- supervised.InputWrite{Data: bytes.Clone(data), Done: done}:
	case <-in.closed:
		return ErrNativeInputClosed
	case <-in.finished:
		return ErrNativeInputClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		switch {
		case errors.Is(err, supervised.ErrInputLimit):
			return invalidRequest("native command input: the write would exceed MaxInput")
		case err != nil:
			return fmt.Errorf("%w: %w", ErrNativeInputClosed, err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close ends the input: the command gets the end of input after every byte
// written before. Close is idempotent and never fails; a command that ended
// before is not affected.
//
//purpose:native
func (in *NativeInput) Close() error {
	in.close.Do(func() { close(in.closed) })
	return nil
}

// NativeCommandResult is the outcome of a supervised native command. Stdout
// and Stderr are separate, raw and binary-safe.
type NativeCommandResult struct {
	Stdout, Stderr []byte
	// ExitCode is the command's exit code; a nonzero one is not an error.
	ExitCode int
	// TimedOut means that the supervisor killed the command's process group
	// at its deadline; the output is what it wrote until then.
	TimedOut bool
	// StdoutTruncated and StderrTruncated mean that a stream exceeded its
	// limit; the result holds its first bytes.
	StdoutTruncated, StderrTruncated bool
	// InputWritten counts the bytes of Input that the input stream to the
	// container took, and InputDelivered those that the supervisor wrote into
	// the command's standard input; the command may not have read all of
	// them. They differ when the command ended, or closed its standard input,
	// before it took everything. InputEOF means that the command's standard
	// input ended after all InputWritten bytes, because Input was closed.
	InputWritten, InputDelivered int64
	InputEOF                     bool
}

// Complete reports whether the command finished within its time and output
// limits, so that its output is complete.
//
//purpose:native
func (r NativeCommandResult) Complete() bool {
	return !r.TimedOut && !r.StdoutTruncated && !r.StderrTruncated
}

// NativeCommandUnstoppedError reports a native command that RunNativeCommand,
// or rgw.Gateway.Admin, could not confirm as stopped after its call ended
// early: it may still run in its container. Stop retries the confirmation;
// Terminate also tries it.
type NativeCommandUnstoppedError struct {
	invocation *nativeInvocation
	err        error
}

func (e *NativeCommandUnstoppedError) Error() string {
	if e.invocation == nil {
		return "native command may still run in its container"
	}
	return fmt.Sprintf("%s may still run in its container: %v", e.invocation.label, e.err)
}

func (e *NativeCommandUnstoppedError) Unwrap() error { return e.err }

// Stop stops the command, or confirms that it ended with its container, and
// returns nil once the command can no longer run: after the container was
// removed, or once it runs again after a restart. A stopped container cannot
// confirm anything until then. Each attempt has its own
// NativeCommandStopTimeout, and waits for a concurrent attempt within ctx.
// Stop is idempotent and can be retried, also after Terminate.
//
//purpose:native,lifetime
func (e *NativeCommandUnstoppedError) Stop(ctx context.Context) error {
	inv := e.invocation
	if inv == nil {
		return errors.New("native command error has no invocation")
	}
	if err := inv.stop(ctx); err != nil {
		return err
	}
	inv.owner.native.calls.forget(inv)
	return nil
}

// nativeInvocation is one RunNativeCommand call, from its start until its
// command is known to have ended.
type nativeInvocation struct {
	owner  *Container
	target testcontainers.Container
	// targetID captures the Docker instance at admission rather than reading a
	// possibly mutable wrapper again while draining invocations.
	targetID string
	label    string
	cancel   context.CancelCauseFunc
	// stopCommand confirms that the command of marker no longer runs:
	// supervised.Cancel, or supervised.CancelShell for a shell supervisor.
	stopCommand func(ctx context.Context, ctr testcontainers.Container, label, marker string) error
	// done is closed when the call has returned. It then has set marker if
	// it could not confirm that its command stopped.
	done   chan struct{}
	marker string
	// mu serializes stop.
	mu      sync.Mutex
	stopped bool
}

// stop returns once the command can no longer run. The canceler confirms
// that inside the container, also for a command of an earlier instance of a
// restarted container. Only a removed container is confirmed from outside: an
// exec that Docker still starts would run in whatever instance runs then.
func (inv *nativeInvocation) stop(ctx context.Context) error {
	if err := lockTopologyMutex(ctx, &inv.mu); err != nil {
		return fmt.Errorf("stop %s: %w", inv.label, err)
	}
	defer inv.mu.Unlock()
	if inv.stopped {
		return nil
	}
	if err := inv.stopCommand(ctx, inv.target, inv.label, inv.marker); err != nil {
		stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nativeInspectTimeout)
		_, stateErr := inv.target.State(stateCtx)
		cancel()
		if !errdefs.IsNotFound(stateErr) {
			return safeNativeError(fmt.Errorf("stop %s: %w", inv.label, err))
		}
	}
	inv.stopped = true
	return nil
}

// nativeError is an error whose text names each Docker or container
// implementation failure in it only by its kind; Unwrap returns the original
// error.
type nativeError struct {
	text string
	err  error
}

func (e *nativeError) Error() string { return e.text }

func (e *nativeError) Unwrap() error { return e.err }

// safeNativeError replaces the text of every supervised.BackendError in err,
// which Docker or the caller's container type wrote and which may quote the
// command or its output, by its operation and kind.
func safeNativeError(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	safe := text
	var visit func(error)
	visit = func(err error) {
		switch e := err.(type) {
		case *supervised.BackendError:
			safe = strings.ReplaceAll(safe, e.Error(), e.Op+": "+backendKind(e.Err))
		case interface{ Unwrap() []error }:
			for _, inner := range e.Unwrap() {
				if inner != nil {
					visit(inner)
				}
			}
		case interface{ Unwrap() error }:
			if inner := e.Unwrap(); inner != nil {
				visit(inner)
			}
		}
	}
	visit(err)
	if safe == text {
		return err
	}
	return &nativeError{text: safe, err: err}
}

// backendKind names the kind of a Docker or container implementation error.
func backendKind(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded.Error()
	case errdefs.IsNotFound(err):
		return "the container does not exist"
	case errdefs.IsConflict(err):
		return "the container is not running, or is paused"
	}
	return "Docker or the container implementation failed, and errors.Unwrap returns its error"
}

// validateNativeCommand checks a command before anything runs and returns
// its label.
func validateNativeCommand(command NativeCommand) (string, error) {
	label := command.Label
	if label == "" {
		label = "native command"
	}
	switch {
	case len(label) > 128 || strings.IndexFunc(label, unicode.IsControl) >= 0:
		return "", errors.New("native command label must be at most 128 bytes without control characters")
	case len(command.Argv) == 0 || command.Argv[0] == "":
		return "", fmt.Errorf("%s needs a program", label)
	case slices.ContainsFunc(command.Argv, func(arg string) bool { return strings.IndexByte(arg, 0) >= 0 }):
		return "", fmt.Errorf("%s arguments must not contain NUL", label)
	case command.Timeout <= 0 || command.Timeout > MaxNativeCommandTimeout:
		return "", fmt.Errorf("%s timeout must be positive and at most %v", label, MaxNativeCommandTimeout)
	case command.MaxStdout <= 0 || command.MaxStdout > MaxNativeCommandOutput || command.MaxStderr <= 0 || command.MaxStderr > MaxNativeCommandOutput:
		return "", fmt.Errorf("%s output limits must be positive and at most %d bytes", label, MaxNativeCommandOutput)
	case command.Input == nil && command.MaxInput != 0:
		return "", fmt.Errorf("%s input limit needs an Input", label)
	case command.Input != nil && (command.MaxInput <= 0 || command.MaxInput > MaxNativeCommandInput):
		return "", fmt.Errorf("%s input limit must be positive and at most %d bytes", label, MaxNativeCommandInput)
	case command.Input != nil && (command.Input.writes == nil || command.Input.claimed.Load()):
		return "", fmt.Errorf("%s input must come from NewNativeInput and serve no other command", label)
	}
	return label, nil
}

// RunNativeCommand runs one native command in target under the supervisor
// that the module's own native queries use. target is the control container,
// a daemon or service container of this cluster, or a container of the caller
// attached to the cluster's network (the host network for a host-network
// cluster); the call never adopts, stops or removes the caller's container.
// target must be running, not paused, and have python3, which runs the
// supervisor. A malformed command or an ended ctx fails before anything runs.
//
// The supervisor starts the command in its own process group, bounds each
// output stream inside the container and kills the group at the deadline,
// Timeout shortened to ctx's deadline. A command that times out or exceeds an
// output limit returns its result, marked by TimedOut or the Truncated flags,
// with an error that wraps ErrNativeCommandIncomplete. A descendant that
// starts its own session leaves the group and is not stopped.
//
// When ctx ends first, the call stops the command inside the container before
// it returns, also when Docker had not started it yet, within its own
// NativeCommandStopTimeout. When that cannot be confirmed, for example because
// the container is paused or a signal killed the supervisor, the error is a
// *NativeCommandUnstoppedError, whose Stop retries. Terminate cancels running
// commands and stops those not confirmed. Stopping a command does not undo
// what it already did, such as a MON command it had sent, and nothing is
// retried. Errors name the command by Label; a failure of Docker or of
// target's implementation appears only by its kind, and errors.Unwrap returns
// the original error, whose text may quote anything. RedactSecrets redacts
// output that a test logs.
//
// With Input, the supervisor runs in an exec of the Docker engine with its
// standard input attached, and passes what Input writes to the command; see
// NativeInput. The call stops taking writes when the command ends, and
// returns only after its input and output work ended. Its deadline and
// cancellation stop the command as without Input, and a write in progress
// then fails. A malformed limit or an Input that served another command fails
// before anything runs or any input is taken.
//
// Failures before the command runs are classified: a malformed command, or a
// target that the cluster neither owns nor has attached, or that is stopped or
// paused, is ErrInvalidRequest; a target without python3, or a program that
// does not exist or is not executable, is ErrImageCapability; a program that
// could not start for want of a process or memory, or that a security policy
// refused, and other Docker failures are ErrEnvironmentUnavailable. A container
// that disappears or stops after the command was sent matches no category,
// since the command may have run, and neither do context errors,
// ErrNativeCommandIncomplete, ErrNativeInputClosed or
// *NativeCommandUnstoppedError. The essential and client roles and the RGW
// setup runtime provide python3; the cephfs role does not promise it.
//
//purpose:native
func (c *Container) RunNativeCommand(ctx context.Context, target testcontainers.Container, command NativeCommand) (NativeCommandResult, error) {
	label, err := validateNativeCommand(command)
	if err != nil {
		return NativeCommandResult{}, classify(ErrInvalidRequest, err)
	}
	if err := ctx.Err(); err != nil {
		return NativeCommandResult{}, err
	}
	if target == nil || target.GetContainerID() == "" {
		return NativeCommandResult{}, invalidRequest("%s needs a started container", label)
	}
	// A Docker call that ctx ended may fail without wrapping ctx's error.
	if err := c.admitNativeTarget(ctx, target); err != nil {
		if ctx.Err() != nil {
			return NativeCommandResult{}, safeNativeError(fmt.Errorf("%s: %w", label, errors.Join(err, ctx.Err())))
		}
		return NativeCommandResult{}, safeNativeError(fmt.Errorf("%s: %w", label, classifyNativeBackend(err, false)))
	}
	state, err := target.State(ctx)
	if err != nil {
		err = &supervised.BackendError{Op: "inspect the container", Err: err}
	}
	switch {
	case ctx.Err() != nil:
		return NativeCommandResult{}, safeNativeError(fmt.Errorf("%s: %w", label, errors.Join(err, ctx.Err())))
	case err != nil && !errdefs.IsNotFound(err) && !errdefs.IsConflict(err):
		return NativeCommandResult{}, safeNativeError(fmt.Errorf("%s: %w", label, classifyNativeBackend(err, false)))
	case err != nil || state == nil || !state.Running || state.Paused:
		return NativeCommandResult{}, safeNativeError(classify(ErrInvalidRequest, errors.Join(fmt.Errorf("%s needs a running, unpaused container", label), err)))
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	inv := &nativeInvocation{owner: c, target: target, targetID: target.GetContainerID(), label: label, cancel: cancel, stopCommand: supervised.Cancel, done: make(chan struct{})}
	var claim func() bool
	if command.Input != nil {
		claim = func() bool { return command.Input.claimed.CompareAndSwap(false, true) }
	}
	switch c.native.calls.admit(inv, claim) {
	case nativeRefusedClosed:
		return NativeCommandResult{}, fmt.Errorf("%s: %w", label, errClusterTerminated)
	case nativeRefusedFenced:
		return NativeCommandResult{}, invalidRequest("%s target is fenced for OSD removal", label)
	case nativeRefusedClaim:
		return NativeCommandResult{}, invalidRequest("%s input serves another command", label)
	}

	var report supervised.Report
	if in := command.Input; in != nil {
		report, err = supervised.RunInputWithExecCushion(runCtx, target, label, command.Timeout, nativeExecCushion, command.MaxStdout, command.MaxStderr,
			supervised.Input{Limit: int64(command.MaxInput), Writes: in.writes, Closed: in.closed, Finished: in.finished}, command.Argv...)
	} else {
		report, err = supervised.RunWithExecCushion(runCtx, target, label, command.Timeout, nativeExecCushion, command.MaxStdout, command.MaxStderr, command.Argv...)
	}
	unstopped, _ := supervised.Unconfirmed(err)
	var notStopped *supervised.NotStoppedError
	if unstopped != "" && !errors.As(err, &notStopped) {
		// A signal killed the supervisor before its cleanup, and nothing has
		// tried to stop its command yet.
		inv.marker = unstopped
		if stopErr := inv.stop(context.WithoutCancel(ctx)); stopErr != nil {
			err = errors.Join(err, stopErr)
		} else {
			unstopped = ""
		}
	}
	var exit *supervised.ExitError
	c.native.calls.finish(inv, unstopped, unstopped != "")
	close(inv.done)
	if cause := context.Cause(runCtx); err != nil && ctx.Err() == nil &&
		(errors.Is(cause, errNativeTerminated) || errors.Is(cause, errNativeTargetRemoved)) {
		err = fmt.Errorf("%s: %w: %w", label, cause, err)
	}
	err = safeNativeError(err)
	switch {
	case unstopped != "":
		return NativeCommandResult{}, &NativeCommandUnstoppedError{invocation: inv, err: err}
	case errors.As(err, &exit) && (exit.Code == 126 || exit.Code == 127):
		return NativeCommandResult{}, &OperationError{Kind: ErrImageCapability, Op: label, Capability: "python3",
			Err: fmt.Errorf("the container cannot run python3, which the supervisor needs: %w", err)}
	case errors.As(err, &exit) && exit.Code == supervised.LaunchFailedExit:
		// The program is not named: errors never contain the arguments.
		return NativeCommandResult{}, &OperationError{Kind: ErrImageCapability, Op: label,
			Err: errors.New("the program could not be started, for example because it does not exist or is not executable")}
	case errors.As(err, &exit) && exit.Code == supervised.LaunchUnavailableExit:
		return NativeCommandResult{}, &OperationError{Kind: ErrEnvironmentUnavailable, Op: label,
			Err: errors.New("the program could not be started for want of a process or memory, or because a security policy refused it")}
	case err != nil && ctx.Err() == nil && !errors.Is(err, errNativeTerminated) && !errors.Is(err, errNativeTargetRemoved):
		// The command may have run: its outcome is unknown.
		return NativeCommandResult{}, classifyNativeBackend(err, true)
	case err != nil:
		return NativeCommandResult{}, err
	}
	result := NativeCommandResult{Stdout: report.Stdout, Stderr: report.Stderr, ExitCode: report.ExitCode,
		TimedOut: report.TimedOut, StdoutTruncated: report.StdoutTruncated, StderrTruncated: report.StderrTruncated,
		InputWritten: report.InputWritten, InputDelivered: report.InputDelivered, InputEOF: report.InputEOF}
	if !result.Complete() {
		return result, fmt.Errorf("%s: %w (timed out %t, stdout truncated %t, stderr truncated %t)", label, ErrNativeCommandIncomplete,
			result.TimedOut, result.StdoutTruncated, result.StderrTruncated)
	}
	return result, nil
}

// admitNativeTarget accepts a container of this cluster, or one of the caller
// attached to the cluster's network.
func (c *Container) admitNativeTarget(ctx context.Context, target testcontainers.Container) error {
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	closed := c.closed
	if closed {
		c.mu.Unlock()
		return errClusterTerminated
	}
	owned, err := c.monitorTopology().ownsNativeTarget(ctx, target.GetContainerID())
	if err != nil {
		c.mu.Unlock()
		return err
	}
	networks := c.nets.bridgeNames()
	hostNetwork := c.settings.hostNetwork
	c.mu.Unlock()
	if !owned {
		var err error
		if owned, err = c.controlPlane.owns(ctx, target.GetContainerID()); err != nil {
			return err
		}
	}
	switch {
	case closed:
		return errClusterTerminated
	case owned:
		return nil
	}
	attached, err := target.Networks(ctx)
	if err != nil {
		return &supervised.BackendError{Op: "inspect the container's networks", Err: err}
	}
	if hostNetwork && slices.Contains(attached, "host") {
		return nil
	}
	for _, name := range networks {
		if slices.Contains(attached, name) {
			return nil
		}
	}
	return invalidRequest("the container is neither part of this cluster nor attached to its network")
}

// classifyNativeBackend classifies a failed Docker call of a native command
// whose context has not ended. Before the command was dispatched, a container
// that is gone, stopped or paused refuses the request; afterwards the
// command may have run, so that error stays unclassified. Any other failure
// is the engine's. An error already classified, or one of the supervisor's
// own, keeps its meaning.
func classifyNativeBackend(err error, dispatched bool) error {
	var backend *supervised.BackendError
	switch {
	case !errors.As(err, &backend) || classified(err):
		return err
	case (errdefs.IsNotFound(backend.Err) || errdefs.IsConflict(backend.Err)) && dispatched:
		return err
	case errdefs.IsNotFound(backend.Err) || errdefs.IsConflict(backend.Err):
		return classify(ErrInvalidRequest, err)
	case errors.Is(backend.Err, context.Canceled) || errors.Is(backend.Err, context.DeadlineExceeded):
		return err
	}
	return classify(ErrEnvironmentUnavailable, err)
}

// RedactSecrets returns a copy of data in which the secrets that the cluster
// knows, and values, are replaced by [REDACTED], as in CollectDiagnostics:
// keyring keys, service credentials, and the private keys, secret assignments
// and bearer tokens that it recognizes. It is meant for output that a test
// logs or reports; an oracle compares the raw bytes. Redaction is best effort.
//
//purpose:evidence
func (c *Container) RedactSecrets(ctx context.Context, data []byte, values ...string) ([]byte, error) {
	snapshot, err := c.diagnosticsSnapshot(ctx, DiagnosticsConfig{RedactValues: values})
	if err != nil {
		return nil, err
	}
	return []byte(diagnosticsRedactor(snapshot.secrets)(string(data))), nil
}

// nativeCommands owns the native-command state and the blocking half of its
// lifecycle: cancelling every call when the cluster terminates, fencing the
// calls of one removed target and waiting for cancelled calls to return.
// nativeState keeps the leaf-locked bookkeeping and never blocks while it
// holds its lock; these methods block and so live here, calling nativeState
// only for its bookkeeping. They take no other lock.
type nativeCommands struct {
	// calls is the bookkeeping of the running calls; see nativeState.
	calls nativeState
}

// cancelAll refuses new native commands, cancels the running
// ones and returns every tracked invocation. It takes only the native lock, so
// Terminate calls it before it waits for the cluster mutex, which an
// invocation's caller may hold.
func (n *nativeCommands) cancelAll() []*nativeInvocation {
	pending, clientCancel := n.calls.close()
	if clientCancel != nil {
		clientCancel()
	}
	for _, inv := range pending {
		inv.cancel(errNativeTerminated)
	}
	return pending
}

// stopAll refuses new native commands, cancels the running
// ones, waits until their calls have returned and stops the commands that
// they could not confirm as stopped. It keeps those it cannot stop and returns
// their errors.
func (n *nativeCommands) stopAll(ctx context.Context) []error {
	return n.waitStopped(ctx, n.cancelAll())
}

// fence closes admission and drains exactly one Docker instance.
// It never closes native admission: unrelated targets and later distinct instances
// remain usable. An unconfirmed stop stays tracked and the fence remains for
// retry. The caller serializes the removal under the cluster topology lock.
func (n *nativeCommands) fence(ctx context.Context, id string) error {
	if id == "" {
		return invalidRequest("OSD removal needs an identified container")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pending := n.calls.fence(id)
	for _, inv := range pending {
		inv.cancel(errNativeTargetRemoved)
	}
	return errors.Join(n.waitStopped(ctx, pending)...)
}

func (n *nativeCommands) waitStopped(ctx context.Context, pending []*nativeInvocation) []error {
	var errs []error
	for _, inv := range pending {
		// A canceled call returns within its own stop budget.
		wait := time.NewTimer(NativeCommandStopTimeout + nativeInspectTimeout)
		select {
		case <-inv.done:
			wait.Stop()
		case <-wait.C:
			errs = append(errs, fmt.Errorf("%s did not return after its cancellation", inv.label))
			continue
		case <-ctx.Done():
			wait.Stop()
			errs = append(errs, fmt.Errorf("wait for %s: %w", inv.label, ctx.Err()))
			continue
		}
		if inv.marker == "" {
			continue
		}
		if err := inv.stop(ctx); err != nil {
			errs = append(errs, err)
			continue
		}
		n.calls.forget(inv)
	}
	return errs
}
