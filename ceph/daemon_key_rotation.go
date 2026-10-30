package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// keyRotationCleanupTimeout bounds the rollback of a rotation that failed
// before the daemon received the new key, and each settling, independently of
// the caller's context, which may be the reason for the failure. Tests shorten
// it.
var keyRotationCleanupTimeout = 30 * time.Second

// keyRotationCommandTimeout bounds each native command of a rotation.
const keyRotationCommandTimeout = time.Minute

// authFence records what earlier calls that change the auth entry of one
// entity, a daemon's key rotations or a client's key replacement, left
// uncertain. The caller holds c.mu.
type authFence struct {
	// handedKey is a key whose handoff may still reach the daemon process that
	// handedTo identifies by its container's ID and start time, or "" while
	// that is unknown. No other key may replace handedKey while that process
	// runs. handedDone means that the rotation to handedKey has finished.
	handedKey, handedTo string
	handedDone          bool
	// clients holds the files in the control container in which the clients
	// of MON commands that may still apply stored their messenger nonces; the
	// next call settles the MONs for them first.
	clients []string
	// running holds the commands that were not confirmed to have ended.
	running []rotationRun
	// transition is a change of a client's auth entry whose outcome is not
	// known yet.
	transition *clientTransition
}

// monitorMutationScript sends one MON command through librados. Before it
// sends the command, it stores in argv[1] the nonce of its messenger, by which
// every MON lists the client's sessions, so that settling can wait until no
// MON still holds a connection that could deliver the command. argv[2] is the
// command in JSON, and the file argv[3], when given, its input; its output
// goes to stdout and a failure exits 1. It shuts its client down before
// exiting, because a librados client left to the interpreter's shutdown
// sometimes aborts there, after the command applied (std::bad_variant_access
// or SIGSEGV, about 2.5% of runs on Squid 19.2.5 and Tentacle 20.2.4).
const monitorMutationScript = `import os,rados,re,sys
c=rados.Rados(conffile='/etc/ceph/ceph.conf')
try:
 c.connect(timeout=5)
 nonce=re.search(r'/(\d+)',c.get_addrs()).group(1)
 with open(sys.argv[1]+'.tmp','w') as f:
  f.write(nonce)
 os.rename(sys.argv[1]+'.tmp',sys.argv[1])
 inbuf=b''
 if len(sys.argv)>3:
  with open(sys.argv[3],'rb') as f:
   inbuf=f.read()
 ret,out,err=c.mon_command(sys.argv[2],inbuf)
finally:
 c.shutdown()
sys.stdout.buffer.write(out)
sys.stderr.write(err)
sys.exit(0 if ret==0 else 1)
`

// authCall collects the MON mutations of one call that changes the auth
// entry of entity, such as one RotateKey or ClientConfig.ReplaceKey call, until
// they are settled. The caller holds c.mu.
type authCall struct {
	c      *Container
	entity string
	// clients holds the nonce files of the call's mutations.
	clients []string
	// unstopped means that the supervisor could not confirm that a mutation
	// stopped; the next call stops it before settling.
	unstopped bool
}

// mutate runs auth <verb> on the call's entity through
// monitorMutationScript and returns its output.
func (call *authCall) mutate(ctx context.Context, verb string) ([]byte, error) {
	return call.send(ctx, map[string]any{"prefix": "auth " + verb, "entity": call.entity, "format": "json"}, nil)
}

// send runs command through monitorMutationScript, with input, when it is
// not nil, as the command's input, and returns its output. The input goes
// to a file in the control container that send removes again.
func (call *authCall) send(ctx context.Context, command map[string]any, input []byte) ([]byte, error) {
	call.c.removeStrayAuthInputs(ctx)
	prefix, _ := command["prefix"].(string)
	path := "/tmp/tc-auth-" + uuid.NewString()
	encoded, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	argv := []string{"python3", "-c", monitorMutationScript, path + ".client", string(encoded)}
	if input != nil {
		if err := call.c.cliContainer().CopyToContainer(ctx, input, path+".in", 0o600); err != nil {
			// Docker may complete the copy after its request failed, also
			// after the removal below, so later calls remove it again.
			call.c.authRecords.addStray(path+".in", true)
			call.removeInput(ctx, path+".in")
			return nil, fmt.Errorf("%s %s: copy the input: %w", prefix, call.entity, err)
		}
		argv = append(argv, path+".in")
		// A script that still runs fails to read a removed input and sends
		// nothing.
		defer call.removeInput(ctx, path+".in")
	}
	call.clients = append(call.clients, path+".client")
	out, err := call.c.rotationCommand(ctx, argv...)
	if err != nil {
		if _, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
			call.c.fenceUnstopped(call.c.authRecords.fenceFor(call.entity), call.c.cliContainer(), err)
			call.unstopped = true
		}
		return out, fmt.Errorf("%s %s: %w", prefix, call.entity, err)
	}
	return out, nil
}

// removeInput removes an input file, which may hold a key, or leaves it to
// later calls when it cannot.
func (call *authCall) removeInput(ctx context.Context, path string) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cancel()
	if _, err := call.c.rotationCommand(cleanupCtx, "rm", "-f", path); err != nil {
		call.c.authRecords.addStray(path, false)
	}
}

// removeStrayAuthInputs removes the files with keys that earlier calls could
// not remove for sure. A file whose copy failed is removed again by every
// later call, since nothing tells when Docker gave up on it. The caller holds
// c.mu.
func (c *Container) removeStrayAuthInputs(ctx context.Context) {
	paths := c.authRecords.strayPaths()
	if len(paths) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cancel()
	if _, err := c.rotationCommand(cleanupCtx, append([]string{"rm", "-f"}, paths...)...); err == nil {
		c.authRecords.forgetRemovedStrays()
	}
}

// keys runs auth get-or-create-pending and returns the key and pending key.
func (call *authCall) keys(ctx context.Context) (struct{ Key, PendingKey string }, error) {
	out, err := call.mutate(ctx, "get-or-create-pending")
	if err != nil {
		return struct{ Key, PendingKey string }{}, err
	}
	return parseAuthKeys(out, "get-or-create-pending", call.entity)
}

// settle returns once no mutation of the call can apply any more, or records
// them in the daemon's fence, for the next call to settle first, and says so.
func (call *authCall) settle(ctx context.Context) error {
	if len(call.clients) == 0 {
		return nil
	}
	clients := call.clients
	call.clients = nil
	var err error
	if call.unstopped {
		err = errors.New("a MON command of this call may still run; the next call stops it and settles the MONs first")
	} else if err = call.c.monitorSettling().settle(ctx, clients); err == nil {
		_, _ = call.c.rotationCommand(ctx, append([]string{"rm", "-f"}, clients...)...)
		return nil
	} else {
		err = fmt.Errorf("a MON command of this call may still apply, so the next call settles the MONs first: %w", err)
	}
	fence := call.c.authRecords.fenceFor(call.entity)
	fence.clients = append(fence.clients, clients...)
	return err
}

// rotationRun is a command of a rotation that may still run: a supervised
// invocation in the control container or a Docker exec in the daemon
// container.
type rotationRun struct {
	ctr testcontainers.Container
	// marker names a supervised invocation, and execID a Docker exec.
	marker, execID string
}

// rotateDaemonKey implements RotateKey, which documents the behavior. The caller
// checks the handle under the topology lock.
func (c *Container) rotateDaemonKey(ctx context.Context, handle daemonHandle) (err error) {
	kind, id := handle.daemonRef()
	daemon := kind + "." + id
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	if !handle.ownedBy(c) {
		return invalid(fmt.Errorf("daemon %s is not owned by this cluster, or this handle is stale", daemon))
	}
	ctr, keyrings := c.daemonKeyrings(kind, id)
	if err := c.stopEarlierAuthCommands(ctx, daemon); err != nil {
		return err
	}
	state, err := ctr.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", daemon, err)
	}
	if !state.Running || state.Paused {
		return fmt.Errorf("daemon %s must be running to receive a new key", daemon)
	}
	if err := c.settleEarlierRotation(ctx, daemon, daemonIdentity(ctr, state)); err != nil {
		return err
	}

	call := &authCall{c: c, entity: daemon}
	defer func() {
		// Whatever the outcome, no MON mutation of this call may apply later
		// unless the daemon's fence records it.
		settleCtx, settleCancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
		defer settleCancel()
		if settleErr := call.settle(settleCtx); settleErr != nil && err != nil {
			err = errors.Join(err, settleErr)
		}
	}()
	r := daemonRotation{c: c, call: call, daemon: daemon, ctr: ctr, keyrings: keyrings}

	before, err := c.authKeys(ctx, "get", daemon)
	if err != nil {
		return err
	}
	if done, err := r.finishEarlierHandoff(ctx, before); done || err != nil {
		return err
	}
	current, next, err := r.pendingKey(ctx, before)
	if err != nil {
		return err
	}
	if err := c.writeDaemonKeyrings(ctx, ctr, daemon, next, keyrings); err != nil {
		return r.abandon(ctx, next, err)
	}
	control, keyPath, err := r.stageKeyFile(ctx, next)
	if err != nil {
		return r.abandon(ctx, next, err)
	}
	removeErr, err := r.handOff(ctx, control, keyPath, next)
	if err != nil {
		return err
	}
	return r.commit(ctx, current, next, removeErr)
}

// daemonRotation is one RotateKey call on an owned daemon after its admission:
// the daemon, its container and keyring files, and the authCall that collects
// the call's MON mutations. Its methods are the phases of rotateDaemonKey and
// run inside its critical section. The caller holds c.mu.
type daemonRotation struct {
	c        *Container
	call     *authCall
	daemon   string
	ctr      testcontainers.Container
	keyrings []string
}

// finishEarlierHandoff settles a key that an earlier call handed to the
// daemon, before reads the auth entry now. It reports done when the MONs
// committed the handed key, which then only needs to reach the keyring files.
// It refuses a rotation while an earlier handoff may still reach the running
// daemon with another key. A pending key from an earlier call may already be
// in the daemon or its keyring files, so a resumed rotation keeps it.
func (r daemonRotation) finishEarlierHandoff(ctx context.Context, before struct{ Key, PendingKey string }) (done bool, err error) {
	fence := r.c.authRecords.fence(r.daemon)
	if fence == nil || fence.handedKey == "" || before.PendingKey == fence.handedKey {
		return false, nil
	}
	if before.PendingKey == "" && before.Key == fence.handedKey && !fence.handedDone {
		// The daemon authenticated with the handed key, and the MONs
		// committed it: the rotation has finished with it.
		if err := r.c.writeDaemonKeyrings(ctx, r.ctr, r.daemon, before.Key, r.keyrings); err != nil {
			return true, err
		}
		fence.handedDone = true
		return true, nil
	}
	return true, fmt.Errorf("an earlier handoff of a key to %s may still reach the running daemon, so RotateKey gives it no other key until its container restarts", r.daemon)
}

// pendingKey makes the MONs hold a pending key next beside the current key
// and returns both. A rotation that resumes an earlier pending key keeps it.
func (r daemonRotation) pendingKey(ctx context.Context, before struct{ Key, PendingKey string }) (current, next string, err error) {
	resumed := before.PendingKey != ""
	keys, err := r.call.keys(ctx)
	if err != nil {
		return "", "", err
	}
	if keys.PendingKey == "" || keys.Key != before.Key || (resumed && keys.PendingKey != before.PendingKey) {
		return "", "", fmt.Errorf("the keys of %s changed during the rotation; inspect ceph auth get %s", r.daemon, r.daemon)
	}
	return keys.Key, keys.PendingKey, nil
}

// abandon reports a failure before the handoff of next. It keeps the pending
// key and whatever the keyring files hold. Undoing it would race with a
// restart: a daemon that restarts from a file with the new key authenticates
// with it, and the MONs then commit the pending key on their own, which
// finishes the rotation.
func (r daemonRotation) abandon(ctx context.Context, next string, cause error) error {
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer readCancel()
	if keys, err := r.c.authKeys(readCtx, "get", r.daemon); err == nil && keys.Key == next && keys.PendingKey == "" {
		if err := r.c.writeDaemonKeyrings(readCtx, r.ctr, r.daemon, next, r.keyrings); err != nil {
			return errors.Join(cause, fmt.Errorf("%s restarted with the new key, which the MONs committed, but not all of its keyring files could get it: %w", r.daemon, err))
		}
		return nil
	}
	return fmt.Errorf("%w; the MONs keep accepting both keys of %s, and its keyring files may hold the new key, which the MONs commit on their own once the daemon authenticates with it. Call RotateKey again to finish the rotation", cause, r.daemon)
}

// stageKeyFile copies next into a new file in the control container, from
// which handOff gives it to the daemon. A copy that Docker completes after an
// error only creates this file, which later calls remove.
func (r daemonRotation) stageKeyFile(ctx context.Context, next string) (control testcontainers.Container, keyPath string, err error) {
	control = r.c.cliContainer()
	keyPath = "/tmp/tc-rotate-" + uuid.NewString() + ".key"
	if err := control.CopyToContainer(ctx, []byte(next), keyPath, 0o600); err != nil {
		r.c.authRecords.addStray(keyPath, true)
		return nil, "", fmt.Errorf("copy the new key of %s: %w", r.daemon, err)
	}
	return control, keyPath, nil
}

// handOff gives next to the running daemon through ceph tell rotate-key from
// keyPath in control, and removes that file. removeErr is the failure to
// remove it, which later calls remove. A failed handoff records in the
// daemon's fence that next may still reach the daemon.
func (r daemonRotation) handOff(ctx context.Context, control testcontainers.Container, keyPath, next string) (removeErr, err error) {
	c, daemon := r.c, r.daemon
	_, tellErr := c.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "tell", daemon, "rotate-key", "-i", keyPath)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cleanupCancel()
	_, removeErr = c.rotationCommand(cleanupCtx, "rm", "-f", keyPath)
	if removeErr != nil {
		c.authRecords.addStray(keyPath, false)
	}
	if tellErr == nil {
		return removeErr, nil
	}
	fence := c.authRecords.fenceFor(daemon)
	// While the CLI may run, it may deliver the key to any later process
	// too, so handedTo stays unknown until a later call stops it.
	fence.handedKey, fence.handedTo, fence.handedDone = next, "", false
	if !c.fenceUnstopped(fence, control, tellErr) {
		// The handoff has ended, so it can reach only the process that
		// runs now, or none.
		if after, err := r.ctr.State(cleanupCtx); err == nil && after != nil {
			fence.handedTo = daemonIdentity(r.ctr, after)
		}
	}
	return removeErr, errors.Join(fmt.Errorf("hand the new key to %s: %w; it is unknown whether the daemon holds it, so the MONs keep accepting both keys and its keyring files hold the new key. Call RotateKey again to finish with the same key; a different key needs a restart of %s first", daemon, tellErr, daemon), removeErr)
}

// commit commits next, which the daemon and its files hold, and reads the
// outcome back. The MONs accept next whether or not the commit applies. They
// also commit a pending key on their own once the daemon authenticates with
// it, and answer a commit that meets such a change without applying it, so
// the outcome is read back in any case, after settling when the commit
// failed. removeErr, the handoff's key-file removal failure, is part of every
// result.
func (r daemonRotation) commit(ctx context.Context, current, next string, removeErr error) error {
	daemon := r.daemon
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cleanupCancel()
	_, commitErr := r.call.mutate(ctx, "commit-pending")
	if commitErr != nil {
		if settleErr := r.call.settle(cleanupCtx); settleErr != nil {
			return errors.Join(fmt.Errorf("commit the new key of %s, which the daemon holds: %w; the commit may still apply, and the MONs accept the new key either way", daemon, commitErr), settleErr, removeErr)
		}
	}
	after, readErr := r.c.authKeys(cleanupCtx, "get", daemon)
	if readErr == nil && after.Key == next && after.PendingKey == "" {
		if fence := r.c.authRecords.fence(daemon); fence != nil && fence.handedKey == next {
			fence.handedDone = true
		}
		return removeErr
	}
	if commitErr == nil {
		commitErr = errors.New("the MONs answered without applying it")
	}
	if readErr == nil && after.Key == current && after.PendingKey == next {
		return errors.Join(fmt.Errorf("commit the new key of %s, which the daemon holds: %w; the commit did not apply and no longer can, and the MONs accept both keys. Call RotateKey again or run ceph auth commit-pending %s", daemon, commitErr, daemon), removeErr)
	}
	return errors.Join(fmt.Errorf("commit the new key of %s: %w; the keys changed in an unexpected way, inspect ceph auth get %s", daemon, commitErr, daemon), readErr, removeErr)
}

// fenceUnstopped records the supervised invocation of err in fence when its
// end is unconfirmed: its cancellation failed, or a signal killed its
// supervisor before its cleanup. It reports whether it did.
func (c *Container) fenceUnstopped(fence *authFence, ctr testcontainers.Container, err error) bool {
	marker, unconfirmed := supervised.Unconfirmed(err)
	if unconfirmed {
		fence.running = append(fence.running, rotationRun{ctr: ctr, marker: marker})
	}
	return unconfirmed
}

// daemonIdentity names the daemon process by its container's ID and start
// time; the daemon is the container's main process. It is "" when the start
// time is unknown.
func daemonIdentity(ctr testcontainers.Container, state *container.State) string {
	if state == nil || state.StartedAt == "" {
		return ""
	}
	return ctr.GetContainerID() + "@" + state.StartedAt
}

// stopEarlierAuthCommands makes sure that no command of an earlier call on
// the auth entity still runs.
func (c *Container) stopEarlierAuthCommands(ctx context.Context, daemon string) error {
	fence := c.authRecords.fence(daemon)
	for fence != nil && len(fence.running) > 0 {
		run := fence.running[0]
		if err := c.stopRotationRun(ctx, run); err != nil {
			return fmt.Errorf("a command of an earlier call on %s may still run and cannot be stopped yet: %w", daemon, err)
		}
		fence.running = fence.running[1:]
	}
	return nil
}

// settleEarlierRotation updates the handoff fence of daemon, whose process is
// now identity, and settles the MONs when an earlier rotation left them
// unsettled.
func (c *Container) settleEarlierRotation(ctx context.Context, daemon, identity string) error {
	fence := c.authRecords.fence(daemon)
	if fence == nil {
		return nil
	}
	switch {
	case fence.handedKey == "":
	case fence.handedTo == "":
		// The process that may hold the handoff is unknown; the one that runs
		// now is the only candidate left, so wait for it to restart.
		fence.handedTo = identity
	case identity != "" && identity != fence.handedTo:
		fence.handedKey, fence.handedTo, fence.handedDone = "", "", false
	}
	if len(fence.clients) > 0 {
		if err := c.monitorSettling().settle(ctx, fence.clients); err != nil {
			return fmt.Errorf("an earlier rotation of %s left a MON command that may still apply, and the MONs cannot be settled yet: %w", daemon, err)
		}
		_, _ = c.rotationCommand(ctx, append([]string{"rm", "-f"}, fence.clients...)...)
		fence.clients = nil
	}
	if fence.handedKey == "" && len(fence.clients) == 0 && len(fence.running) == 0 {
		c.authRecords.dropFence(daemon)
	}
	return nil
}

// stopRotationRun returns once run can no longer run.
func (c *Container) stopRotationRun(ctx context.Context, run rotationRun) error {
	if run.execID != "" {
		return c.daemonExecs.finish(ctx, run.execID)
	}
	// The canceler also confirms a command that ended with an earlier
	// instance of a restarted container. A stopped container confirms
	// nothing, because Docker may still start an exec that it accepted.
	err := supervised.Cancel(ctx, run.ctr, "key rotation", run.marker)
	if err == nil {
		return nil
	}
	if _, stateErr := run.ctr.State(ctx); errdefs.IsNotFound(stateErr) {
		return nil
	}
	return err
}

// rotationCommand runs one native command of a rotation in the control
// container under the supervisor, which stops it inside the container before
// returning when ctx ends first, or reports a supervised.NotStoppedError. A
// nonzero exit is a *NativeFailure, which reads only standard error: the
// standard output of a rotation can hold keys. A ceph CLI that no MON
// answered connects again, as in command.
func (c *Container) rotationCommand(ctx context.Context, argv ...string) ([]byte, error) {
	return reconnectCeph(ctx, argv, func() ([]byte, error) {
		result, err := c.rotationCommandResult(ctx, 1<<20, 64<<10, argv...)
		if err != nil {
			return nil, err
		}
		if result.ExitCode != 0 {
			return result.Stdout, nativeFailure(argv[0], result.ExitCode, nil, result.Stderr)
		}
		return result.Stdout, nil
	})
}

func (c *Container) rotationCommandResult(ctx context.Context, stdoutLimit, stderrLimit int, argv ...string) (supervised.Result, error) {
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return supervised.Result{}, fmt.Errorf("select Ceph CLI container: %w", err)
	}
	return supervised.Command(ctx, control, "key rotation", keyRotationCommandTimeout, stdoutLimit, stderrLimit, argv...)
}

// authKeys runs ceph auth <verb> daemon, which must not change anything, and
// returns its key and pending key.
func (c *Container) authKeys(ctx context.Context, verb, daemon string) (struct{ Key, PendingKey string }, error) {
	out, err := c.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "auth", verb, daemon, "--format", "json")
	if err != nil {
		return struct{ Key, PendingKey string }{}, fmt.Errorf("auth %s %s: %w", verb, daemon, err)
	}
	return parseAuthKeys(out, verb, daemon)
}

func parseAuthKeys(out []byte, verb, daemon string) (struct{ Key, PendingKey string }, error) {
	var result struct{ Key, PendingKey string }
	var entries []struct {
		Key        string `json:"key"`
		PendingKey string `json:"pending_key"`
	}
	if err := json.Unmarshal(out, &entries); err != nil || len(entries) != 1 || entries[0].Key == "" {
		return result, fmt.Errorf("auth %s %s returned an unexpected form", verb, daemon)
	}
	result.Key, result.PendingKey = entries[0].Key, entries[0].PendingKey
	return result, nil
}

func daemonKeyring(daemon, key string) []byte {
	return []byte("[" + daemon + "]\n\tkey = " + strings.TrimSpace(key) + "\n")
}

func (c *Container) writeDaemonKeyrings(ctx context.Context, ctr testcontainers.Container, daemon, key string, paths []string) error {
	for _, path := range paths {
		if err := c.placeDaemonKeyring(ctx, ctr, daemon, key, path); err != nil {
			return err
		}
	}
	return nil
}

// placeDaemonKeyring replaces the keyring file path in the daemon container.
// Docker may complete an archive copy after its request failed, so the
// content goes to a new file first, which only a Docker exec of mv, whose end
// RotateKey can confirm, renames into place. An exec that may still run
// is recorded in the daemon's fence.
func (c *Container) placeDaemonKeyring(ctx context.Context, ctr testcontainers.Container, daemon, key, path string) error {
	staged := path + ".tc-" + uuid.NewString()
	if err := ctr.CopyToContainer(ctx, daemonKeyring(daemon, key), staged, 0o600); err != nil {
		return fmt.Errorf("write %s keyring %s: %w", daemon, path, err)
	}
	code, err := c.daemonExec(ctx, ctr, daemon, "mv", "-f", staged, path)
	if err != nil {
		return fmt.Errorf("write %s keyring %s: %w", daemon, path, err)
	}
	if code != 0 {
		return fmt.Errorf("write %s keyring %s: mv exited %d", daemon, path, code)
	}
	return nil
}

// daemonExec runs argv in ctr through Docker and returns its exit code once it
// ended. When the end cannot be confirmed, the exec goes into the fence of
// daemon and the error says so.
func (c *Container) daemonExec(ctx context.Context, ctr testcontainers.Container, daemon string, argv ...string) (int, error) {
	code, unended, err := c.daemonExecs.run(ctx, ctr, argv...)
	if unended != "" {
		fence := c.authRecords.fenceFor(daemon)
		fence.running = append(fence.running, rotationRun{ctr: ctr, execID: unended})
	}
	return code, err
}

// daemonKeyrings returns the container of an owned daemon and the keyring
// files from which it authenticates when it starts. The caller holds c.mu.
func (c *Container) daemonKeyrings(kind, id string) (testcontainers.Container, []string) {
	if kind == "mgr" {
		return c.roster.manager(id).Container, []string{"/etc/ceph/mgr.keyring", "/var/lib/ceph/mgr/ceph-" + id + "/keyring"}
	}
	n, _ := strconv.Atoi(id)
	return c.osdHandles.get(n).Container, []string{"/etc/ceph/osd.keyring", "/var/lib/ceph/osd/ceph-" + id + "/keyring"}
}
