package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
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
// entry of entity, such as one RotateDaemonKey or ReplaceClientKey call, until
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
			call.c.strayAuthInput(path+".in", true)
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
		var notStopped *supervised.NotStoppedError
		if errors.As(err, &notStopped) {
			call.c.fenceUnstopped(ctx, call.c.fenceAuth(call.entity), call.c.cliContainer(), err)
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
		call.c.strayAuthInput(path, false)
	}
}

// strayAuthInput records a file with a key for later removal; copying means
// that Docker may still write it. The caller holds c.mu.
func (c *Container) strayAuthInput(path string, copying bool) {
	if c.strayAuthInputs == nil {
		c.strayAuthInputs = make(map[string]bool)
	}
	c.strayAuthInputs[path] = c.strayAuthInputs[path] || copying
}

// removeStrayAuthInputs removes the files with keys that earlier calls could
// not remove for sure. A file whose copy failed is removed again by every
// later call, since nothing tells when Docker gave up on it. The caller holds
// c.mu.
func (c *Container) removeStrayAuthInputs(ctx context.Context) {
	if len(c.strayAuthInputs) == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cancel()
	paths := slices.Sorted(maps.Keys(c.strayAuthInputs))
	if _, err := c.rotationCommand(cleanupCtx, append([]string{"rm", "-f"}, paths...)...); err == nil {
		maps.DeleteFunc(c.strayAuthInputs, func(_ string, copying bool) bool { return !copying })
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
	} else if err = call.c.settleMonitors(ctx, clients); err == nil {
		_, _ = call.c.rotationCommand(ctx, append([]string{"rm", "-f"}, clients...)...)
		return nil
	} else {
		err = fmt.Errorf("a MON command of this call may still apply, so the next call settles the MONs first: %w", err)
	}
	fence := call.c.fenceAuth(call.entity)
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
	// started is the container's start time when the command ran; a later
	// start time means that the command ended with the container.
	started string
}

// keyRotationExecClient is the part of the Docker client that writes keyring
// files in a daemon container.
type keyRotationExecClient interface {
	ExecCreate(context.Context, string, mobycl.ExecCreateOptions) (mobycl.ExecCreateResult, error)
	ExecStart(context.Context, string, mobycl.ExecStartOptions) (mobycl.ExecStartResult, error)
	ExecInspect(context.Context, string, mobycl.ExecInspectOptions) (mobycl.ExecInspectResult, error)
	Close() error
}

// RotateDaemonKey replaces the Cephx key of a running MGR or OSD that the
// cluster owns, as cephadm's daemon key rotation does, without a restart:
//
//  1. auth get-or-create-pending creates the new key on the MONs, which then
//     accept both keys.
//  2. The daemon's keyring files in its container get the new key, so that a
//     later restart authenticates with it.
//  3. ceph tell <daemon> rotate-key hands the key to the running daemon.
//  4. auth commit-pending makes it the entity's only key.
//
// daemon is "mgr.<name>" or "osd.<id>". MONs share the mon. key and are
// refused.
//
// The MONs accept both keys until step 4, and they also commit the pending key
// on their own when the daemon authenticates with it, after a restart or a
// handoff. A failure before step 4 therefore keeps both keys valid, and the
// next call finishes the rotation with the same key; a failed step 4 is
// reported with the outcome that the MONs show once settled. A call returns only once none of its
// commands can still change the keys or the keyring files, or after recording
// such commands for the daemon: the next call stops them, waits for them and
// settles the MONs for them before it changes anything, and fails while it
// cannot. After a handoff that may still reach the daemon, a later call
// finishes the rotation with the same key and generates another key only once
// the daemon's container has restarted.
func (c *Container) RotateDaemonKey(ctx context.Context, daemon string) (err error) {
	kind, id, err := parseAdminSocketDaemon(daemon)
	if err != nil {
		return err
	}
	if kind == "mon" {
		return errors.New("MONs share the mon. key; RotateDaemonKey rotates MGR and OSD keys")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	if !c.ownsDaemon(kind, id) {
		return fmt.Errorf("daemon %s is not owned by this cluster", daemon)
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

	before, err := c.authKeys(ctx, "get", daemon)
	if err != nil {
		return err
	}
	// A pending key from an earlier call may already be in the daemon or its
	// keyring files, so a resumed rotation must not take it away.
	resumed := before.PendingKey != ""
	if fence := c.authFences[daemon]; fence != nil && fence.handedKey != "" && before.PendingKey != fence.handedKey {
		if before.PendingKey == "" && before.Key == fence.handedKey && !fence.handedDone {
			// The daemon authenticated with the handed key, and the MONs
			// committed it: the rotation has finished with it.
			if err := c.writeDaemonKeyrings(ctx, ctr, daemon, before.Key, keyrings); err != nil {
				return err
			}
			fence.handedDone = true
			return nil
		}
		return fmt.Errorf("an earlier handoff of a key to %s may still reach the running daemon, so RotateDaemonKey gives it no other key until its container restarts", daemon)
	}
	keys, err := call.keys(ctx)
	if err != nil {
		return err
	}
	if keys.PendingKey == "" || keys.Key != before.Key || (resumed && keys.PendingKey != before.PendingKey) {
		return fmt.Errorf("the keys of %s changed during the rotation; inspect ceph auth get %s", daemon, daemon)
	}
	current, next := keys.Key, keys.PendingKey
	// A failure before the handoff keeps the pending key and whatever the
	// keyring files hold. Undoing it would race with a restart: a daemon that
	// restarts from a file with the new key authenticates with it, and the
	// MONs then commit the pending key on their own, which finishes the
	// rotation.
	abandon := func(cause error) error {
		readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
		defer readCancel()
		if keys, err := c.authKeys(readCtx, "get", daemon); err == nil && keys.Key == next && keys.PendingKey == "" {
			if err := c.writeDaemonKeyrings(readCtx, ctr, daemon, next, keyrings); err != nil {
				return errors.Join(cause, fmt.Errorf("%s restarted with the new key, which the MONs committed, but not all of its keyring files could get it: %w", daemon, err))
			}
			return nil
		}
		return fmt.Errorf("%w; the MONs keep accepting both keys of %s, and its keyring files may hold the new key, which the MONs commit on their own once the daemon authenticates with it. Call RotateDaemonKey again to finish the rotation", cause, daemon)
	}
	if err := c.writeDaemonKeyrings(ctx, ctr, daemon, next, keyrings); err != nil {
		return abandon(err)
	}
	control := c.cliContainer()
	keyPath := "/tmp/tc-rotate-" + uuid.NewString() + ".key"
	// A copy that Docker completes after an error only creates this file,
	// which later calls remove.
	if err := control.CopyToContainer(ctx, []byte(next), keyPath, 0o600); err != nil {
		c.strayAuthInput(keyPath, true)
		return abandon(fmt.Errorf("copy the new key of %s: %w", daemon, err))
	}
	_, tellErr := c.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "tell", daemon, "rotate-key", "-i", keyPath)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cleanupCancel()
	_, removeErr := c.rotationCommand(cleanupCtx, "rm", "-f", keyPath)
	if removeErr != nil {
		c.strayAuthInput(keyPath, false)
	}
	if tellErr != nil {
		fence := c.fenceAuth(daemon)
		// While the CLI may run, it may deliver the key to any later process
		// too, so handedTo stays unknown until a later call stops it.
		fence.handedKey, fence.handedTo, fence.handedDone = next, "", false
		if !c.fenceUnstopped(cleanupCtx, fence, control, tellErr) {
			// The handoff has ended, so it can reach only the process that
			// runs now, or none.
			if after, err := ctr.State(cleanupCtx); err == nil && after != nil {
				fence.handedTo = daemonIdentity(ctr, after)
			}
		}
		return errors.Join(fmt.Errorf("hand the new key to %s: %w; it is unknown whether the daemon holds it, so the MONs keep accepting both keys and its keyring files hold the new key. Call RotateDaemonKey again to finish with the same key; a different key needs a restart of %s first", daemon, tellErr, daemon), removeErr)
	}
	// The daemon and its files hold the new key, which the MONs accept
	// whether or not the commit applies. The MONs also commit a pending key
	// on their own once the daemon authenticates with it, and answer a commit
	// that meets such a change without applying it, so the outcome is read
	// back in any case, after settling when the commit failed.
	_, commitErr := call.mutate(ctx, "commit-pending")
	if commitErr != nil {
		if settleErr := call.settle(cleanupCtx); settleErr != nil {
			return errors.Join(fmt.Errorf("commit the new key of %s, which the daemon holds: %w; the commit may still apply, and the MONs accept the new key either way", daemon, commitErr), settleErr, removeErr)
		}
	}
	after, readErr := c.authKeys(cleanupCtx, "get", daemon)
	if readErr == nil && after.Key == next && after.PendingKey == "" {
		if fence := c.authFences[daemon]; fence != nil && fence.handedKey == next {
			fence.handedDone = true
		}
		return removeErr
	}
	if commitErr == nil {
		commitErr = errors.New("the MONs answered without applying it")
	}
	if readErr == nil && after.Key == current && after.PendingKey == next {
		return errors.Join(fmt.Errorf("commit the new key of %s, which the daemon holds: %w; the commit did not apply and no longer can, and the MONs accept both keys. Call RotateDaemonKey again or run ceph auth commit-pending %s", daemon, commitErr, daemon), removeErr)
	}
	return errors.Join(fmt.Errorf("commit the new key of %s: %w; the keys changed in an unexpected way, inspect ceph auth get %s", daemon, commitErr, daemon), readErr, removeErr)
}

func (c *Container) fenceAuth(daemon string) *authFence {
	if c.authFences == nil {
		c.authFences = make(map[string]*authFence)
	}
	fence := c.authFences[daemon]
	if fence == nil {
		fence = &authFence{}
		c.authFences[daemon] = fence
	}
	return fence
}

// fenceUnstopped records the supervised invocation of err in fence when its
// supervisor could not confirm that it stopped, and reports whether it did.
func (c *Container) fenceUnstopped(ctx context.Context, fence *authFence, ctr testcontainers.Container, err error) bool {
	var notStopped *supervised.NotStoppedError
	if !errors.As(err, &notStopped) {
		return false
	}
	fence.running = append(fence.running, rotationRun{ctr: ctr, marker: notStopped.Marker, started: containerStarted(ctx, ctr)})
	return true
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

func containerStarted(ctx context.Context, ctr testcontainers.Container) string {
	if state, err := ctr.State(ctx); err == nil && state != nil {
		return state.StartedAt
	}
	return ""
}

// stopEarlierAuthCommands makes sure that no command of an earlier call on
// the auth entity still runs.
func (c *Container) stopEarlierAuthCommands(ctx context.Context, daemon string) error {
	fence := c.authFences[daemon]
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
	fence := c.authFences[daemon]
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
		if err := c.settleMonitors(ctx, fence.clients); err != nil {
			return fmt.Errorf("an earlier rotation of %s left a MON command that may still apply, and the MONs cannot be settled yet: %w", daemon, err)
		}
		_, _ = c.rotationCommand(ctx, append([]string{"rm", "-f"}, fence.clients...)...)
		fence.clients = nil
	}
	if fence.handedKey == "" && len(fence.clients) == 0 && len(fence.running) == 0 {
		delete(c.authFences, daemon)
	}
	return nil
}

// stopRotationRun returns once run can no longer run.
func (c *Container) stopRotationRun(ctx context.Context, run rotationRun) error {
	if run.execID != "" {
		return c.finishDaemonExec(ctx, run.execID)
	}
	err := supervised.Cancel(ctx, run.ctr, "key rotation", run.marker)
	if err == nil {
		return nil
	}
	// The command also ended when its container stopped or was removed.
	state, stateErr := run.ctr.State(ctx)
	switch {
	case errdefs.IsNotFound(stateErr):
		return nil
	case stateErr == nil && state != nil && (!state.Running || (run.started != "" && state.StartedAt != run.started)):
		return nil
	}
	return err
}

// rotationCommand runs one native command of a rotation in the control
// container under the supervisor, which stops it inside the container before
// returning when ctx ends first, or reports a supervised.NotStoppedError. A
// nonzero exit is an error.
func (c *Container) rotationCommand(ctx context.Context, argv ...string) ([]byte, error) {
	result, err := c.rotationCommandResult(ctx, 1<<20, 64<<10, argv...)
	if err != nil {
		return nil, err
	}
	if result.ExitCode != 0 {
		message := strings.TrimSpace(string(result.Stderr))
		if len(message) > 512 {
			message = message[:512]
		}
		return result.Stdout, fmt.Errorf("%s exited %d: %s", strings.Join(argv[:min(len(argv), 4)], " "), result.ExitCode, message)
	}
	return result.Stdout, nil
}

func (c *Container) rotationCommandResult(ctx context.Context, stdoutLimit, stderrLimit int, argv ...string) (supervised.Result, error) {
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return supervised.Result{}, fmt.Errorf("select Ceph CLI container: %w", err)
	}
	return supervised.Command(ctx, control, "key rotation", keyRotationCommandTimeout, stdoutLimit, stderrLimit, argv...)
}

// monitorView is the quorum and monmap that settling relies on.
type monitorView struct {
	election, epoch int
	mons            []monitorRoute
}

// monitorRoute is one MON: the address vector that the mon_host_override
// option takes, and the forms in which a MON client logs its addresses.
type monitorRoute struct {
	name, override string
	logged         []string
}

func (c *Container) monitorView(ctx context.Context) (monitorView, error) {
	out, err := c.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "quorum_status", "--format", "json")
	if err != nil {
		return monitorView{}, fmt.Errorf("read the MON quorum: %w", err)
	}
	var status QuorumStatus
	var view struct {
		Election *int `json:"election_epoch"`
		MonMap   struct {
			Epoch *int `json:"epoch"`
			Mons  []struct {
				PublicAddrs struct {
					Addrvec []struct {
						Type  string `json:"type"`
						Addr  string `json:"addr"`
						Nonce uint64 `json:"nonce"`
					} `json:"addrvec"`
				} `json:"public_addrs"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if json.Unmarshal(out, &status) != nil || json.Unmarshal(out, &view) != nil || view.Election == nil || view.MonMap.Epoch == nil || len(view.MonMap.Mons) != len(status.MonMap.Mons) || len(status.MonMap.Mons) == 0 {
		return monitorView{}, errors.New("read the MON quorum: unexpected quorum_status")
	}
	result := monitorView{election: *view.Election, epoch: *view.MonMap.Epoch}
	for i, member := range status.MonMap.Mons {
		single := status
		single.QuorumNames = []string{member.Name}
		single.MonMap.Mons = append(single.MonMap.Mons[:0:0], member)
		override, err := monitorBootstrapAddresses(single)
		if err != nil {
			return monitorView{}, fmt.Errorf("read the MON quorum: %w", err)
		}
		route := monitorRoute{name: member.Name, override: override}
		for _, addr := range view.MonMap.Mons[i].PublicAddrs.Addrvec {
			route.logged = append(route.logged, addr.Type+":"+addr.Addr+"/"+strconv.FormatUint(addr.Nonce, 10))
		}
		result.mons = append(result.mons, route)
	}
	return result, nil
}

// settleMonitors returns once no command that the clients whose nonce files
// are listed sent to a MON can apply any more. The caller has made sure that
// those clients stopped. First every MON must have closed their sessions: TCP
// delivers a connection's data before its end, and a MON drops a command whose
// connection has closed before it handles it (PaxosService::dispatch), so
// each command has then been handled or dropped. A MON forwards the commands
// it handles to the leader in order, and the leader commits auth changes in
// order, so a harmless auth change committed through every MON, each reached
// alone, comes after every handled command. The quorum must stay the same
// meanwhile, because an election can send a forwarded command on through
// another MON.
func (c *Container) settleMonitors(ctx context.Context, clients []string) error {
	nonces, err := c.clientNonces(ctx, clients)
	if err != nil || len(nonces) == 0 {
		// A client that stored no nonce has not sent its command.
		return err
	}
	before, err := c.monitorView(ctx)
	if err != nil {
		return fmt.Errorf("settle the MONs: %w", err)
	}
	for _, mon := range before.mons {
		if err := c.waitMonitorSessionsClosed(ctx, mon, nonces); err != nil {
			return fmt.Errorf("settle the MONs: %w", err)
		}
	}
	for _, mon := range before.mons {
		if err := c.monitorBarrier(ctx, mon, before.mons); err != nil {
			return fmt.Errorf("settle the MONs through mon.%s: %w", mon.name, err)
		}
	}
	after, err := c.monitorView(ctx)
	if err != nil {
		return fmt.Errorf("settle the MONs: %w", err)
	}
	if after.election != before.election || after.epoch != before.epoch {
		return errors.New("settle the MONs: the quorum changed while settling")
	}
	return nil
}

// clientNonces reads the nonce files that monitorMutationScript writes; a
// missing file means that its client never connected.
func (c *Container) clientNonces(ctx context.Context, clients []string) (map[uint64]bool, error) {
	nonces := map[uint64]bool{}
	for _, path := range clients {
		out, err := c.rotationCommand(ctx, "sh", "-c", `test ! -e "$1" || cat "$1"`, "sh", path)
		if err != nil {
			return nil, fmt.Errorf("read the nonce of a MON client: %w", err)
		}
		if text := strings.TrimSpace(string(out)); text != "" {
			nonce, err := strconv.ParseUint(text, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("read the nonce of a MON client: %q", text)
			}
			nonces[nonce] = true
		}
	}
	return nonces, nil
}

var sessionNonce = regexp.MustCompile(`"nonce":\s*(\d+)`)

// waitMonitorSessionsClosed returns once mon lists no session of a client
// with one of nonces.
func (c *Container) waitMonitorSessionsClosed(ctx context.Context, mon monitorRoute, nonces map[uint64]bool) error {
	for {
		out, err := c.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "tell", "mon."+mon.name, "sessions", "--format", "json")
		var sessions []json.RawMessage
		if err == nil {
			if err = json.Unmarshal(out, &sessions); err == nil {
				open := false
				for _, session := range sessions {
					for _, match := range sessionNonce.FindAllSubmatch(session, -1) {
						nonce, _ := strconv.ParseUint(string(match[1]), 10, 64)
						open = open || nonces[nonce]
					}
				}
				if !open {
					return nil
				}
				err = errors.New("a stopped client still has a session")
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for mon.%s to close the sessions of stopped clients: %w", mon.name, errors.Join(ctx.Err(), err))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// monitorBarrier adds and removes an identity without capabilities through
// mon. mon_host_override makes the CLI contact only mon, and its MON client
// log shows that it never reached another MON.
func (c *Container) monitorBarrier(ctx context.Context, mon monitorRoute, mons []monitorRoute) error {
	entity := "client.tc-key-rotation-" + uuid.NewString()
	result, err := c.rotationCommandResult(ctx, 64<<10, 1<<20, "ceph", "--mon_host_override", mon.override, "--connect-timeout", "5",
		"--debug_monc", "10/10", "--log_to_stderr", "true", "--err_to_stderr", "true", "auth", "add", entity)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("auth add exited %d", result.ExitCode)
	}
	// The identity has no capabilities; remove it best effort.
	_, _ = c.rotationCommand(ctx, "ceph", "--connect-timeout", "5", "auth", "rm", entity)
	picked := 0
	for _, line := range strings.Split(string(result.Stderr), "\n") {
		if !strings.Contains(line, "picked mon.") {
			continue
		}
		picked++
		for _, other := range mons {
			reached := false
			for _, addr := range other.logged {
				reached = reached || strings.Contains(line, addr)
			}
			if reached != (other.name == mon.name) {
				return fmt.Errorf("the CLI did not reach mon.%s alone: %s", mon.name, strings.TrimSpace(line))
			}
		}
	}
	if picked == 0 {
		return errors.New("the CLI logged no MON it contacted")
	}
	return nil
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
// RotateDaemonKey can confirm, renames into place. An exec that may still run
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

func (c *Container) keyRotationExec(ctx context.Context) (keyRotationExecClient, error) {
	if c.keyRotationDocker != nil {
		return c.keyRotationDocker(ctx)
	}
	return testcontainers.NewDockerClientWithOpts(ctx)
}

// daemonExec runs argv in ctr through Docker and returns its exit code once it
// ended. When the end cannot be confirmed, the exec goes into the fence of
// daemon and the error says so.
func (c *Container) daemonExec(ctx context.Context, ctr testcontainers.Container, daemon string, argv ...string) (int, error) {
	docker, err := c.keyRotationExec(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = docker.Close() }()
	// An exec that Docker creates after this request failed never starts:
	// nothing else knows its ID.
	created, err := docker.ExecCreate(ctx, ctr.GetContainerID(), mobycl.ExecCreateOptions{Cmd: argv})
	if err != nil {
		return 0, err
	}
	_, err = docker.ExecStart(ctx, created.ID, mobycl.ExecStartOptions{Detach: true})
	if err == nil {
		var inspect mobycl.ExecInspectResult
		if inspect, err = waitDaemonExec(ctx, docker, created.ID); err == nil {
			return inspect.ExitCode, nil
		}
	}
	// The start request may still reach Docker, or the exec may still run.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keyRotationCleanupTimeout)
	defer cancel()
	if finishErr := c.finishDaemonExec(cleanupCtx, created.ID); finishErr != nil {
		fence := c.fenceAuth(daemon)
		fence.running = append(fence.running, rotationRun{ctr: ctr, execID: created.ID})
		return 0, errors.Join(err, fmt.Errorf("the exec may still run: %w", finishErr))
	}
	return 0, fmt.Errorf("%w; the exec has ended, so it may or may not have applied", err)
}

// finishDaemonExec returns once the exec has ended and can no longer start. It
// starts an exec that Docker has not started, so that a start request still on
// its way finds it done. Docker also refuses to start an exec in a paused
// container, so a refusal proves nothing until the exec shows a process.
func (c *Container) finishDaemonExec(ctx context.Context, execID string) error {
	docker, err := c.keyRotationExec(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	if _, err := docker.ExecStart(ctx, execID, mobycl.ExecStartOptions{Detach: true}); err != nil {
		switch {
		case errdefs.IsNotFound(err):
			// Docker drops the execs of a container that stopped.
			return nil
		case !errdefs.IsConflict(err):
			return err
		}
	}
	_, err = waitDaemonExec(ctx, docker, execID)
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

// waitDaemonExec returns once the exec has had a process and it ended.
func waitDaemonExec(ctx context.Context, docker keyRotationExecClient, execID string) (mobycl.ExecInspectResult, error) {
	for {
		inspect, err := docker.ExecInspect(ctx, execID, mobycl.ExecInspectOptions{})
		if err != nil || (!inspect.Running && inspect.PID != 0) {
			return inspect, err
		}
		select {
		case <-ctx.Done():
			return inspect, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// daemonKeyrings returns the container of an owned daemon and the keyring
// files from which it authenticates when it starts. The caller holds c.mu.
func (c *Container) daemonKeyrings(kind, id string) (testcontainers.Container, []string) {
	if kind == "mgr" {
		return c.managers[id].Container, []string{"/etc/ceph/mgr.keyring", "/var/lib/ceph/mgr/ceph-" + id + "/keyring"}
	}
	n, _ := strconv.Atoi(id)
	return c.osds[n].Container, []string{"/etc/ceph/osd.keyring", "/var/lib/ceph/osd/ceph-" + id + "/keyring"}
}
