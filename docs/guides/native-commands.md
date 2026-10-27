# Running native commands

## Introduction

The typed API covers the operations tests need most often, but Ceph has far
more knobs than any wrapper. Everything else is available through Ceph's own
command-line tools, which this module runs inside containers. The Go module
never links librados or uses cgo; commands always execute inside a Linux
container.

This guide shows where each kind of command runs, how to read its output, how
to change any configuration option, how to attach your own client, and which
native changes conflict with what the fixture tracks.

| You want to run | Use | Runs in |
| --- | --- | --- |
| The `ceph` CLI as `client.admin` | `cluster.Ceph(ctx, args...)` | The control container |
| `rados`, `rbd`, `radosgw-admin`, `ceph-authtool`, Python bindings | `control.Exec(ctx, argv, ...)` with `control` from `cluster.ControlContainer(ctx)` | The control container |
| A command with bounded run time and output whose process must not outlive the test | `cluster.RunNativeCommand(ctx, target, command)` | The control container, a daemon container, or your attached container |
| A workload, a restricted identity or your own client software | A container attached with `WithClient` or `WithClientIdentity` | Your container |
| A native client in the test process | `cluster.ConnectionConfig(ctx)` with host networking | Your process |
| `radosgw-admin` in a gateway's realm and zone | `rgw.Gateway.Admin(ctx, args...)` | The gateway container |
| `radosgw-admin` in a multisite zone | `multisite.Fixture.SourceAdmin`, `DestinationAdmin`, `ZoneAdmin` | The fixture's setup client for that zone |
| `rbd` on either side of a mirror link | `rbdmirror.Mirror.SourceRBD`, `DestinationRBD` | The link's setup clients |
| A daemon's admin socket command | `cluster.Ceph(ctx, "tell", daemon, ...)`, or `ceph --admin-daemon` in the daemon's container | Control container or daemon container |
| A read-only snapshot for failure analysis | `cluster.CollectDiagnostics(ctx, config)` | Docker API and the control container |

## The ceph CLI

`Ceph` runs `ceph --connect-timeout 5 <args>` in the control container with
the admin identity and returns standard output.

```golang
out, err := cluster.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
if err != nil {
	t.Fatal(err)
}
var pools []struct {
	PoolName string `json:"pool_name"`
	Size     int    `json:"size"`
}
if err := json.Unmarshal(out, &pools); err != nil {
	t.Fatal(err)
}
```

- Arguments are passed directly, without a shell. Do not quote them for a
  shell; pass each argument as its own string.
- Add `--format json` for anything you parse.
- On a non-zero exit, `Ceph` returns the standard output it got and a
  `*ceph.NativeFailure` with the exit code, the errno that Ceph printed and
  Ceph's reason without its log lines, with keys redacted. Standard output, which holds the
  keys of commands such as `auth get`, never appears in the error. Match the
  errno with `errors.Is(err, fs.ErrNotExist)` or read it with `errors.As`;
  see [Classified errors](errors.md#native-failures).
- The CLI's `--connect-timeout 5` bounds only its connection to a MON: when
  it expires, the CLI exits with `timed out` before it sends the command.
  `Ceph` and the fixture's own CLI commands then connect again, until a MON
  answers, `ctx` ends or a minute has passed. A MON that stalls, for example
  on a runner whose disk pauses, or an election delays the command instead
  of failing it, and the command runs once. A call that `ctx` ends this way
  matches `ctx`'s error.
- `Ceph` does not take the cluster's topology gate, so it can run while
  another goroutine adds or removes daemons. See
  [Contexts, ownership and cleanup](contexts-and-cleanup.md).

The control container is the MON in a single-MON cluster, and a separate CLI
container when the cluster has several MONs. It keeps working while a MON is
stopped as long as a quorum remains.

## Other CLIs in the control container

`ControlContainer(ctx)` returns the control container handle. It has every Ceph
CLI, `/etc/ceph/ceph.conf` and the admin keyring, so `rados`, `rbd` and
`radosgw-admin` work without extra flags. For RGW administration prefer
`rgw.Gateway.Admin`, which runs in the gateway's own realm and zone.

```golang
// exec runs argv in ctr and returns standard output. Standard error is
// included in the error when the command fails.
func exec(ctx context.Context, ctr testcontainers.Container, argv ...string) ([]byte, error) {
	code, reader, err := ctr.Exec(ctx, argv)
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, err
	}
	if code != 0 {
		return stdout.Bytes(), fmt.Errorf("%s exited %d: %s", argv[0], code, stderr.String())
	}
	return stdout.Bytes(), nil
}

control, err := cluster.ControlContainer(ctx)
if err != nil {
	t.Fatal(err)
}
if _, err := exec(ctx, control, "rbd", "create", "--size", "64M", "rbd/disk"); err != nil {
	t.Fatal(err)
}
info, err := exec(ctx, control, "rbd", "info", "rbd/disk", "--format", "json")
```

`stdcopy` is `github.com/moby/moby/api/pkg/stdcopy`; it separates Docker's
multiplexed stdout and stderr streams. If you only need combined text, pass
`tcexec.Multiplexed()` (from `github.com/testcontainers/testcontainers-go/exec`)
to `Exec` and read the stream directly.

Notes:

- The cluster owns the control container. Do not stop, pause or reconfigure
  it, and do not edit files under `/etc/ceph` in it.
- Long or heavy workloads compete with the MON in a single-MON cluster. Run
  them in your own client container instead.
- Cancelling `ctx` stops waiting for the command but does not prove that the
  process inside the container stopped, and Docker buffers all of its output.
  Use [RunNativeCommand](#bounded-commands-with-confirmed-cleanup) when that
  matters, or native timeouts: `--connect-timeout` for `ceph`, and Ceph
  options such as `--rados-osd-op-timeout 10` or `--rados-mon-op-timeout 10`
  for `rados` and `rbd`.
- For pipes or redirections, run a shell explicitly:
  `exec(ctx, control, "sh", "-c", "rados -p volumes ls | wc -l")`.
- Python with the `rados`, `rbd` and `cephfs` bindings is available for
  anything the CLIs do not expose:
  `exec(ctx, control, "python3", "-c", script)`.

## Reading output

Most Ceph tools print JSON with `--format json` (or `-f json`). The known
exceptions:

| Command | Output | Typed alternative |
| --- | --- | --- |
| `rados listwatchers` | Always text: `watcher=IP:port/nonce client.<id> cookie=<n>` | `cluster.ObjectWatchers` |
| `radosgw-admin bucket sync status` on Squid | Text; `--format json` is ignored | `multisite.Fixture.BucketSyncStatus`, `WaitBucketSyncReady` |
| `radosgw-admin sync status` | The comparison with remote logs appears only in the human-readable output, even with `--format json` | `multisite.Fixture.SyncStatus`, `WaitSyncReady` |
| `rbd mirror pool info` on Squid | JSON without `mirror_uuid` and `remote_namespace` | `rbdmirror.Mirror.PolicyStatus` |

Decode only the fields you need, and keep unknown fields tolerated: Ceph adds
fields between releases. See [Ceph releases and images](releases-and-images.md)
for release-specific output.

## Bounded commands with confirmed cleanup

`RunNativeCommand` runs one command under the supervisor that the module uses
for its own native queries. Use it for oracle and probe commands whose run
time and output must be bounded, and whose process must not outlive a
canceled call.

```golang
result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
	Argv:      []string{"rados", "-p", "volumes", "stat", "greeting"},
	Timeout:   30 * time.Second,
	MaxStdout: 64 << 10,
	MaxStderr: 64 << 10,
	Label:     "stat greeting",
})
if err != nil {
	t.Fatal(err)
}
if result.ExitCode != 0 {
	t.Fatalf("rados stat exited %d", result.ExitCode)
}
```

| Field | Meaning |
| --- | --- |
| `Argv` | Program and arguments, run without a shell. Required; no argument may contain NUL. |
| `Timeout` | Run time, at most `ceph.MaxNativeCommandTimeout` (one hour). The deadline of `ctx` shortens it. |
| `MaxStdout`, `MaxStderr` | Bytes kept of each stream, at most `ceph.MaxNativeCommandOutput` (64 MiB). The limit applies inside the container, before Docker buffers anything. |
| `Label` | Names the command in errors: at most 128 bytes without control characters. Defaults to `native command`. |
| `Input`, `MaxInput` | Optional standard input, and the bytes that it may carry in all, at most `ceph.MaxNativeCommandInput` (64 MiB). See [Standard input](#standard-input). Without `Input` the command's standard input is empty. |

A malformed command, or a context that has already ended, fails before
anything runs.

### Where it runs

The target is one of:

- the control container, or a MON, MGR, OSD, RGW or MDS container of this
  cluster;
- a container of yours attached to the cluster's network, such as one started
  with `WithClient` or `WithClientIdentity`. For a host-network cluster, it is
  a container on the host network.

Any other container is refused. The call never adopts, stops or removes your
container. The target must be running and not paused, and it needs `python3`,
which runs the supervisor. The official Ceph images and every control image
have it. OSD, RGW and MDS role images need not (see
[Ceph releases and images](releases-and-images.md)); for a container without
it the error says so, and the control container or a client container can
run the command instead.

### Reading the result

`Stdout` and `Stderr` are separate raw bytes and binary-safe. `ExitCode` is the
command's exit code, and a nonzero exit is not an error.

A command that runs into its timeout or exceeds an output limit still returns
what it wrote, with `TimedOut`, `StdoutTruncated` or `StderrTruncated` set,
together with an error that wraps `ceph.ErrNativeCommandIncomplete`.
`result.Complete()` reports whether neither happened. A truncated object, map
or JSON document is not evidence for an oracle:

```golang
if errors.Is(err, ceph.ErrNativeCommandIncomplete) {
	t.Fatalf("incomplete output: timed out %t, stdout truncated %t", result.TimedOut, result.StdoutTruncated)
}
```

Errors name the command by its label and never contain its arguments or
output. A failure of Docker or of your container type appears in the text
only by its kind, such as a container that does not exist or is paused;
`errors.Unwrap`, `errors.Is` and `errdefs.IsNotFound` still reach the original
error, whose text may quote anything. Failures before the command runs are
classified: `ceph.ErrInvalidRequest` for a malformed command or a container
that is not admitted, stopped or paused, `ceph.ErrImageCapability` for a
container without `python3` or a program that does not exist or is not
executable, and `ceph.ErrEnvironmentUnavailable` for a program that could not
be started for want of a process or memory or because a security policy
refused it, and for another Docker failure. A container that disappears or
stops after the command was sent matches no category: the command may have
run. See [Classified errors](errors.md). Before you log output, mask the
cluster's keys and your own secrets with `RedactSecrets`, which uses the same
rules as `CollectDiagnostics`:

```golang
safe, err := cluster.RedactSecrets(ctx, result.Stderr, applicationToken)
if err == nil {
	t.Logf("stderr: %s", safe)
}
```

Compare the raw bytes in assertions; redaction changes the data.

### Cancellation and cleanup

- At its deadline, the supervisor kills the command's process group: the
  command and every descendant that stayed in its group; a killed process
  cannot run on. On Linux the supervisor also reaps those descendants for up
  to 200 ms, which normally leaves no zombie, even in a daemon container whose
  first process does not reap orphans. A descendant that starts its own
  session, for example with `setsid`, leaves the group and is not stopped. If
  the supervisor cannot signal the group, it keeps the invocation unconfirmed
  rather than report it stopped.
- The supervisor runs as the container's user and signals what that user may
  signal. The cluster's containers, and client containers by default, run as
  root with Docker's default capabilities, where this covers the whole group.
  In a container whose user lacks `CAP_KILL`, a descendant that switches to
  another user stays out of reach and is not stopped.
- When `ctx` ends first, the call stops the command inside the container
  before it returns. The stop has its own budget of
  `ceph.NativeCommandStopTimeout` (15 seconds), independent of `ctx`. If
  Docker had not started the command yet, the stop claims the invocation, and
  a start that Docker delivers later runs nothing. Such a claim can stay as a
  small file in the local validation artifact inside the container.
- When the stop cannot be confirmed, the error is a
  `*ceph.NativeCommandUnstoppedError`, and the command may still run. This
  happens when the container is paused or stopped, when the Docker engine does
  not answer, and when a signal killed the supervisor itself, for example the
  kernel's OOM killer, or it could not signal the command's group. Keep the error and call `Stop` with a fresh context,
  for example after resuming the container.

```golang
var unstopped *ceph.NativeCommandUnstoppedError
if errors.As(err, &unstopped) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := unstopped.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
}
```

- `Stop` confirms inside the container. It also confirms a command of an
  earlier instance of a restarted container, without signalling the
  processes of the new instance. A stopped container confirms nothing until it
  runs again, because Docker may still start an exec that it accepted before.
  After the container was removed, `Stop` returns nil.
- A container instance is recognised by the start time of the first process
  in its PID namespace. A container that shares another PID namespace, for
  example with `--pid=host`, keeps that identity across a restart, so a
  command that a killed supervisor left there is confirmed only after the
  container was removed.
- `Terminate` cancels the commands that still run, waits until their calls have
  returned, and stops the commands that they could not confirm as stopped. A
  command that `Terminate` cannot stop stays tracked, and `Terminate` reports
  it. Afterwards `RunNativeCommand` fails.
- Stopping the process does not undo what the command already did. A MON
  command that it had sent may have applied, and the call never runs a
  command again. Typed helpers such as `ClientConfig.ReplaceKey` settle their own
  changes; a raw command's outcome is yours to read back.

### Standard input

A worker that starts before the data it needs exists can read that data from
its standard input. Create a `NativeInput`, pass it with `MaxInput`, run the
command in a goroutine, and write when the worker is ready, for example after
it published a receipt:

```golang
input := ceph.NewNativeInput()
runCtx, cancel := context.WithCancel(ctx)
done := make(chan error, 1)
finished := make(chan struct{})
t.Cleanup(func() {
	cancel()
	_ = input.Close()
	<-finished // Also join the call when an assertion below fails.
})
go func() {
	defer close(finished)
	result, err := cluster.RunNativeCommand(runCtx, client, ceph.NativeCommand{
		Argv:      []string{"python3", "-c", worker, workdir, token},
		Timeout:   2 * time.Minute,
		MaxStdout: 4096,
		MaxStderr: 64 << 10,
		Label:     "comparison worker",
		Input:     input,
		MaxInput:  1 << 20,
	})
	var unstopped *ceph.NativeCommandUnstoppedError
	if errors.As(err, &unstopped) {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Minute)
		defer stopCancel()
		err = errors.Join(err, unstopped.Stop(stopCtx))
	}
	if err == nil && (!result.InputEOF || result.InputDelivered != result.InputWritten || result.ExitCode != 0) {
		err = fmt.Errorf("worker exited %d after %d of %d input bytes", result.ExitCode, result.InputDelivered, result.InputWritten)
	}
	if err != nil {
		t.Error(err) // Preserve cleanup failures even if another assertion failed.
	}
	done <- err
}()
// Wait for the worker's ready receipt, then:
if err := input.Write(ctx, record); err != nil {
	t.Fatal(err)
}
// Later records, then the end of input:
_ = input.Close()
if err := <-done; err != nil {
	t.Fatal(err)
}
```

- Input is transported byte for byte, in order, with NUL and non-UTF-8 bytes
  intact, and the end of input after `Close`. Writes may come
  at any time while it runs: before the command starts, `Write` waits for it,
  within its `ctx`.
- `MaxInput` bounds all writes together. A write that would pass it is
  refused whole with `ceph.ErrInvalidRequest`, and the input stays open.
  An invalid limit fails the call before anything runs.
- The supervisor reads what arrives at once and passes it on as the command
  reads. A command that reads slowly or not at all does not hold up `Write`;
  it holds up at most `MaxInput` bytes inside the container.
- `Write` returns once the input stream to the container took the data. When
  its `ctx` ends after that, it returns the context's error, and the command
  may still get the data.
- Once the command ended, closed its standard input, or was canceled or
  stopped, writes fail with `ceph.ErrNativeInputClosed`. The result says what
  the command could read: `InputWritten` bytes reached the container,
  `InputDelivered` of them went into the command's standard input, and
  `InputEOF` means that the end of input followed all of them.
- `Complete()` checks time and output limits only. It does not require all
  input to be delivered or EOF to be sent. Even equal byte counts and
  `InputEOF` show delivery into a pipe, not that the application consumed or
  applied the records: check the worker's own receipt as well. Do not repeat
  a write after an uncertain context error without settling its outcome.
- The call stops taking writes when the command ends, and returns only after
  its input and output work ended. A deadline, a canceled `ctx` or
  `Terminate` stops the command as in
  [Cancellation and cleanup](#cancellation-and-cleanup); a write in progress
  then fails.
- An input serves one command. Pass a new one to the next call.
- Written data never appears in arguments, the environment, files, errors,
  labels or diagnostics. The command's output is returned as the command wrote
  it, and no redaction applies to it automatically: a worker that handles
  secrets must not echo them, and should print only what the test compares,
  such as counts and equality.
- The supervisor's exec has its standard input attached through the Docker
  engine's attach API, so a target with `Input` must be a container of the
  Docker engine that the cluster uses.

`TestNativeCommandInput` sends two binary records in separate phases to such
a worker and checks its receipts and the result;
`TestNativeCommandInputCancellation` cancels a call while it writes to a
command that does not read.

### Oracle recipe: stat2, read and ceph-dencoder

These commands run in a client container started with `WithClient`, so a
test process without Ceph libraries or cgo can use the results. The first one
calls `rados_stat2` and `rados_read` through `ctypes` and prints one JSON
document with the object's size, modification time and base64 data:

```golang
const statRead = `import base64,ctypes,json,sys
pool,name=sys.argv[1].encode(),sys.argv[2].encode()
lib=ctypes.CDLL("librados.so.2")
class timespec(ctypes.Structure):
    _fields_=[("tv_sec",ctypes.c_int64),("tv_nsec",ctypes.c_long)]
def check(ret,call):
    if ret<0:
        raise OSError(-ret,call)
cluster,ioctx=ctypes.c_void_p(),ctypes.c_void_p()
check(lib.rados_create(ctypes.byref(cluster),None),"rados_create")
try:
    check(lib.rados_conf_read_file(cluster,b"/etc/ceph/ceph.conf"),"rados_conf_read_file")
    check(lib.rados_connect(cluster),"rados_connect")
    check(lib.rados_ioctx_create(cluster,pool,ctypes.byref(ioctx)),"rados_ioctx_create")
    size,mtime=ctypes.c_uint64(),timespec()
    check(lib.rados_stat2(ioctx,name,ctypes.byref(size),ctypes.byref(mtime)),"rados_stat2")
    buf=ctypes.create_string_buffer(max(size.value,1))
    read=lib.rados_read(ioctx,name,buf,ctypes.c_size_t(size.value),ctypes.c_uint64(0))
    check(read,"rados_read")
    print(json.dumps({"size":size.value,"mtime":[mtime.tv_sec,mtime.tv_nsec],"data":base64.b64encode(buf.raw[:read]).decode()}))
finally:
    if ioctx.value:
        lib.rados_ioctx_destroy(ioctx)
    lib.rados_shutdown(cluster)
`

result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
	Argv:    []string{"python3", "-c", statRead, "volumes", "greeting"},
	Timeout: time.Minute,
	// Base64 needs 4/3 of the object's size.
	MaxStdout: 1 << 20,
	MaxStderr: 64 << 10,
	Label:     "stat2 and read greeting",
})
if err != nil || result.ExitCode != 0 {
	t.Fatalf("stat2 and read: exit %d, %v", result.ExitCode, err)
}
var object struct {
	Size  int      `json:"size"`
	MTime [2]int64 `json:"mtime"`
	Data  []byte   `json:"data"`
}
if err := json.Unmarshal(result.Stdout, &object); err != nil {
	t.Fatal(err)
}
```

`rados_create` with `None` connects as `client.admin`. For an identity attached
with `WithClientIdentity`, pass its `User()` instead, such as `b"reader"`. The
script shuts the client down in `finally`: a librados client left to the
interpreter's exit sometimes aborts after its work is done, which turns into a
nonzero exit code.

`ceph-dencoder` decodes a native structure from a file. Fetch the current
OSDMap and decode it in two bounded commands:

```golang
var decoded []byte
for _, step := range []ceph.NativeCommand{
	{Argv: []string{"ceph", "osd", "getmap", "-o", "/tmp/osdmap"}, Label: "get OSDMap"},
	{Argv: []string{"ceph-dencoder", "type", "OSDMap", "import", "/tmp/osdmap", "decode", "dump_json"}, Label: "decode OSDMap"},
} {
	step.Timeout, step.MaxStdout, step.MaxStderr = time.Minute, 4<<20, 64<<10
	result, err := cluster.RunNativeCommand(ctx, client, step)
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("%s: exit %d, %v", step.Label, result.ExitCode, err)
	}
	decoded = result.Stdout
}
```

`ceph-dencoder` is part of the official `quay.io/ceph/ceph` images. In an
image without it, the call fails with an error saying that the program could
not be started. The supervisor bounds only the execution; parsing the decoded
structure and comparing it with your own decoder stays in your test.

### Oracle recipe: self-managed snapshot write contexts

A client that manages its own snapshots, as RBD does, asks the MONs for each
snapshot ID and sends a write context with every write: a sequence and the
existing snapshot IDs, newest first. When a write to an existing object names
a snapshot that still exists and is newer than the object's own sequence, the
OSD first preserves the object as a clone. The Python binding of librados,
which the official images ship, can produce these contexts, so a test can
compare its own client with native results.

Create an owned pool, then run the writer and an independent reader as two
librados clients in one bounded command:

```golang
const selfSnap = `import hashlib,json,sys,rados
pool,name=sys.argv[1],sys.argv[2]
def connected():
    c=rados.Rados(conffile="/etc/ceph/ceph.conf")
    c.connect(); return c
def read(ioctx,snap):
    ioctx.set_read(snap)
    try:
        data=ioctx.read(name,4096,0)
        return hashlib.sha256(data).hexdigest(),ioctx.get_last_version()
    finally:
        ioctx.set_read(rados.LIBRADOS_SNAP_HEAD)
writer,reader=connected(),connected()
a,b=writer.open_ioctx(pool),reader.open_ioctx(pool)
snap=None
try:
    a.write_full(name,b"original\0payload"*64)
    snap=a.create_self_managed_snap()
    a.set_self_managed_snap_write([snap])
    a.write(name,b"AFTER",7)
    a.append(name,b"APPEND")
    a.trunc(name,64)
    written=a.get_last_version()
    print(json.dumps({"snap":snap,"written":written,
        "snapshot":read(b,snap),"head":read(b,rados.LIBRADOS_SNAP_HEAD)}))
finally:
    a.set_self_managed_snap_write([])
    if snap is not None:
        a.remove_self_managed_snap(snap)
    a.close(); b.close(); writer.shutdown(); reader.shutdown()
`

if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "selfsnap", Application: "rados", PGNum: 1}); err != nil {
	t.Fatal(err)
}
result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
	Argv:      []string{"python3", "-c", selfSnap, "selfsnap", "object"},
	Timeout:   2 * time.Minute,
	MaxStdout: 64 << 10,
	MaxStderr: 64 << 10,
	Label:     "self-managed snapshot",
})
if err != nil || result.ExitCode != 0 {
	t.Fatalf("self-managed snapshot: exit %d, %v", result.ExitCode, err)
}
```

`set_self_managed_snap_write` sorts the IDs and uses the newest as the
sequence, so the binding cannot send a malformed context. Each version in the
output comes from the request that read or wrote those bytes: a read at a
snapshot reports the version that HEAD had before the write that preserved
it. Versions increase but are not consecutive; in 20.2.4 and 19.2.5 the write
that makes a clone skips one. Reading `get_last_version` of an ioctx after an
unrelated request gives that request's version, not the object's.

Ceph 20.2.4 and 19.2.5 handle the contexts alike:

| Write context | Native result |
| --- | --- |
| Names an existing snapshot newer than the object's sequence | Before writing an existing object, the OSD preserves it as a clone. The clone keeps the context's snapshots that are newer than the object's sequence and still exist; `rados listsnaps` shows it with its size |
| Sequence older than the object's | Accepted, without a clone. With the `LIBRADOS_OPERATION_ORDERSNAP` flag (512) the OSD refuses it with `EOLDSNAPC` (85) and changes nothing |
| Names a removed snapshot | Accepted; the clone keeps only the snapshots that exist. A context of removed snapshots alone makes no clone |
| An ID that the MONs never allocated | Accepted: it becomes a clone ID and the object's sequence. A snapshot that the MONs allocate later with a lower ID is then not preserved on that object, and a read at it finds no object. The MONs refuse to remove the ID with `ENOENT`, so its clone stays until the pool is removed |

Other outcomes:

- **Removal.** `remove_self_managed_snap` returns before the OSDs trim the
  snapshot's clone, and until then a read at the snapshot may still return
  the clone's data. Poll `rados listsnaps` and a read, with a deadline, until
  both show it gone.
- **Delete.** Removing HEAD under a context leaves the snapshots readable;
  reads and `stat` of HEAD fail with `ENOENT`.
- **Restart.** The default BlueStore OSD keeps the bytes, versions, sizes
  and modification times (as `stat` reports them, in seconds) of the
  snapshots and HEAD across `OSDContainer.Stop` and `Start`.
  MemStore saves its data only on a clean stop, so it does not show
  durability.
- **Cleanup.** A command that times out or is canceled is killed before its
  `finally` runs. `Pool.Remove` deletes the owned pool with every clone,
  including the ones that no snapshot removal trims.

`TestRADOSSelfManagedSnapshots` in `internal/integration` runs these cases
with nonaligned writes, append, truncation and zero extension, each read back
by the independent client, an OSD restart and digests of every snapshot and
HEAD.

### Oracle recipe: write outcomes

A writer cannot tell from its own result whether a write happened. Decide it
with a second client that reads the object back, and keep the three things
apart:

- **The completion.** librados completes a write when the OSD has committed
  it, and that completion is the receipt. On a file-backed BlueStore OSD,
  writes whose completion returned 0 survive a `SIGKILL` of the OSD. Receiving
  the message is not the receipt: in the delayed case below, the OSD's record
  of the write starts when the message arrived, seconds before the write ran.
- **A local timeout.** `rados_osd_op_timeout` ends the call with `ETIMEDOUT`
  (`-110` in an asynchronous completion, reported once). It does not cancel
  the write: an OSD that still holds it runs it later, while the client stays
  connected. See [Delaying operations inside an OSD](fault-injection.md#delaying-operations-inside-an-osd).
- **A killed client.** When the OSD is about to start a write, it discards
  the write if it has already seen the client's connection close
  (`osd_discard_disconnected_ops`, true by default). Killing the client does
  not guarantee that order by itself, so confirm the outcome from the OSD's
  record of the operation (no `started` event) and an independent read.
  `RunNativeCommand` confirms that the process group is gone after a timeout
  or cancellation.

Do not retry an uncertain append or write blindly: if the first attempt ran,
the retry applies it twice. Guard the retry with the version that the
independent client read before the first attempt:

```python
with rados.WriteOpCtx() as op:
    op.assert_version(version)  # read before the first attempt
    op.append(b"record;")
    ioctx.operate_write_op(op, "log")
```

The guarded retry fails with `ERANGE` (34) when the object changed since that
read, for example because the first attempt ran, and with `EOVERFLOW` (75)
when the version is newer than the object's. An object's version is the
version of its PG's log at the object's last write, so the versions of objects
in one PG interleave. A refused write still takes a version, because the PG
log records it to detect resends: after two refused retries, the next append
was three versions newer, in 20.2.4 and 19.2.5 alike.

`TestRADOSMutationOutcomes` in `internal/integration` shows each case with
native clients and the OSD's admin socket: acknowledged asynchronous writes
across a `SIGKILL`, a write that ran after its client's timeout, a guarded and
an unguarded retry, and a killed writer whose write the OSD discarded.

### Oracle recipe: erasure-coded pools

An erasure-coded pool splits each object into K data shards and M parity
shards on separate OSDs, and accepts fewer write operations than a replicated
pool. Which ones depends on `AllowOverwrites` in `ceph.ErasureCodeConfig`
(the pool's `allow_ec_overwrites`). The Python binding reports the
difference: `ioctx.alignment()` returns the required alignment of a pool
without overwrites and `None` otherwise.

On k=2, m=1 pools created by `CreatePool`, Ceph 20.2.4 and 19.2.5 return:

| Operation | Without overwrites | With overwrites |
| --- | --- | --- |
| `write_full` | Accepted, also of an unaligned size | Accepted |
| `append`, or `write` at the end of the object | Accepted only while the end is a multiple of the alignment (8192); otherwise `EOPNOTSUPP` (95) | Accepted |
| `write` over existing bytes, `trunc`, `zero` | `EOPNOTSUPP` | Accepted |
| omap writes | `EOPNOTSUPP` | `EOPNOTSUPP` |
| xattrs | Accepted | Accepted |
| A write under a self-managed snapshot context | `write_full` preserves the object as a clone first | A partial write preserves the object as a clone first |

So data can be added once after an aligned size, and is refused once the
size is no longer aligned. Check the refusals in your test instead of
skipping them: they are the native outcome of the pool's configuration.

A pool with `MinSize` equal to K stays active while one shard is missing.
After a clean stop of the OSD that holds shard 1, `Pool.PGs` reported the PG as
`active+undersized+degraded`, with `2147483647` (no OSD) in that slot of
`Acting`. Reads returned the same bytes, rebuilt from the other shards, and a
write under a snapshot context still preserved a clone. After the OSD started
again and recovery finished, every byte and version was unchanged. Hold
`noout` with `OverrideOSDFlag` while the OSD is stopped.

`TestRADOSErasureCodedOutcomes` in `internal/integration` runs these cases
with an independent reader client.

## Changing configuration

| Need | Use |
| --- | --- |
| Inspect stored central configuration | `cluster.Configuration(ctx)` returns exact entries, including masked ones |
| Change any option for the duration of a test | `cluster.OverrideConfig(ctx, ceph.ConfigSetting{...})` and `Restore` |
| Options that must be present at bootstrap | `ceph.WithConfigFile(path)` |
| A daemon's effective value | `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_op_complaint_time")` or `cluster.Ceph(ctx, "tell", "osd.0", "config", "get", "osd_op_complaint_time")` |

`OverrideConfig` accepts any option, not only those the module knows:

```golang
change, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{
	Section: "osd",
	Mask:    "class:ssd",
	Name:    "osd_scrub_min_interval",
	Value:   "7200",
})
if change != nil {
	defer change.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
```

- `Section` is `global`, a daemon type (`mon`, `mgr`, `osd`, `mds`, `client`)
  or one daemon (`osd.0`, `mds.a`). `Mask` optionally restricts the entry, for
  example `class:ssd`, `host:node-a` or `host:node-a/class:ssd`. MGR module
  options such as `mgr/volumes/max_concurrent_clones` work too.
- The central database has lower precedence than local configuration files,
  command-line arguments and runtime overrides. A stored value is not proof
  that a daemon uses it, and some options only take effect after a restart.
  Check the effective value with `config show`.
- Keys set by `ceph.WithConfigFile` live in every daemon's local `ceph.conf`
  and cannot be changed through the central database.
- `ceph tell <daemon> config set` changes one running daemon only. It is not
  stored centrally, is lost when the daemon restarts, and no handle restores
  it.

Use `WithConfigFile` for settings that must exist before daemons start:

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
	ceph.WithConfigFile(filepath.Join("testdata", "ceph.conf")),
)
```

The file uses native `ceph.conf` syntax and is merged into the bootstrap
configuration of every daemon, `WithClient` container and `ConnectionConfig`.
Keys the fixture manages (identity, addresses, networks, Cephx, messenger
modes, logging, data paths, pool size defaults, BlueStore file size) are
rejected with a hint to the option to use instead. RGW runs as `client.admin`,
so RGW settings belong in `[client]` or `[client.admin]`. See
[ceph.md](../ceph.md) for the full contract.

## Running your own client container

`WithClient` attaches a container to the cluster network and copies
`/etc/ceph/ceph.conf` and the admin keyring into it. The control image is a
convenient client image because it has every Ceph CLI and the Python bindings:

```golang
client, err := testcontainers.Run(ctx, cluster.ControlImage(),
	cluster.WithClient(),
	ceph.WithIdleEntrypoint(),
)
testcontainers.CleanupContainer(t, client)
if err != nil {
	t.Fatal(err)
}
out, err := exec(ctx, client, "python3", "-c", `
import rados
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect()
ioctx = cluster.open_ioctx("volumes")
ioctx.write_full("greeting", b"hello")
print(ioctx.read("greeting").decode())
ioctx.close()
cluster.shutdown()
`)
```

For a least-privilege identity, create a client and attach it with
`WithClientIdentity`. The container gets `ceph.conf` and only that client's
keyring, at `client.KeyringPath()`:

```golang
reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow r pool=volumes",
})
if err != nil {
	t.Fatal(err)
}
restricted, err := testcontainers.Run(ctx, cluster.ControlImage(),
	cluster.WithClientIdentity(reader),
	ceph.WithIdleEntrypoint(),
)
testcontainers.CleanupContainer(t, restricted)
if err != nil {
	t.Fatal(err)
}
_, err = exec(ctx, restricted, "rados", "--id", reader.User(), "-p", "volumes", "ls")
```

- `CreateClient` sets exactly the capabilities you pass. Existing identities
  are rejected. `ClientConfig.UpdateCaps` replaces all capabilities;
  `ClientConfig.Capabilities` reads them back; `ClientConfig.Delete` revokes the identity.
  Sessions that are already established may keep their tickets, so check
  permission changes with a fresh connection.
- `reader.Name()` is `client.reader`; `reader.User()` is `reader`, the form
  librados and go-ceph expect.
- Neither option installs software. Your own image must contain the Ceph
  client libraries it uses.
- You own these containers: terminate them before the cluster. After a MON
  replacement, `cluster.RefreshClientMonitorConfig(ctx, client)` updates the
  `mon_host` entry in the client's `ceph.conf`.

## Clients in the test process

`cluster.ConnectionConfig(ctx)` returns copies of `ceph.conf` and the admin
keyring for a native client in your test process. Daemons must be reachable
from the process, which normally requires `ceph.WithHostNetwork()`. See
[Host network and native clients](host-network.md).

## Daemon admin sockets

Every daemon has an admin socket with commands for status, performance
counters and debugging.

For MON, MGR and OSD daemons, use
the `AdminSocket` method of `OSDContainer`, `MonitorContainer` and `ManagerContainer` (see [AdminSocket](../ceph.md#adminsocket)). It reaches the socket of
any owned daemon, whatever its image, without the Messenger or Cephx:

```golang
status, err := osd.AdminSocket(ctx, "status") // osd is the *ceph.OSDContainer of osd.0
```

`ceph tell` from the control container also works for MON, MGR, OSD and MDS
daemons regardless of the daemon's image, but it travels over the Messenger
like any client request:

```golang
help, err := cluster.Ceph(ctx, "tell", "osd.0", "help")
sessions, err := cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":0",
	"session", "ls", "--format", "json")
```

`ceph --admin-daemon <socket>` inside the daemon's own container reaches the
socket directly. It needs the `ceph` CLI in that container. Control and `all`
images always have it; OSD, MDS and RGW role images are not required to (see
[image requirements](releases-and-images.md#image-requirements-by-role)).

| Daemon | Socket inside its container | Handle |
| --- | --- | --- |
| MON | `/var/run/ceph/ceph-mon.<name>.asok` | element `i` of `cluster.Monitors(ctx)`, name `DaemonName` |
| MGR | `/var/run/ceph/ceph-mgr.<name>.asok` | element `i` of `cluster.Managers(ctx)`, name `DaemonName` |
| OSD | `/var/run/ceph/ceph-osd.<id>.asok` | element `i` of `cluster.OSDs(ctx)`, `ID` |
| MDS | `/var/run/ceph/ceph-mds.<id>.asok` | `fs.MDSs()[i]`, `ID` |
| `rbd-mirror` | the local validation artifact | `rbdMirror.Daemons()[i]` |
| `cephfs-mirror` | `/var/run/ceph/cephfs-mirror.asok` | `cephfsMirror.Daemons()[i]` |

Mirror daemons run from the control image, so `--admin-daemon` always works
there:

```golang
daemon := rbdMirror.Daemons()[0]
out, err := exec(ctx, daemon, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok",
	"rbd", "mirror", "status")

fsDaemon := cephfsMirror.Daemons()[0]
// The argument is <filesystem>@<fscid>; the fscid is the "id" field of
// "ceph fs get <filesystem> --format json" on the source cluster.
out, err = exec(ctx, fsDaemon, "ceph", "--admin-daemon",
	"/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", "tc-cephfs@1")
```

Prefer the typed observers when they cover your case: `rbdmirror.Daemon.Status`,
`rbdmirror.Mirror.ReceiverStatus`, `ImageStatus`, `cephfsmirror.Mirror.DirectoryStatus`.

### Observing Cephx keys from the daemon side

No admin socket command prints a daemon's own key or the rotating service
secrets that it holds. Observe them in two ways instead.

**The daemon's key.** After
[`RotateKey`](../ceph.md#rotatekey) of `OSDContainer` and `ManagerContainer`, `auth get-key` on
the MONs returns the new key, and a restart of the daemon proves that it
authenticates with the key in its keyring files.

**Service secrets.** The MONs rotate the secret of each service (MON, MGR, OSD,
MDS) every `auth_service_ticket_ttl`, and daemons fetch them as they change.
Raise the daemon's `debug_auth` through its admin socket. The daemon then logs
two things to its container output:
- the secrets it holds, at every MON client tick;
- the secret that decrypted each client ticket it accepts.

```golang
_, err := osd.AdminSocket(ctx, "config", "set", "debug_auth", "10/10") // osd is the *ceph.OSDContainer of osd.0
// ... let a client connect to osd.0 ...
osds, err := cluster.OSDs(ctx)
if err != nil {
	t.Fatal(err)
}
logs, err := osds[0].Logs(ctx)
```

| Event | Tentacle 20.2.4 | Squid 19.2.5 |
| --- | --- | --- |
| Secrets held, one line per secret | `RKR(...).dump_rotating  id 3 <secret> expires <time>` | `auth:  id 3 <secret> expires <time>` after `auth: dump_rotating:` |
| A client ticket accepted | `cephx: cephx_verify_authorizer: decoded service osd secret_id=3`, then `global_id=<client>` | `cephx: verify_authorizer decrypted service osd secret_id=3` |

- With short tickets, for example `AuthConfig.ServiceTicketTTL` of 10 seconds
  and a `mon_tick_interval` of 1 second through
  [WithConfigFile](../ceph.md#withconfigfile), the highest held secret ID grows
  by one per lifetime. The daemon holds three secrets: the previous, the
  current and the next one.
- The log contains the secrets themselves. Keep it to disposable test
  clusters, and set `debug_auth` back to `1/5` when you are done.
- On Tentacle 20.2.4, a MON at `debug_auth` 10 also logs the session key and
  the service secret of every ticket that it builds (`session_key=` and
  `service_secret=` after `build_service_ticket`). Do not raise a MON's
  `debug_auth` in a run whose logs you keep or upload. Squid 19.2.5 has no
  such lines.
- On Tentacle, `ceph auth dump-keys --format json` on the MONs lists the same
  secret IDs and expirations under `rotating_secrets`. Squid has no such
  command.

To keep the first rejected authentication and the metadata around it without
any key, see the
[authentication first-failure bundle](messenger-client-fixtures.md#authentication-first-failure-bundle).

## RBD mirror setup clients

`rbdmirror.Run` starts one setup client per side. `SourceRBD` and
`DestinationRBD` run `rbd <args>` in them, with that cluster's configuration
and credentials. Use them for operations the link does not wrap, such as
promotion, demotion, resync or explicit peer inspection:

```golang
status, err := mirror.DestinationRBD(ctx,
	"mirror", "image", "status", "images/disk", "--format", "json")
if err != nil {
	t.Fatal(err)
}
if _, err := mirror.SourceRBD(ctx, "mirror", "image", "demote", "images/disk"); err != nil {
	t.Fatal(err)
}
if _, err := mirror.DestinationRBD(ctx, "mirror", "image", "promote", "images/disk"); err != nil {
	t.Fatal(err)
}
```

Arguments are passed without a shell. Writer fencing, waiting for replication
and choosing the authoritative image after a split brain remain your test's
decisions. Changing the link's mirroring mode, scope, namespace mapping or peer
through these clients can make later typed operations on the link refuse to
run, because they check the policies captured at setup.

## RGW administration

`rgw.Gateway.Admin` runs `radosgw-admin` inside a running gateway, with the
gateway's realm, zonegroup, zone, configuration and keyring. It adds
`--format json`.

```golang
gateways, err := rgw.Gateways(ctx, cluster)
if err != nil {
	t.Fatal(err)
}
gateway := gateways[0]
stats, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "photos")
```

- Flags that would change the scope or the Ceph connection (`--rgw-realm`,
  `--rgw-zonegroup`, `--rgw-zone` and their ID forms, `--conf`, `--cluster`,
  `--keyring`, `--key`, `--id`, `--name`, `--mon-host`, `-c`, `-m`, `-n`, `-k`,
  ...) and arguments with control characters are refused.
- Successful output may contain credentials. A failed command's output is
  redacted from the error, which still carries a `*ceph.NativeFailure` with
  the exit code and errno but no reason.
- The call is bounded by the cluster's startup timeout and waits for any
  running topology operation on the same cluster.
- Do not modify users that the fixture created with `CreateUser` while it is
  using them.

For multisite fixtures, use the setup clients of each zone. They pass the
zone's realm, zonegroup and zone for you:

```golang
period, err := site.SourceAdmin(ctx, "period", "get", "--format", "json")
zone, err := site.ZoneAdmin(ctx, "c", "zone", "get", "--format", "json")
```

`SourceAdmin` and `DestinationAdmin` address the two zones of
`multisite.Run`; `ZoneAdmin(ctx, name, ...)` addresses any owned zone of
`multisite.RunTopology`, including a partially added one. These do not add
`--format json`. Do not edit periods or zones while `AddZone`, `AddZonegroup`,
`RemoveZone` or `ApplySyncGroup` runs, or while a metadata master transition is
in progress.

## Diagnostics

`CollectDiagnostics` gathers a JSON-serializable, read-only snapshot:
container state, network metadata, bounded log tails and a fixed set of Ceph
queries (`version`, `status`, `health`, `pgs`, `quorum`, `monmap`, `mgrmap`,
`osdmap`, `fsmap`). It works on partially bootstrapped, stopped and
terminated clusters.

```golang
report, err := cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{
	Timeout:          30 * time.Second,
	OperationTimeout: 5 * time.Second,
	AdditionalContainers: []ceph.DiagnosticsContainer{
		{Role: "rbd-mirror", Name: "receiver", Container: rbdMirror.Daemons()[0]},
		{Role: "client", Name: "application", Container: client},
	},
	RedactValues: []string{applicationToken},
})
// Keep report even when err is not nil: it holds every observation that worked.
```

| Field | Default | Meaning |
| --- | --- | --- |
| `Timeout` | 1 minute (max 5 minutes) | Whole collection |
| `OperationTimeout` | 10 seconds (max 1 minute) | Each observation |
| `MaxOutputBytes` | 64 KiB (max 1 MiB) | Per artifact |
| `LogTail` | 1000 lines (max 10000) | Log lines per container |
| `Concurrency` | 4 (max 8) | Parallel workers |
| `MaxContainers` | 128 | Containers in one report |
| `AdditionalContainers` | none (max 64) | Caller-owned containers to include; they are only inspected |
| `RedactValues` | none (max 128) | Extra secret strings to mask |

- `Complete` is true only if every observation succeeded without truncation;
  it says nothing about cluster health.
- No arbitrary command is run, and no environment, keyring, configuration file
  or auth database is collected. Known admin and RGW secrets are masked, but
  application logs can contain other secrets; review a report before sharing
  it.
- Use a fresh context; a cancelled test context cannot collect.
- For anything outside the fixed queries, run the command yourself with
  `cluster.Ceph`.

`CollectCrashMetadata` adds the crash reports that daemons wrote into
`/var/lib/ceph/crash`: daemon, version, time, assertion and backtrace, read
through Docker's archive API also from stopped containers. Crash logs and
dumps are not collected. See [ceph.md](../ceph.md#collectcrashmetadata).

## Avoiding conflicts with fixture bookkeeping

The fixture records what it created and what it changed, and checks native
state before acting on it again. Native commands that change the same things
behind its back make later typed calls fail, or make `Restore` refuse to run.
Ceph offers no compare-and-swap for most of these changes, so some conflicts
cannot even be detected.

| Native change | Conflicts with | Prefer |
| --- | --- | --- |
| `config set`/`config rm` on a key held by an `OverrideConfig` handle | That handle's `Restore` | `OverrideConfig` |
| `osd set`/`osd unset` of a global flag | `OverrideOSDFlag` | `OverrideOSDFlag` |
| `osd set-nearfull-ratio`, `set-backfillfull-ratio`, `set-full-ratio` | `OverrideFullRatios` | `OverrideFullRatios` |
| `mgr module enable`/`disable` | `OverrideMGRModule`; disabling `volumes` or `mirroring` breaks CephFS fixtures | `OverrideMGRModule` |
| `osd blocklist add`/`rm` of an entry a handle owns | `TemporaryBlocklist` | `TemporaryBlocklist` |
| Deleting, renaming or changing owned pools, their CRUSH rules or EC profiles | `CreatePool` descriptors, `Pool.Remove`, `Pool.Set*`, which check the native pool ID | `Pool.SetReplication`, `Pool.SetQuota`, `Pool.SetPGCount`, `Pool.SetPlacement`, `Pool.Remove` |
| `osd out`/`in`/`destroy`/`purge`, CRUSH edits for owned OSDs | `OSDContainer.SetIn`, `OSDContainer.Remove`, which check the OSD's UUID | `OSDContainer.SetIn`, `OSDContainer.Remove` |
| `auth` changes to identities created by `CreateClient`, or to daemon identities | `ClientConfig.UpdateCaps`, `ClientConfig.Delete`, daemon restarts | `ClientConfig.UpdateCaps`, `ClientConfig.Delete` |
| `mon add`/`mon remove`, editing `/etc/ceph/ceph.conf` in any container | MON membership and the `mon_host` bookkeeping | `AddMonitor`, `MonitorContainer.Remove`, `RefreshMonitorConfig` |
| `fs set max_mds`, `fs fail`, `fs rm` | `ScaleMDS`, `WaitReady`, `cephfs.Remove` | `ScaleMDS`, `cephfs.Remove` |
| `fs set session_timeout`/`session_autoclose` | `OverrideSessionTimeouts` | `OverrideSessionTimeouts` |
| `fs subvolume`/`subvolumegroup` changes to owned subvolumes, snapshots or clones | Subvolume helpers, which check native identity | The typed subvolume helpers |
| `radosgw-admin` changes to fixture-created users, accounts or placement | `UserInfo`, `RemoveUser`, `ApplyPlacement`, which verify keys and identity | The typed RGW helpers |
| `radosgw-admin period`/`zone`/`zonegroup` edits in a multisite fixture | `AddZone`, `RemoveZone`, `ApplySyncGroup` | Typed topology calls, one at a time |
| `rbd mirror pool enable`/`peer` changes on a mirrored pool | `rbdmirror.Mirror` operations, which compare captured policies | `rbdmirror.Mirror` methods, `Rebootstrap` |
| Messenger mode and bind options with `MessengerV2Secure` | The fixed bootstrap policy; `OverrideConfig` refuses them | Choose `WithMessengerMode` at `Run` |
| `docker stop`, `docker pause`, `docker network disconnect` on owned containers | Pause and interruption tracking, `Terminate` order | `Stop`/`Start`, `fault.Pause`, `InterruptNetwork` |

Resources you create natively (pools, images, users, buckets, CRUSH rules)
are not owned by the fixture. Typed removal helpers refuse them; they disappear
when the cluster is terminated. Prefer the `Temporary*` helpers for any change
you want to undo within a test, because they record the exact original state
and refuse to overwrite outside edits.

## Recipes

| Task | Command |
| --- | --- |
| Enable compression on a pool | `cluster.Ceph(ctx, "osd", "pool", "set", "volumes", "compression_mode", "aggressive")` |
| Find an object's PG and primary | `cluster.Ceph(ctx, "osd", "map", "volumes", "object-01", "--format", "json")` |
| Query one PG in detail | `cluster.Ceph(ctx, "pg", "1.0", "query")` |
| Mute a health warning for ten minutes | `cluster.Ceph(ctx, "health", "mute", "POOL_NO_REDUNDANCY", "10m")` |
| Keep one OSD from being marked out | `cluster.Ceph(ctx, "osd", "add-noout", "osd.1")`, undo with `osd rm-noout osd.1` |
| Read a daemon's effective option | `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_op_complaint_time")` |
| Create an RBD image | `exec(ctx, control, "rbd", "create", "--size", "64M", "rbd/disk")` |
| Write an object | `exec(ctx, control, "rados", "-p", "volumes", "put", "greeting", "/etc/hostname")` |
| Show a filesystem's state | `cluster.Ceph(ctx, "fs", "status", "tc-cephfs", "--format", "json")` |
| List an MDS rank's sessions | `cluster.Ceph(ctx, "tell", "mds.tc-cephfs:0", "session", "ls", "--format", "json")` |
| Inspect a mirrored image on the destination | `mirror.DestinationRBD(ctx, "mirror", "image", "status", "images/disk", "--format", "json")` |
| Read bucket statistics | `gateway.Admin(ctx, "bucket", "stats", "--bucket", "photos")` |
| Read a multisite zone's period | `site.ZoneAdmin(ctx, "b", "period", "get", "--format", "json")` |

## Related APIs

- [ceph.md](../ceph.md): `Ceph`, `RunNativeCommand`, `RedactSecrets`,
  `ControlContainer`, `ControlImage`, `Configuration`, `OverrideConfig`,
  `WithConfigFile`, `WithClient`, `WithClientIdentity`, `CreateClient`,
  `ConnectionConfig`, `CollectDiagnostics`.
- [rbd-mirror.md](../rbd-mirror.md): `Mirror.SourceRBD`, `Mirror.DestinationRBD`,
  `Daemon.Status`.
- [cephfs.md](../cephfs.md): `Filesystem.Sessions`.
- [cephfs-mirror.md](../cephfs-mirror.md): `Mirror.DirectoryStatus`.
- [rgw.md](../rgw.md): `Gateway.Admin`, `Multisite.SourceAdmin`,
  `DestinationAdmin`, `ZoneAdmin`.
- [Fault injection](fault-injection.md): messenger options and other faults set
  through `OverrideConfig`.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): `Restore`
  semantics and partial failures.
