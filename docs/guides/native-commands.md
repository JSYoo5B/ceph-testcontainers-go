# Running native commands

Anything the typed API does not cover can be done with Ceph's own tools,
which this module runs inside containers. The Go module never links librados
or uses cgo.

| You want to run | Use | Runs in |
| --- | --- | --- |
| The `ceph` CLI as `client.admin` | `cluster.Ceph(ctx, args...)` | The control container |
| `rados`, `rbd`, Python bindings | `Exec` on `cluster.ClientContainer(ctx)` | The client toolkit (client role) |
| `ceph-authtool` | `Exec` on `cluster.ControlContainer(ctx)` | The control container (essential image) |
| A command with bounded run time and output that must not outlive the call | `cluster.RunNativeCommand(ctx, target, command)` | The control container, a daemon container, or your attached container |
| A workload, a restricted identity or your own client software | A container attached with `WithClient` or `ClientConfig.WithIdentity` | Your container |
| A native client in the test process | `cluster.ConnectionConfig(ctx)` with host networking | Your process |
| `radosgw-admin` | `rgw.Gateway.Admin`, or `multisite.Fixture.ZoneAdmin` | The gateway or the zone's setup client |
| `rbd` on either side of a mirror link | `rbdmirror.Mirror.SourceRBD`, `DestinationRBD` | The link's setup clients |
| A daemon's admin socket command | `AdminSocket`, `ceph tell`, or `ceph --admin-daemon` | Control container or daemon container |
| A read-only snapshot for failure analysis | `cluster.CollectDiagnostics(ctx, config)` | Docker API and the control container |

The API contracts are in [`ceph`](../../ceph/README.md),
[RunNativeCommand](../../ceph/README.md#runnativecommand),
[ControlContainer](../../ceph/README.md#controlcontainer) and
[ClientContainer](../../ceph/README.md#clientcontainer). This guide adds the
details and traps that matter when you use them.

## The ceph CLI

`Ceph` runs `ceph --connect-timeout 5 <args>` in the control container as
`client.admin` and returns standard output. Arguments are passed without a
shell; add `--format json` for anything you parse.

```golang
out, err := cluster.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
if err != nil {
	t.Fatal(err)
}
var pools []struct {
	PoolName string `json:"pool_name"`
	Size     int    `json:"size"`
}
err = json.Unmarshal(out, &pools)
```

- A non-zero exit returns the standard output with a `*ceph.NativeFailure`
  (exit code, errno, Ceph's reason with keys redacted). Standard output, which
  holds the keys of commands such as `auth get`, never appears in the error.
  See [Classified errors](errors.md#native-failures).
- `--connect-timeout 5` bounds only the connection to a MON. When it expires
  the CLI exits before sending the command, and `Ceph` connects again until a
  MON answers, `ctx` ends or a minute has passed. A stalled or electing MON
  therefore delays the command instead of failing it, and the command runs
  once.
- `Ceph` does not take the cluster's topology gate, so it can run while
  another goroutine adds or removes daemons. See
  [Contexts, ownership and cleanup](contexts-and-cleanup.md).

## Other CLIs

The client container has `rados`, `rbd` and the Python `rados`, `rbd` and
`cephfs` bindings, with `/etc/ceph/ceph.conf` and the admin keyring. The
control container is built from the essential image and has `ceph` and
`ceph-authtool`, not `rados` or `rbd`. The examples below use this helper:

```golang
// exec runs argv in ctr and returns standard output; standard error is in the error.
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

client, err := cluster.ClientContainer(ctx)
if err != nil {
	t.Fatal(err)
}
if _, err := exec(ctx, client, "rbd", "create", "--size", "64M", "rbd/disk"); err != nil {
	t.Fatal(err)
}
info, err := exec(ctx, client, "rbd", "info", "rbd/disk", "--format", "json")
```

`stdcopy` is `github.com/moby/moby/api/pkg/stdcopy`.

- The cluster owns the control and client containers. Do not stop, pause or
  reconfigure them, and do not edit files under `/etc/ceph` in them.
- In a single-MON cluster the control container is the MON. Run long or heavy
  workloads in your own client container so they do not compete with it.
- Cancelling `ctx` stops waiting for an `Exec` but does not stop the process
  in the container, and Docker buffers all of its output. Use
  [RunNativeCommand](#bounded-commands-with-confirmed-cleanup) when that
  matters, or native timeouts such as `--rados-osd-op-timeout 10` and
  `--rados-mon-op-timeout 10`.
- For pipes or redirections, run `sh -c` explicitly.

## Reading output

Decode only the fields you need and tolerate unknown ones: Ceph adds fields
between releases (see [Ceph releases and images](releases-and-images.md)).
These commands do not give usable JSON:

| Command | Output | Typed alternative |
| --- | --- | --- |
| `rados listwatchers` | Always text: `watcher=IP:port/nonce client.<id> cookie=<n>` | `Observer.ObjectWatchers` |
| `radosgw-admin bucket sync status` on Squid | Text; `--format json` is ignored | `multisite.Fixture.BucketSyncStatus`, `WaitBucketSyncReady` |
| `radosgw-admin sync status` | The comparison with remote logs appears only in the text output | `multisite.Fixture.SyncStatus`, `WaitSyncReady` |
| `rbd mirror pool info` on Squid | JSON without `mirror_uuid` and `remote_namespace` | `rbdmirror.Mirror.PolicyStatus` |

## Bounded commands with confirmed cleanup

`RunNativeCommand` runs one command under the supervisor the module uses for
its own native queries. Use it for oracle and probe commands whose run time
and output must be bounded and whose process must not outlive a canceled
call. The fields and error categories are listed in
[RunNativeCommand](../../ceph/README.md#runnativecommand).

```golang
result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
	Argv:      []string{"rados", "-p", "volumes", "stat", "greeting"},
	Timeout:   30 * time.Second,
	MaxStdout: 64 << 10,
	MaxStderr: 64 << 10,
	Label:     "stat greeting",
})
if errors.Is(err, ceph.ErrNativeCommandIncomplete) {
	t.Fatalf("incomplete: timed out %t, stdout truncated %t", result.TimedOut, result.StdoutTruncated)
}
if err != nil || result.ExitCode != 0 {
	t.Fatalf("rados stat: exit %d, %v", result.ExitCode, err)
}
```

- The target is the control container, a MON, MGR, OSD, RGW or MDS container
  of this cluster, or a container of yours on the cluster's network (the host
  network for a host-network cluster). Other containers are refused. The
  target must be running, not paused, and have `python3`; the cephfs role
  image does not promise it.
- Output limits apply inside the container, before Docker buffers anything.
  A truncated object, map or JSON document is not evidence for an oracle:
  check `result.Complete()` or `ErrNativeCommandIncomplete`.
- A nonzero exit is not an error. Errors name the command by its label and
  never contain its arguments or output.
- Before logging output, mask keys and your own secrets with
  `cluster.RedactSecrets(ctx, result.Stderr, token)`. Assert on the raw bytes;
  redaction changes the data.

### Cancellation and cleanup

- At its deadline the supervisor kills the command's process group and, on
  Linux, reaps the descendants for up to 200 ms, so no zombie is left even
  where the first process does not reap orphans.
- A descendant that starts its own session (for example with `setsid`)
  leaves the group and is not stopped. So is a descendant that switches to
  another user in a container whose user lacks `CAP_KILL`. The cluster's
  containers and client containers run as root with Docker's default
  capabilities.
- When `ctx` ends first, the call stops the command inside the container
  before returning, within `ceph.NativeCommandStopTimeout` (15 seconds)
  independent of `ctx`. A start that Docker delivers after the stop runs
  nothing.
- When the stop cannot be confirmed (container paused or stopped, Docker not
  answering, supervisor killed, for example by the OOM killer), the error is a
  `*ceph.NativeCommandUnstoppedError` and the command may still run. Keep it
  and call `Stop` with a fresh context, for example after resuming the
  container:

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

- `Stop` also confirms a command of an earlier instance of a restarted
  container without signalling the new instance. A stopped container confirms
  nothing until it runs again, and after removal `Stop` returns nil.
- A container instance is identified by the start time of the first process
  in its PID namespace. A container that shares another PID namespace (for
  example `--pid=host`) keeps that identity across a restart, so a leftover
  command there is confirmed only after the container is removed.
- `Terminate` cancels running commands, waits for their calls, stops the ones
  they could not confirm and reports any it cannot stop. Afterwards
  `RunNativeCommand` fails.
- Stopping the process does not undo what it did. A MON command it already
  sent may have applied, and the call never runs a command again. Read back
  the outcome of a raw command yourself.

### Standard input

A worker that starts before its data exists can read the data from standard
input. Pass a `ceph.NewNativeInput()` with `MaxInput`, run the call in a
goroutine, and write once the worker is ready:

```golang
input := ceph.NewNativeInput()
runCtx, cancel := context.WithCancel(ctx)
done := make(chan error, 1)
t.Cleanup(func() {
	cancel()
	_ = input.Close()
	<-done // Join the call even when an assertion below fails.
})
go func() {
	result, err := cluster.RunNativeCommand(runCtx, client, ceph.NativeCommand{
		Argv:      []string{"python3", "-c", worker},
		Timeout:   2 * time.Minute,
		MaxStdout: 4096,
		MaxStderr: 64 << 10,
		Label:     "comparison worker",
		Input:     input,
		MaxInput:  1 << 20,
	})
	if err == nil && (!result.InputEOF || result.InputDelivered != result.InputWritten || result.ExitCode != 0) {
		err = fmt.Errorf("worker exited %d after %d of %d input bytes", result.ExitCode, result.InputDelivered, result.InputWritten)
	}
	done <- err
}()
// Wait for the worker's ready receipt, then:
if err := input.Write(ctx, record); err != nil {
	t.Fatal(err)
}
_ = input.Close()
```

Handle a `*ceph.NativeCommandUnstoppedError` from the goroutine as in
[Cancellation and cleanup](#cancellation-and-cleanup).

- An input serves one command. `MaxInput` bounds all writes together; a write
  that would pass it is refused whole with `ceph.ErrInvalidRequest` and the
  input stays open. A slow reader does not hold up `Write`, because the
  supervisor buffers up to `MaxInput` bytes inside the container.
- `Write` returns once the stream to the container took the data. If its
  `ctx` ends after that, it returns the context error and the command may
  still get the data, so do not repeat such a write blindly.
- Once the command ended, closed its standard input, or was canceled or
  stopped, writes fail with `ceph.ErrNativeInputClosed`.
- `Complete()` does not check input. Equal `InputWritten` and
  `InputDelivered` with `InputEOF` show delivery into a pipe, not that the
  worker applied the records, so check the worker's own receipt as well.
- Input never appears in arguments, the environment, errors or diagnostics,
  but output is returned raw: a worker that handles secrets must not echo
  them.
- The target must be a container of the cluster's Docker engine, since the
  input uses the Docker attach API.

`TestNativeCommandInput` and `TestNativeCommandInputCancellation` show the
full pattern.

### Oracle recipe: stat2, read and ceph-dencoder

Run oracle commands in a `WithClient` container so the test process needs
neither Ceph libraries nor cgo. For values that the CLIs and the Python
binding do not expose, such as the nanosecond modification time of
`rados_stat2`, call `librados.so.2` through Python `ctypes` and print one JSON
document. `rados_create` with `None` connects as `client.admin`; pass the
`User()` of a `ClientConfig.WithIdentity` identity instead. Call `rados_shutdown` in
`finally`: a client left to interpreter exit sometimes aborts after its work
is done and turns the result into a nonzero exit.

`ceph-dencoder`, part of the official images, decodes a native structure from
a file:

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

### Oracle recipe: self-managed snapshot write contexts

The Python binding produces self-managed snapshot write contexts:
`create_self_managed_snap`, `set_self_managed_snap_write([snap])` (it sorts the
IDs and uses the newest as sequence, so it cannot send a malformed context)
and `remove_self_managed_snap`. Run the writer and an independent reader as
two librados clients in one bounded command, on a pool created with
`CreatePool`. `TestRADOSSelfManagedSnapshots` in `internal/integration` is the
complete example.

Ceph 20.2.4 and 19.2.5 handle the contexts alike:

| Write context | Native result |
| --- | --- |
| Names an existing snapshot newer than the object's sequence | The OSD first preserves an existing object as a clone, which keeps the context's snapshots that are newer than the object's sequence and still exist |
| Sequence older than the object's | Accepted without a clone. With `LIBRADOS_OPERATION_ORDERSNAP` (512) it is refused with `EOLDSNAPC` (85) |
| Names a removed snapshot | Accepted; the clone keeps only existing snapshots. Removed snapshots alone make no clone |
| An ID the MONs never allocated | Accepted as clone ID and sequence. A snapshot allocated later with a lower ID is then not preserved on that object, and the MONs refuse to remove the ID (`ENOENT`), so the clone stays until the pool is removed |

Traps:

- `remove_self_managed_snap` returns before the OSDs trim the clone, and a
  read at the snapshot may still return its data. Poll `rados listsnaps` and
  a read with a deadline.
- Versions increase but are not consecutive; a write that makes a clone skips
  one. `get_last_version` reports the last request on that ioctx, not the
  object's version, and a read at a snapshot reports HEAD's version before
  the preserving write.
- Removing HEAD under a context leaves the snapshots readable.
- MemStore saves data only on a clean stop, so use the default BlueStore OSD
  to check durability across `OSDContainer.Stop` and `Start`.
- A timed-out or canceled command is killed before its `finally` runs.
  `Pool.Remove` deletes the owned pool with every clone.

### Oracle recipe: write outcomes

A writer cannot tell from its own result whether a write happened. Decide it
with a second client that reads the object back, and keep three cases apart:

- **Completion.** librados completes a write when the OSD committed it. On a
  file-backed BlueStore OSD, writes whose completion returned 0 survive a
  `SIGKILL` of the OSD. Message receipt is not the commit.
- **Local timeout.** `rados_osd_op_timeout` ends the call with `ETIMEDOUT`
  (`-110`) but does not cancel the write: an OSD still holding it runs it
  later. See [Delaying operations inside an OSD](fault-injection.md#delaying-operations-inside-an-osd).
- **Killed client.** An OSD discards a write it is about to start if it has
  already seen the client's connection close (`osd_discard_disconnected_ops`).
  Killing the client does not guarantee that order, so confirm from the OSD's
  operation record (no `started` event) and an independent read.

Do not retry an uncertain write or append blindly; if the first attempt ran,
the retry applies twice. Guard it with the version the independent client read
before the first attempt:

```python
with rados.WriteOpCtx() as op:
    op.assert_version(version)  # read before the first attempt
    op.append(b"record;")
    ioctx.operate_write_op(op, "log")
```

The guarded retry fails with `ERANGE` (34) when the object changed since that
read and with `EOVERFLOW` (75) when the version is newer than the object's.
Object versions come from the PG log, so versions of objects in one PG
interleave, and a refused write also consumes a version.
`TestRADOSMutationOutcomes` in `internal/integration` shows each case.

### Oracle recipe: erasure-coded pools

Which writes an erasure-coded pool accepts depends on
`ceph.ErasureCodeConfig.AllowOverwrites`. In the Python binding,
`ioctx.alignment()` returns the required alignment without overwrites and
`None` with them. On k=2, m=1 pools from `CreatePool`, Ceph 20.2.4 and 19.2.5
return:

| Operation | Without overwrites | With overwrites |
| --- | --- | --- |
| `write_full` | Accepted, also of an unaligned size | Accepted |
| `append`, or `write` at the end | Accepted only while the end is a multiple of the alignment (8192); otherwise `EOPNOTSUPP` (95) | Accepted |
| `write` over existing bytes, `trunc`, `zero` | `EOPNOTSUPP` | Accepted |
| omap writes | `EOPNOTSUPP` | `EOPNOTSUPP` |
| xattrs | Accepted | Accepted |
| A write under a self-managed snapshot context | `write_full` preserves a clone first | A partial write preserves a clone first |

Assert the refusals instead of skipping them. A pool with `MinSize` equal to K
stays active (`active+undersized+degraded`, `2147483647` in the missing
`Acting` slot) and serves reads while one shard's OSD is stopped. Hold `noout`
with `override.SetOSDFlag` meanwhile. `TestRADOSErasureCodedOutcomes` in
`internal/integration` runs these cases.

## Changing configuration

| Need | Use |
| --- | --- |
| Inspect stored central configuration | `Observer.Configuration(ctx)` |
| Change any option for a test and restore it | [override.SetConfig](../../override/README.md#setconfig) |
| Options that must exist at bootstrap | [WithConfigFile](../../ceph/README.md#withconfigfile) |
| A daemon's effective value | `cluster.Ceph(ctx, "config", "show", "osd.0", name)` |

- `override.SetConfig` accepts any option, with a `Section` and optional
  `Mask` (such as `class:ssd`), including MGR module options such as
  `mgr/volumes/max_concurrent_clones`.
- Local files, command-line arguments and runtime overrides take precedence
  over the central database, and some options apply only after a restart.
  Check the effective value with `config show`.
- A key set with `WithConfigFile` cannot be changed later through the central
  database.
- `ceph tell <daemon> config set` changes one running daemon only, is lost on
  restart, and no handle restores it.

## Running your own client container

`WithClient` attaches your container to the cluster network with `ceph.conf`
and the admin keyring. `ClientConfig.WithIdentity` copies only a restricted
identity's keyring, at `client.KeyringPath()`. See
[WithClient](../../ceph/README.md#withclient),
[ClientConfig.WithIdentity](../../ceph/README.md#clientconfigwithidentity) and
[CreateClient](../../ceph/README.md#createclient).

```golang
reader, err := cluster.CreateClient(ctx, "reader", ceph.ClientCaps{
	Mon: "allow r",
	OSD: "allow r pool=volumes",
})
if err != nil {
	t.Fatal(err)
}
facts, err := ceph.FactsOf(cluster)
if err != nil {
	t.Fatal(err)
}
restricted, err := testcontainers.Run(ctx, facts.Images.Client,
	reader.WithIdentity(),
	ceph.WithIdleEntrypoint(),
)
testcontainers.CleanupContainer(t, restricted)
if err != nil {
	t.Fatal(err)
}
_, err = exec(ctx, restricted, "rados", "--id", reader.User(), "-p", "volumes", "ls")
```

- The client must select its identity: `reader.User()` (`reader`) for
  `--id`, librados and go-ceph; `reader.Name()` is `client.reader`.
- Established sessions may keep their tickets after
  `ClientConfig.UpdateCaps` or `Delete`, so check permission changes with a
  fresh connection.
- The essential image is not a client image; use `FactsOf(cluster).Images.Client`
  or an image with your own client libraries. Neither option installs software.
- You own these containers: terminate them before the cluster. After a MON
  replacement, `cluster.RefreshClientMonitorConfig(ctx, client)` updates their
  `mon_host`.

For a native client in the test process, `cluster.ConnectionConfig(ctx)`
returns `ceph.conf` and the admin keyring. Daemons must be reachable from the
process, which normally requires `ceph.WithHostNetwork()`; see
[Host network and native clients](host-network.md).

## Daemon admin sockets

For MON, MGR and OSD daemons, use the `AdminSocket` method of
`MonitorContainer`, `ManagerContainer` and `OSDContainer` (see
[AdminSocket](../../ceph/README.md#adminsocket)). It reaches the socket of any
owned daemon whatever its image, without the Messenger or Cephx:

```golang
status, err := osd.AdminSocket(ctx, "status") // osd is the *ceph.OSDContainer of osd.0
```

`ceph tell` from the control container works for MON, MGR, OSD and MDS
daemons, but travels over the Messenger like any client request:

```golang
sessions, err := cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":0",
	"session", "ls", "--format", "json")
```

`ceph --admin-daemon <socket>` inside a daemon's own container needs the
`ceph` CLI there. OSD, MDS and RGW role images are not required to have it
(see [Role images](releases-and-images.md#role-images)).

| Daemon | Socket inside its container | Handle |
| --- | --- | --- |
| MON | `/var/run/ceph/ceph-mon.<name>.asok` | `cluster.Monitors(ctx)[i]`, `DaemonName` |
| MGR | `/var/run/ceph/ceph-mgr.<name>.asok` | `cluster.Managers(ctx)[i]`, `DaemonName` |
| OSD | `/var/run/ceph/ceph-osd.<id>.asok` | `cluster.OSDs(ctx)[i]`, `ID` |
| MDS | `/var/run/ceph/ceph-mds.<id>.asok` | `fs.MDSs()[i]`, `ID` |
| `rbd-mirror` | `/tmp/rbd-mirror.asok` | `rbdMirror.Daemons()[i]` |
| `cephfs-mirror` | `/var/run/ceph/cephfs-mirror.asok` | `cephfsMirror.Daemons()[i]` |

The mirror daemon images contain the `ceph` CLI:

```golang
out, err := exec(ctx, rbdMirror.Daemons()[0], "ceph", "--admin-daemon",
	"/tmp/rbd-mirror.asok", "rbd", "mirror", "status")
// The argument is <filesystem>@<fscid>; fscid is the "id" of "ceph fs get <filesystem>".
out, err = exec(ctx, cephfsMirror.Daemons()[0], "ceph", "--admin-daemon",
	"/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", "tc-cephfs@1")
```

Prefer the typed observers when they cover your case:
`rbdmirror.Daemon.Status`, `rbdmirror.Mirror.ReceiverStatus`, `ImageStatus`,
`cephfsmirror.Mirror.DirectoryStatus`.

### Observing Cephx keys from the daemon side

No admin socket command prints a daemon's own key or its rotating service
secrets.

- **The daemon's key.** After [RotateKey](../../ceph/README.md#rotatekey) of
  `OSDContainer` or `ManagerContainer`, `auth get-key` on the MONs returns the
  new key, and a restart of the daemon proves it authenticates with its
  keyring files.
- **Service secrets.** The MONs rotate each service's secret every
  `auth_service_ticket_ttl`. Raise the daemon's `debug_auth` and read its
  container logs: it logs the secrets it holds at every MON client tick, and
  the secret ID that decrypted each client ticket it accepts.

```golang
_, err := osd.AdminSocket(ctx, "config", "set", "debug_auth", "10/10")
// ... let a client connect to osd.0 ...
logs, err := osd.Logs(ctx)
```

| Event | Tentacle 20.2.4 | Squid 19.2.5 |
| --- | --- | --- |
| Secrets held, one line each | `RKR(...).dump_rotating  id 3 <secret> expires <time>` | `auth:  id 3 <secret> expires <time>` after `auth: dump_rotating:` |
| A client ticket accepted | `cephx: cephx_verify_authorizer: decoded service osd secret_id=3`, then `global_id=<client>` | `cephx: verify_authorizer decrypted service osd secret_id=3` |

- A daemon holds three secrets: previous, current and next. With a short
  `AuthConfig.ServiceTicketTTL` (for example 10 seconds) and a
  `mon_tick_interval` of 1 second through `WithConfigFile`, the highest ID
  grows by one per lifetime.
- The log contains the secrets. Use it only on disposable clusters and set
  `debug_auth` back to `1/5` afterwards.
- On Tentacle 20.2.4, a MON at `debug_auth` 10 also logs the session key and
  service secret of every ticket it builds. Do not raise a MON's `debug_auth`
  in a run whose logs you keep or upload.
- On Tentacle, `ceph auth dump-keys --format json` lists the same secret IDs
  and expirations under `rotating_secrets`. Squid has no such command.

To keep the first rejected authentication without any key, see the
[authentication first-failure bundle](messenger-client-fixtures.md#authentication-first-failure-bundle).

## RBD mirror setup clients

`SourceRBD` and `DestinationRBD` run `rbd <args>`, without a shell, in the
link's setup client for that side. Use them for operations the link does not
wrap, such as promotion, demotion or resync:

```golang
_, err := mirror.DestinationRBD(ctx, "mirror", "image", "promote", "images/disk")
```

Writer fencing, waiting for replication and split-brain resolution stay your
test's decisions. Changing the link's mode, scope, namespace mapping or peer
through these clients makes later typed operations refuse to run, because
they check the policies captured at setup.

Commands that RBD and RBD mirroring tests commonly need:

```sh
# images, snapshots, locks and watchers (in a client container)
rbd create --size 64M --image-feature layering,exclusive-lock volumes/disk
rbd snap create volumes/disk@before
rbd status volumes/disk --format json          # watchers
rbd lock ls volumes/disk --format json
rbd namespace list --pool volumes --format json
rbd trash list --pool volumes --namespace tenant-a --format json

# mirroring (through SourceRBD/DestinationRBD, without the leading "rbd")
rbd mirror pool info volumes --format json
rbd mirror pool status volumes --verbose --format json
rbd mirror image status volumes/disk --format json
rbd mirror image snapshot volumes/disk
rbd mirror image demote volumes/disk
rbd mirror image promote volumes/disk            # add --force only to simulate split-brain
rbd mirror image resync volumes/disk
rbd mirror pool peer remove volumes <peer-uuid>
rbd mirror snapshot schedule add --pool volumes 1m

# inside an rbdmirror.Daemon container
ceph --admin-daemon /tmp/rbd-mirror.asok rbd mirror status
```

`rbd-mirror` reads its options from the destination cluster's central
configuration (`client` section) when it starts; set them with
`override.SetConfig` on the destination before `AddDaemon`. Its logs go to
standard error (`daemon.Logs(ctx)`).

## CephFS state and settings

Anything without a typed CephFS API is a `cluster.Ceph` call (no shell; add
`--format json` for machine-readable output):

```golang
out, err := cluster.Ceph(ctx, "fs", "status", fs.FilesystemName, "--format", "json")
out, err = cluster.Ceph(ctx, "fs", "dump", "--format", "json")
_, err = cluster.Ceph(ctx, "fs", "set", fs.FilesystemName, "max_file_size", "1073741824")
out, err = cluster.Ceph(ctx, "fs", "subvolume", "ls", fs.FilesystemName, "--group_name", "tenants")
out, err = cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":0", "session", "ls", "--format", "json")
out, err = cluster.Ceph(ctx, "tell", "mds."+fs.FilesystemName+":0", "dump_ops_in_flight")
_, err = cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(gid, 10)) // GID from MDSStatus
```

- Each MDS has its admin socket at `/var/run/ceph/ceph-mds.<ID>.asok`. It runs
  with `--mds-cache-memory-limit 134217728` (128 MiB) on its command line, so
  central configuration cannot change that option.
- CephFS mirror policy and assignment are `fs snapshot mirror ls`, `dirmap`,
  `peer_list`, `daemon status` and `show distribution` on the source; each
  mirror daemon's admin socket (`/var/run/ceph/cephfs-mirror.asok`) takes
  `fs mirror status <fs>@<fscid>` and `fs mirror peer status <fs>@<fscid> <peer>`.
- Retained snapshots (`fs subvolume rm --retain-snapshots`) and custom metadata
  have no typed API. On Ceph 20.2.4 a `snapshot-retained` subvolume has no live
  path and refuses custom metadata calls; snapshots and clones do not copy a
  subvolume's custom metadata, and Ceph lower-cases metadata keys. Recreating a
  retained name makes a new subvolume with a new UUID (`EAGAIN` while Ceph still
  purges is safe to retry); typed handles do not adopt either state.

Raw commands on resources that typed handles own are outside the handles'
contract: fence other writers and do not run fixture operations on the same
resources at the same time.

## RGW administration

`rgw.Gateway.Admin` runs `radosgw-admin --format json` inside a running
gateway, with its realm, zonegroup, zone and keyring:

```golang
gateways, err := rgw.Gateways(ctx, cluster)
if err != nil {
	t.Fatal(err)
}
stats, err := gateways[0].Admin(ctx, "bucket", "stats", "--bucket", "photos")
```

- Flags that change the scope or connection (`--rgw-realm`, `--rgw-zone`,
  `--conf`, `--keyring`, `--id`, `-c`, `-m`, ...) and arguments with control
  characters are refused.
- Successful output may contain credentials. A failure's output is redacted
  from the error, which carries a `*ceph.NativeFailure` without a reason.
- Do not modify users that the fixture created with `CreateUser` while it
  uses them.

Commands that RGW tests commonly need:

```golang
stats, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "reports")
_, err = gateway.Admin(ctx, "lc", "process", "--bucket", "logs")
_, err = gateway.Admin(ctx, "reshard", "add", "--bucket", "big", "--num-shards", "17")
_, err = gateway.Admin(ctx, "quota", "set", "--quota-scope", "bucket", "--bucket", "big", "--max-objects", "100")
_, err = gateway.Admin(ctx, "caps", "add", "--uid", user.ID(), "--caps", "usage=write")
_, err = gateway.Admin(ctx, "role", "create", "--role-name", "reader", "--assume-role-policy-doc", trust)
```

- To see a failure's native text, run the same `radosgw-admin` command with
  `gateway.Exec`; the container has the cluster configuration and admin keyring
  at the default paths. The control container also has `radosgw-admin`, which
  works for the default zone of a standalone cluster without a running gateway.
- Admin Ops REST requests are SigV4-signed HTTP requests to the gateway
  endpoint, signed with a dedicated user created with `UserConfig.AdminCaps`
  (such as `"users=read;usage=read;buckets=read"`). The fixture never grants the
  global `admin` flag.
- Server-side RGW options go to `override.SetConfig` with section
  `client.admin` before `rgw.Start`, or to `[client]`/`[client.admin]` of a
  `ceph.WithConfigFile` file; many take effect only when the gateway starts, so
  restart it and resolve `S3Endpoint` again. The Beast frontend, a thread pool of
  4, a 1-second exit timeout and, for multisite gateways, a 30-second sync lease
  are on the daemon command line and cannot be changed through configuration.
- On Ceph 19 (Squid), `role get` for a missing role exits with status 2 like on
  Tentacle but also prints one ENOENT log line.

In multisite fixtures, `ZoneAdmin(ctx, name, ...)` addresses any owned zone by
name, including those of a `multisite.WithTopology` fixture. It passes the
zone's scope but does not add `--format json`. Do not edit periods or zones while `AddZone`,
`AddZonegroup`, `RemoveZone`, `SyncGroup.Apply` or a metadata master transition
runs.

## Diagnostics

[CollectDiagnostics](../../ceph/README.md#collectdiagnostics) gathers a
bounded, read-only, JSON-serializable snapshot with a fixed set of Ceph
queries, also from partially bootstrapped or terminated clusters, and
[CollectCrashMetadata](../../ceph/README.md#collectcrashmetadata) adds the
daemons' crash reports.

- Call it with a fresh context; a canceled test context cannot collect.
- Keep the report even when the error is not nil: it holds every
  observation that worked.
- `Complete` means every observation succeeded untruncated, not that Ceph is
  healthy.
- It runs no arbitrary command. For other queries, run them with
  `cluster.Ceph`. Review application logs in a report before sharing it:
  only known secrets are masked.

## Avoiding conflicts with fixture bookkeeping

The fixture records what it created and changed, and checks native state
before acting on it again. Native commands that change the same things make
later typed calls fail or `Restore` refuse to run, and because Ceph offers no
compare-and-swap for most of these changes, some conflicts go undetected.

| Native change | Conflicts with | Prefer |
| --- | --- | --- |
| `config set`/`config rm` on a key held by an `override.SetConfig` handle | That handle's `Restore` | `override.SetConfig` |
| `osd set`/`osd unset` of a global flag | `override.SetOSDFlag` | `override.SetOSDFlag` |
| `osd set-nearfull-ratio`, `set-backfillfull-ratio`, `set-full-ratio` | `override.SetFullRatios` | `override.SetFullRatios` |
| `mgr module enable`/`disable` | `override.SetMGRModule`; disabling `volumes` or `mirroring` breaks CephFS fixtures | `override.SetMGRModule` |
| `osd blocklist add`/`rm` of an entry a handle owns | `override.AddBlocklist` | `override.AddBlocklist` |
| Deleting, renaming or changing owned pools, their CRUSH rules or EC profiles | `CreatePool` descriptors, `Pool.Remove`, `Pool.Set*`, which check the native pool ID | `Pool.SetReplication`, `Pool.SetQuota`, `Pool.SetPGCount`, `Pool.SetPlacement`, `Pool.Remove` |
| `osd out`/`in`/`destroy`/`purge`, CRUSH edits for owned OSDs | `OSDContainer.SetIn`, `OSDContainer.Remove`, which check the OSD's UUID | `OSDContainer.SetIn`, `OSDContainer.Remove` |
| `auth` changes to identities created by `CreateClient`, or to daemon identities | `ClientConfig.UpdateCaps`, `ClientConfig.Delete`, daemon restarts | `ClientConfig.UpdateCaps`, `ClientConfig.Delete` |
| `mon add`/`mon remove`, editing `/etc/ceph/ceph.conf` in any container | MON membership and the `mon_host` bookkeeping | `AddMonitor`, `MonitorContainer.Remove`, `RefreshMonitorConfig` |
| `fs set max_mds`, `fs fail`, `fs rm` | `ScaleMDS`, `WaitReady`, `cephfs.Remove` | `ScaleMDS`, `cephfs.Remove` |
| `fs set session_timeout`/`session_autoclose` | `OverrideSessionTimeouts` | `OverrideSessionTimeouts` |
| `fs subvolume`/`subvolumegroup` changes to owned subvolumes, snapshots or clones | Subvolume helpers, which check native identity | The typed subvolume helpers |
| `radosgw-admin` changes to fixture-created users, accounts or placement | `User.Info`, `User.Remove`, `Gateway.ApplyPlacement`, which verify keys and identity | The typed RGW helpers |
| `radosgw-admin period`/`zone`/`zonegroup` edits in a multisite fixture | `AddZone`, `RemoveZone`, `SyncGroup.Apply` | Typed topology calls, one at a time |
| `rbd mirror pool enable`/`peer` changes on a mirrored pool | `rbdmirror.Mirror` operations, which compare captured policies | `rbdmirror.Mirror` methods, `Rebootstrap` |
| Messenger mode and bind options with `MessengerV2Secure` | The fixed bootstrap policy; `override.SetConfig` refuses them | Choose `NetworkConfig.WithMessengerMode` at `Run` |
| `docker stop`, `docker pause`, `docker network disconnect` on owned containers | Pause and interruption tracking, `Terminate` order | `Stop`/`Start`, `fault.Pause`, `InterruptNetwork` |

Resources you create natively (pools, images, users, buckets, CRUSH rules)
are not owned by the fixture: typed removal helpers refuse them, and they
disappear with the cluster. For changes you want undone within a test, prefer
the `Temporary*` helpers, which record the original state and refuse to
overwrite outside edits.

## Related APIs

- [`ceph`](../../ceph/README.md): `Ceph`, `RunNativeCommand`, `RedactSecrets`,
  `ControlContainer`, `ClientContainer`, `WithClient`, `CreateClient`,
  `ConnectionConfig`, `CollectDiagnostics`.
- [`override`](../../override/README.md): `SetConfig` and the other restorable
  changes.
- [`rbd/mirror`](../../rbd/mirror/README.md), [`cephfs/mirror`](../../cephfs/mirror/README.md),
  [`rgw`](../../rgw/README.md) and [`rgw/multisite`](../../rgw/multisite/README.md):
  the setup clients and typed observers above.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): `Restore`
  semantics and partial failures.
