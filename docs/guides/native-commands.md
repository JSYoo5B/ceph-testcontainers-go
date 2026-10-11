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
| `rados`, `rbd`, `radosgw-admin`, `ceph-authtool`, Python bindings | `cluster.ControlContainer().Exec(ctx, argv, ...)` | The control container |
| A workload, a restricted identity or your own client software | A container attached with `WithClient` or `WithClientIdentity` | Your container |
| A native client in the test process | `cluster.ConnectionConfig()` with host networking | Your process |
| `radosgw-admin` in a gateway's realm and zone | `rgw.Gateway.Admin(ctx, args...)` | The gateway container |
| `radosgw-admin` in a multisite zone | `rgw.Multisite.SourceAdmin`, `DestinationAdmin`, `ZoneAdmin` | The fixture's setup client for that zone |
| `rbd` on either side of a mirror link | `rbd.Mirror.SourceRBD`, `DestinationRBD` | The link's setup clients |
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
- On a non-zero exit, `Ceph` returns the standard output it got and an error
  that includes standard output and standard error. Output of commands such as
  `auth get` contains keys; be careful when logging such errors.
- `Ceph` does not take the cluster's topology gate, so it can run while
  another goroutine adds or removes daemons. See
  [Contexts, ownership and cleanup](contexts-and-cleanup.md).

The control container is the MON in a single-MON cluster, and a separate CLI
container when the cluster has several MONs. It keeps working while a MON is
stopped as long as a quorum remains.

## Other CLIs in the control container

`ControlContainer()` returns the control container handle. It has every Ceph
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

control := cluster.ControlContainer()
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
  process inside the container stopped. Prefer native timeouts:
  `--connect-timeout` for `ceph`, and Ceph options such as
  `--rados-osd-op-timeout 10` or `--rados-mon-op-timeout 10` for `rados` and
  `rbd`.
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
| `radosgw-admin bucket sync status` on Squid | Text; `--format json` is ignored | `rgw.Multisite.BucketSyncStatus`, `WaitBucketSyncReady` |
| `radosgw-admin sync status` | The comparison with remote logs appears only in the human-readable output, even with `--format json` | `rgw.Multisite.SyncStatus`, `WaitSyncReady` |
| `rbd mirror pool info` on Squid | JSON without `mirror_uuid` and `remote_namespace` | `rbd.Mirror.PolicyStatus` |

Decode only the fields you need, and keep unknown fields tolerated: Ceph adds
fields between releases. See [Ceph releases and images](releases-and-images.md)
for release-specific output.

## Changing configuration

| Need | Use |
| --- | --- |
| Inspect stored central configuration | `cluster.Configuration(ctx)` returns exact entries, including masked ones |
| Change any option for the duration of a test | `cluster.TemporaryConfig(ctx, ceph.ConfigSetting{...})` and `Restore` |
| Options that must be present at bootstrap | `ceph.WithConfigFile(path)` |
| A daemon's effective value | `cluster.Ceph(ctx, "config", "show", "osd.0", "osd_op_complaint_time")` or `cluster.Ceph(ctx, "tell", "osd.0", "config", "get", "osd_op_complaint_time")` |

`TemporaryConfig` accepts any option, not only those the module knows:

```golang
change, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{
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
  are rejected. `UpdateClientCaps` replaces all capabilities;
  `ClientCapabilities` reads them back; `DeleteClient` revokes the identity.
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

`cluster.ConnectionConfig()` returns copies of `ceph.conf` and the admin
keyring for a native client in your test process. Daemons must be reachable
from the process, which normally requires `ceph.WithHostNetwork()`. See
[Host network and native clients](host-network.md).

## Daemon admin sockets

Every daemon has an admin socket with commands for status, performance
counters and debugging.

For MON, MGR and OSD daemons, use
[`Container.AdminSocket`](../ceph.md#adminsocket). It reaches the socket of
any owned daemon, whatever its image, without the Messenger or Cephx:

```golang
status, err := cluster.AdminSocket(ctx, "osd.0", "status")
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
| MON | `/var/run/ceph/ceph-mon.<name>.asok` | `cluster.Monitors()[i]`, name `DaemonName` |
| MGR | `/var/run/ceph/ceph-mgr.<name>.asok` | `cluster.Managers()[i]`, name `DaemonName` |
| OSD | `/var/run/ceph/ceph-osd.<id>.asok` | `cluster.OSDs()[i]`, `ID` |
| MDS | `/var/run/ceph/ceph-mds.<id>.asok` | `fs.MDSs()[i]`, `ID` |
| `rbd-mirror` | `/tmp/rbd-mirror.asok` | `rbdMirror.Daemons()[i]` |
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

Prefer the typed observers when they cover your case: `rbd.MirrorDaemon.Status`,
`rbd.Mirror.ReceiverStatus`, `ImageStatus`, `cephfs.Mirror.DirectoryStatus`.

### Observing Cephx keys from the daemon side

No admin socket command prints a daemon's own key or the rotating service
secrets that it holds. Observe them in two ways instead.

**The daemon's key.** After
[`Container.RotateDaemonKey`](../ceph.md#rotatedaemonkey), `auth get-key` on
the MONs returns the new key, and a restart of the daemon proves that it
authenticates with the key in its keyring files.

**Service secrets.** The MONs rotate the secret of each service (MON, MGR, OSD,
MDS) every `auth_service_ticket_ttl`, and daemons fetch them as they change.
Raise the daemon's `debug_auth` through its admin socket. The daemon then logs
two things to its container output:
- the secrets it holds, at every MON client tick;
- the secret that decrypted each client ticket it accepts.

```golang
_, err := cluster.AdminSocket(ctx, "osd.0", "config", "set", "debug_auth", "10/10")
// ... let a client connect to osd.0 ...
logs, err := cluster.OSDs()[0].Logs(ctx)
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
- On Tentacle, `ceph auth dump-keys --format json` on the MONs lists the same
  secret IDs and expirations under `rotating_secrets`. Squid has no such
  command.

## RBD mirror setup clients

`rbd.RunMirror` starts one setup client per side. `SourceRBD` and
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
gateway := rgw.Gateways(cluster)[0]
stats, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", "photos")
```

- Flags that would change the scope or the Ceph connection (`--rgw-realm`,
  `--rgw-zonegroup`, `--rgw-zone` and their ID forms, `--conf`, `--cluster`,
  `--keyring`, `--key`, `--id`, `--name`, `--mon-host`, `-c`, `-m`, `-n`, `-k`,
  ...) and arguments with control characters are refused.
- Successful output may contain credentials. A failed command's output is
  redacted from the error.
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
`rgw.RunMultisite`; `ZoneAdmin(ctx, name, ...)` addresses any owned zone of
`rgw.RunTopology`, including a partially added one. These do not add
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

## Avoiding conflicts with fixture bookkeeping

The fixture records what it created and what it changed, and checks native
state before acting on it again. Native commands that change the same things
behind its back make later typed calls fail, or make `Restore` refuse to run.
Ceph offers no compare-and-swap for most of these changes, so some conflicts
cannot even be detected.

| Native change | Conflicts with | Prefer |
| --- | --- | --- |
| `config set`/`config rm` on a key held by a `TemporaryConfig` handle | That handle's `Restore` | `TemporaryConfig` |
| `osd set`/`osd unset` of a global flag | `TemporaryOSDFlag` | `TemporaryOSDFlag` |
| `osd set-nearfull-ratio`, `set-backfillfull-ratio`, `set-full-ratio` | `TemporaryFullRatios` | `TemporaryFullRatios` |
| `mgr module enable`/`disable` | `TemporaryMGRModule`; disabling `volumes` or `mirroring` breaks CephFS fixtures | `TemporaryMGRModule` |
| `osd blocklist add`/`rm` of an entry a handle owns | `TemporaryBlocklist` | `TemporaryBlocklist` |
| Deleting, renaming or changing owned pools, their CRUSH rules or EC profiles | `CreatePool` descriptors, `RemovePool`, `SetPool*`, which check the native pool ID | `SetPoolReplication`, `SetPoolQuota`, `SetPoolPGCount`, `SetPoolPlacement`, `RemovePool` |
| `osd out`/`in`/`destroy`/`purge`, CRUSH edits for owned OSDs | `SetOSDIn`, `RemoveOSD`, which check the OSD's UUID | `SetOSDIn`, `RemoveOSD` |
| `auth` changes to identities created by `CreateClient`, or to daemon identities | `UpdateClientCaps`, `DeleteClient`, daemon restarts | `UpdateClientCaps`, `DeleteClient` |
| `mon add`/`mon remove`, editing `/etc/ceph/ceph.conf` in any container | MON membership and the `mon_host` bookkeeping | `AddMonitor`, `RemoveMonitor`, `RefreshMonitorConfig` |
| `fs set max_mds`, `fs fail`, `fs rm` | `ScaleMDS`, `WaitReady`, `cephfs.Remove` | `ScaleMDS`, `cephfs.Remove` |
| `fs set session_timeout`/`session_autoclose` | `TemporarySessionTimeouts` | `TemporarySessionTimeouts` |
| `fs subvolume`/`subvolumegroup` changes to owned subvolumes, snapshots or clones | Subvolume helpers, which check native identity | The typed subvolume helpers |
| `radosgw-admin` changes to fixture-created users, accounts or placement | `UserInfo`, `RemoveUser`, `ApplyPlacement`, which verify keys and identity | The typed RGW helpers |
| `radosgw-admin period`/`zone`/`zonegroup` edits in a multisite fixture | `AddZone`, `RemoveZone`, `ApplySyncGroup` | Typed topology calls, one at a time |
| `rbd mirror pool enable`/`peer` changes on a mirrored pool | `rbd.Mirror` operations, which compare captured policies | `rbd.Mirror` methods, `Rebootstrap` |
| Messenger mode and bind options with `MessengerV2Secure` | The fixed bootstrap policy; `TemporaryConfig` refuses them | Choose `WithMessengerMode` at `Run` |
| `docker stop`, `docker pause`, `docker network disconnect` on owned containers | Pause and interruption tracking, `Terminate` order | `Stop`/`Start`, `PauseContainer`, `InterruptNetwork` |

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

- [ceph.md](../ceph.md): `Ceph`, `ControlContainer`, `ControlImage`,
  `Configuration`, `TemporaryConfig`, `WithConfigFile`, `WithClient`,
  `WithClientIdentity`, `CreateClient`, `ConnectionConfig`,
  `CollectDiagnostics`.
- [rbd.md](../rbd.md): `Mirror.SourceRBD`, `Mirror.DestinationRBD`,
  `MirrorDaemon.Status`.
- [cephfs.md](../cephfs.md): `Filesystem.Sessions`, `Mirror.DirectoryStatus`.
- [rgw.md](../rgw.md): `Gateway.Admin`, `Multisite.SourceAdmin`,
  `DestinationAdmin`, `ZoneAdmin`.
- [Fault injection](fault-injection.md): messenger options and other faults set
  through `TemporaryConfig`.
- [Contexts, ownership and cleanup](contexts-and-cleanup.md): `Restore`
  semantics and partial failures.
