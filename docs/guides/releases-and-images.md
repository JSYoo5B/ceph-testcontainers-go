# Ceph releases and images

## Introduction

The module runs real Ceph daemons from a Ceph container image and configures
them at run time. This guide explains which image is used by default, which
Ceph releases are supported and how they differ, how to select a separate image
for each daemon role, and what an image must contain.

## Default image

`ceph.DefaultImage` is the published, pruned Tentacle dependency-role aggregate,
pinned to the tested native AMD64/ARM64 index digest:

```text
ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:104315166259499ea6855fc163ac052c37fd7261faab638f13a78be4a2004a80
```

The digest is a multi-architecture manifest for `linux/amd64` and
`linux/arm64`; Docker pulls the variant of the engine's native architecture.
The module needs no image of its own. It supplies the FSID, keys,
configuration, daemon IDs, addresses, bootstrap commands and entrypoints when
it starts each container, so any image that meets the
[requirements](#image-requirements-by-role) can replace the default.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage)
```

## Supported releases

| Release | Status | Tested image |
| --- | --- | --- |
| Tentacle 20.2.x | Default; every feature on 20.2.4. 20.2.0 to 20.2.3 have no `aes256k` keys | Pruned official aggregate pinned by `ceph.DefaultImage` |
| Squid 19.2.x | Supported, with the differences listed below | `quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b` |

Squid 19.2.6 behaves like 19.2.5 except that RGW multisite cannot start; see
[Known release-specific issues](#known-release-specific-issues). Tentacle
releases before 20.2.4 have no `aes256k` Cephx keys; see the same section. The
repository's continuous integration runs its test suites on both releases; see
[CI](../contributing/ci.md). Native limits that both releases share, such as a
daemon stranded without a MON session after its ticket expired, are listed in
[Upgrading to v0.2.0](upgrading-to-v0.2.md#remaining-native-limits).

To run against Squid, pass a Squid image to `Run`:

```golang
cluster, err := ceph.Run(ctx,
	"quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b")
```

## Detecting the release at run time

Right after the first MON starts, `Run` executes `ceph --version` in the
first MON's essential image and records the release. `FactsOf(cluster).CephVersion` returns it, for
example `"20.2.4"`. The fixture uses this value to choose commands that differ
between releases.

Where a tool reveals a feature, the fixture checks the tool instead of the
release number, so that patched and stripped images behave as they are: the
essential image's `ceph-authtool` decides Cephx key types, and the `rbd` of each
RBD mirror setup client and each cluster's client toolkit decides namespace
mapping. A feature that the relevant
target lacks fails before any change with `ceph.ErrUnsupported`, whose
`*ceph.OperationError` names the target, its version and the feature. Assert
that outcome instead of skipping on the version string; see
[Classified errors](errors.md#tests-across-releases).

- The version describes the image of the first MON and of the fixture's Ceph
  CLI. Daemons started from other role images report their own release.
- `Run` fails if `ceph --version` does not print
  `ceph version <major>.<minor>.<patch> ...`. The image requirements demand this
  output.
- Combine only images of the same release in one cluster. Mixing releases is
  not supported: the fixture chooses command forms from the essential image and the client toolkit
  alone.

## Behavioral bug compatibility policy

Feature availability, release command/output differences and behavioral bugs
are separate compatibility questions. For future bug-specific paths, the
[contributor policy](../contributing/bug-compatibility.md) requires explicit
bug-ID build selection rather than inferring a fix from a release or image-tag
suffix. Custom images can contain backports without changing the reported
release. The [supported-tag catalogue](bug-compatibility-tags.md) currently
has no registered entries; this documentation does not add selectors or change
the existing runtime adaptations described below.

## Behaviour that differs on Squid

Most differences are handled inside the module and need no action. The table
lists every known difference and what you can observe.

| Area | Tentacle 20.2.x | Squid 19.2.x | Effect on your test |
| --- | --- | --- | --- |
| Cephx key types and MonMap ciphers | 20.2.4 supports `aes` and `aes256k`; earlier patches support only `aes`. | Only `aes`. | `WithAuth` and `CreateClient` check the target tools and return `ceph.ErrUnsupported` for an unavailable key type; no silent fallback. |
| CephFS filesystem creation | `fs new` sets `max_mds` and `standby_count_wanted` inline. | Squid does not accept the inline settings, so the fixture runs `fs set` right after `fs new`. | None. Both forms complete before any MDS starts. |
| RBD mirroring of named namespaces only | The default namespace is put in `init-only` mode. | Squid has no `init-only` mode, so the default namespace is put in `image` mode. | On Squid nothing in the default namespace replicates unless you enable an image explicitly. |
| RBD namespace mapping | `Config.SourceNamespace` and `DestinationNamespace` may differ. | Squid has no remote namespaces and its `rbd` no `--remote-namespace`, so only namespaces with the same name can be mirrored. | `rbdmirror.Run` returns `ceph.ErrUnsupported`, naming the side whose `rbd` lacks the option, before changing anything when the names differ. |
| RBD mirror UUID | `rbd mirror pool info` prints `mirror_uuid` and `remote_namespace`. | Neither field is printed. The fixture reads the UUID from the pool's `rbd_mirroring` object and uses the same namespace name as the remote namespace. | None for the typed API. Your own parsing of `rbd mirror pool info` must tolerate the missing fields. |
| RBD receiver status | The rbd-mirror admin socket's `rbd mirror status` reports `namespace_replayers` with local and remote namespace names. | The default namespace's `image_replayers` sit in the pool entry and named namespaces appear by name only under `namespaces`. | None. `rbdmirror.Mirror.ReceiverStatus` and `WaitReceiverReady` normalize both forms. |
| RGW sync pipe records | A system-mode pipe has no `user` in its `params`. | Squid records `"user": ""`. | None. Pipe readback accepts the empty value on Squid. |
| RGW account root as user-mode pipe principal | Objects replicate between buckets owned by the same account. | Squid resolves no source for such a pipe and replicates nothing. Ordinary-user user-mode pipes work. | `multisite.Fixture.CreateSyncPipe` returns `ceph.ErrUnsupported`, naming the Squid zone, before changing anything when any zone runs Ceph 19. |
| RGW bucket sync status | `radosgw-admin bucket sync status --format json` prints JSON. | Squid ignores `--format json` and prints text. | None for `BucketSyncStatus` and `WaitBucketSyncReady`, which parse the text into the same structure and fail on unknown lines. Your own calls get text. |

### Differences you may hit in your own native checks

These affect commands or client libraries you run yourself, not the typed API:

| Topic | Squid behaviour |
| --- | --- |
| Messenger encryption | There is no `messenger dump` admin command showing per-connection encryption. Check the daemons' `ms_*_mode` settings (for example with `ceph config show`) and, for your own librados or libcephfs clients, the `secure` mode on READY connections in `debug_ms=1` logs. |
| RGW STS roles | `radosgw-admin role get` for a missing role exits with code 2 and prints one ENOENT line. |
| Python `rbd` group API | `Group.id()` and `get_snap_info()` do not exist. Read the group ID from the `rbd_group_directory` object and member snapshots from each image's group snapshot namespace. |
| `rbd mirror pool info` | No `mirror_uuid` or `remote_namespace` fields; no `--remote-namespace` option. |
| `radosgw-admin bucket sync status` | Text output only. |

## Known release-specific issues

- **Squid 19.2.6, RGW multisite.** On a secondary zone, `radosgw-admin realm
  pull` is rejected by the master gateway with HTTP 403. The RGW debug log gives
  the reason `'x-amz-content-sha256' supplied, but not in CanonicalHeaders`: the
  release's `radosgw-admin` sends that header without signing it, and its RGW
  rejects such requests. No setting disables the check. `multisite.Run` and
  `multisite.RunTopology` therefore cannot start on 19.2.6. Use 19.2.5 for
  multisite tests on Squid; everything else behaves on 19.2.6 as on 19.2.5.
- **Tentacle before 20.2.4, Cephx key types.** `ceph-authtool` has no
  `--key-type` and `monmaptool` no cipher options in 20.2.0 to 20.2.3, so these
  releases generate only `aes` keys, as Squid does. `ceph.WithAuth` and
  `CreateClient` detect the options in the essential image: they accept `aes`
  and refuse `aes256k` before any change with `ceph.ErrUnsupported`, naming
  `ceph-authtool --key-type` as the capability. The repository tests this
  with 20.2.3.
- **Tentacle 20.2.4, CephFS mirror daemons.** When another `cephfs-mirror`
  daemon joins, the MGR `mirroring` module fails to redistribute directories
  that are already assigned (`'DirectoryState' object is not subscriptable`).
  Failover to a surviving daemon still works. Call
  `cephfsmirror.Mirror.RebalanceDirectories` after `AddDaemon`. Ceph's Tentacle
  documentation also marks deployments with several CephFS mirror daemons as
  untested upstream. See [cephfs-mirror.md](../cephfs-mirror.md).

## Role images

`ceph.ImageSet` selects Essential, Client, CephFS, RBDAux and RGW, or an
exclusive All image. This is the image model used by the module and its CI.
`Run(ctx, image)` treats the image as an aggregate. Pass
`ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(set))` to `Run` for explicit roles, with an empty
image argument (or the set's essential image, which is not a second input); a
missing optional feature image is an error.

| Role | Runtime responsibility |
| --- | --- |
| essential | MON, MGR and OSD in separate containers; bootstrap/administration, MGR modules, OSD classes/plugins |
| client | Ordinary CLI, Python rados/rbd/cephfs bindings, encryption/striper clients, offline map diagnostics |
| cephfs | MDS and CephFS mirror runtime |
| rbd-aux | RBD mirror and cache-capable client runtime |
| rgw | Gateway and RGW administration/setup |
| all | Aggregate supplying every role |

```go
cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{
    Essential: essentialRef,
    Client: clientRef,
    CephFS: cephFSRef,
    RGW: rgwRef,
})), cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
   rgw.WithGateways(rgw.Config{Name: "s3"}))
if cluster != nil { testcontainers.CleanupContainer(t, cluster) }
if err != nil { t.Fatal(err) }
```

Use matching Ceph releases. Published immutable references are recorded in
[the role digest map](../../.github/ceph-role-images.json), grouped by release,
variant and role. The SDK default is the qualified official-derived aggregate.

Cluster administration remains on MON/the stable multi-MON CLI. Ordinary
RBD/CephFS and native userspace oracles use the owned lazy client toolkit;
aggregate selections reuse their existing executor. Client setup and mirror
daemons use their respective client and feature images. RGW administration
runs in rgw without requiring a generic client. Cache daemon/plugin/socket
stay with their consuming librbd client. MGR modules and OSD plugins stay
with essential. Caller-owned external clients remain caller-owned.

`RunNativeCommand` requires Python in the actual target container, and keeps
explicit capability checks. Selecting a toolkit does not add utilities to a
daemon. The complete [image requirements](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/IMAGE_REQUIREMENTS.md)
and [role contract](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/ROLES.md)
define the supported runtime dependencies.

### Only what the official image ships

Every baseline module feature works with the unmodified official image of each
supported release. The module relies only on components that image ships and
never installs packages at run time. This has visible consequences:

- No `tc`, `ip`, `iptables` or `nft` in the Ceph containers. Network faults
  of the baseline are whole-endpoint Docker operations or Ceph messenger
  options (see [Fault injection](fault-injection.md)).
- No kernel RBD mapping or kernel/FUSE CephFS mounts: clients use librbd and
  libcephfs in user space.
- No privileged containers, host disks or LVM: OSDs default to sparse
  BlueStore files; MemStore and memory-backed BlueStore are opt-in alternatives.
- Operation-only components of the official image, such as the dashboard,
  cephadm, ceph-volume, NFS Ganesha and iSCSI gateways, are never used, so
  custom images may omit them.

Extra tools and services for your own tests (go-ceph programs, S3 SDKs, a
Vault server for RGW server-side encryption, and so on) belong in their own
images, not in the Ceph role images.

### Opt-in extensions

An extension is a feature that needs tools the official image does not ship.
You select it explicitly, together with the image that provides the tools; no
baseline feature and no default uses it.

- The module checks the selected image against the extension's contract, and
  separately what the engine must grant (capabilities, kernel features), before
  it changes anything. A missing selection or tool is
  `ceph.ErrImageCapability`, and a denied permission or an unsupported kernel
  is `ceph.ErrEnvironmentUnavailable`, each with a narrower error of the
  extension (see [Classified errors](errors.md#image-capabilities-and-optional-extensions)).
  A tag name is not trusted, nothing is installed at run time, and there is
  no fallback.
- The Ceph containers keep their images. An extension's tools run in a helper
  container that the module starts and removes.
- [ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/EXTENSIONS.md)
  defines each contract, checks images against it and publishes one image per
  extension. Any image that meets the contract works, including one from a
  private registry or a local build.

| Extension | API | Helper image | Contract |
| --- | --- | --- | --- |
| netem packet delay and loss | `ApplyNetworkProfile` (see [Fault injection](fault-injection.md#delaying-packets-below-ceph)) | `fault.NetemHelperImage` | netem contract 1 with its selector profiles: `sh`, `sleep`, `tc` and `ip` with JSON output, netem, `prio` and u32, `python3`; the engine grants `NET_ADMIN` to the helper and has the netem qdisc, and for peers the `prio` qdisc and the u32 classifier |

`NET_ADMIN` is granted only to the opt-in helper, not to the Ceph daemons.
This extension does not require Docker's privileged mode. It delays or drops
the packets that a container sends, on the whole interface or only to selected
peers, families and TCP ports. The module checks the kernel's `prio` qdisc and
u32 classifier in its preflight helper before it selects peers. Packets that a
container receives, reordering, rate limits and packet inspection are not
provided.

## One daemon from another image

`WithDaemonImage` starts one new MON or MGR, and `OSDOptions.WithImage` one new
OSD, from an image other than the cluster's essential image. They are options of
`AddMonitor`, `AddManager` and `AddOSD`; see
[Per-daemon images](../ceph.md#per-daemon-images) for the full contract.

```golang
osd, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithImage("registry.example/ceph:other"))
// osd.Image() is "registry.example/ceph:other"; FactsOf(cluster).Images.Essential is unchanged.
release, err := osd.Version(ctx) // what the running daemon reports, such as "20.2.4"
```

Read this scope before relying on it:

- It overrides one container. The cluster's images, `ClusterFacts` and the
  other daemons are untouched.
- `CephVersion` is recorded once from the first MON and the CLI image. It does
  not describe an overridden daemon: use the daemon handle's `Version`, which
  reads the daemon's own admin socket and fails when the reply has no
  recognizable version. `Image` reports the image the container was started
  from, not its release.
- A re-launched OSD keeps its image: `OSDContainer.Detach` and
  `DetachedOSD.Reopen` start it from the image it ran from, and `Reopen` takes
  no image option. MON and MGR containers are not recreated.
- The release check of a separately started client container still compares
  with `CephVersion` and is unchanged.
- Nothing checks that the image exists, has the daemon binary, or is
  compatible with the running cluster. The option is not a supported
  mixed-version or upgrade path, and no native run of it exists yet.

## Related APIs

- [ceph.md](../ceph.md): `DefaultImage`, `Run`, `FactsOf`,
  `ImageSet`, `ImageOptions.WithImageSet`, `WithDaemonImage`, the error categories.
- [Classified errors](errors.md): `ErrUnsupported`, `ErrImageCapability` and
  how to write tests that hold on every release.
- [cephfs.md](../cephfs.md): `WithMDSImage`.
- [cephfs-mirror.md](../cephfs-mirror.md): `Run`, `Mirror.RebalanceDirectories`.
- [rgw.md](../rgw.md): `WithImage`.
- [rgw-multisite.md](../rgw-multisite.md): `Run`, `RunTopology`,
  `Config.SetupImage`.
- [rbd-mirror.md](../rbd-mirror.md): `Run`, namespace mapping.
- [Testing this repository](../contributing/testing.md): running the module's
  own suites against another image or release.
