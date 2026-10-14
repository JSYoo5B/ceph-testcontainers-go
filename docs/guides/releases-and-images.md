# Ceph releases and images

## Introduction

The module runs real Ceph daemons from a Ceph container image and configures
them at run time. This guide explains which image is used by default, which
Ceph releases are supported and how they differ, how to select a separate image
for each daemon role, and what an image must contain.

## Default image

`ceph.DefaultImage` is the official Ceph Tentacle image, pinned by digest:

```text
quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
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
| Tentacle 20.2.x | Primary test release: 20.2.4; earlier patches lack AES256K key support; native limits below still apply | `quay.io/ceph/ceph:v20.2.4` (`ceph.DefaultImage`) |
| Squid 19.2.x | Supported, with the differences listed below | `quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b` |

Squid 19.2.6 behaves like 19.2.5 except that RGW multisite cannot start; see
[Known release-specific issues](#known-release-specific-issues). Tentacle
releases before 20.2.4 have no `aes256k` Cephx keys; see the same section. The
repository's continuous integration runs its test suites on both releases; see
[CI](../contributing/ci.md).

To run against Squid, pass a Squid image to `Run`:

```golang
cluster, err := ceph.Run(ctx,
	"quay.io/ceph/ceph:v19.2.5@sha256:1bb011052bc6d347d3418adcbf7d88156860d45697bc6323594a11410084064b")
```

## Detecting the release at run time

Right after the first MON starts, `Run` executes `ceph --version` in the
control image and records the release. `cluster.CephVersion()` returns it, for
example `"20.2.4"`. The fixture uses this value to choose commands that differ
between releases.

Where a tool reveals a feature, the fixture checks the tool instead of the
release number, so that patched and stripped images behave as they are: the
control image's `ceph-authtool` decides Cephx key types, and the `rbd` of each
RBD mirror setup client and each cluster's control image decides namespace
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
  not supported: the fixture chooses command forms from the control image
  alone.

## Behaviour that differs on Squid

Most differences are handled inside the module and need no action. The table
lists every known difference and what you can observe.

| Area | Tentacle 20.2.x | Squid 19.2.x | Effect on your test |
| --- | --- | --- | --- |
| Cephx key types and MonMap ciphers | 20.2.4 supports `aes` and `aes256k`; earlier patches support only `aes`. | Only `aes`. | `WithAuth` and `CreateClient` check the target tools and return `ceph.ErrUnsupported` for an unavailable key type; no silent fallback. |
| CephFS filesystem creation | `fs new` sets `max_mds` and `standby_count_wanted` inline. | Squid does not accept the inline settings, so the fixture runs `fs set` right after `fs new`. | None. Both forms complete before any MDS starts. |
| RBD mirroring of named namespaces only | The default namespace is put in `init-only` mode. | Squid has no `init-only` mode, so the default namespace is put in `image` mode. | On Squid nothing in the default namespace replicates unless you enable an image explicitly. |
| RBD namespace mapping | `MirrorConfig.SourceNamespace` and `DestinationNamespace` may differ. | Squid has no remote namespaces and its `rbd` no `--remote-namespace`, so only namespaces with the same name can be mirrored. | `rbd.RunMirror` returns `ceph.ErrUnsupported`, naming the side whose `rbd` lacks the option, before changing anything when the names differ. |
| RBD mirror UUID | `rbd mirror pool info` prints `mirror_uuid` and `remote_namespace`. | Neither field is printed. The fixture reads the UUID from the pool's `rbd_mirroring` object and uses the same namespace name as the remote namespace. | None for the typed API. Your own parsing of `rbd mirror pool info` must tolerate the missing fields. |
| RBD receiver status | The rbd-mirror admin socket's `rbd mirror status` reports `namespace_replayers` with local and remote namespace names. | The default namespace's `image_replayers` sit in the pool entry and named namespaces appear by name only under `namespaces`. | None. `rbd.Mirror.ReceiverStatus` and `WaitReceiverReady` normalize both forms. |
| RGW sync pipe records | A system-mode pipe has no `user` in its `params`. | Squid records `"user": ""`. | None. Pipe readback accepts the empty value on Squid. |
| RGW account root as user-mode pipe principal | Objects replicate between buckets owned by the same account. | Squid resolves no source for such a pipe and replicates nothing. Ordinary-user user-mode pipes work. | `rgw.Multisite.CreateSyncPipe` returns `ceph.ErrUnsupported`, naming the Squid zone, before changing anything when any zone runs Ceph 19. |
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
  rejects such requests. No setting disables the check. `rgw.RunMultisite` and
  `rgw.RunTopology` therefore cannot start on 19.2.6. Use 19.2.5 for
  multisite tests on Squid; everything else behaves on 19.2.6 as on 19.2.5.
- **Tentacle before 20.2.4, Cephx key types.** `ceph-authtool` has no
  `--key-type` and `monmaptool` no cipher options in 20.2.0 to 20.2.3, so these
  releases generate only `aes` keys, as Squid does. `ceph.WithAuth` and
  `CreateClient` detect the options in the control image: they accept `aes`
  and refuse `aes256k` before any change with `ceph.ErrUnsupported`, naming
  `ceph-authtool --key-type` as the capability. The repository tests this
  with 20.2.3.
- **Tentacle 20.2.4, CephFS mirror daemons.** When another `cephfs-mirror`
  daemon joins, the MGR `mirroring` module fails to redistribute directories
  that are already assigned (`'DirectoryState' object is not subscriptable`).
  Failover to a surviving daemon still works. Call
  `cephfs.Mirror.RebalanceDirectories` after `AddDaemon`. Ceph's Tentacle
  documentation also marks deployments with several CephFS mirror daemons as
  untested upstream. See [cephfs.md](../cephfs.md).

## Role images

The official image contains every Ceph component. Lighter images can be used by
role, so that each container pulls only what it runs. A role describes the
components an image must contain, not a container: MON and MGR, for example,
run as separate containers from the same `control` image.

| Role | Runs |
| --- | --- |
| `control` | MON, MGR, the fixture's CLI container, client and helper containers, `rbd-mirror` and `cephfs-mirror` daemons, multisite setup clients |
| `osd` | One container per OSD |
| `rgw` | RGW gateways |
| `mds` | CephFS metadata servers |
| `all` | Every component of the four roles in one image, like the official image |

### Which option selects which image

| Containers | Image selected by | Fallback |
| --- | --- | --- |
| MON, MGR, control CLI, helper containers (host-port reservations, the in-memory OSD storage keeper) | The `img` argument of `ceph.Run` (or `rbd.Run`, `cephfs.Run`, `rgw.Run`). `cluster.ControlImage()` returns it. | None |
| OSDs, including OSDs added later | `ceph.WithOSDImage(image)` | `Run` image |
| MDS daemons, including daemons added by `ScaleMDS` | `cephfs.WithMDSImage(image)` | `Run` image |
| Gateways from `rgw.WithGateways` and `rgw.Start` | `rgw.WithImage(image)` | `Run` image |
| `rbd-mirror` daemons and both setup clients | The `image` argument of `rbd.RunMirror`; must be a `control` or `all` image. Pass `source.ControlImage()`. | None |
| `cephfs-mirror` daemons | The `image` argument of `cephfs.RunMirror`; must be a `control` or `all` image. | None |
| Multisite gateways | The `image` argument of `rgw.RunMultisite`, `rgw.RunTopology`, `Multisite.AddZone` and `Multisite.AddZonegroup` (an `rgw` or `all` image) | None |
| Multisite setup clients (`radosgw-admin`) | `rgw.MultisiteConfig.ControlImage` or `rgw.TopologyConfig.ControlImage` | Each zone cluster's own `ControlImage()` |
| Your client containers | Your choice; `cluster.ControlImage()` has every Ceph CLI and the Python bindings | None |
| Netem helper | `ceph.NetworkDelay.HelperImage`, selected explicitly (for example `ceph.NetemHelperImage`) | None; never uses the Ceph role image implicitly |

If you omit a role override, that role runs from the `Run` image, which must
then contain the role's components too. With a `control`-only image you must
at least set `WithOSDImage`, and also `cephfs.WithMDSImage` or `rgw.WithImage`
when you start CephFS or RGW.

`RunNativeCommand` needs `python3` in the container that runs its supervisor.
The `osd`, `mds` and `rgw` role contracts do not require it, so a supervised
command in one of those containers may return `ceph.ErrImageCapability`.
Run client oracles in a `control`/`all` client container; `AdminSocket` can
query owned MON/MGR/OSD admin sockets through the shared run volume from the
control container. Selecting a helper does not add tools to a daemon image.

```golang
cluster, err := ceph.Run(ctx, "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-control",
	ceph.WithOSDImage("ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-osd"),
	cephfs.WithMDSImage("ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-mds"),
	rgw.WithImage("ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-rgw"),
	cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
	rgw.WithGateways(rgw.Config{Name: "s3"}),
)
if cluster != nil {
	testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
	t.Fatal(err)
}
```

### Published role images

The companion repository
[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images)
checks and publishes role images for every supported release:

```text
ghcr.io/jsyoo5b/ceph-testcontainers-images:<variant>-<release>-<role>
```

- `<variant>`: `official` (files copied only from the official image),
  `debian` or `ubuntu` (built from Ceph's own distribution packages);
- `<release>`: for example `20.2.4` or `19.2.5`;
- `<role>`: `control`, `osd`, `rgw`, `mds` or `all`.

Each tag covers `linux/amd64` and `linux/arm64`. You can also build your own
images; any image that meets the requirements below works.

## Image requirements by role

Requirements that apply to every role:

- Linux `amd64` or `arm64`; containers run as root.
- No dependency on cephadm, systemd or an image-specific entrypoint. The module
  overrides entrypoint and command, and daemons run in the foreground.
- `/bin/sh` and `mkdir`, `cp`, `cat`, `rm`, `test`, `sleep`, `hostname` and
  `awk` on `PATH`. `hostname -i` prints the container address and
  `sleep infinity` keeps an idle container running.
- Writable `/etc/ceph`, `/var/lib/ceph`, `/var/run/ceph` (a symlink to
  `/run/ceph` is fine) and `/var/log/ceph`; a sticky `/tmp`.
- Every executable loads with its shared libraries, Python modules and runtime
  plugins.
- No cluster inside the image: FSID, keys, daemon IDs, addresses and ports are
  injected at run time. No address or port is fixed in the image.
- OSDs initialize with `--mkfs` on a sparse BlueStore file. Block devices, LVM,
  privileged mode and kernel mounts are not used.
- All Ceph executables report `ceph version <release> (<commit>)` for
  `--version`, all with the same release; images combined in one cluster report
  the same release.

| Role | Executables on `PATH` | Additional runtime dependencies |
| --- | --- | --- |
| `control` | `ceph-mon`, `ceph-mgr`, `ceph`, `ceph-authtool`, `monmaptool`, `rados` with `--striper`, `rbd`, `cryptsetup`, `rbd-mirror`, `cephfs-mirror`, `radosgw-admin`, `python3` | Python `rados`, `rbd`, `cephfs`, `ceph_argparse`, `ceph_daemon`; librbd LUKS1/LUKS2 encryption, libcryptsetup, libradosstriper; the release's always-on MGR modules plus `volumes`, `rbd_support` and `mirroring` with their Python dependencies |
| `osd` | `ceph-osd` | BlueStore; the `rbd`, `rgw`, `cephfs`, `hello` and `lock` object classes; compressor and erasure-code plugins |
| `rgw` | `radosgw`, `radosgw-admin`, `readlink` | Beast HTTP frontend with TLS |
| `mds` | `ceph-mds` | MDS shared libraries |
| `all` | Union of the four roles | Union of the four roles |

Python and the CLIs of other roles are not required in `osd`, `rgw` or `mds`
images. Do not rely on `ceph` or `python3` inside OSD, MDS or RGW containers
when you run your own commands; use the control container instead (see
[Running native commands](native-commands.md)).

The control image runs more than daemons: the fixture executes the `ceph` CLI,
Python probes (RGW HTTP and TLS readiness, CephFS and RBD client helpers) and
mirror daemons from it. A missing MGR module dependency shows up as a health
error, so `HEALTH_OK` also depends on the control image.

The authoritative list, with the reason each component is needed, is the
[image requirements](https://github.com/JSYoo5B/ceph-testcontainers-images/blob/main/docs/IMAGE_REQUIREMENTS.md)
document of the images repository, which also provides a checker for any
local image.

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
| netem packet delay | `TemporaryNetworkDelay` (see [Fault injection](fault-injection.md#delaying-packets-below-ceph)) | `ceph.NetemHelperImage` | netem contract 1: `sh`, `sleep`, `tc` and `ip` with JSON output and netem, `python3`; the engine grants `NET_ADMIN` to the helper and has the netem qdisc |

`NET_ADMIN` is granted only to the opt-in helper, not to the Ceph daemons.
This extension does not require Docker's privileged mode. The current API
delays all outgoing packets on the selected interface; peer-selective loss,
reordering and packet inspection are not provided by this contract.

## Related APIs

- [ceph.md](../ceph.md): `DefaultImage`, `Run`, `CephVersion`, `ControlImage`,
  `WithOSDImage`, the error categories.
- [Classified errors](errors.md): `ErrUnsupported`, `ErrImageCapability` and
  how to write tests that hold on every release.
- [cephfs.md](../cephfs.md): `WithMDSImage`, `RunMirror`,
  `Mirror.RebalanceDirectories`.
- [rgw.md](../rgw.md): `WithImage`, `RunMultisite`, `RunTopology`,
  `MultisiteConfig.ControlImage`.
- [rbd.md](../rbd.md): `RunMirror`, namespace mapping.
- [Testing this repository](../contributing/testing.md): running the module's
  own suites against another image or release.
