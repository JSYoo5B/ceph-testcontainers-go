# Ceph releases and images

## Introduction

The module runs real Ceph daemons from a Ceph container image and configures
them at run time. This guide explains which image is used by default, which
Ceph releases are supported and how they differ, how to select a separate image
for each daemon role, and what an image must contain.

## Default image

`ceph.DefaultImage` is the published, pruned Tentacle dependency-role
aggregate, named by its release tag:

```text
ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all
```

The tag is a multi-architecture manifest for `linux/amd64` and
`linux/arm64`; Docker pulls the variant of the engine's native architecture.
[ceph-testcontainers-images](https://github.com/JSYoo5B/ceph-testcontainers-images#published-images)
publishes every image as `<variant>-<release>-<role>` and replaces a
published image under the same tag when it is rebuilt, for example after a
packaging correction or when a role gains a binary the module needs. The
default therefore follows compatible rebuilds without a new module version.

Which build a host runs is the caller's choice, as for any image reference:

- The module passes the reference to Docker unchanged. An engine that already
  has the tag keeps its local image until you pull the tag again
  (`docker pull`), so refreshing and caching follow your own pull policy.
- For one exact build, pass its digest reference
  (`ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:...`) or a local image
  ID to `Run` instead of the default. Tags, digests and local image IDs are
  all accepted, for `Run` and for every `ImageSet` role.
- The module neither resolves a tag to a digest, checks that an image is
  current, nor reports which build ran. Record `docker image inspect` output
  in your own test run when you need that evidence.

The module needs no image of its own. It supplies the FSID, keys,
configuration, daemon IDs, addresses, bootstrap commands and entrypoints when
it starts each container, so any image that meets the
[requirements](#role-images) can replace the default.

```golang
cluster, err := ceph.Run(ctx, ceph.DefaultImage)
```

## Supported releases

| Release | Status | Tested image |
| --- | --- | --- |
| Tentacle 20.2.x | Default; every feature on 20.2.4. 20.2.0 to 20.2.3 have no `aes256k` keys | Pruned official aggregate that `ceph.DefaultImage` names, and the `official-20.2.4-<role>` role tags |
| Umbrella 21.2.x | Periodic CI on 21.2.0, with the [known issues](known-release-issues.md) of that release; the default remains Tentacle | Published official role images, the `official-21.2.0-<role>` tags; official amd64 requires x86-64-v3 |
| Squid 19.2.x | Supported on 19.2.5, with the [differences](#behaviour-that-differs-on-squid) listed below | Published official role images, the `official-19.2.5-<role>` tags; the upstream image is `quay.io/ceph/ceph:v19.2.5` |

Support is established on the patch each row names: the repository's
continuous integration runs its test suites on 20.2.4, 21.2.0 and 19.2.5 (see
[Supported releases](../contributing/ci.md#supported-releases) in the CI
guide). Another patch of the same release is expected to behave the same, but
only what has been run on it is known:

- Squid 19.2.6: RGW multisite cannot start, because the master gateway
  refuses a secondary zone's `realm pull`; see
  [Known release issues](known-release-issues.md#a-secondary-zone-cannot-pull-the-realm-on-squid-1926),
  which holds the guidance. When Periodic `discovery` draws 19.2.6, it runs
  `TestMultiClusterRGWMultisite` there as a control next to its sampled core
  tests. The CephFS entries of the known issues that name 19.2.6
  were run on it and behaved as on 19.2.5. Nothing else of the fixture has
  been run on 19.2.6, so its other behaviour is untested.
- Tentacle 20.2.0 to 20.2.3 have no `aes256k` Cephx keys; see
  [Known release issues](known-release-issues.md#cephx-key-types-before-tentacle-2024).
  The key-type tests run the official 20.2.3 image.
- Other patches, such as Tentacle 20.2.5 and Squid 19.2.7, appear as fixed or
  affected releases in the known issues and the experiment cases, and
  Periodic `discovery` can sample a few core tests on them (see
  [Periodic runs](../contributing/ci.md#periodic-runs)). A sample is not
  support.

Native defects of particular releases, and limits shared across releases
such as a daemon stranded without a MON session after its ticket expired, are
listed by service in [Known release issues](known-release-issues.md).

To run against Squid, pass a Squid image to `Run`:

```golang
cluster, err := ceph.Run(ctx, "quay.io/ceph/ceph:v19.2.5")
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
  not supported: the fixture chooses command forms from the essential image
  and the client toolkit alone. A run that mixes releases establishes only
  what it observed for its roles and pairing; see
  [Mixed-release runs](#mixed-release-runs).

## Behavioral bug compatibility policy

Feature availability, release command/output differences and behavioral bugs
are separate compatibility questions. For future bug-specific paths, the
[bug compatibility policy](../contributing/bug-compatibility.md) requires explicit
bug-ID build selection rather than inferring a fix from a release or image-tag
suffix. Custom images can contain backports without changing the reported
release. No tag is
[registered](bug-compatibility.md#registered-tags) yet; this documentation does not add selectors or change
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
| RBD receiver status | The rbd-mirror admin socket's `rbd mirror status` reports `namespace_replayers` with local and remote namespace names. | The default namespace's `image_replayers` sit in the pool entry and named namespaces appear by name only under `namespaces`. | None. `rbdmirror.Container.ReceiverStatus` and `WaitReceiverReady` normalize both forms. |
| RGW sync pipe records | A system-mode pipe has no `user` in its `params`. | Squid records `"user": ""`. | None. Pipe readback accepts the empty value on Squid. |
| RGW account root as user-mode pipe principal | Objects replicate between buckets owned by the same account. | Squid resolves no source for such a pipe and replicates nothing. Ordinary-user user-mode pipes work. | `multisite.SyncGroup.CreatePipe` returns `ceph.ErrUnsupported`, naming the Squid zone, before changing anything when any zone runs Ceph 19. |
| RGW user-scope rate limit | A user's policy throttles every request that the user's S3 keys sign. | The gateway stores and reports the policy but never applies it to a request signed with the user's S3 key: the authenticated request user carries no stored user attributes, where the policy lives. A bucket's policy is applied. | `rgw.User.OverrideRateLimit` with `rgw.RateLimitScope{}` returns `ceph.ErrUnsupported`, naming the release, before changing anything when the cluster runs Ceph 19. The bucket scope works on both. |
| RGW bucket sync status | `radosgw-admin bucket sync status --format json` prints JSON. | Squid ignores `--format json` and prints text. | None for `BucketSyncStatus` and `WaitBucketSyncReady`, which parse the text into the same structure and fail on unknown lines. Your own calls get text. |

### Differences you may hit in your own native checks

These affect commands or client libraries you run yourself, not the typed API:

| Topic | Squid behaviour |
| --- | --- |
| Messenger encryption | There is no `messenger dump` admin command showing per-connection encryption. Check the daemons' `ms_*_mode` settings (for example with `ceph config show`) and, for your own librados or libcephfs clients, the `secure` mode on READY connections in `debug_ms=1` logs. |
| RGW STS roles | `radosgw-admin role get` for a missing role exits with code 2 and prints one ENOENT line. |
| Python `rbd` group API | `Group.id()` and `get_snap_info()` do not exist. Read the group ID from the `rbd_group_directory` object and member snapshots from each image's group snapshot namespace. `list_snaps()` of a group that does not exist returns an empty list; Tentacle 20.2.5 and Umbrella raise `ObjectNotFound`, and 20.2.4 crashes (see [Known release issues](known-release-issues.md#python-rbd-snapshot-list-of-a-missing-group)). |
| `rbd mirror pool info` | No `mirror_uuid` or `remote_namespace` fields; no `--remote-namespace` option. |
| `radosgw-admin bucket sync status` | Text output only. |
| `ceph fs status --format json` | Up to 19.2.6, the `rank` of a standby-replay daemon is the integer rank it follows. 19.2.7, Tentacle and Umbrella print the string `<rank>-s`, as the plain output does. Classify daemons by `state`, or read `ceph fs dump`. |

## Role images

`ceph.ImageSet` selects Essential, Client, CephFS, RBDAux, RGW and MGR, or an
All image that supplies every role and may be combined only with MGR. This is
the image model used by the module and its CI.
`Run(ctx, image)` treats the image as an aggregate. Pass
`ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(set))` to `Run` for explicit roles, with an empty
image argument (or the set's essential image, which is not a second input); a
missing optional feature image is an error.

| Role | Runtime responsibility |
| --- | --- |
| essential | MON, MGR and OSD in separate containers; bootstrap/administration, MGR modules, OSD classes/plugins |
| mgr | The MGR daemons and their modules, when `ImageSet.MGR` selects an image other than essential |
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

Use matching Ceph releases. The published role images are the
`<variant>-<release>-<role>` tags of
`ghcr.io/jsyoo5b/ceph-testcontainers-images`, for example
`official-20.2.4-essential`; this repository keeps no digest list of them.
The module default is the official-derived aggregate, `official-20.2.4-all`.

Cluster administration remains on MON/the stable multi-MON CLI. Ordinary
RBD/CephFS and native userspace oracles use the owned lazy client toolkit;
aggregate selections reuse their existing executor. Client setup and mirror
daemons use their respective client and feature images. RGW administration
runs in rgw without requiring a generic client. Cache daemon/plugin/socket
stay with their consuming librbd client. OSD plugins stay with essential, and
MGR modules with the MGR image. Caller-owned external clients remain caller-owned.

### The MGR image

`ImageSet.MGR` is the image of every manager the cluster starts: the initial
active and standby candidates of `Run` and the managers that `AddManager`
adds later. It replaces the whole MGR container, not a sidecar. Empty, it is
Essential (with `All`, the All image), so a set without it runs managers as
before; no other role has a default. Use it to run an image that ships
modules the essential role image omits, together with the role images of the
other daemons: the `mgr-modules` role image of the release, which the official
variant publishes (`official-<release>-mgr-modules`), the official image, or a
consumer image built from either with additional modules:

```go
// mgrModulesRef is the mgr-modules role image of the same release.
images := ceph.ImageSet{Essential: essentialRef, Client: clientRef, MGR: mgrModulesRef}
cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)))
// FactsOf(cluster).Images.MGR is the MGR image; every ManagerContainer.Image reports it.
```

- The precedence for one manager is `DaemonOptions.WithImage` of its
  `AddManager` call, then `ImageSet.MGR`, then Essential. The override is the
  explicit per-daemon building block and is not checked.
- The MGR image must be of the cluster's release. When it is not the
  essential image, every manager start first runs `ceph-mgr --version` in a
  short-lived probe container of that image, which gets no keyring or cluster
  configuration and is removed afterwards. When the release differs from
  `ClusterFacts.CephVersion` the start fails before the manager's identity, key
  or container exists, so the daemon never runs: `Run` returns the partial
  cluster without that manager, and `AddManager` returns no handle. A missing
  `ceph-mgr` is `ceph.ErrImageCapability`. Adding modules does not make another
  release acceptable. The manager container is then created from the image ID
  that the probe checked, so rebuilding or retagging the image in between does
  not start an unchecked image; `ManagerContainer.Image` and
  `ClusterFacts.Images.MGR` keep the reference as given. The probe's removal is
  bounded (30 seconds) even when the start's context is cancelled, and a
  removal failure fails that start; `Terminate` removes a probe left behind.
- `ClusterFacts.Images.MGR` reports the resolved image and is never empty;
  finalization manifests and verification reports record it with the other
  roles. MGR containers are never recreated: `Stop` and `Start` keep the
  container, and a manager keeps its image until it is removed.
- Nothing in the module installs or names a module. Enable a module of the
  image with `override.SetMGRModule`, configure it with `override.SetConfig`
  (`mgr/<module>/<option>`), and read its state with `Observer.MGRModules`:
  a module whose dependency is missing is advertised with `CanRun` false and
  its `ErrorString`, and `SetMGRModule` refuses to enable it with an error that
  carries that `ErrorString` (for example `influxdb python module not found`). A membership
  change respawns every manager; `SetMGRModule` and its `Restore` return once
  a respawned active manager is available, so `Observer.MGRModules` and other
  MGR commands work right after them.
  `Observer.WaitMGRModuleReady` knows only built-in modules; probe a module of
  your own through its own command. The Dashboard, which the official image
  ships and essential does not, is prepared by the
  [`dashboard`](../../dashboard/README.md) package. The prometheus module
  that `Observer.ManagerMetrics` scrapes is not a baseline module either;
  the `mgr-modules` role ships it.
- The enabled list of `ceph mgr dump` also names the release's
  `mgr_initial_modules` defaults (iostat, nfs and nvmeof since Squid). The
  essential role need not ship them; a manager without them stays
  `HEALTH_OK`, and `Observer.MGRModules` lists them as enabled.

`TestMGRImageOfficialSlot`, `TestMGRImageWithoutEssentialManager` and
`TestMGRImageConsumerModule` (batch `mgr_image`) run the official image in the
MGR slot, an essential image without `ceph-mgr`, and a consumer image built
from the official image with one small module, including a module whose
dependency is missing and a module option value that Ceph refuses. In the same
batch, `TestManagerPrometheusExporter`, `TestManagerMetrics`,
`TestManagerWatchReportsFailoverAndServiceAdvertisement`,
`TestMGRRuntimeRestoration` and the receiver recipes below run their managers
from the `mgr-modules` role image. The batch declares
`image-roles=essential,client,mgr-modules`, so CI supplies
`CEPH_TEST_MGR_MODULES_IMAGE`; a role run without it fails before any cluster
starts, rather than falling back to the essential or the official image.

### Receivers of MGR modules

A test whose SUT scrapes or receives what an MGR module sends composes the
existing primitives; no helper wraps them. It holds one `override.SetConfig`
handle per `mgr/<module>/<option>`, enables the module with
`override.SetMGRModule`, and runs its scraper or receiver in a container it
owns on `ClusterFacts.NetworkName`, which needs no Ceph configuration or
keyring. It restores the module before the options and stops the receiver
last. On 21.2.0 the essential role ships none of these modules, so the MGR
slot needs the `mgr-modules` role or another image that ships them.

- prometheus (`TestManagerPrometheusIndependentScraper`):
  `Observer.ManagerMetrics` scrapes through the control container; a SUT that
  scrapes on its own must check content, not the status code. A standby
  answers `/metrics` with 200 and an empty body under either
  `standby_behaviour` (`error` changes only the index page), and an
  unchanged body is also a 200. Identify a pool by its native `pool_id` and
  require a fresh increase of its `ceph_pool_wr` after a write the SUT made;
  the counter comes from PG statistics, so the deadline covers that report
  as well as `scrape_interval`. Follow the active manager with
  `Observer.WatchManagers` or `Observer.ManagerServices`: after a failover
  the advertisement can take more than ten seconds, and a changed
  `mgr/prometheus/` option restarts the listener while the old URL stays
  advertised. Daemon perf counters are excluded by default; they need
  `mgr/prometheus/exclude_perf_counters` set to false or a separate
  ceph-exporter, which the fixture does not run.
- alerts (`TestManagerAlertsSMTPDelivery`): the module mails when the set of
  health checks changes or a count grows, and never periodically. Its first
  pass after an enable or a failover reports every current check as New,
  and the new active manager sends from its own address. It has no retry: a
  change made while the receiver is down is lost, `ALERTS_SMTP_ERROR` (read
  with `Observer.HealthDetails`) is the only native signal, and the next
  change can arrive as a Cleared without its New. `ceph alerts send` is a
  transport control that reports no change. The message carries no fsid;
  put an identity the SUT can match into `smtp_from_name` or
  `smtp_sender`. `smtp_ssl` is implicit TLS against the MGR image's system
  trust store, with no STARTTLS.
- telegraf (`TestManagerTelegrafDelivery`): the module reads `address` and
  `interval` once when it starts, so set them before enabling it and change
  the address only while it is disabled. Its first pass comes about ten
  seconds after the start. Over UDP every line is its own datagram, over TCP
  every pass is one connection, and the lines of one pass share one
  timestamp. Match the cluster by the `fsid` tag, never by the sender, which
  changes on a failover. A send failure appears only in the active
  manager's container log: health stays unchanged, `ceph telegraf send`
  still reports success, and the passes of an outage are lost.

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
This extension does not require Docker's privileged mode. It delays, drops,
rate-limits, reorders, duplicates or corrupts the packets that a container
sends, on the whole interface or only to selected peers, families and TCP
ports, and with `NetworkProfile.Direction` also what peers send to it (shaped
at the peers' endpoints). The module checks the kernel's `prio` qdisc and u32
classifier in its preflight helper before it selects peers. Packet inspection
is the separate [`capture`](../../capture/README.md) package.

## One daemon from another image

`DaemonOptions.WithImage` starts one new MON or MGR, and `OSDOptions.WithImage` one new
OSD, from an image other than the cluster's essential image. They are options of
`AddMonitor`, `AddManager` and `AddOSD`; see
[Per-daemon images](https://pkg.go.dev/github.com/JSYoo5B/ceph-testcontainers-go/ceph#DaemonOptions.WithImage) for the full contract.

```golang
osd, err := cluster.AddOSD(ctx, ceph.OSDOptions{}.WithImage("registry.example/ceph:other"))
// osd.Image() is "registry.example/ceph:other"; FactsOf(cluster).Images.Essential is unchanged.
release, err := osd.Version(ctx) // what the running daemon reports, such as "20.2.4"
```

Read this scope before relying on it:

- It overrides one container. The cluster's images, `ClusterFacts` and the
  other daemons are untouched. For a MGR it takes precedence over
  `ImageSet.MGR`; to run every manager from another image, select it there.
- `CephVersion` is recorded once from the first MON and the CLI image. It does
  not describe an overridden daemon: use the daemon handle's `Version`, which
  reads the daemon's own admin socket and fails when the reply has no
  recognizable version. `Image` reports the image the container was started
  from, not its release.
- A re-launched daemon keeps its image unless the call names another:
  `DetachedOSD.Reopen` and `MonitorContainer.Replace` (with
  `MonitorStorage.WithVolume`) start the daemon from the image it ran from, or
  from the one their `WithImage` option names. MGR containers are not
  recreated; add a manager from the other image and remove the old one.
- The release check of a separately started client container still compares
  with `CephVersion` and is unchanged.
- Nothing checks that the image exists, has the daemon binary, or is
  compatible with the running cluster. See
  [Mixed-release runs](#mixed-release-runs) for the pairings that have been
  run.

## Rolling upgrade from Squid to Tentacle

`TestRollingUpgradeSquidToTentacle` (optional batch `rolling_upgrade`) moves a
running 19.2.5 cluster to 20.2.4 one daemon at a time, in the upstream order,
with the published official role images of both releases (the
`official-19.2.5-<role>` and `official-20.2.4-<role>` tags), on the host network and
on a bridge:

1. `osd set noout`.
2. Each of three MONs: `MonitorContainer.Replace` with the Tentacle image, on
   the store and address the MON keeps. The monmap's members and addresses do
   not change; after the last MON, `min_mon_release_name` becomes `tentacle`
   and the MONs commit a new monmap epoch for it.
3. The MGR: a Tentacle manager is added, then the Squid one is removed.
4. Each of three OSDs: `Detach`, then `Reopen` with the Tentacle image, so the
   Squid-written store is opened by Tentacle.
5. `osd require-osd-release tentacle`, `osd unset noout`, all PGs clean.

After every step `ceph versions` must show exactly the expected per-role
layout, every acknowledged write reads back with its digest, and a new batch of
writes is acknowledged. Tentacle MONs report the `AUTH_INSECURE_*` health
checks for the keys Squid created, so the cluster is in `HEALTH_ERR` from the
first Tentacle MON on; the fixture rotates no key.

What stays on the source release: `ClusterFacts.CephVersion`, the fixture's
Ceph CLI in the control container and the client toolkit. Command forms are
chosen from them, so the fixture keeps using Squid command forms against the
upgraded daemons. Mixing releases is therefore still not a general contract:
this upgrade is one pair, run forward only. Downgrade is not attempted.

### An interrupted upgrade, resumed

`TestRollingUpgradeResumesAfterInterruptions` (same batch, both network modes)
stops the same upgrade part-way in each daemon phase and resumes it forward:

| Phase | Interruption | State while stopped | Resume |
| --- | --- | --- | --- |
| MON | `Replace` of mon.b from an image without `ceph-mon`; the old container is already gone | quorum of a (Tentacle) and c (Squid); the monmap members and addresses are unchanged; writes are acknowledged | `Replace` on the handle the failed call returned, with the Tentacle image, on the retained store |
| MON | mon.c (Squid) replaced on its own Squid image while mon.a runs Tentacle | a Squid MON rejoins a quorum with a Tentacle MON | mon.c later moves to Tentacle |
| OSD | `Reopen` of osd.1 from an image without `ceph-osd` | the PGs keep two of three replicas; writes are acknowledged | `Detach` of the failed handle, then `Reopen` with the Tentacle image |
| OSD | osd.2 reopened on Squid while the MONs and MGR run Tentacle | a Squid OSD rejoins under Tentacle MONs | osd.2 later moves to Tentacle |

At the end every acknowledged write reads back, the monmap's members and
addresses (on the host network, their ports) are the ones the Squid cluster
started with, and the layout is all Tentacle. The monmap epoch moves once, when
the last MON runs Tentacle and the MONs commit `min_mon_release`. A stopped step never needs anything but the call that failed:
the retained store and the returned handle are enough to continue.

## Mixed-release runs

The module supports clusters whose roles all run one release. Three runs
have mixed releases, and each establishes only what it observed:

| Run | Roles and releases | What it established |
| --- | --- | --- |
| [Rolling upgrade from Squid to Tentacle](#rolling-upgrade-from-squid-to-tentacle) (`TestRollingUpgradeSquidToTentacle`, `TestRollingUpgradeResumesAfterInterruptions`) | MONs, MGR and OSDs moved one at a time from 19.2.5 to 20.2.4; the fixture's CLI and client toolkit stay on 19.2.5 | Acknowledged writes read back and new writes are acknowledged after every step, and an interrupted upgrade resumes forward |
| RGW incremental sync timing profile (`TestRGWTopologyStartupTimingEffectiveValues`, `TestRGWStartupPollTimingProfile`) | 19.2.5 essential and client role images with a 20.2.4 rgw image | `TimingProfile.IncrementalSync` is accepted because the gateway's `radosgw` is 20.2.4, although the cluster reports 19.2.5, and its effective values reach the gateways. In the same run the workload of `TestRGWStartupPollTimingProfile` failed afterwards: a second PUT of an object answered 403 `AccessDenied` on this pair |
| [Legacy replication destination readback](../../internal/integration/experiments/rgw_legacy_replication_destination/README.md) (`TestLegacyReplicationDestination`, an experiment case) | A 19.2.5 cluster and writer RGW, with a second RGW reader on that same cluster from the selected image | Only the readback of a replication destination stored by the 19.2.5 writer: a 20.2.2 reader doubled the ARN prefix (`REPRODUCED`) and a 20.2.4 reader returned the original destination (`NOT_REPRODUCED`). It does not exercise inter-zone object transfer |

The reverse pairing, a 20.2.4 cluster with a 19.2.5 rgw image, does not
start: `multisite.Run` fails before any gateway starts, because the 19.2.5
`radosgw-admin` cannot fetch the MON configuration of the 20.2.4 cluster. So
the refusal of the timing profile for a gateway other than 20.2.4 was not
reached natively. It is checked only by the Docker-free
`TestPollTimingProfileOutcomeFollowsTheGatewayBinary`, which judges the
outcome from the release the gateway's `radosgw` reports.

None of these runs makes another pairing, another role or a downgrade supported.

## Related APIs

- [`ceph`](../../ceph/README.md): `DefaultImage`, `Run`, `FactsOf`,
  `ImageSet` (including `MGR`), `ImageOptions` (`WithImageSet`, `WithCephFSImage`, `WithRGWImage`),
  `DaemonOptions.WithImage`, the error categories.
- [Classified errors](errors.md): `ErrUnsupported`, `ErrImageCapability` and
  how to write tests that hold on every release.
- [`cephfs/mirror`](../../cephfs/mirror/README.md): `Run`, `Container.RebalanceDirectories`.
- [`rgw/multisite`](../../rgw/multisite/README.md): `Run`, `WithTopology`,
  `Config.SetupImage`.
- [`rbd/mirror`](../../rbd/mirror/README.md): `Run`, namespace mapping.
- [Testing this repository](../contributing/testing.md): running the module's
  own suites against another image or release.
