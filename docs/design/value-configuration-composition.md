# Value configuration composition

Change `312-add-osd-options`, issue #312. This record states the composition
rules of the operation-specific value configurations that replace overload
families. The first user is `ceph.OSDOptions` for `Container.AddOSD`. Later
candidates (`Run` with image selection, tenant client attachment, the mirror
`Run` functions) are meant to reuse these rules; none of them is implemented by
this change. The rules are a proposal of the implementation side for review:
the issue delegated the concrete names and signatures, and nothing here records
an owner approval.

## Shape

- A configuration is a plain struct value with unexported fields and chainable
  `WithX` methods. Each method has a value receiver and returns a modified copy.
  There is no pointer builder, no shared mutable state and no universal options
  type: each operation domain has its own type (`OSDOptions` is not a Run
  option).
- The entry point takes one trailing typed variadic, `configs ...OSDOptions`.
  Go allows only one trailing variadic, so inputs that used to arrive as a
  second variadic (`...DaemonOption`) are reconciled into the configuration type
  (`OSDOptions.WithImage`) instead of an untyped `...any` bag.
- A `WithX` method exists only when it sets a field that callers really vary.
  No method is added for visual uniformity.

## Composition rules

1. **Unspecified is not empty.** Every field records whether it was set. The
   zero value sets nothing. An explicit empty setting (`WithPlacement(OSDConfig{})`,
   `WithImage("")`) is a setting: the first is an explicit request for the
   defaults, the second is an invalid selection that is refused, never read as
   "no override".
2. **Order and scope.** `AddOSD` merges its values in argument order. A value
   that leaves a field unset never erases what another value set.
3. **One owner per field across values.** A field set by two values is refused
   with `ErrInvalidRequest`, also when both agree. There is no
   last-value-wins across values, so argument order never silently decides a
   conflict, and moving a value cannot change the effective configuration.
4. **Replacement within one value.** Calling the same `WithX` twice on one
   value replaces the earlier setting completely, so a derived value can
   override its base (`base.WithImage(a)` then `.WithImage(b)`). This is the one
   place where a later call wins.
5. **Validate the assembled configuration once.** The effective configuration
   (merged fields, cluster defaults resolved, placement normalized, image
   validated) is assembled and validated in one place before any external
   effect: before the topology lock, any registration and any container. A
   refusal changes no state and does not wait for a lock.
6. **Ordering of checks is part of the contract and is unchanged:** the
   context, then configuration validation, then
   the topology lock and the existing registration and cleanup path. Existing
   cancellation and error categories are kept.
7. **Ownership.** A value owns its data. Copies must not share mutable storage:
   a configuration that holds a slice or map clones it with `slices.Clone` or
   `maps.Clone`, never with `append([]T(nil), v...)`, because the latter turns a
   non-nil empty slice into nil and so erases the explicit-empty distinction that
   validators such as the messenger mode list rely on. Live handles that the
   operation is meant to reference (for example the `*ClientConfig` of a tenant
   attachment, whose identity the ownership check compares by pointer) keep their
   identity and are never cloned. `OSDOptions` holds only strings, so a struct
   copy is already independent.
8. **No second option surface for the dependency.** The grouped value is the
   primary API. The testcontainers escape stays narrow and is documented per
   entry point. Module settings that are `ceph.Option` values are applied to the
   settings and validated, they are not forwarded to a container request (their
   `Customize` does nothing on purpose), so a later Run migration must map those
   inputs into its one effective configuration explicitly.

## AddOSD

| Old | New |
| --- | --- |
| `AddOSD(ctx, ...DaemonOption)` | `AddOSD(ctx, ...OSDOptions)` |
| `AddOSDWithConfig(ctx, cfg, ...DaemonOption)` | `AddOSD(ctx, OSDOptions{}.WithPlacement(cfg))` |
| `AddOSD(ctx, WithDaemonImage(img))` | `AddOSD(ctx, OSDOptions{}.WithImage(img))` |

- Placement reuses `OSDConfig`. `WithPlacement` takes the four fields as one
  unit, so an explicit placement is never mixed field by field with another
  value's placement (rule 3).
- The image check is the same function as `WithDaemonImage` (`validateDaemonImage`),
  so MON, MGR and OSD images are validated alike. `WithDaemonImage` remains the
  option of `AddMonitor` and `AddManager`, whose entry points are not changed by
  this slice.
- `AddOSDWithConfig` is removed without an alias (0.x policy). The shared-gate
  table loses its row; `AddOSD` keeps the topology-change policy.
- Hosts, racks, device class and root are still validated and resolved by the
  existing `resolveOSDDefaults` and `normalizeOSDConfig`; registration, key
  generation, the publish-before-start ownership record and cleanup are
  unchanged.

## Run

Change `312-image-options`. Proposal of the implementation side, not an owner
approval. `Run(ctx, img string, opts ...testcontainers.ContainerCustomizer)` and
the `rbd`, `cephfs` and `rgw` `Run` signatures are unchanged: `Run` is the
testcontainers-compatible entry point and its single trailing variadic stays the
only place for configuration.

- **The grouped-carrier pattern (used by every Ceph configuration group).** A
  group is a plain value type with unexported fields and chainable,
  value-returning methods (`ImageOptions`, later `ClusterConfig`, `OSDConfig`,
  `NetworkConfig`). The value is **not** a customizer and has no `Customize`
  method; passing it straight to `Run` does not compile. Each group has exactly
  one carrier option function, `With<Group>(value) Option` (here
  `WithImageOptions(ImageOptions)`), a `ceph.Option` that `Run` already
  recognises, applies to the cluster settings and validates before any effect.
  The carrier's `Customize` does nothing, so a group never reaches a container
  request. The carrier is not a `WithX` per field: one function per group.
  Docker and container execution policy remains testcontainers-style `WithX`.
- **Service inputs are not forwarded.** `rbd.WithPools`, `cephfs.WithFilesystems`,
  `cephfs.WithMDSImage`, `rgw.WithGateways`, `rgw.WithImage` and every
  `ceph.WithX` setting stay `ceph.Option` values that `Run` applies to the
  settings; they never reach a container request. A test applies a service
  option together with an image set and fails if either is dropped.
- **The `img` argument with an image set.** It is not a second image input: empty
  or equal to the set's Essential (after `All` expansion) is accepted and
  anything else is `ErrInvalidRequest` before effects. Without an image set the
  argument is the aggregate image as before.
- **Composition (rules 1 to 5).** The zero value selects nothing. An explicit set
  is a selection: an explicit empty set is refused, not read as unspecified. At
  most one `WithImageOptions` in the variadic selects a set; a second selection
  is refused even when equal, so argument order never decides a conflict. Within
  one value the later `WithImageSet` replaces the earlier one. Everything is
  validated once, before any container, network or volume is created, and
  missing roles that a requested feature needs are refused with no fallback to
  Essential.
- **Copies (rule 7).** `ImageOptions` holds an `ImageSet`, a struct of strings, so a
  struct copy is independent and there is no slice to clone. A future member that
  holds a slice must use `slices.Clone`, which keeps nil and non-nil empty apart.
- **Raw customizers.** They reach the first MON's container request only, in
  argument order, after the module's own request options and before the
  messenger environment, the `WithConfigFile` file and the ownership label.
- `RunWithImages` and `WithImages` are removed without aliases. The mirror
  `RunWithImages` functions are removed by `312-mirror-client` (below).

### OSD storage group (change `312-osd-storage`)

Proposal of the implementation side, not an owner approval. The second group
after images follows the carrier pattern above:

```go
storage := ceph.OSDStorage{}.WithBlockSize(256 << 20).WithObjectStore(ceph.OSDMemStore)
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage))
```

The four former per-field options (block size, object store, in-memory size,
volume) are fields of this value; they are not kept under a second spelling.

- **Why this group first.** The full topology group (counts, no-initial flags,
  pool defaults, default CRUSH root, pools, startup timeout, auth) has about 300
  call sites in 150 files. The storage group has the fewest (about 50 call sites
  in 30 files), is cohesive (where every OSD keeps its data) and has the most
  incompatible-combination rules, so it is the smallest reviewable slice that
  exercises explicit-versus-unspecified and conflict refusal.
- **Rules applied.** The zero value changes nothing. Every field records that it
  was set, so `WithBlockSize(0)`, `WithObjectStore("")` and `WithInMemory(0)` are
  refused as explicit invalid settings and never read as the default. Two
  `WithOSDStorage` options that set the same field are refused even when equal;
  different fields compose in any order. Within one value a later call replaces
  the earlier one. The existing combination rules (MemStore with in-memory or
  volume storage, in-memory with volume storage) are unchanged and still refuse
  before any resource is created. Repeating a legacy option used to mean last
  wins; across carriers it is now a refusal, which is the only behavior change.
- **Reconciliation with `OSDConfig` and `OSDOptions`.** `OSDConfig` is the
  per-OSD placement and `OSDOptions` (AddOSD) is per-call placement and image.
  These storage settings are cluster-wide: the runtime keeps one volume or one
  tmpfs for all OSDs and AddOSD reads the cluster settings. A per-OSD
  `OSDConfig{}.WithVolume()` would need per-OSD storage plumbing that does not
  exist, so it is not offered and `OSDConfig` is not made to carry storage.
  Placement stays in `OSDConfig`; storage stays cluster-wide in `OSDStorage`.
- **No slice or map member**, so a struct copy is independent. A member that holds
  one must use `slices.Clone`, which keeps nil and non-nil empty apart.

### Network and messenger group (change `312-network-config`)

Proposal of the implementation side, not an owner approval. The third group
follows the carrier pattern above:

```go
network := ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack).WithSeparateClusterNetwork()
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNetworkConfig(network))
```

| Old (removed, no alias) | New |
| --- | --- |
| `WithAddressFamily(family)` | `NetworkConfig.WithAddressFamily(family)` |
| `WithSeparateClusterNetwork()` | `NetworkConfig.WithSeparateClusterNetwork()` |
| `WithHostAddress(address)` | `NetworkConfig.WithHostAddress(address)` |
| `WithMessengerMode(mode)` | `NetworkConfig.WithMessengerMode(mode)` |
| `WithMessengerPolicy(policy)` | `NetworkConfig.WithMessengerPolicy(policy)` |

- **Why this group next.** It is the smallest cohesive group left: five settings
  with about 130 references in 45 files, against about 300 call sites in 150
  files for the topology group (whose cohesive subsets are not independent: the
  counts, the no-initial flags and the initial layouts refuse each other) and
  three separate packages for the service settings. All five settings decide
  which addresses the daemons bind and advertise and over which Messenger
  protocol and connection modes they talk, and their combination rules already
  live together in `Run` (host address needs host network, host network excludes
  a separate cluster network and non-IPv4 families, the mapped family excludes a
  separate cluster network, a policy excludes `MessengerV2Secure`). Those rules
  are unchanged and still refuse before any resource is created.
- **What stays out.** `WithHostNetwork` is Docker execution policy and stays a
  testcontainers-style function. `WithClientMessengerPolicy` is a
  `ClientContainerOption` of `WithClient`, not a `Run` setting. `WithConfigFile`
  stays with the later configuration group.
- **Rules applied.** The zero value changes nothing. Every field records that it
  was set, so the default family `AddressIPv4` and `MessengerDefault` are settings
  (a second carrier cannot override them), while `WithHostAddress("")`, an unknown
  family or mode and an empty list in a policy are refused as explicit invalid
  settings. Two `WithNetworkConfig` options that set the same field are refused
  even when equal; different fields compose in any order. Within one value a
  later call replaces the earlier one. A value is validated as a whole before any
  of its fields is written, so a refused value changes no setting. Repeating a
  legacy option used to mean last wins; across carriers it is now a refusal,
  which is the only behavior change.
- **Copies (rule 7).** `MessengerPolicy` holds three slices. The value clones them
  with `slices.Clone` when the policy enters it and again when the settings are
  written, so a caller reusing its slices, a derived value and the settings never
  share storage, a nil list stays nil (Ceph's default) and a non-nil empty list
  stays empty and is refused.

### `WithConfigFile` (proposal)

`WithConfigFile(path)` is Ceph configuration, not container-execution policy:
the module parses the file, refuses keys it manages (for example
`bluestore_block_size`, which points at `OSDStorage.WithBlockSize`), and renders
the accepted sections into the daemons' Ceph configuration. No Docker setting
is involved. It therefore belongs to a later Ceph configuration group and is not
kept as a testcontainers-style function. `WithHostNetwork` and
`WithIdleEntrypoint` are Docker or container execution policy and stay functions.

### Grouping plan for the remaining `ceph.WithX` options (proposal; follow-up slices)

Each slice removes the old functions it replaces, so no setting has two
spellings. Docker and container execution policy stays testcontainers-style
`WithX`.

| Group (value type, carrier) | Settings | Slice |
| --- | --- | --- |
| Images (`ImageOptions`, `WithImageOptions`) | `WithImageSet`; later the service image roles | done (`312-image-options`) |
| OSD storage (`OSDStorage`, `WithOSDStorage`) | block size, object store, in-memory, volume | done (`312-osd-storage`) |
| Topology (`ClusterConfig`, `WithClusterConfig`) | `WithMonitorCount`, `WithManagerCount`, `WithNoInitialManagers`, `WithOSDCount`, `WithInitialOSDs`, `WithNoInitialOSDs`, `WithPoolDefaults`, `WithDefaultCRUSHRoot`, `WithPools`, `WithStartupTimeout`, `WithAuth`, `WithConfigFile` | next, probably split by cohesion (counts and layout; pools and defaults; auth and config) |
| Network and messenger (`NetworkConfig`, `WithNetworkConfig`) | address family, separate cluster network, host address, Messenger mode, Messenger policy | done (`312-network-config`) |
| Services (own packages, own value types) | `rbd.WithPools`, `cephfs.WithFilesystems`, `rgw.WithGateways` | per package |
| Stay functional (Docker or container execution) | `WithHostNetwork`, `WithIdleEntrypoint` | none |

Open for the owner: whether `ceph.WithPools` belongs with topology or with the
service packages.

## Mirror `Run` (change `312-mirror-client`)

Proposal of the implementation side, not an owner approval. `rbd/mirror.Run` and
`cephfs/mirror.Run` each had a `RunWithImages` sibling that only derived the
daemon image from the clusters and forwarded to `Run`. Premise: one `Run` path
per package, the derived role image as the default, an explicit image as a
grouped value.

```go
link, err := rbdmirror.Run(ctx, cfg)                                  // image from the destination's RBDAux
link, err = rbdmirror.Run(ctx, cfg, rbdmirror.WithImageOptions(
    rbdmirror.ImageOptions{}.WithDaemonImage("registry.example.com/rbd-aux:20.2.4")))
```

| Old (removed, no alias) | New |
| --- | --- |
| `rbdmirror.RunWithImages(ctx, cfg, opts...)` | `rbdmirror.Run(ctx, cfg, opts...)` |
| `rbdmirror.Run(ctx, image, cfg, opts...)` | `rbdmirror.Run(ctx, cfg, rbdmirror.WithImageOptions(rbdmirror.ImageOptions{}.WithDaemonImage(image)), opts...)` |
| `cephfsmirror.RunWithImages` and `cephfsmirror.Run(ctx, image, ...)` | the same two forms in `cephfs/mirror` |

- **Why the positional `image` leaves.** It was the only image input, but it was
  also the only way to ask for the derived image (through the sibling), so two
  entry points existed for one setting. These `Run` functions are not the
  testcontainers `Run`: they take a `Config` and are module API, so the
  testcontainers signature rule does not apply. The remaining variadic keeps the
  raw daemon customizers.
- **Carrier.** `ImageOptions` is a plain value with a single chainable
  `WithDaemonImage`; `WithImageOptions(ImageOptions) Option` is the one carrier.
  `Option` has a no-op `Customize` like `ceph.Option`, so it satisfies the
  variadic's type, is consumed by `Run` before any effect and is never recorded
  among the daemon customizers that later `AddDaemon` calls reuse. The type and
  carrier exist once per package because the role differs; they are not shared
  with `ceph.ImageOptions`, which selects a cluster `ImageSet`.
- **Role inference is unchanged.** RBD derives the daemon image from the
  destination's `Images().RBDAux`, CephFS from the source's `Images().CephFS`.
  A cluster without that role is refused with `ErrInvalidRequest`; no other role
  substitutes. Explicit images still go through `RequireRoleImage`, so an image
  that a cluster selects for another role is refused. Setup clients still come
  from each cluster's `Images().Client`, and `AddDaemon` and the relaunch recipe
  keep using the image `Run` resolved (`Mirror.image`, `daemonImage`).
- **Composition (rules 1 to 5).** Unspecified is not empty: the zero value
  selects nothing, `WithDaemonImage("")` is a selection and is refused instead of
  being read as "derive". Two selections across values are refused even when
  equal; an unset value never conflicts. Within one value a later
  `WithDaemonImage` replaces the earlier one. A typed-nil `Option` is refused.
  Options are resolved right after the context check, before the topology
  checks, the cluster pair validation and every container or client call.
- **Copies (rule 7).** The value holds a string, so a struct copy is independent.
  The raw customizer list is a fresh slice in argument order and stays nil when
  no raw customizer was passed.

### Client entry points stay separate

`Container.WithClient` and `Container.WithClientIdentity` are not configuration
variants of one operation, so this change leaves them as they are. The source
shows two privilege contracts:

- `WithClient` copies the cluster's admin keyring and `ceph.conf` into the
  request. It takes no topology lock, does not check that the cluster is open and
  validates no identity.
- `WithClientIdentity` copies only a scoped keyring and a `ceph.conf` that
  references it. It takes the topology lock, refuses a terminated cluster, and
  refuses a descriptor that another cluster created, an incomplete creation, a
  revoked identity or a key that is no longer current. Its input is a live
  `*ClientConfig` handle whose pointer identity the ownership check compares,
  not a value to configure.

Folding them into one option would make "admin credentials" a default-looking
setting of an identity call. The issue requires admin access to stay visibly
distinct, so the two names stay. The shared tenant harness that the original
candidate named was removed, and `Tenant.WithClient` no longer exists.

## Not decided here

- How a future tenant-like attachment would keep a `*ClientConfig` identity
  while cloning its owned collections (no such API exists after the harness
  removal).
