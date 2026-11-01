# Value configuration composition

These are the composition rules of the value configurations and of the entry
points that take them: the `Run` groups (`ceph.ClusterConfig`, `ceph.ImageOptions`,
`ceph.OSDStorage`, `ceph.NetworkConfig`) and the operation values
(`ceph.OSDOptions`, `ceph.DaemonOptions`, `ceph.ClientOptions`,
`ceph.ClientContainerOptions`). The `ceph` doc comments link to this page as the design note of these
values.

## Shape

- A configuration is a plain struct value with unexported fields and chainable
  `WithX` methods. Each method has a value receiver and returns a modified copy.
  There is no pointer builder, no shared mutable state and no universal options
  type: each operation domain has its own type (`OSDOptions` is not a Run
  option).
- An operation takes one trailing typed variadic of its value, for example
  `AddOSD(ctx, ...OSDOptions)`, `AddMonitor(ctx, name, ...DaemonOptions)`,
  `CreateClient(ctx, name, caps, ...ClientOptions)` and
  `WithClient(...ClientContainerOptions)`.
  Go allows only one trailing variadic, so a second input such as the daemon
  image is a field of the configuration type (`OSDOptions.WithImage`), never an
  untyped `...any` bag.
- A `WithX` method exists only when it sets a field that callers really vary.
  No method is added for visual uniformity.

## Composition rules

1. **Unspecified is not empty.** Every field records whether it was set. The
   zero value sets nothing. An explicit empty setting (`WithPlacement(OSDConfig{})`,
   `WithImage("")`) is a setting: the first is an explicit request for the
   defaults, the second is an invalid selection that is refused, never read as
   "no override".
2. **Order and scope.** An operation merges its values in argument order. A value
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
6. **Ordering of checks is part of the contract:** the context, then
   configuration validation, then the topology lock and the registration and
   cleanup path.
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
   `Customize` does nothing on purpose).

## AddOSD

`AddOSD(ctx, ...OSDOptions)` takes placement and image in one value type:
`AddOSD(ctx, OSDOptions{}.WithPlacement(cfg).WithImage(img))`.

- Placement reuses `OSDConfig`. `WithPlacement` takes the four fields as one
  unit, so an explicit placement is never mixed field by field with another
  value's placement (rule 3).
- The image check is the same function as `DaemonOptions.WithImage`
  (`validateDaemonImage`), so MON, MGR and OSD images are validated alike.
  `DaemonOptions` is the value of `AddMonitor` and `AddManager`.
- Hosts, racks, device class and root are validated and resolved by
  `resolveOSDDefaults` and `normalizeOSDConfig`.

## Run

`Run(ctx, img string, opts ...testcontainers.ContainerCustomizer)` and the
`rbd`, `cephfs` and `rgw` `Run` functions keep the testcontainers signature:
`Run` is the testcontainers-compatible entry point and its single trailing
variadic is the only place for configuration.

- **The grouped-carrier pattern (used by every Ceph configuration group).** A
  group is a plain value type with unexported fields and chainable,
  value-returning methods (`ClusterConfig`, `ImageOptions`, `OSDStorage`,
  `NetworkConfig`). The value is **not** a customizer and has no `Customize`
  method; passing it straight to `Run` does not compile. Each group has exactly
  one carrier option function, `With<Group>(value) Option` (here
  `WithImageOptions(ImageOptions)`), a `ceph.Option` that `Run` already
  recognises, applies to the cluster settings and validates before any effect.
  The carrier's `Customize` does nothing, so a group never reaches a container
  request. The carrier is not a `WithX` per field: one function per group.
  Docker and container execution policy remains testcontainers-style `WithX`.
- **Service inputs are not forwarded.** `rbd.WithPools`, `cephfs.WithFilesystems`,
  `rgw.WithGateways` and every group carrier are `ceph.Option` values that `Run`
  applies to the settings; they never reach a container request. A test applies a service
  option together with an image set and fails if either is dropped.
- **The `img` argument with an image set.** It is not a second image input: empty
  or equal to the set's Essential (after `All` expansion) is accepted and
  anything else is `ErrInvalidRequest` before effects. Without an image set the
  argument is the aggregate image.
- **Composition (rules 1 to 5).** The zero value selects nothing. An explicit set
  is a selection: an explicit empty set is refused, not read as unspecified. At
  most one `WithImageOptions` in the variadic selects a set; a second selection
  is refused even when equal, so argument order never decides a conflict. Within
  one value the later `WithImageSet` replaces the earlier one. Everything is
  validated once, before any container, network or volume is created, and
  missing roles that a requested feature needs are refused with no fallback to
  Essential.
- **Copies (rule 7).** `ImageOptions` holds an `ImageSet`, a struct of strings, so a
  struct copy is independent and there is no slice to clone. A member that
  holds a slice must use `slices.Clone`, which keeps nil and non-nil empty apart.
- **Raw customizers.** They reach the first MON's container request only, in
  argument order, after the module's own request options and before the
  messenger environment, the `WithConfigFile` file and the ownership label.

### Topology and placement group

```go
cluster := ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(3).WithPoolDefaults(3, 2)
c, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(cluster))
```

The group holds the MON count, the managers, the initial OSDs, the default
CRUSH root, the pool defaults and the initial pools. Their combination rules
(pool defaults derived from the OSD count, a layout that can hold the default
root and the pools, no initial pools or services without initial OSDs, no
initial CephFS or RGW without initial managers) are checked together in `Run`
before any resource is created.

- **One field per choice.** `WithManagerCount` and `WithNoInitialManagers` set
  one field, and `WithOSDCount`, `WithInitialOSDs` and `WithNoInitialOSDs` set
  another, because each pair or triple is one choice. Within one value the
  later call replaces the earlier one; two values that set the field are
  refused, so "no OSDs" and "three OSDs" can never both be requested.
- **Rules applied.** The zero value changes nothing. A count below one, an
  empty layout, an invalid root and invalid pool defaults are refused as
  explicit invalid settings. `WithPools()` with no argument is an explicit empty
  list. A value is validated as a whole before any of its fields is written.
- **Copies (rule 7).** The layout is cloned with `slices.Clone` and the pools with
  their nested erasure-code settings, when they enter the value and again when
  the settings are written.

### Image group

`ImageOptions` holds the image set and the CephFS and RGW role images.
`WithCephFSImage` and `WithRGWImage` replace their role of the set, or of the
aggregate `Run` image without a set; each is set at most once across values and
a blank image is refused.

### OSD storage group

```go
storage := ceph.OSDStorage{}.WithBlockSize(256 << 20).WithObjectStore(ceph.OSDMemStore)
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDStorage(storage))
```

- **Rules applied.** The zero value changes nothing. Every field records that it
  was set, so `WithBlockSize(0)`, `WithObjectStore("")` and `WithInMemory(0)` are
  refused as explicit invalid settings and never read as the default. Two
  `WithOSDStorage` options that set the same field are refused even when equal;
  different fields compose in any order. Within one value a later call replaces
  the earlier one. The combination rules (MemStore with in-memory or
  volume storage, in-memory with volume storage) refuse
  before any resource is created.
- **Reconciliation with `OSDConfig` and `OSDOptions`.** `OSDConfig` is the
  per-OSD placement and `OSDOptions` (AddOSD) is per-call placement and image.
  These storage settings are cluster-wide: the runtime keeps one volume or one
  tmpfs for all OSDs and AddOSD reads the cluster settings. A per-OSD
  `OSDConfig{}.WithVolume()` would need per-OSD storage plumbing that does not
  exist, so it is not offered and `OSDConfig` is not made to carry storage.
  Placement stays in `OSDConfig`; storage stays cluster-wide in `OSDStorage`.
- **No slice or map member**, so a struct copy is independent. A member that holds
  one must use `slices.Clone`, which keeps nil and non-nil empty apart.

### Network and messenger group

```go
network := ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack).WithSeparateClusterNetwork()
cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNetworkConfig(network))
```

The group holds the address family, the separate cluster network, the host
address, the Messenger mode and the Messenger policy. Their combination rules
live together in `Run` (host address needs host network, host network excludes
a separate cluster network and non-IPv4 families, the mapped family excludes a
separate cluster network, a policy excludes `MessengerV2Secure`) and refuse
before any resource is created.

- **What stays out.** `WithHostNetwork` is Docker execution policy and stays a
  testcontainers-style function. `ClientContainerOptions.WithMessengerPolicy` is
  a setting of the `WithClient` container, not a `Run` setting.
- **Rules applied.** The zero value changes nothing. Every field records that it
  was set, so the default family `AddressIPv4` and `MessengerDefault` are settings
  (a second carrier cannot override them), while `WithHostAddress("")`, an unknown
  family or mode and an empty list in a policy are refused as explicit invalid
  settings. Two `WithNetworkConfig` options that set the same field are refused
  even when equal; different fields compose in any order. Within one value a
  later call replaces the earlier one. A value is validated as a whole before any
  of its fields is written, so a refused value changes no setting.
- **Copies (rule 7).** `MessengerPolicy` holds three slices. The value clones them
  with `slices.Clone` when the policy enters it and again when the settings are
  written, so a caller reusing its slices, a derived value and the settings never
  share storage, a nil list stays nil (Ceph's default) and a non-nil empty list
  stays empty and is refused.

### What stays a function

Docker and container execution policy (`WithHostNetwork`, `WithIdleEntrypoint`,
`WithStartupTimeout`) stays a testcontainers-style `WithX`. `WithConfigFile`
follows the `WithConfigFile` convention of testcontainers-go modules, and
`WithAuth` already carries one `AuthConfig` value. Each value group has exactly
one carrier, so no setting has two spellings.

## Cross-cluster fixture `Run`

`rbd/mirror`, `cephfs/mirror` and `rgw/multisite` have the same single entry
point as every module, `Run(ctx, img string, opts ...testcontainers.ContainerCustomizer)`.

- `img` is the image of the containers the fixture creates (the rbd-mirror,
  cephfs-mirror or gateway daemons) and is required. It is never derived from
  the clusters.
- The fixture configuration is a plain `Config` struct carried by exactly one
  `WithConfig` option (multisite: `WithConfig` or `WithTopology`). The option's
  `Customize` does nothing. `Run` refuses a missing carrier and a second one
  before it queries any cluster (`multicluster.SplitConfig`). The carrier copies
  the slices of its config.
- The other customizers reach the daemon containers, as before.

## Client entry points stay separate

`Container.WithClient` copies the admin keyring and takes no lock, closed check
or identity validation. `ClientConfig.WithIdentity` copies a scoped keyring,
takes the topology lock, and refuses a terminated cluster, a descriptor from
another cluster, an incomplete creation, a revoked identity and a stale key; its
input is a live `*ClientConfig` handle compared by pointer. They are different
privilege contracts, not configuration variants, so both names stay.
