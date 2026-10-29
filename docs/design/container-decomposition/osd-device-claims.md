# OSD device claims

Component record of `Container.deviceClaims`, the cluster-local claim table of
the host block devices that OSDs of one cluster are launched on
(`ceph/osd_device_claims.go`, type `osdDeviceClaims`, no mutex). The table and
its release hooks landed with #299; this record also covers the one serialized
wiring step added afterwards (`ceph/osd_device_wiring.go`). The design context
is [OSD device launch seam](../osd-device-launch-seam.md). 0.x: old names are
removed without aliases; this change renames only a guard test and removes no
public name.

## Callers and locks

| Function | Role | Lock |
| --- | --- | --- |
| `registerBlockDeviceOSD` | the only caller of `osdDeviceClaims.claim` and of `osdLaunchRecipe.attachBlockDevice`; no production caller of its own | holds `Container.mu`, does not take it |
| `removeOSD` | `release` after the container terminated | takes `lockTopology` |
| `terminate` | `release` per OSD, then `settle` after the labeled-container removal | takes `lockTopology` |

`Container.mu` is acquired by `lockTopology` (`ceph/topology.go:380`, which
calls `lockTopologyMutex`, `ceph/context_lock.go:11`). The wiring step runs
where `registerOSD` runs, inside the hold that `AddOSDWithConfig` keeps from
`ceph/ceph.go:548` (released by the defer at `:551`) over `registerOSD` (`:560`)
and `startOSD` (`:564`); `startOSD` itself does not use the lock
(`ceph/ceph.go:668-669`). The step adds no lock, goroutine or mutex. Its inner
order is fixed: input checks (pure), `registerOSD`, `attachBlockDevice`,
`claim`. The claim is written last, so the table never holds a claim for a
recipe without a device, and an input check publishes nothing.

## Lease-token ownership

The token is opaque: the table compares it and never interprets it, and the
step passes it through unchanged. The caller owns the external lease (file,
loop and device-mapper chain, its identity checks and its release barrier) and
keeps it through uncertain startup and owned cleanup. Nothing here releases,
shortens or hands over that lease. A dropped claim says only that no container
of this cluster can hold the device; it never authorizes release of the
DM/loop/backing chain. A claim that survives `Terminate` is named, with its
token, in the `Terminate` error.

## Failure unwinding

| Failure | Registered OSD | Recipe | Claim table |
| --- | --- | --- | --- |
| invalid device, invalid token, cancelled context | none | none | unchanged |
| `registerOSD` | as `registerOSD` leaves it | none | unchanged |
| attach refused (MemStore, volume storage) | stays owned for `Terminate` | sealed, never attempted | unchanged |
| claim refused (duplicate OSD, token, node, number) | stays owned for `Terminate` | sealed, never attempted | unchanged |
| success, then `release(osd, false)` before start | owned | sealed by the release | claim dropped |
| success, then any attempted or uncertain `startOSD` | owned | spent | claim kept until `removeOSD`, `terminate` or `settle` has container evidence |

Sealing spends the recipe's one-shot without creating a container, so a late
`startOSD` reports the ordinary "do not replay" error. A refused claim after
`registerOSD` leaves the registered OSD in place, as every later `registerOSD`
failure does; the table cannot predict an OSD ID before registration.

## Guards

`ceph/osd_device_wiring_test.go`: the caller table (`osdDeviceWiringCallers`,
empty: the single permitted caller is none, tests take the lock around it), the
pinned call sequence, the seal in both failure branches, the pass-through of
the token, device and the recipe's own one-shot, no lock, goroutine, defer or
extra import, no exported identifier; twelve mutated copies of the source and a
synthetic production caller must each be rejected.
`osd_device_claims_test.go` lists the step as the lock-holding caller of `claim`
and the two release hooks as lock-taking callers.
`osd_block_device_test.go` allows `attachBlockDevice` only inside the step.

## Chain-release gate (internal, no production caller)

`ceph/osd_device_release_gate.go` types the two receipts that must both exist
before a device-backed experiment (#63) can say its device chain is unused, and
the verdict that joins them. It is the lease-owner side of the claim table and
releases nothing.

| Receipt | Producer | Says | Does not say |
| --- | --- | --- | --- |
| `osdClusterExclusion` | `osdDeviceClaims.exclusionOf` (read-only, caller holds `Container.mu`) | no claim of this cluster names the lease, the `/dev/mapper` node or the major:minor | anything about the DM/loop/backing chain |
| `osdChainEvidence` | the external lease owner through `osdChainLeaseOwner` (one read-only method) | daemon stopped, borrows closed, a complete fresh opener scan found no user, all for the same lease and device identity | anything about this cluster's containers |

`judgeChainRelease` is pure. Zero or missing fields are unknown and retain;
a held claim retains whatever the owner says; a claim, an owner error, a
late answer (`collectChainEvidence` checks the context after the owner returns),
another identity, a running daemon, an open borrow, an incomplete scan or an
opener each retain with their own outcome. The only non-retaining outcome,
`osdChainClear`, means neither receipt objects. It is not authority to release:
the interface has no release method and the file adds no action on the chain.

Lock: `exclusionOf` runs under the `lockTopology` hold
(`ceph/topology.go:380`, `ceph/context_lock.go:11`) like `claimOf`. The owner
call can block on `/proc` and must run before or after that hold, never inside
it. The verdict takes no lock. A receipt is point-in-time: a later wiring of the
same device invalidates it, and sequencing that against the owner's release
barrier stays the first production caller's job. Guards in
`ceph/osd_device_release_gate_test.go`: empty caller table
(`osdChainReleaseCallers`), closed import set, no lock, goroutine, defer or
exported name, claim check before the owner outcome, a read-only table access,
and mutated copies of the source that must each be rejected.

Decisions taken under the repository owner's delegation of implementation-level
choices (2026-10-29):

- The lease token stays opaque and owned by the external lease owner. The gate
  never mints, rewrites or retires it; the owner is asked about it by value.
- The owner side is a one-method read-only interface (`chainEvidence`), so no
  type in the cluster can be handed release authority. Release authority stays
  outside this repository's code.
- Observations are tri-state with unknown as the zero value. An incomplete scan
  is unknown, not busy; a counted opener or open borrow is busy; evidence for
  another lease or device identity is its own outcome.
- A late owner answer is not evidence (context checked after the call), and a
  typed-nil owner is refused at admission.
- The first production caller collects owner evidence outside the topology lock,
  then reads `exclusionOf` under it, then judges. This order is documented, not
  enforced here, because there is no caller yet.
- The attachment observer itself is not built in Go: the merged Python observers
  already own descriptor interpretation, and a Go copy would be a second shape.
  A native evidence scenario is not added because nothing here can run natively.

It carries no capacity, allocation or ENOSPC notion (a finite device size is an
identity field only) and is separate from the file-backed tmpfs oracle in
`internal/integration`, which is untouched.

## What this does not establish

No attachment observer, native BlueStore device opening, effect window,
restart/reopen or cross-process exclusion. No native run: the controls use the
Docker-free OSD fakes only. The file-backed tmpfs capacity oracle is untouched.
The chain-release gate above is likewise uncalled: it observes no `/proc`, opens no
device and implements no lease owner.
