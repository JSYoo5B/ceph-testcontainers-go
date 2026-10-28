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

## What this does not establish

No attachment observer, native BlueStore device opening, effect window,
restart/reopen or cross-process exclusion. No native run: the controls use the
Docker-free OSD fakes only. The file-backed tmpfs capacity oracle is untouched.
