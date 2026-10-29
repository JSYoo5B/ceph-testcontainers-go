# Ceph RGW multisite

## Introduction

The `rgw/multisite` package builds RGW multisite fixtures on independent Ceph
clusters: one realm spanning two or more clusters, adding and removing zones
and zonegroups, period transfer for planned failover, sync status observation,
and selective replication with sync policies.

Ordinary gateway use stays in the `rgw` package ([`rgw`](../README.md)). `rgw/multisite`
imports `rgw` and reuses `rgw.Gateway` and its lifecycle; `rgw` does not import
`rgw/multisite`. Gateways, S3 users, tenants, quotas and placement keep their
`rgw` documentation.

## Adding this module to your project dependencies

The package is part of the `rgw` module path:

```
go get github.com/JSYoo5B/ceph-testcontainers-go/rgw
```

```golang
import "github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
```

## Module Reference

### Run function

RGW multisite connects independent Ceph clusters as zones of one realm. Users
and bucket metadata are shared across the realm; object data replicates between
the zones of a zonegroup. The multisite fixture owns the gateways, one setup CLI
container per zone and, in bridge mode, a private HTTP network between the
gateways. It does not own the clusters.

Use fresh clusters created with `ceph.Run`: a cluster must not have gateways or
existing RGW configuration (`rgw.Run` and `rgw.Start` are not used on them).
All clusters must use the same network mode. In host mode the advertised
addresses of all clusters must be reachable from each other.

Register the clusters' cleanup first and the fixture's cleanup afterwards, so
that the fixture terminates first. A non-nil fixture returned with an error
owns partial resources and must still be terminated.

```golang
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Fixture, error)
func WithConfig(config Config) Option
func WithTopology(config TopologyConfig) Option
```

`img` is the gateway image and is required; the setup clients use each
cluster's RGW image unless `SetupImage` is set. Exactly one `WithConfig` or
`WithTopology` among the options selects the realm; the other options customise
every gateway and are applied last.

#### WithConfig

Configures a two-zone realm. `Config.Source` becomes the metadata master zone;
both zones accept object writes.

It creates one ordinary S3 test user on the master. Both gateways expose its
keys in `AccessKey` and `SecretKey`, and their `Region` is the zonegroup name.
Setup is bounded by six minutes in addition to `ctx`. Before writing objects
into a new bucket, wait for its metadata in both zones (see
[`WaitSyncReady`](#waitsyncready)).

```golang
source, err := ceph.Run(ctx, ceph.DefaultImage)
testcontainers.CleanupContainer(t, source)
if err != nil {
	t.Fatal(err)
}
destination, err := ceph.Run(ctx, ceph.DefaultImage)
testcontainers.CleanupContainer(t, destination)
if err != nil {
	t.Fatal(err)
}

ms, err := multisite.Run(ctx, ceph.DefaultImage, multisite.WithConfig(multisite.Config{
	Source: source, Destination: destination,
	SourceZone: "east", DestinationZone: "west",
}))
if ms != nil {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := ms.Terminate(ctx); err != nil {
			t.Error(err)
		}
	})
}
if err != nil {
	t.Fatal(err)
}
```

#### WithTopology

Creates one realm over two or more fresh clusters, with the same ownership and
cleanup contract as `WithConfig`. The zone lists are copied. Use `Zones`, `Zonegroup` and
`MetadataMaster` for a single zonegroup, or `Zonegroups` and `MasterZonegroup`
for several zonegroups, each with its own master zone. The initial metadata
master becomes `Source`; the first remaining zone becomes `Destination`.
`Zones()` returns all zones.

```golang
topology, err := multisite.Run(ctx, ceph.DefaultImage, multisite.WithTopology(multisite.TopologyConfig{
	Realm: "application", Zonegroup: "us-east-1", MetadataMaster: "a",
	Zones: []multisite.ZoneConfig{
		{Name: "a", Cluster: clusterA},
		{Name: "b", Cluster: clusterB},
		{Name: "c", Cluster: clusterC},
	},
}))
```

#### Config

| Field | Meaning |
| --- | --- |
| `Source`, `Destination` | `*ceph.Container` of two fresh clusters. `Source` is the initial metadata master. |
| `Realm` | Realm name. Defaults to a generated name. |
| `Zonegroup` | Zonegroup name. Defaults to `us-east-1`. |
| `SourceZone`, `DestinationZone` | Zone names. Default to generated names. Set them when the test refers to zones by name (sync policies, `ZoneAdmin`, `SyncStatus`). |
| `SetupImage` | Optional image for every setup client. When empty, each zone's client uses its own cluster's RGW image. |

#### TopologyConfig

| Field | Meaning |
| --- | --- |
| `Zones` | `[]ZoneConfig` for a single zonegroup; at least two. |
| `Realm` | Realm name. Defaults to a generated name. |
| `Zonegroup` | Zonegroup name for `Zones`. Defaults to `us-east-1`. |
| `MetadataMaster` | Initial metadata master zone. Defaults to the first zone. |
| `SetupImage` | Optional shared setup client image, also used for zones added later. |
| `Zonegroups` | `[]ZonegroupConfig` for several zonegroups. Cannot be combined with `Zones`, `Zonegroup` or `MetadataMaster`. |
| `MasterZonegroup` | Zonegroup whose master zone becomes the realm metadata master. Defaults to the first zonegroup. Requires `Zonegroups`. |

Zone names and clusters must be unique across the whole topology.

#### ZoneConfig

| Field | Meaning |
| --- | --- |
| `Name` | Zone name (required). |
| `Zonegroup` | Zonegroup of the zone. In `AddZone`, empty selects the initial master zonegroup. |
| `Cluster` | `*ceph.Container` of a fresh, independent cluster. |

#### ZonegroupConfig

| Field | Meaning |
| --- | --- |
| `Name` | Zonegroup name. |
| `MasterZone` | Master zone of this zonegroup. Defaults to the first zone. |
| `Zones` | At least one `ZoneConfig`. |

#### Fixture fields

| Field | Meaning |
| --- | --- |
| `Source`, `Destination` | `*rgw.Gateway` of the initial master zone and of the first other zone. They are not reassigned when the master changes. |
| `RealmID`, `SourceZoneID`, `DestinationZoneID` | Native IDs. |

### Fixture Methods: Topology

#### Zones

```golang
func (m *Fixture) Zones() []Zone
```

Returns the owned zones sorted by name, including a partial zone returned by a
failed `AddZone`. Private system-user credentials are never exposed. See
[`Zone`](#zone).

#### Zonegroups

```golang
func (m *Fixture) Zonegroups() []Zonegroup
```

Returns the owned zonegroups sorted by name, each with its attached zones.
Removed zones disappear once their removal period is committed. `MasterZone`
reflects the last topology operation; after a native master promotion done
with raw commands it is refreshed by the next topology operation.

#### AddZone

```golang
func (m *Fixture) AddZone(ctx context.Context, image string, config ZoneConfig, opts ...testcontainers.ContainerCustomizer) (*Zone, error)
```

Joins another fresh cluster to an existing zonegroup of the realm. It finds the
metadata master from the current committed period, verifies every registered
zone's identity and endpoint, and refreshes the period in all zones. Running
gateways restart to load the final period, keeping their listener ports and
storage; gateways you stopped stay stopped. Running secondaries finish initial
metadata sync before they reload. `image` selects the new gateway; the new
setup client uses the new cluster's RGW image unless the fixture has a
`SetupImage`. Bounded by six minutes in addition to `ctx`.

Call `S3Endpoint` again on every gateway afterwards. Finish any raw master
promotion before calling `AddZone`, or keep the previous master's gateway
stopped. If `AddZone` fails, the partial zone stays owned: terminate the
fixture rather than retrying the same name or adopting an external zone.

#### AddZonegroup

```golang
func (m *Fixture) AddZonegroup(ctx context.Context, image string, config ZonegroupConfig, opts ...testcontainers.ContainerCustomizer) (*Zonegroup, error)
```

Adds a non-master zonegroup with one or more fresh clusters. `MasterZone`
selects its local master; the realm metadata master does not change. Metadata
is shared across zonegroups; object data replicates only within each
zonegroup. Refresh `S3Endpoint` values afterwards. On error, the new groups,
zones and containers stay owned until `Terminate`.

#### RemoveZone

```golang
func (m *Fixture) RemoveZone(ctx context.Context, name string) error
```

Detaches a non-master zone from its zonegroup, commits and propagates the new
period, and then removes the zone's gateway and setup client. The realm, zone
configuration, users, pools and objects stay in that zone's cluster; nothing is
purged. A removed name or cluster cannot be re-added with `AddZone`. The last
zone of a zonegroup and the current realm or zonegroup master cannot be
removed; promote another zone first with native commands. A partial failure can
be retried with the same name. Remaining gateways may reload, so refresh their
endpoints.

#### Terminate

```golang
func (m *Fixture) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
```

Removes the owned gateways, setup clients and the private HTTP network. It
restores interrupted zone links. The clusters are not terminated, and the
realm, zone configuration and data remain in them until the clusters are
terminated.

### Fixture Methods: Periods, failover and native commands

#### SourceAdmin

```golang
func (m *Fixture) SourceAdmin(ctx context.Context, args ...string) ([]byte, error)
```

Runs `radosgw-admin` in the source cluster's setup client, scoped to the realm,
the source zonegroup and the source zone. It keeps working after the
destination has become metadata master.

#### DestinationAdmin

```golang
func (m *Fixture) DestinationAdmin(ctx context.Context, args ...string) ([]byte, error)
```

Same as `SourceAdmin` for the destination zone.

#### ZoneAdmin

```golang
func (m *Fixture) ZoneAdmin(ctx context.Context, name string, args ...string) ([]byte, error)
```

Runs `radosgw-admin` in any owned zone by name, including a partial zone whose
setup client started. Do not edit period or zone state with it while `AddZone`
or a raw master change is in progress.

```golang
period, err := topology.ZoneAdmin(ctx, "c", "period", "get", "--format", "json")
```

#### PullSourcePeriod

```golang
func (m *Fixture) PullSourcePeriod(ctx context.Context) error
```

Imports the destination's realm and current committed period into the source
cluster with native `realm pull`, using the fixture's private system-user
credentials. `realm pull` also moves the realm's current-period pointer and
updates the local zonegroups; a plain `period pull` only stores the fetched
period and is not enough to recover a site. Use it to bring back the former
master after the destination was promoted. Stop the former master's gateway
before calling it, and start the gateway again afterwards. It does not promote
a zone or restart a gateway.

#### PullDestinationPeriod

```golang
func (m *Fixture) PullDestinationPeriod(ctx context.Context) error
```

Imports the source's realm and current committed period into the destination
cluster. Use it after a failback, and to deliver a period published by the
source (for example after `ApplyPlacement`). Promotion and gateway restart
remain caller decisions.

#### InterruptZoneLink

```golang
func (m *Fixture) InterruptZoneLink(ctx context.Context, zoneName string) (*fault.NetworkInterruption, error)
```

Disconnects one zone's gateway from the private multisite HTTP network. Its S3
endpoint and its connection to its own cluster stay up, so local reads and
writes continue while replication to and from that zone stops. Realm and
period membership do not change. `Restore` on the returned handle reconnects
the original IP and aliases. A non-nil handle returned with an error must also
be restored, because a transport failure cannot prove that Docker did not
disconnect the endpoint. The fixture restores remaining interruptions during
`Terminate`. Host mode has no Docker endpoint to disconnect and is rejected.
See [guides/fault-injection.md](../../docs/guides/fault-injection.md).

#### ApplyZoneLinkProfile

```golang
func (m *Fixture) ApplyZoneLinkProfile(ctx context.Context, zoneName, peerZone string, delay fault.NetworkProfile) (*fault.NetworkProfileLease, error)
```

Delays or drops, below RGW, only the packets that the gateway of `zoneName`
sends to the gateway of `peerZone` on the private multisite HTTP network: to
the peer gateway's address there and the port of its peer endpoint. They carry
the requests that `zoneName` makes of `peerZone`, its sync reads and its
notifications. Connections that `peerZone` opens to `zoneName` answer from
`zoneName`'s own port and are not selected, so `peerZone` keeps syncing from
`zoneName`: one direction of replication stalls while the other continues.
Local S3 endpoints, the clusters and other zones' links are untouched.

It is an opt-in extension like
[ApplyNetworkProfile](../../fault/README.md#applynetworkprofile), applied through
[fault.ApplyLinkProfile](../../fault/README.md#standalone-bridge-functions): `delay` names the netem
helper image and the delay, jitter or loss, and leaves `Plane`, `Peer`,
`PeerPort` and `Peers` empty; the fixture selects the network and the peer and
returns them in the lease. The checks, errors, `Status` and `Restore` are
those of `ApplyNetworkProfile`. A non-nil lease returned with an error must
be restored. The fixture restores remaining leases during `Terminate`. Host
mode has no private multisite network and is rejected.

```golang
cut, err := ms.ApplyZoneLinkProfile(ctx, "west", "east", fault.NetworkProfile{
	HelperImage: fault.NetemHelperImage,
	Loss:        100,
})
if cut != nil {
	defer cut.Restore(cleanupCtx)
}
if err != nil {
	t.Fatal(err)
}
// west no longer reads east's changes; east still reads west's.
if err := cut.Restore(ctx); err != nil {
	t.Fatal(err)
}
if _, err := ms.WaitSyncReady(ctx, "west", "east"); err != nil {
	t.Fatal(err)
}
```

### Fixture Methods: Sync status

#### SyncStatus

```golang
func (m *Fixture) SyncStatus(ctx context.Context, zoneName string) (SyncStatus, error)
```

Reads the metadata and data sync state of an owned zone: committed period,
realm epoch, metadata shards and, for each source zone, the native comparison
with that source's log (see [`SyncStatus`](#syncstatus-type)). It is a
point-in-time observation bounded by 30 seconds.

#### WaitSyncReady

```golang
func (m *Fixture) WaitSyncReady(ctx context.Context, zoneName string, sourceZones ...string) (SyncStatus, error)
```

Waits up to four minutes (or until `ctx` ends) until the zone's metadata sync
and the data sync from each listed source zone are caught up. Without source
zones it waits for metadata only. It never changes lease durations, unlocks
objects or restarts daemons; after an abrupt gateway restart, native sync
leases must expire before sync resumes. Caught-up status does not prove that a
particular object arrived: quiesce writes and verify the objects with the S3
client.

```golang
if _, err := ms.WaitSyncReady(ctx, "west", "east"); err != nil {
	t.Fatal(err)
}
```

Wait for metadata in every zone after you create a bucket and before you
write objects into it. RGW can otherwise lose those objects in another zone,
as seen with Tentacle 20.2.4: that zone's data sync stores the new bucket's
metadata itself, a second data sync shard may sync the bucket while its index
does not exist yet, and RGW takes the failed index updates for deleted
objects. The zone then reports that it is caught up, and the objects are
missing until they change again.

```golang
// Create the bucket through the S3 client, then:
for _, zone := range ms.Zones() {
	if _, err := ms.WaitSyncReady(ctx, zone.Name); err != nil {
		t.Fatal(err)
	}
}
// Write objects into the bucket.
```

### Fixture Methods: Sync policy

Sync policies select what replicates: groups hold flows (which zones may
exchange data) and pipes (which buckets, prefixes and tags replicate, and how).
A group is stored on the zonegroup or on one existing bucket. The fixture
creates fresh groups only and never adopts an existing group with the same ID.
Every mutation compares the native policy before and after the change and
refuses unrelated or external changes. If the reply of a confirmed mutation is
lost, the next call on the same handle reconciles the pending change before
continuing. Do not edit the same policy with raw commands while these methods
run. Group mutations are bounded by two minutes. See
[Selective sync with a sync policy](#selective-sync-with-a-sync-policy) for a
complete recipe and the Ceph
[multisite sync policy](https://docs.ceph.com/en/tentacle/radosgw/multisite-sync-policy/)
documentation for the native semantics.

#### CreateSyncGroup

```golang
func (m *Fixture) CreateSyncGroup(ctx context.Context, scope SyncPolicyScope, config SyncGroupConfig) (*SyncGroup, error)
```

Stores a fresh, empty group on the current metadata master (see
[`SyncPolicyScope`](#syncpolicyscope) and [`SyncGroupConfig`](#syncgroupconfig)).
A bucket-scoped group records the bucket's native instance ID, so a bucket that
is deleted and recreated with the same name is refused later. The bucket must
already exist; create it with the S3 client first.

Zonegroup groups take effect after `ApplySyncGroup`. Bucket groups are dynamic
metadata and need no period commit. If creation fails ambiguously, the returned
handle is unconfirmed and cannot remove the uncertain configuration: inspect it
with `SourceAdmin` (`sync group get`) or discard the clusters.

#### CreateSyncFlow

```golang
func (m *Fixture) CreateSyncFlow(ctx context.Context, group *SyncGroup, config SyncFlowConfig) error
```

Adds a flow to an owned group (see [`SyncFlowConfig`](#syncflowconfig)). A
directional flow is identified natively by its source/destination pair, not by
an ID. Bucket groups may only narrow the flows allowed by their zonegroup.

#### CreateSyncPipe

```golang
func (m *Fixture) CreateSyncPipe(ctx context.Context, group *SyncGroup, config SyncPipeConfig) error
```

Adds a pipe to an owned group after validating every zone, bucket and principal
(see [`SyncPipeConfig`](#syncpipeconfig)). The pipe ID must be unique in the
group. A stored pipe does not prove effective replication; verify positive and
negative writes with the S3 client. This helper never creates buckets or
grants S3 permissions.

#### SetSyncGroupStatus

```golang
func (m *Fixture) SetSyncGroupStatus(ctx context.Context, group *SyncGroup, status SyncGroupStatus) error
```

Changes a group between `SyncAllowed`, `SyncEnabled` and `SyncForbidden`. A
zonegroup group needs `ApplySyncGroup` afterwards; for a bucket group, wait with
`WaitBucketSyncPolicyReady` before writing objects that depend on the change.

#### SetSyncPipePrefix

```golang
func (m *Fixture) SetSyncPipePrefix(ctx context.Context, group *SyncGroup, id, prefix string) error
```

Changes the source prefix of one pipe; an empty prefix removes the filter. It
affects future replication only: objects written earlier are not backfilled,
and objects already copied are not deleted. The method confirms the master's
stored policy, not its import into other zones. For a bucket group, call
`WaitBucketSyncPolicyReady` for the destination before writing objects that
depend on the new prefix.

#### ApplySyncGroup

```golang
func (m *Fixture) ApplySyncGroup(ctx context.Context, group *SyncGroup) error
```

For a zonegroup group: publishes this group's change in a new period, pulls the
period into every owned zone and reloads the running gateways. It refuses to
run when the current period, a locally pending change or the staging period
contains unrelated edits, so that `period update` cannot overwrite them. Call
`S3Endpoint` again afterwards, and use `WaitSyncReady` before relying on
replication. A partial publication or reload is not rolled back; retry with the
same handle after fixing the cause. Also call it after removing a zonegroup
group with `RemoveSyncGroup`. Bounded by four minutes.

For a bucket group it only verifies that the stored policy still matches the
handle, because bucket policy is dynamic.

#### RemoveSyncPipe

```golang
func (m *Fixture) RemoveSyncPipe(ctx context.Context, group *SyncGroup, id string) error
```

Removes one pipe from an owned group. The pipe must exist in the group's native
policy, and the removal must leave the rest of the group unchanged. Already
replicated objects stay.

#### RemoveSyncFlow

```golang
func (m *Fixture) RemoveSyncFlow(ctx context.Context, group *SyncGroup, config SyncFlowConfig) error
```

Removes a flow present in the group. A symmetrical flow is removed as a whole
named set; a directional flow is selected by its zone pair. Other groups are
never changed.

#### RemoveSyncGroup

```golang
func (m *Fixture) RemoveSyncGroup(ctx context.Context, group *SyncGroup) error
```

Removes a confirmed group if its native policy is unchanged; an externally
modified group is refused. Bucket and object data remain. Removal is
idempotent across copies of the handle. A zonegroup group needs
`ApplySyncGroup` afterwards to publish the removal.

#### WaitBucketSyncPolicyReady

```golang
func (m *Fixture) WaitBucketSyncPolicyReady(ctx context.Context, group *SyncGroup, destinationZone string) (BucketSyncPolicyStatus, error)
```

Waits until a bucket group's policy has been imported into the metadata of a
destination zone selected by its pipes. It compares the master's committed
period and the group snapshot with the destination's copy, and checks that the
destination has every bucket instance the pipes need (see
[`BucketSyncPolicyStatus`](#bucketsyncpolicystatus)). Missing or older local
metadata is waited for; a recreated bucket or a changed master policy is
refused. It waits up to four minutes (including time spent queued behind other
fixture operations), and each native read is bounded by 30 seconds.

RGW decides where to fetch data from using the destination's local copy of
the source bucket's policy, so objects written before the import may not be
replicated. Call this after creating or changing pipes and before writing
the objects the test expects to replicate. It is read-only: it does not pull
periods, restart gateways or replay objects written before the import, and it
does not prove permissions, payload delivery or data checkpoints.

Zonegroup groups, removed handles, pending mutations and zones outside the
pipes' destinations are rejected. Groups with status `allowed` or `forbidden`
can also be observed as imported.

#### BucketSyncStatus

```golang
func (m *Fixture) BucketSyncStatus(ctx context.Context, group *SyncGroup, pipeID, sourceZone, destinationZone string) (BucketSyncStatus, error)
```

Reads the per-bucket replication checkpoint for the source and destination
buckets captured by an owned, unchanged group and pipe, between two explicit
owned zones (see [`BucketSyncStatus`](#bucketsyncstatus-type)). A pipe with a
wildcard bucket selector requires a bucket group. Bounded by 30 seconds.

#### WaitBucketSyncReady

```golang
func (m *Fixture) WaitBucketSyncReady(ctx context.Context, group *SyncGroup, pipeID, sourceZone, destinationZone string) (BucketSyncStatus, error)
```

Waits up to four minutes (or until `ctx` ends) until the bucket checkpoint is
caught up: incremental state, every shard current, the same imported period on
both ends. Disabled, full-sync, missing-source and native per-source error
states never satisfy it, even when the CLI exits with 0. It changes no sync
state, leases, permissions or data.

RGW combines the checkpoints of all pipes that match the same bucket pair, so a
caught-up checkpoint does not prove the filter or user mode of a particular
pipe. Stop writing, wait for the checkpoint, and then check the replicated
objects and the absence of excluded ones with the S3 client.

### Multisite types

#### Zone

| Field | Meaning |
| --- | --- |
| `Name`, `ID`, `Zonegroup` | Zone identity. |
| `PeerEndpoint` | Address used between gateways and setup clients (private HTTP network or Docker host). Not for application clients. |
| `Gateway` | `*rgw.Gateway` of the zone. Use `Gateway.S3Endpoint` for application clients. |

#### Zonegroup

| Field | Meaning |
| --- | --- |
| `Name`, `ID` | Zonegroup identity. |
| `MasterZone` | Master zone as of the last topology operation. |
| `Zones` | Attached zones, sorted by name. |

#### SyncPolicyScope

| Field | Meaning |
| --- | --- |
| `Zonegroup` | Owned zonegroup. Empty selects the source zone's zonegroup. |
| `Bucket` | Existing bucket for a bucket-scoped group. Empty selects the zonegroup. |
| `Tenant` | Tenant of `Bucket`. Requires `Bucket`. |

#### SyncGroupConfig

| Field | Meaning |
| --- | --- |
| `ID` | Group ID (no spaces or `,/:$`, at most 128 characters). Empty generates one. |
| `Status` | A `SyncGroupStatus` constant. Required. |

#### SyncGroupStatus

| Constant | Value | Meaning |
| --- | --- | --- |
| `SyncAllowed` | `allowed` | Permits replication without activating it. A bucket group may enable a subset. |
| `SyncEnabled` | `enabled` | Activates the group's pipes. |
| `SyncForbidden` | `forbidden` | Blocks replication; takes precedence over other matching enabled groups. |

#### SyncGroup

A handle to a group created by `CreateSyncGroup`. `ID()` returns the group ID.
Copies share the native readback and lifecycle state. `String` and `GoString`
print only an identifying label.

#### SyncFlowConfig

| Field | Meaning |
| --- | --- |
| `ID`, `Zones` | Symmetrical flow: a named set of zones that replicate with each other. |
| `SourceZone`, `DestinationZone` | Directional flow. Leave `ID` and `Zones` empty; the pair is the native identity. |

Zone names must be owned by the fixture and attached to the group's zonegroup.

#### SyncPipeConfig

| Field | Meaning |
| --- | --- |
| `ID` | Pipe ID, unique in the group. Required. |
| `SourceZones`, `DestinationZones` | Owned zone names, or a single `"*"`. Same-zone replication is not supported by RGW. |
| `Prefix` | Source object key prefix. Empty selects all keys. |
| `SourceBucket`, `DestinationBucket` | `*SyncBucketSelector`. `nil` keeps Ceph's wildcard (the bucket of a bucket group, or every bucket). A concrete selector captures the bucket's current instance ID. |
| `Tags` | `[]SyncObjectTag`. Objects match if they carry any of the pairs. Combined with `Prefix`, both must match. |
| `Priority` | Native pipe priority. See [Selective sync limitations](#selective-sync-limitations-on-the-default-image). |
| `DestinationOwner` | `*User` (ordinary user). Replicated objects are owned by this user (ACL translation). Requires a concrete destination bucket. |
| `User` | `*User` for user mode: replication uses this principal's permissions instead of system permissions. An ordinary user, or an account root user when both concrete buckets belong to that account and `DestinationOwner` is empty. Requires a concrete destination bucket. |
| `DestinationStorageClass` | Storage class for replicated objects. Requires a concrete destination bucket and `DestinationPlacements`. |
| `DestinationPlacements` | `map[string]*Placement`: a confirmed placement for each concrete destination zone, created with `CreatePlacement` in that zone. Publish, import and reload the placements before replicating. |

Principal rules: the tenant of `DestinationOwner` or `User` must match the
tenant of every selected bucket. IAM non-root users, cross-account and
cross-tenant principal translation are refused by this helper (this is a scope
decision of the module, not a statement about Ceph). In system mode (no
`User`), concrete source and destination buckets may belong to different
tenants, provided no owner or user translation is requested. RGW preserves
source object ACLs, so the destination bucket owner may not be the principal
that can read the replicated objects.

#### SyncBucketSelector

| Field | Meaning |
| --- | --- |
| `Name` | Existing bucket name. |
| `Tenant` | Bucket tenant, if any. |

#### SyncObjectTag

| Field | Meaning |
| --- | --- |
| `Key` | Exact tag key: 1 to 128 characters, no `,` or `=`. |
| `Value` | Exact tag value: up to 256 characters, no `,` (`=` is allowed). |

Pairs must be unique; repeated keys with different values are allowed. Commas
cannot be represented because the native CLI uses them as separators.

#### SyncBucketIdentity

| Field | Meaning |
| --- | --- |
| `Name`, `Tenant` | Bucket namespace. |
| `ID` | Native bucket instance ID. A recreated bucket has a different ID. |

#### SyncStatus type

| Field | Meaning |
| --- | --- |
| `RealmID`, `PeriodID`, `RealmEpoch` | Committed period seen by the zone. |
| `Zone`, `ZoneID`, `ZonegroupID` | The observed zone. |
| `MetadataMaster` | Whether this zone is the metadata master. |
| `Metadata` | `MetadataSyncStatus`. |
| `DataSources` | `[]DataSyncStatus`, one per source zone. |

#### MetadataSyncStatus

| Field | Meaning |
| --- | --- |
| `State`, `PeriodID`, `RealmEpoch` | Native metadata sync state and period. |
| `Shards`, `IncrementalShards` | Shard counts. |
| `CaughtUp` | All shards incremental and current. |

Metadata keys, markers and credentials are omitted.

#### DataSyncStatus

| Field | Meaning |
| --- | --- |
| `SourceZone`, `SourceZoneID` | Source of this data stream. |
| `Syncing` | Sync is running. |
| `Shards`, `IncrementalShards`, `FullShards`, `BehindShards`, `RecoveringShards` | Shard counts. |
| `CaughtUp` | Running, all shards incremental, none recovering, and the native comparison with the source log reports no lag. A disabled pipe never counts as caught up. |

#### BucketSyncStatus type

| Field | Meaning |
| --- | --- |
| `RealmID`, `PeriodID`, `RealmEpoch`, `ZonegroupID` | Period seen by both ends. |
| `SourceZone`, `SourceZoneID`, `Zone`, `ZoneID` | Source and destination zones. |
| `SourceBucket`, `DestinationBucket` | `SyncBucketIdentity` of the pair. |
| `State` | `incremental`, `init`, `full`, `stopped`, `error` or `disabled_or_no_sources`. |
| `Shards`, `BehindShards` | Shard counts. |
| `CaughtUp` | Checkpoint complete. |

Native markers and error text are never returned.

#### BucketSyncPolicyStatus

| Field | Meaning |
| --- | --- |
| `RealmID`, `PeriodID`, `RealmEpoch`, `ZonegroupID`, `GroupID` | Master configuration captured by the group. |
| `Zone`, `ZoneID` | Destination zone. |
| `Bucket` | Bucket of the group. |
| `Buckets` | Distinct bucket instances needed by pipes that select this destination. |
| `PeriodImported`, `PolicyImported`, `BucketsImported` | Individual checks. |
| `Imported` | All three are true. |

## Examples

### Replicate between two clusters with multisite

```golang
source := startCluster(t)
destination := startCluster(t)
ms, err := multisite.Run(ctx, ceph.DefaultImage, multisite.WithConfig(multisite.Config{
	Source: source, Destination: destination,
	SourceZone: "east", DestinationZone: "west",
}))
if ms != nil {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := ms.Terminate(ctx); err != nil {
			t.Error(err)
		}
	})
}
if err != nil {
	t.Fatal(err)
}

eastEndpoint, err := ms.Source.S3Endpoint(ctx)
westEndpoint, err := ms.Destination.S3Endpoint(ctx)
east := newS3Client(eastEndpoint, ms.Source.Region, ms.Source.AccessKey, ms.Source.SecretKey)
west := newS3Client(westEndpoint, ms.Destination.Region, ms.Destination.AccessKey, ms.Destination.SecretKey)

// Create a bucket and an object through east, then wait for them in west.
eventually(t, 4*time.Minute, func() bool {
	_, err := west.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("shared"), Key: aws.String("from-east"),
	})
	return err == nil
})
```

Both zones accept writes, and deletes replicate too, so a replica is not a
backup. Users and buckets are realm metadata owned by the metadata master
(`east` here). Create users through the master (`ms.Source.CreateUser` or
`ms.SourceAdmin`); a bucket created through another zone's gateway is forwarded
to the master, so the master must be reachable. Replication is asynchronous:
allow minutes rather than seconds, especially after a gateway restart.

To test a zone outage, stop one gateway (`ms.Source.Stop(ctx, &timeout)`),
keep working through the other one, start it again (`ms.Source.Start(ctx)`),
resolve `S3Endpoint` again, and wait for catch-up with `WaitSyncReady`.

### Planned metadata master failover and failback

Promotion uses native commands; the fixture moves periods between clusters. On
Ceph 20.2.4, run `period update` and `period commit` as separate commands: the
combined `period update --commit` can see an empty sync status on the new master
and refuse the promotion. Do not use `--yes-i-really-mean-it` to skip the check.

```golang
restart := func(g *rgw.Gateway) {
	timeout := 3 * time.Second
	if err := g.Stop(ctx, &timeout); err != nil {
		t.Fatal(err)
	}
	if err := g.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// Resolve g.S3Endpoint again and wait until signed S3 requests succeed.
}

// 1. The destination must have all metadata, and nothing may write meanwhile.
if _, err := ms.WaitSyncReady(ctx, "west"); err != nil {
	t.Fatal(err)
}
// 2. Fence the old master.
timeout := 3 * time.Second
if err := ms.Source.Stop(ctx, &timeout); err != nil {
	t.Fatal(err)
}
// 3. Promote the destination.
for _, args := range [][]string{
	{"zone", "modify", "--master", "--default"},
	{"zonegroup", "modify", "--master"},
	{"period", "update"},
	{"period", "commit"},
} {
	if _, err := ms.DestinationAdmin(ctx, args...); err != nil {
		t.Fatal(err)
	}
}
restart(ms.Destination)
// New users and buckets are now created through west.

// 4. Bring back the old master as a secondary.
if err := ms.PullSourcePeriod(ctx); err != nil {
	t.Fatal(err)
}
if err := ms.Source.Start(ctx); err != nil {
	t.Fatal(err)
}

// Failback is the same sequence in the other direction: wait for metadata in
// east, fence west, promote with SourceAdmin, restart east,
// PullDestinationPeriod, start west.
```

Verify the current master with `period get` (`master_zone` equals the zone ID).
`Fixture.Source` and `Destination` keep pointing at the original zones.
Automatic failover, request routing and recovery point guarantees are outside
the fixture.

### Interrupt replication between zones

In bridge mode a zone can be cut off from the private multisite network while
its S3 endpoint stays up:

```golang
cut, err := ms.InterruptZoneLink(ctx, "west")
if err != nil {
	if cut != nil {
		_ = cut.Restore(context.WithoutCancel(ctx))
	}
	t.Fatal(err)
}
// Writes in east and west succeed locally but do not replicate.
if err := cut.Restore(ctx); err != nil {
	t.Fatal(err)
}
if _, err := ms.WaitSyncReady(ctx, "west", "east"); err != nil {
	t.Fatal(err)
}
// Now check the objects written during the interruption in both zones.
```

The link is cut as a whole: there is no per-pair packet filtering or loss
injection. See [guides/fault-injection.md](../../docs/guides/fault-injection.md).

### More zones and zonegroups

```golang
// One zonegroup, three zones, "a" is the metadata master.
topology, err := multisite.Run(ctx, ceph.DefaultImage, multisite.WithTopology(multisite.TopologyConfig{
	Realm: "application", MetadataMaster: "a",
	Zones: []multisite.ZoneConfig{
		{Name: "a", Cluster: clusterA},
		{Name: "b", Cluster: clusterB},
		{Name: "c", Cluster: clusterC},
	},
}))
for _, zone := range topology.Zones() {
	endpoint, err := zone.Gateway.S3Endpoint(ctx)
	// S3 client per zone with zone.Gateway.AccessKey, SecretKey and Region.
}

// Two zonegroups. Users and bucket metadata are shared; object data
// replicates only inside each zonegroup.
config := multisite.TopologyConfig{
	Realm: "application", MasterZonegroup: "us",
	Zonegroups: []multisite.ZonegroupConfig{
		{Name: "us", MasterZone: "a", Zones: []multisite.ZoneConfig{
			{Name: "a", Cluster: clusterA},
			{Name: "b", Cluster: clusterB},
		}},
		{Name: "eu", MasterZone: "c", Zones: []multisite.ZoneConfig{
			{Name: "c", Cluster: clusterC},
		}},
	},
}

// Grow and shrink a running topology.
zone, err := topology.AddZone(ctx, ceph.DefaultImage, multisite.ZoneConfig{Name: "d", Cluster: clusterD})
err = topology.RemoveZone(ctx, "b")
// Resolve every remaining gateway's S3Endpoint again after both calls.
```

### Selective sync with a sync policy

This recipe replicates only objects under `published/` in one bucket, from
`east` to `west`. A zonegroup group with status `allowed` permits replication
without activating it; once it exists, only bucket groups with status
`enabled` replicate objects. Bucket metadata still replicates, so the bucket
exists in both zones; only its objects are filtered.

```golang
// ms from multisite.Run with SourceZone "east" and DestinationZone "west".

// 1. Zonegroup permission.
permit, err := ms.CreateSyncGroup(ctx, multisite.SyncPolicyScope{},
	multisite.SyncGroupConfig{ID: "permit", Status: multisite.SyncAllowed})
if err != nil {
	t.Fatal(err)
}
if err := ms.CreateSyncFlow(ctx, permit, multisite.SyncFlowConfig{
	ID: "pair", Zones: []string{"east", "west"},
}); err != nil {
	t.Fatal(err)
}
if err := ms.CreateSyncPipe(ctx, permit, multisite.SyncPipeConfig{
	ID: "east-to-west", SourceZones: []string{"east"}, DestinationZones: []string{"west"},
}); err != nil {
	t.Fatal(err)
}
if err := ms.ApplySyncGroup(ctx, permit); err != nil { // reloads the gateways
	t.Fatal(err)
}
if _, err := ms.WaitSyncReady(ctx, "west", "east"); err != nil {
	t.Fatal(err)
}
// Resolve both S3 endpoints again and rebuild the clients.

// 2. Create the bucket through east (the metadata master), then the bucket policy.
selected, err := ms.CreateSyncGroup(ctx, multisite.SyncPolicyScope{Bucket: "app-data"},
	multisite.SyncGroupConfig{ID: "published", Status: multisite.SyncEnabled})
if err != nil {
	t.Fatal(err)
}
if err := ms.CreateSyncPipe(ctx, selected, multisite.SyncPipeConfig{
	ID: "published", SourceZones: []string{"east"}, DestinationZones: []string{"west"},
	Prefix: "published/",
}); err != nil {
	t.Fatal(err)
}

// 3. Wait until west has imported the bucket policy, then write.
if _, err := ms.WaitBucketSyncPolicyReady(ctx, selected, "west"); err != nil {
	t.Fatal(err)
}
// Write app-data/published/report and app-data/private/notes through east.

// 4. Wait for the selected object, then for the bucket checkpoint.
eventually(t, 4*time.Minute, func() bool {
	_, err := west.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("app-data"), Key: aws.String("published/report"),
	})
	return err == nil
})
status, err := ms.WaitBucketSyncReady(ctx, selected, "published", "east", "west")
if err != nil {
	t.Fatal(err)
}
_ = status // State "incremental", CaughtUp true

// 5. Excluded objects must stay absent. Check over a window (for example
// 35 seconds), not once.
```

Variations:

- Tags: `Tags: []multisite.SyncObjectTag{{Key: "class", Value: "public"}}` selects
  objects carrying that tag. The S3 client sets object tags.
- Different destination bucket: `SourceBucket` and `DestinationBucket` with
  concrete names. Both buckets must exist; their instance IDs are captured.
- Destination owner: `DestinationOwner: someUser` makes replicated objects
  owned by an ordinary user created with `CreateUser`.
- Storage class: `DestinationStorageClass: "STANDARD_IA"` with
  `DestinationPlacements: map[string]*rgw.Placement{"west": westPlacement}`.
  Create, publish, import and reload the placement in both zones first (see
  [Placement targets and storage classes](../README.md#placement-targets-and-storage-classes)).
  Check the actual class in the S3 listing and the data in the destination
  pool, not only the stored policy.
- User mode: `User: replicator` replicates with that user's permissions. The
  user needs both `s3:GetObject` and `s3:GetObjectAcl` on the source objects.
  On the default image, ordinary-user permissions are not enforced (see
  [Selective sync limitations](#selective-sync-limitations-on-the-default-image)).
  With an account root user, a destination bucket policy that denies
  `s3:ReplicateObject` blocks replication while ordinary S3 requests keep
  working.
- Changing the prefix: `SetSyncPipePrefix(ctx, selected, "published", "reports/")`,
  then `WaitBucketSyncPolicyReady` before writing.
- Removing: `RemoveSyncGroup` for the bucket group (already copied objects
  stay); `RemoveSyncGroup` plus `ApplySyncGroup` for the zonegroup group.

### Pause and resume a bucket policy

Setting a bucket group to `forbidden` stops replication of new writes. After
switching back to `enabled`, new objects replicate again, but log entries
recorded while the policy was forbidden may remain unprocessed. Replay them
explicitly on a quiet fixture:

```golang
if err := ms.SetSyncGroupStatus(ctx, selected, multisite.SyncForbidden); err != nil {
	t.Fatal(err)
}
if _, err := ms.WaitBucketSyncPolicyReady(ctx, selected, "west"); err != nil {
	t.Fatal(err)
}
// BucketSyncStatus now reports State "stopped" or "disabled_or_no_sources".
// Writes to published/ are not replicated.

if err := ms.SetSyncGroupStatus(ctx, selected, multisite.SyncEnabled); err != nil {
	t.Fatal(err)
}
if _, err := ms.WaitBucketSyncPolicyReady(ctx, selected, "west"); err != nil {
	t.Fatal(err)
}

// Replay the existing bucket logs with the current policy.
before, err := ms.BucketSyncStatus(ctx, selected, "published", "east", "west")
if err != nil || before.State != "incremental" {
	t.Fatalf("state %q: %v", before.State, err)
}
args := []string{"bucket", "sync", "run",
	"--bucket", before.DestinationBucket.Name, "--bucket-id", before.DestinationBucket.ID,
	"--source-zone", before.SourceZoneID,
	"--source-bucket", before.SourceBucket.Name, "--source-bucket-id", before.SourceBucket.ID,
}
if _, err := ms.ZoneAdmin(ctx, before.Zone, args...); err != nil {
	t.Fatal(err)
}
if _, err := ms.WaitBucketSyncReady(ctx, selected, "published", "east", "west"); err != nil {
	t.Fatal(err)
}
```

- For tenant buckets, pass `tenant/name` as `--bucket` and add
  `--source-tenant`.
- Pass the captured native zone ID with `--source-zone`. On Ceph 20.2.4,
  `--source-zone-id` is a sync-policy option and does not select the source
  for `bucket sync run`.
- The replay processes the existing log entries of all matching pipes,
  including deletes, so objects written while the policy was forbidden may
  replicate now.
- Do not use `bucket sync init`, marker edits, log trims or forced unlocks.
  An exit status of 0 in the `stopped` state can be a no-op and proves nothing.
- Canceling the Docker exec does not stop a running native worker. Do not
  retry an uncertain replay automatically; discard the fixture instead.

## Running native commands

### radosgw-admin in multisite zones

```golang
period, err := ms.SourceAdmin(ctx, "period", "get")
status, err := ms.DestinationAdmin(ctx, "sync", "status")
policy, err := ms.ZoneAdmin(ctx, "c", "sync", "group", "get", "--group-id", group.ID())
```

These run in the fixture's setup client of each zone (rgw image unless `SetupImage` is set), scoped
to the realm, zonegroup and zone. Use them for promotion, period inspection and
explicit `bucket sync run`. Do not edit periods or zones while `AddZone`,
`AddZonegroup`, `RemoveZone` or `ApplySyncGroup` runs.

## Limitations

### Selective sync limitations on the default image

Ceph 20.2.4 from the default image stores every pipe parameter, but two
parameters have no reliable effect. A stored policy and a caught-up checkpoint
are therefore not evidence that these conditions work.

- **Numeric priority between overlapping pipes.** When several pipes of a
  bucket share the same prefix, RGW's tagged-object selection starts from one
  matching candidate and does not compare the earlier candidates with the same
  prefix. Which pipe's parameters (for example the destination storage class)
  apply depends on the native rebuild order of the pipes, not on `Priority`.
  Do not rely on `Priority` to choose between overlapping pipes.
- **Source permissions in user mode for ordinary users.** RGW reads the
  permission-check user of a replication GET too early, so the request is
  authorised on the system path. Objects replicate even when the user-mode
  principal is denied `s3:GetObject` on the source, and the destination skips
  its ACL fallback. User mode with an ordinary user therefore does not test
  source-side denial. User mode with an account root user honours a
  destination bucket policy that denies `s3:ReplicateObject`.

Both limitations are native RGW behaviour. They can only be removed with a
patched gateway image supplied through the `image` argument of `Run`; this project does not build such images.

Further native behaviour of sync policies:

- RGW combines the checkpoints of all pipes matching the same bucket pair. A
  caught-up `BucketSyncStatus` does not prove the filter or user mode of one
  pipe. Verify object bytes and absences with the S3 client.
- Policy changes affect future writes. They neither backfill earlier objects
  nor delete objects that were already replicated.
- After `forbidden` to `enabled`, earlier log entries may stay unprocessed
  until an explicit `bucket sync run` (see
  [Pause and resume a bucket policy](#pause-and-resume-a-bucket-policy)).
- The S3 `PutBucketReplication` API rejects account-owned requests with
  `NotImplemented` on Ceph 20.2.4. Use the sync policy methods instead.
- Same-zone replication is not supported by RGW multisite.
- The helpers refuse IAM non-root principals and cross-account or cross-tenant
  principal translation. This limits what the module configures; it is not a
  statement about Ceph.
