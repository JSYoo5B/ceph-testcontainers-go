# API purpose and classification

This guide states what the public API is for, the purpose tags that describe
each public function and method, and how each package's API is tagged. The go
doc of each identifier remains the contract; this page is the map.

## Purpose boundary

This repository is a test tool. Every public API is judged against two rules.

1. **It serves repeatable integration tests of services that use Ceph.** It
   does not aim to implement or operate a complete Ceph cluster. It reproduces
   as many operational scenarios as a disposable test cluster can, but
   large-scale experiments, performance measurement and tuning are out of
   scope. Fault injection exists only to reproduce the environment or condition
   a test asks for.
2. **It builds and checks the collaborator, not the system under test.** An API
   either brings Ceph into the fixture state a test wants, independently of the
   SUT and the test code, or lets the test check after the run that Ceph and
   the integration environment reached the expected state. An API that goes
   beyond this, such as a convenience that only shortens what the SUT or a
   native command already does, or an operation aimed at running a real
   service, does not belong here.

APIs that make a condition easier to build or a state easier to assert may keep
being added; the boundary is the test purpose, not a method count.

## Purpose tags

A purpose tag names one test purpose an API serves. One API often serves
several, so every public function and method carries one or more tags. Types and
constants follow the API that uses them.

Purpose tags are documentation only. They are unrelated to the Go build tags
and the CI scenario tags that `internal/tools/tagcatalog` manages.

**Boundary rule:** an API must pass the "belongs" test of at least one of its
tags. An API that passes none is outside the boundary.

| Tag | Definition | Belongs when | Does not belong when |
| --- | --- | --- | --- |
| **Construction** | Creates the disposable cluster, a service on it, or a resource in it, in the state the test needs before the SUT runs. | It establishes a precondition the test cannot get from the SUT, and the fixture owns what it creates. | It adopts arbitrary existing state as if the fixture owned it, or provisions something only a production deployment needs. |
| **Condition** | Moves an existing fixture into an abnormal, transitional or reconfigured state (a fault, a failover, a policy change) that the test wants the SUT to meet. | The state is a scenario a test asserts the SUT's reaction to, and the API records what it changed when undo is possible. | It is an operational procedure with no test scenario, or it changes state without recording how to undo it when undo is possible. |
| **Observation** | Reads native state, or waits for it, so the test can assert the expected result. It never changes Ceph state. | It reports exactly what Ceph says, with the identity checks needed to trust it, or waits for a stated native condition. | It reinterprets or summarizes beyond what an assertion needs, or mutates while observing. |
| **Hand-off** | Gives the SUT or a test client what it needs to talk to the fixture: configuration, keyrings, endpoints, credentials, a prepared client container. | The SUT would otherwise have no way to reach the fixture as a real client. | It performs the SUT's work for it (a client library wrapper, a data-path helper). |
| **Native access** | Runs a native Ceph, RBD, radosgw-admin or admin-socket operation inside an owned container, for what the typed API does not cover. | It keeps the fixture's scope (realm, zone, identity, container ownership) and its timeout and output limits. | It lets the caller override the fixture's scope or connection, or it becomes the typed API's replacement. |
| **Evidence** | Collects material for investigating a failed test: diagnostics, crash metadata, packet captures, redaction. | It is bounded, redacts secrets, and does not change what the test observed. | It is a monitoring or export feature for a running service. |
| **Lifetime** | Ends or releases what the fixture owns: termination, test-cleanup registration, removal of fixture-created resources, restoration of a changed state. | It removes or restores only what the fixture created or changed, in an order that keeps the cluster consistent. | It deletes caller or native state the fixture did not create. |

Two tagging notes:

- A handle's `Restore`, `Resume` or `Stop` carries the tags of the call that
  returned the handle, plus **Lifetime**.
- A `Remove` of a member (MON, MGR, OSD, MDS, gateway, mirror daemon) is
  **Condition** + **Lifetime**: the test may use it to cause a member loss, and
  it also releases what the fixture owns. A `Remove` of a plain
  fixture-created resource (pool, subvolume, user, namespace) is **Lifetime**.

Abbreviations in the tables: Cons = Construction, Cond = Condition,
Obs = Observation, Hand = Hand-off, Native = Native access, Evid = Evidence,
Life = Lifetime.

## ceph

| API | Tags | Purpose |
| --- | --- | --- |
| `Run` | Cons | Starts the disposable MON/MGR/OSD cluster. |
| `WithClusterConfig` + `ClusterConfig.With*` | Cons | Initial MON/MGR/OSD counts and locations, CRUSH root, pool defaults and pools. |
| `WithImageOptions` + `ImageOptions.With*` | Cons | Role images (essential, CephFS, RGW, image set). |
| `WithNetworkConfig` + `NetworkConfig.With*` | Cons | Address family, host address, Messenger mode/policy, separate cluster network. |
| `WithOSDStorage` + `OSDStorage.With*` | Cons | OSD object store, in-memory or volume storage, block size. |
| `WithAuth`, `WithConfigFile`, `WithHostNetwork`, `WithStartupTimeout` | Cons | Cephx and ticket policy, extra ceph.conf, host networking, startup bound. |
| `DaemonOptions.WithImage`, `OSDOptions.WithImage`, `OSDOptions.WithPlacement` | Cons | Per-daemon image and CRUSH location for added daemons. |
| `Container.AddOSD`, `AddMonitor`, `AddManager` | Cons, Cond | Grows the topology a test needs, or adds a member as a scenario step. |
| `Container.CreatePool` | Cons | Fresh pool with explicit placement and replication. |
| `Container.CreateClient`, `ClientOptions.WithKeyType` | Cons, Hand | Fresh client identity with exact caps and key type, for the SUT to use. |
| `Pool.SetQuota`, `SetReplication`, `SetPlacement`, `SetPGCount` | Cons, Cond | Pool policy a test depends on, or changes during a scenario. |
| `ClientConfig.UpdateCaps` | Cons, Cond | Replaces a fixture client's capabilities. |
| `OSDContainer.Remove`, `MonitorContainer.Remove`, `ManagerContainer.Remove` | Cond, Life | Member loss with drain/quorum/candidate rules; releases the member. |
| `OSDContainer.Detach` | Cond | OSD container loss with a retained store. |
| `DetachedOSD.Reopen` | Cond, Life | Returns the detached OSD. |
| `OSDContainer.SetIn` | Cond | OSDMap in/out membership. |
| `OSDContainer.RotateKey`, `ManagerContainer.RotateKey`, `ClientConfig.ReplaceKey` | Cond | Key rotation as an operational event. |
| `ClientConfig.Revoke` | Cond | Temporary loss of a client identity. |
| `ClientRevocationHandle.Restore` | Cond, Life | Puts the revoked identity back. |
| `ClientConfig.Delete` | Cond, Life | Permanent revocation of a fixture client identity. |
| `Container.DeepScrubPG`, `RepairPG` | Cond, Obs | Scrub and repair as maintenance events, waiting for their native result. |
| `Container.RefreshMonitorConfig`, `RefreshClientMonitorConfig` | Cond, Hand | Moves daemons and clients to the current MON addresses after a membership change. |
| `Observe` + every `Observer` method | Obs | Native status, MGR, MON quorum and bootstrap addresses, OSD states and flags, pools, configuration, blocklist, health, full ratios, PG inconsistencies, object watchers, MGR modules, services and metrics. |
| `ManagerMetrics.Select`, `Value` | Obs | Selects samples from one metrics scrape. |
| `Container.WaitForClean`, `WaitForPGClean`, `WaitForQuorum`, `WaitMGRModuleReady` | Obs | Waits for a stated native condition. |
| `Container.Monitors`, `Managers`, `OSDs`, `ServiceContainers`, `Pool` | Obs | Owned descriptors and handles. |
| `Pool.Status`, `Usage`, `PGs`, `WaitForPGCount` | Obs | Native pool state. |
| `ClientConfig.Capabilities`; `*Container.Version`, `Image`, `Placement`; `DetachedOSD.*` accessors; `ClientConfig.Name`, `User`, `ClientRevocationHandle.Name` | Obs | Recorded and reported identity of handles. |
| `FactsOf`, `ParsePGID` | Obs | Static facts of a cluster; PG ID parsing for the PG APIs. |
| `Finalization.Check`, `Verify`, `Verification.Evaluate` | Obs | Post-test checks and the verification window. |
| `Container.ConnectionConfig`, `ClientConfig.ConnectionConfig`, `ClientConfig.KeyringPath` | Hand | ceph.conf and keyrings for the SUT. |
| `Container.WithClient`, `ClientConfig.WithIdentity`, `ClientContainerOptions.WithMessengerPolicy`, `WithIdleEntrypoint` | Hand | Prepares a client container with admin or a specific identity. |
| `Container.ClientContainer`, `ControlContainer` | Hand, Native | The owned toolkit and control containers, for test clients and native commands. |
| `Container.Ceph`, `RunNativeCommand`, `NewNativeInput`, `NativeInput.Write`/`Close`, `NativeCommandResult.Complete` | Native | Supervised native commands with bounded output. |
| `NativeCommandUnstoppedError.Stop` | Native, Life | Stops a native command whose end was not confirmed. |
| `OSDContainer.AdminSocket`, `MonitorContainer.AdminSocket`, `ManagerContainer.AdminSocket` | Native | Admin-socket commands on an owned daemon. |
| `Container.CollectDiagnostics`, `CollectCrashMetadata`, `RedactSecrets` | Evid | Bounded diagnostics, crash metadata, redaction. |
| `Finalization.Capture`, `CaptureWithFile`, `TrackDebugLevel` | Evid | Captures and debug windows settled into the failure archive. |
| `Container.Terminate` | Life | Ordered teardown. |
| `Container.TerminateOnCleanup`, `Finalization.Cleanup` | Life, Obs | Test-cleanup registration that runs the post-test checks before teardown. |
| `Pool.Remove` | Life | Removes a fixture-created pool with its rule and profile. |

`NativeFailure.Is` and `OperationError.Is` classify errors for every tag.

## cephfs

| API | Tags | Purpose |
| --- | --- | --- |
| `Run`, `WithFilesystems`, `Start` | Cons | Cluster with filesystems; a filesystem on an existing cluster. |
| `Filesystem.CreateSubvolumeGroup`, `CreateSubvolume`, `AddDataPool`, `AddSnapshotSchedule` | Cons | Groups, subvolumes, extra data pools, snapshot schedules. |
| `Subvolume.CreateSnapshot`, `SubvolumeSnapshot.Clone` | Cons | Snapshots and clones. |
| `Subvolume.Authorize` | Cons, Hand | A fresh identity granted on one subvolume, for the SUT to mount with. |
| `Subvolume.Resize`, `SubvolumeGroup.Resize` | Cons, Cond | Quotas a test depends on, or changes during a scenario. |
| `Filesystem.ScaleMDS`, `MDS.AddReplacement` | Cons, Cond | MDS topology for the test, or a change and replacement during a scenario. |
| `MDS.RemoveStopped` | Cond, Life | Retires a lost MDS. |
| `Filesystem.QuiesceSubvolumes` | Cond | Pauses I/O to subvolumes. |
| `Quiesce.Release` | Cond, Life | Resumes the paused I/O. |
| `Filesystem.OverrideSessionTimeouts` | Cond | Short session timeouts so evictions are observable. |
| `SessionTimeoutsOverrideHandle.Restore` | Cond, Life | Restores the timeouts. |
| `Subvolume.OverridePin`, `SubvolumeGroup.OverridePin` | Cond | Pinning policy changes. |
| `PinOverrideHandle.Restore` | Cond, Life | Restores the pinning field. |
| `SubvolumeAuthorization.Deauthorize` | Cond, Life | Revokes a grant. |
| `SubvolumeAuthorization.EvictClients` | Cond | Evicts the grant's sessions. |
| `SubvolumeClone.Cancel` | Cond | Interrupts a clone. |
| `Filesystems`, `Filesystem.MDSs`, `MDSStatus`, `WaitReady`, `DataPools`, `Sessions`, `SessionTimeouts` | Obs | Filesystem, MDS and session state. |
| `Filesystem.Subvolumes`, `SubvolumeInfo`, `SubvolumeGroups`, `SubvolumeGroupInfo`, `SubvolumeSnapshotInfo`, `SubvolumeAuthorizedClients` | Obs | Native subvolume metadata. |
| `Subvolume.Snapshots`, `PinPolicy`, `SubvolumeGroup.PinPolicy`, `SubvolumeClone.Status`, `Wait`, `Quiesce.ID`, `Status`, `SnapshotScheduleLease.Status`, `Snapshots` | Obs | Handle state. |
| `Filesystem.Remove`, `Subvolume.Remove`, `SubvolumeGroup.Remove`, `SubvolumeSnapshot.Remove`, `SubvolumeClone.RemovePartial`, `DataPool.RemoveUnused` | Life | Removes fixture-created resources. |
| `SnapshotScheduleLease.Restore` | Cons, Life | Removes the schedule the lease added. |

## cephfs/mirror

| API | Tags | Purpose |
| --- | --- | --- |
| `Run`, `WithConfig` | Cons | One-way snapshot mirroring between two existing filesystems. |
| `Mirror.AddDaemon`, `AddDirectory`, `BeginDirectoryAddition` | Cons, Cond | More daemons and mirrored directories, before or during a scenario. |
| `Daemon.Remove` | Cond, Life | Loss of a mirror daemon; releases it. |
| `Daemon.Terminate` | Cond, Life | Stops a daemon's process without changing policy. |
| `Daemon.InterruptPeerLink` | Cond | Cuts a daemon's link to the destination. |
| `Mirror.RemoveDirectory`, `BeginDirectoryRemoval`, `RemovePeer`, `BeginPeerRemoval` | Cond, Life | Directory and peer removal. |
| `Mirror.RebootstrapPeer`, `RebalanceDirectories`, `AttachManagers` | Cond | Peer recovery and directory redistribution. |
| `DirectoryRemoval.AcknowledgeProcessQuiescence`, `PeerRemoval.AcknowledgeProcessQuiescence` | Cond | Accepts an interrupted removal after explicit daemon removal. |
| `Mirror.RefreshMonitorConfig`, `RefreshPeerMonitorConfig` | Cond | Follows MON address changes on either side. |
| `Mirror.Daemons`, `PeerIDs`, `DirectoryStatus`, `WaitDirectoryReady`, `WaitSnapshotSynced` | Obs | Mirroring state and sync waits. |
| `DirectoryAddition.Status`, `DirectoryRemoval.Status`, `ProcessQuiescence`, `WaitReleased`, `PeerRemoval.Status`, `ProcessQuiescence`, `WaitDrained`, `Daemon.ProcessObserverBindingStatus` | Obs | Progress of additions and removals. |
| `Mirror.Terminate` | Life | Removes owned containers and MGR attachments. |

## rbd

| API | Tags | Purpose |
| --- | --- | --- |
| `Run`, `WithPools`, `InitPool`, `CreateNamespace` | Cons | RBD pools and namespaces. |
| `RestoreBackup`, `RestoreIncremental` | Cons, Native | Seeds an image from a known archive or diff through the rbd CLI. |
| `ExportBackup`, `ExportIncremental` | Obs, Native | Reads an image or diff out through the rbd CLI so the test can compare what the SUT wrote. |
| `StartImmutableObjectCache` | Cons, Cond | Runs the immutable object cache in a client container. |
| `ImmutableObjectCacheLease.Stop` | Cond | Stops the cache daemon while keeping its directory. |
| `ImmutableObjectCacheLease.Restore` | Cons, Life | Stops the daemon and removes its files. |
| `ImmutableObjectCacheLease.ClientOptions` | Hand | Options that make a librbd client use the cache. |
| `ImageClients`, `ImageClientStatus.ExclusiveOwner`, `ListNamespaces`, `Namespace.Name`, `PoolName`, `ImmutableObjectCacheLease.Status` | Obs | Watchers, locks, namespaces, cache state. |
| `RemoveNamespace` | Life | Removes a fixture-created namespace. |

## rbd/mirror

| API | Tags | Purpose |
| --- | --- | --- |
| `Run`, `WithConfig` | Cons | Pool/namespace mirroring between two clusters with receiver daemons. |
| `Mirror.AddDaemon` | Cons, Cond | Another receiver, before or during a scenario. |
| `Mirror.EnableImage`, `BindNamespace` | Cons | Mirroring for an image; a namespace view. |
| `Daemon.Remove` | Cond, Life | Loss of a receiver; releases it. |
| `Daemon.Terminate` | Cond, Life | Removes a receiver's container. |
| `Daemon.InterruptPeerLink`, `Mirror.Rebootstrap` | Cond | Cuts a receiver's link; re-bootstraps the peer. |
| `Mirror.RefreshMonitorConfig` | Cond | Follows MON address changes. |
| `Mirror.Daemons`, `ImageStatus`, `PolicyStatus`, `ReceiverStatus`, `WaitReceiverReady`, `WaitReplayReady`, `Daemon.Status` | Obs | Mirroring and election state. |
| `Namespace.ImageStatus`, `ReceiverStatus`, `WaitReceiverReady`, `WaitReplayReady` | Obs | The same, scoped to a bound namespace. |
| `Mirror.SourceRBD`, `DestinationRBD` | Native | RBD CLI in the setup clients. |
| `Mirror.Terminate` | Life | Removes daemons and setup clients. |

## rgw

| API | Tags | Purpose |
| --- | --- | --- |
| `Run`, `WithGateways`, `Start` | Cons | Cluster with gateways; a gateway on an existing cluster. |
| `Gateway.CreateUser`, `CreateAccount`, `Account.CreateRootUser` | Cons, Hand | Identities and accounts whose credentials the SUT uses. |
| `Gateway.CreatePlacement` | Cons | Placement targets. |
| `User.SetQuota`, `SetBucketQuota`, `SetPlacement`, `Account.SetQuota`, `SetBucketQuota` | Cons, Cond | Policies a test depends on, or changes during a scenario. |
| `Gateway.ApplyPlacement`, `ReloadPlacement` | Cons, Cond | Activates a placement; reload restarts the gateway. |
| `User.Suspend` | Cond | Disables or re-enables an identity's access. |
| `Gateway.Remove` | Cond, Life | Loss of a gateway; releases it. |
| `Gateways`, `User.Info`, `Usage`, `ID`, `Account.Info`, `ID`, `Placement.Status` | Obs | Native identity, policy and usage. |
| `Gateway.S3Endpoint`, `S3SecureEndpoint`, `DaemonEndpoint`, `User.Credentials` | Hand | Endpoints and S3 keys for the SUT. |
| `Gateway.Admin` | Native | radosgw-admin in the gateway's scope. |
| `User.Remove`, `Account.Remove` | Life | Removes fixture-created identities. |

## rgw/multisite

| API | Tags | Purpose |
| --- | --- | --- |
| `Run`, `WithConfig`, `WithTopology` | Cons | A realm over existing clusters. |
| `Fixture.AddZone`, `AddZonegroup` | Cons, Cond | Zones and zonegroups, before or during a scenario. |
| `Fixture.CreateSyncGroup`, `SyncGroup.CreateFlow`, `CreatePipe`, `SetPipePrefix`, `SetStatus`, `Apply` | Cons, Cond | Sync policy, its changes and their publication. |
| `Fixture.RemoveZone` | Cond, Life | Detaches a zone and removes its owned gateway and CLI. |
| `Fixture.PullSourcePeriod`, `PullDestinationPeriod` | Cond | Period import for failover and failback. |
| `Fixture.InterruptZoneLink`, `ApplyZoneLinkProfile` | Cond | Cut or shaped link between gateways. |
| `SyncGroup.RemoveFlow`, `RemovePipe` | Cond | Removes part of a sync policy. |
| `SyncGroup.Remove` | Cond, Life | Removes a fixture-created sync group. |
| `Fixture.Zones`, `Zonegroups`, `SyncStatus`, `WaitSyncReady`, `SyncGroup.ID`, `BucketSyncStatus`, `WaitBucketSyncReady`, `WaitBucketPolicyReady` | Obs | Realm, sync and bucket-sync state. |
| `Fixture.ZoneAdmin` | Native | radosgw-admin in one zone's scope. |
| `Fixture.Terminate` | Life | Removes gateways, CLI clients and the bridge. |

## fault

| API | Tags | Purpose |
| --- | --- | --- |
| `InterruptNetwork`, `InterruptLink` | Cond | Disconnects a bridge endpoint. |
| `NetworkInterruption.Restore` | Cond, Life | Reattaches it with its original addresses. |
| `ApplyNetworkProfile`, `ApplyLinkProfile` | Cond | Delay, loss and other netem shaping below Ceph. |
| `NetworkProfileLease.Restore` | Cond, Life | Removes the shaping. |
| `Pause` | Cond | Freezes a container. |
| `PauseLease.Resume` | Cond, Life | Thaws it. |
| `InjectObjectDataError` | Cond, Native | One-shot replica read error through the native injectdataerr command. |
| `NetworkProfileLease.Status`, `NetworkPacketFault.IsZero` | Obs | Applied qdiscs; whether a fault is set. |

## override

Every `Set*` and `AddBlocklist` records the prior value and returns a handle
whose `Restore` returns it; each `Restore` is Cond, Life.

| API | Tags | Purpose |
| --- | --- | --- |
| `SetConfig` | Cons, Cond | One central config entry. |
| `SetOSDFlag` | Cond | One global OSD flag. |
| `SetFullRatios` | Cond | Near-full, backfill-full and full thresholds. |
| `SetMGRModule` | Cons, Cond | MGR module membership. |
| `SetMessengerPolicy` | Cond | Connection modes running daemons accept. |
| `AddBlocklist` | Cond | Fences one client session. |
| `SetDebugLevel` | Cond, Evid | One OSD runtime debug level, recorded for the failure archive. |
| `*Handle.Restore` | Cond, Life | Returns the changed value. |
| `BlocklistHandle.Address`, `DebugLevelHandle.Receipt` | Obs | What the handle changed. |

## capture

| API | Tags | Purpose |
| --- | --- | --- |
| `Start` | Evid | Packet capture on one endpoint. |
| `Session.Stop`, `Settle` | Evid, Life | Ends the capture; settles it into the failure archive. |
| `Session.CaptureFile`, `SettlementAllowance` | Evid | The capture output and its settle bound. |

## Boundary review

The first review against the boundary removed `ceph.Container.ManagerContainer`
(a nil-able compatibility accessor covered by `Managers` and
`Observer.ManagerStatus`), the forwarding methods `cephfs.Container.Filesystems`,
`rgw.Container.Gateways` and `rgw.Container.S3Endpoint` (the package functions
and `Gateway.S3Endpoint` remain), and made the `User` and `Placement`
ownership checks internal to `rgw` and `rgw/multisite`. These APIs were
reviewed and kept:

| API | Reason |
| --- | --- |
| `cephfs/mirror.Daemon.ProcessObserverBindingStatus` | Integration tests judge process quiescence by it; without it a test cannot tell an unbound observer from a quiet one. |
| `ceph.WithIdleEntrypoint` | Client containers need a long-lived process to receive the handed-off configuration and run commands. |
| `rbd.ExportBackup`, `ExportIncremental`, `RestoreBackup`, `RestoreIncremental` | They seed an image with known data and read back what the SUT wrote, with the stream and snapshot rules the fixture enforces. |
| `cephfs.Filesystem.AddSnapshotSchedule`, `SnapshotScheduleLease` | A test needs scheduled snapshots as a precondition, and the lease restores the schedule. |
| `rgw/multisite.Fixture.PullSourcePeriod`, `PullDestinationPeriod` | Failover and failback scenarios need the period import as a condition step. |
| `ceph.Container.WaitForPGClean` beside `WaitForClean` | It deliberately accepts out or down OSDs, which a degraded-cluster test needs. |

## Adding or changing an API

A new or changed public function or method names its purpose tags in the pull
request and is added to its package's table here. It must pass the "belongs"
test of at least one of its tags. A proposal that passes none, such as a
service-operation API or a convenience that only repeats what the SUT, the test
or `Native access` already does, is refused. Keeping an existing API that
passes none needs a written reason on this page.
