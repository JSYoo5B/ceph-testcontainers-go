# API purpose and classification

This guide states what the public API is for, the categories every public
function and method belongs to, and how each package's API falls into them.
The go doc of each identifier remains the contract; this page is the map.

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

## Categories

Each public function and method belongs to exactly one category. Types and
constants follow the API that uses them.

| Category | Definition | Belongs when | Does not belong when |
| --- | --- | --- | --- |
| **Construction** | Creates the disposable cluster, a service on it, or a resource in it, in the state the test needs before the SUT runs. | It establishes a precondition the test cannot get from the SUT, and the fixture owns what it creates. | It adopts arbitrary existing state as if the fixture owned it, or provisions something only a production deployment needs. |
| **Condition** | Moves an existing fixture into an abnormal, transitional or reconfigured state (a fault, a failover, a policy change) that the test wants the SUT to meet, usually with an explicit restore. | The state is a scenario a test asserts the SUT's reaction to, and the API records what it changed so the fixture can return. | It is an operational procedure with no test scenario, or it changes state without recording how to undo it when undo is possible. |
| **Observation** | Reads native state, or waits for it, so the test can assert the expected result. It never changes Ceph state. | It reports exactly what Ceph says, with the identity checks needed to trust it, or waits for a stated native condition. | It reinterprets or summarizes beyond what an assertion needs, or mutates while observing. |
| **Hand-off** | Gives the SUT or a test client what it needs to talk to the fixture: configuration, keyrings, endpoints, credentials, a prepared client container. | The SUT would otherwise have no way to reach the fixture as a real client. | It performs the SUT's work for it (a client library wrapper, a data-path helper). |
| **Native access** | Runs a native Ceph, RBD, radosgw-admin or admin-socket command inside an owned container, for what the typed API does not cover. | It keeps the fixture's scope (realm, zone, identity, container ownership) and its timeout and output limits. | It lets the caller override the fixture's scope or connection, or it becomes the typed API's replacement. |
| **Evidence** | Collects material for investigating a failed test: diagnostics, crash metadata, packet captures, redaction. | It is bounded, redacts secrets, and does not change what the test observed. | It is a monitoring or export feature for a running service. |
| **Lifetime** | Ends what the fixture owns: termination, test-cleanup registration, removal of fixture-created resources. | It removes only what the fixture created, in an order that keeps the cluster consistent. | It deletes caller or native state the fixture did not create. |

Judgment rules used below:

- `Remove` of a daemon or gateway is **Condition** when the scenario is the
  loss of that member (MON/MGR/OSD removal, MDS retirement), and **Lifetime**
  when it only returns a fixture-created resource (pool, subvolume, user).
- A restore handle's `Restore`/`Resume` belongs to the category of the call that
  returned it.
- Accessors that return a handle's recorded identity (`Name`, `ID`, `Image`,
  `Placement`) belong to **Observation**.
- Config values with chainable methods and their grouped option function
  (`WithClusterConfig`, `WithConfig`, ...) are **Construction**.

## ceph

| Category | API | Purpose |
| --- | --- | --- |
| Construction | `Run` | Starts the disposable MON/MGR/OSD cluster. |
| Construction | `WithClusterConfig` + `ClusterConfig.With*` | Initial MON/MGR/OSD counts and locations, CRUSH root, pool defaults and pools. |
| Construction | `WithImageOptions` + `ImageOptions.With*` | Role images (essential, CephFS, RGW, image set). |
| Construction | `WithNetworkConfig` + `NetworkConfig.With*` | Address family, host address, Messenger mode/policy, separate cluster network. |
| Construction | `WithOSDStorage` + `OSDStorage.With*` | OSD object store, in-memory or volume storage, block size. |
| Construction | `WithAuth`, `WithConfigFile`, `WithHostNetwork`, `WithStartupTimeout` | Cephx and ticket policy, extra ceph.conf, host networking, startup bound. |
| Construction | `DaemonOptions.WithImage`, `OSDOptions.WithImage`, `OSDOptions.WithPlacement` | Per-daemon image and CRUSH location for added daemons. |
| Construction | `Container.AddOSD`, `AddMonitor`, `AddManager` | Grows the topology to the size a test needs. |
| Construction | `Container.CreatePool`, `CreateClient`, `ClientOptions.WithKeyType` | Fresh pool; fresh client identity with exact caps and key type. |
| Construction | `Pool.SetQuota`, `SetReplication`, `SetPlacement`, `SetPGCount` | Pool policy a test depends on. |
| Construction | `ClientConfig.UpdateCaps` | Replaces a fixture client's capabilities. |
| Condition | `OSDContainer.Remove`, `MonitorContainer.Remove`, `ManagerContainer.Remove` | Loss of a member, with drain/quorum/candidate rules. |
| Condition | `OSDContainer.Detach`, `DetachedOSD.Reopen` | OSD container loss with a retained store, and its return. |
| Condition | `OSDContainer.SetIn` | OSDMap in/out membership. |
| Condition | `OSDContainer.RotateKey`, `ManagerContainer.RotateKey`, `ClientConfig.ReplaceKey` | Key rotation as an operational event. |
| Condition | `ClientConfig.Revoke`, `ClientRevocationHandle.Restore` | Temporary loss of a client identity. |
| Condition | `ClientConfig.Delete` | Permanent revocation of a client identity. |
| Condition | `Container.DeepScrubPG`, `RepairPG` | Scrub and repair as native maintenance events. |
| Condition | `Container.RefreshMonitorConfig`, `RefreshClientMonitorConfig` | Moves daemons/clients to the current MON addresses after a membership change. |
| Observation | `Observe` + every `Observer` method | Native status, MGR, MON quorum and bootstrap addresses, OSD states and flags, pools, configuration, blocklist, health, full ratios, PG inconsistencies, object watchers, MGR modules, services and metrics. |
| Observation | `ManagerMetrics.Select`, `Value` | Selects samples from one metrics scrape. |
| Observation | `Container.WaitForClean`, `WaitForPGClean`, `WaitForQuorum`, `WaitMGRModuleReady` | Waits for a stated native condition. |
| Observation | `Container.Monitors`, `Managers`, `OSDs`, `ServiceContainers`, `Pool`, `ManagerContainer` | Owned descriptors and handles. |
| Observation | `Pool.Status`, `Usage`, `PGs`, `WaitForPGCount` | Native pool state. |
| Observation | `ClientConfig.Capabilities`; `*Container.Version`, `Image`, `Placement`; `DetachedOSD.*` accessors; `ClientConfig.Name`, `User`, `ClientRevocationHandle.Name` | Recorded and reported identity of handles. |
| Observation | `FactsOf`, `ParsePGID` | Static facts of a cluster; PG ID parsing for the PG APIs. |
| Observation | `Container.TerminateOnCleanup` → `Finalization.Check`, `Verify`, `Verification.Evaluate` | Post-test checks and the verification window. |
| Hand-off | `Container.ConnectionConfig`, `ClientConfig.ConnectionConfig`, `ClientConfig.KeyringPath` | ceph.conf and keyrings for the SUT. |
| Hand-off | `Container.WithClient`, `ClientConfig.WithIdentity`, `ClientContainerOptions.WithMessengerPolicy`, `WithIdleEntrypoint` | Prepares a client container with admin or a specific identity. |
| Hand-off | `Container.ClientContainer`, `ControlContainer` | The owned toolkit and control containers. |
| Native access | `Container.Ceph`, `RunNativeCommand`, `NewNativeInput`, `NativeInput.Write`/`Close`, `NativeCommandResult.Complete`, `NativeCommandUnstoppedError.Stop` | Supervised native commands with bounded output and stop semantics. |
| Native access | `OSDContainer.AdminSocket`, `MonitorContainer.AdminSocket`, `ManagerContainer.AdminSocket` | Admin-socket commands on an owned daemon. |
| Evidence | `Container.CollectDiagnostics`, `CollectCrashMetadata`, `RedactSecrets` | Bounded diagnostics, crash metadata, redaction. |
| Evidence | `Finalization.Capture`, `CaptureWithFile`, `TrackDebugLevel` | Captures and debug windows settled into the failure archive. |
| Lifetime | `Container.Terminate`, `TerminateOnCleanup`, `Finalization.Cleanup` | Ordered teardown and test-cleanup registration. |
| Lifetime | `Pool.Remove` | Removes a fixture-created pool with its rule and profile. |

`NativeFailure.Is` and `OperationError.Is` classify errors for every category.

## cephfs

| Category | API | Purpose |
| --- | --- | --- |
| Construction | `Run`, `WithFilesystems`, `Start` | Cluster with filesystems; a filesystem on an existing cluster. |
| Construction | `Filesystem.CreateSubvolumeGroup`, `CreateSubvolume`, `AddDataPool`, `AddSnapshotSchedule` | Groups, subvolumes, extra data pools, snapshot schedules. |
| Construction | `Subvolume.CreateSnapshot`, `SubvolumeSnapshot.Clone`, `Subvolume.Authorize` | Snapshots, clones and a client grant on a subvolume. |
| Construction | `Subvolume.Resize`, `SubvolumeGroup.Resize` | Quotas a test depends on. |
| Condition | `Filesystem.ScaleMDS`, `MDS.AddReplacement`, `MDS.RemoveStopped` | MDS topology changes and replacement of a lost MDS. |
| Condition | `Filesystem.QuiesceSubvolumes`, `Quiesce.Release` | Pauses I/O to subvolumes. |
| Condition | `Filesystem.OverrideSessionTimeouts`, `SessionTimeoutsOverrideHandle.Restore` | Short session timeouts so evictions are observable. |
| Condition | `Subvolume.OverridePin`, `SubvolumeGroup.OverridePin`, `PinOverrideHandle.Restore` | Pinning policy changes. |
| Condition | `SubvolumeAuthorization.Deauthorize`, `EvictClients` | Revokes a grant; evicts its sessions. |
| Condition | `SubvolumeClone.Cancel` | Interrupts a clone. |
| Observation | `Filesystems`, `Container.Filesystems`, `Filesystem.MDSs`, `MDSStatus`, `WaitReady`, `DataPools`, `Sessions`, `SessionTimeouts` | Filesystem, MDS and session state. |
| Observation | `Filesystem.Subvolumes`, `SubvolumeInfo`, `SubvolumeGroups`, `SubvolumeGroupInfo`, `SubvolumeSnapshotInfo`, `SubvolumeAuthorizedClients` | Native subvolume metadata. |
| Observation | `Subvolume.Snapshots`, `PinPolicy`, `SubvolumeGroup.PinPolicy`, `SubvolumeClone.Status`, `Wait`, `Quiesce.ID`, `Status`, `SnapshotScheduleLease.Status`, `Snapshots` | Handle state. |
| Lifetime | `Filesystem.Remove`, `Subvolume.Remove`, `SubvolumeGroup.Remove`, `SubvolumeSnapshot.Remove`, `SubvolumeClone.RemovePartial`, `DataPool.RemoveUnused`, `SnapshotScheduleLease.Restore` | Removes fixture-created resources. |

## cephfs/mirror

| Category | API | Purpose |
| --- | --- | --- |
| Construction | `Run`, `WithConfig` | One-way snapshot mirroring between two existing filesystems. |
| Construction | `Mirror.AddDaemon`, `AddDirectory`, `BeginDirectoryAddition` | More mirror daemons; mirrored directories. |
| Condition | `Daemon.Remove`, `Daemon.Terminate`, `Daemon.InterruptPeerLink` | Loss of a mirror daemon or of its peer link. |
| Condition | `Mirror.RemoveDirectory`, `BeginDirectoryRemoval`, `RemovePeer`, `BeginPeerRemoval`, `RebootstrapPeer`, `RebalanceDirectories`, `AttachManagers` | Directory and peer changes and recovery. |
| Condition | `DirectoryRemoval.AcknowledgeProcessQuiescence`, `PeerRemoval.AcknowledgeProcessQuiescence` | Accepts an interrupted removal after explicit daemon removal. |
| Condition | `Mirror.RefreshMonitorConfig`, `RefreshPeerMonitorConfig` | Follows MON address changes on either side. |
| Observation | `Mirror.Daemons`, `PeerIDs`, `DirectoryStatus`, `WaitDirectoryReady`, `WaitSnapshotSynced` | Mirroring state and sync waits. |
| Observation | `DirectoryAddition.Status`, `DirectoryRemoval.Status`, `ProcessQuiescence`, `WaitReleased`, `PeerRemoval.Status`, `ProcessQuiescence`, `WaitDrained`, `Daemon.ProcessObserverBindingStatus` | Progress of additions and removals. |
| Lifetime | `Mirror.Terminate` | Removes owned containers and MGR attachments. |

## rbd

| Category | API | Purpose |
| --- | --- | --- |
| Construction | `Run`, `WithPools`, `InitPool`, `CreateNamespace` | RBD pools and namespaces. |
| Construction | `RestoreBackup`, `RestoreIncremental` | Seeds an image from a known archive or diff. |
| Condition | `StartImmutableObjectCache`, `ImmutableObjectCacheLease.Stop`, `Restore` | Runs, stops and removes the immutable object cache in a client container. |
| Observation | `ImageClients`, `ImageClientStatus.ExclusiveOwner`, `ListNamespaces`, `Namespace.Name`, `PoolName`, `ImmutableObjectCacheLease.Status` | Watchers, locks, namespaces, cache state. |
| Observation | `ExportBackup`, `ExportIncremental` | Reads an image or diff out so the test can compare what the SUT wrote. |
| Hand-off | `ImmutableObjectCacheLease.ClientOptions` | Options that make a librbd client use the cache. |
| Lifetime | `RemoveNamespace` | Removes a fixture-created namespace. |

## rbd/mirror

| Category | API | Purpose |
| --- | --- | --- |
| Construction | `Run`, `WithConfig` | Pool/namespace mirroring between two clusters with receiver daemons. |
| Construction | `Mirror.AddDaemon`, `EnableImage`, `BindNamespace` | More receivers; mirroring for an image; a namespace view. |
| Condition | `Daemon.Remove`, `Daemon.Terminate`, `Daemon.InterruptPeerLink`, `Mirror.Rebootstrap` | Loss of a receiver or of its link; peer re-bootstrap. |
| Condition | `Mirror.RefreshMonitorConfig` | Follows MON address changes. |
| Observation | `Mirror.Daemons`, `ImageStatus`, `PolicyStatus`, `ReceiverStatus`, `WaitReceiverReady`, `WaitReplayReady`, `Daemon.Status` | Mirroring and election state. |
| Observation | `Namespace.ImageStatus`, `ReceiverStatus`, `WaitReceiverReady`, `WaitReplayReady` | The same, scoped to a bound namespace. |
| Native access | `Mirror.SourceRBD`, `DestinationRBD` | RBD CLI in the setup clients. |
| Lifetime | `Mirror.Terminate` | Removes daemons and setup clients. |

## rgw

| Category | API | Purpose |
| --- | --- | --- |
| Construction | `Run`, `WithGateways`, `Start` | Cluster with gateways; a gateway on an existing cluster. |
| Construction | `Gateway.CreateUser`, `CreateAccount`, `Account.CreateRootUser`, `Gateway.CreatePlacement` | Identities, accounts and placement targets. |
| Construction | `User.SetQuota`, `SetBucketQuota`, `SetPlacement`, `Account.SetQuota`, `SetBucketQuota`, `Gateway.ApplyPlacement`, `ReloadPlacement` | Policies a test depends on, and their activation. |
| Condition | `User.Suspend` | Disables or re-enables an identity's access. |
| Condition | `Gateway.Remove` | Loss of a gateway. |
| Observation | `Gateways`, `Container.Gateways`, `User.Info`, `Usage`, `ID`, `Account.Info`, `ID`, `Placement.Status` | Native identity, policy and usage. |
| Observation | `User.ServedBy`, `Placement.ServedBy` | Whether a gateway can serve a handle. |
| Hand-off | `Gateway.S3Endpoint`, `S3SecureEndpoint`, `DaemonEndpoint`, `Container.S3Endpoint`, `User.Credentials` | Endpoints and S3 keys for the SUT. |
| Native access | `Gateway.Admin` | radosgw-admin in the gateway's scope. |
| Lifetime | `User.Remove`, `Account.Remove` | Removes fixture-created identities. |

## rgw/multisite

| Category | API | Purpose |
| --- | --- | --- |
| Construction | `Run`, `WithConfig`, `WithTopology` | A realm over existing clusters. |
| Construction | `Fixture.AddZone`, `AddZonegroup`, `CreateSyncGroup`, `SyncGroup.CreateFlow`, `CreatePipe`, `SetPipePrefix`, `SetStatus`, `Apply` | Zones, zonegroups and sync policy. |
| Condition | `Fixture.RemoveZone`, `PullSourcePeriod`, `PullDestinationPeriod` | Zone removal; period import for failover and failback. |
| Condition | `Fixture.InterruptZoneLink`, `ApplyZoneLinkProfile` | Cut or shaped link between gateways. |
| Condition | `SyncGroup.RemoveFlow`, `RemovePipe`, `Remove` | Removes sync policy. |
| Observation | `Fixture.Zones`, `Zonegroups`, `SyncStatus`, `WaitSyncReady`, `SyncGroup.ID`, `BucketSyncStatus`, `WaitBucketSyncReady`, `WaitBucketPolicyReady` | Realm, sync and bucket-sync state. |
| Native access | `Fixture.ZoneAdmin` | radosgw-admin in one zone's scope. |
| Lifetime | `Fixture.Terminate` | Removes gateways, CLI clients and the bridge. |

## fault

| Category | API | Purpose |
| --- | --- | --- |
| Condition | `InterruptNetwork`, `InterruptLink`, `NetworkInterruption.Restore` | Disconnects and reattaches a bridge endpoint. |
| Condition | `ApplyNetworkProfile`, `ApplyLinkProfile`, `NetworkProfileLease.Restore` | Delay, loss and other netem shaping below Ceph. |
| Condition | `Pause`, `PauseLease.Resume` | Freezes and thaws a container. |
| Condition | `InjectObjectDataError` | One-shot replica read error for scrub and recovery. |
| Observation | `NetworkProfileLease.Status`, `NetworkPacketFault.IsZero` | Applied qdiscs; whether a fault is set. |

## override

Every `Set*` records the prior value and returns a handle whose `Restore`
returns it.

| Category | API | Purpose |
| --- | --- | --- |
| Condition | `SetConfig`, `ConfigHandle.Restore` | One central config entry. |
| Condition | `SetOSDFlag`, `OSDFlagHandle.Restore` | One global OSD flag. |
| Condition | `SetFullRatios`, `FullRatiosHandle.Restore` | Near-full, backfill-full and full thresholds. |
| Condition | `SetMGRModule`, `MGRModuleHandle.Restore` | MGR module membership. |
| Condition | `SetMessengerPolicy`, `MessengerPolicyHandle.Restore` | Connection modes running daemons accept. |
| Condition | `AddBlocklist`, `BlocklistHandle.Restore` | Fences one client session. |
| Condition | `SetDebugLevel`, `DebugLevelHandle.Restore` | One OSD runtime debug level. |
| Observation | `BlocklistHandle.Address`, `DebugLevelHandle.Receipt` | What the handle changed. |

## capture

| Category | API | Purpose |
| --- | --- | --- |
| Evidence | `Start`, `Session.Stop`, `Settle`, `CaptureFile`, `SettlementAllowance` | Packet capture on one endpoint, settled into the failure archive. |

## Adding or changing an API

A new or changed public function or method names its category in the pull
request and passes that category's "belongs" test. A proposal that falls under
"does not belong" in every category, such as a service-operation API or a
convenience that only repeats what the SUT, the test or `Native access`
already does, is refused. Keeping an existing API that fails the test needs a
written reason in this page.
