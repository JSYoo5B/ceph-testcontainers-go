.PHONY: test integration topology hostnetwork hostnetwork-multicluster multicluster goceph-linux vet image-compatibility image-matrix
.PHONY: check race tag-compile scenario-default topology-smoke scenario-rgw-sync-supported rgw-sync-native-regressions
.PHONY: scenario-topology scenario-multicluster-topology scenario-cephfs-removal scenario-topology-extensions scenario-rbd-receivers
.PHONY: scenario-diagnostics
.PHONY: scenario-cluster-fixtures scenario-cephfs-fixtures scenario-rados-fixtures scenario-rbd-fixtures scenario-rgw-fixtures scenario-rgw-sync-fixtures scenario-goceph-linux

MULTICLUSTER_TIMEOUT ?= 60m
HOSTNETWORK_TIMEOUT ?= 40m
TOPOLOGY_TIMEOUT ?= 40m
TOPOLOGY_EXTENSIONS_TIMEOUT ?= 90m
CLUSTER_FEATURES_TIMEOUT ?= 40m
CLUSTER_FEATURE_EXTENSIONS_TIMEOUT ?= 40m
CLIENT_FIXTURES_TIMEOUT ?= 120m
RGW_CLIENT_FIXTURES_TIMEOUT ?= 40m
INTEGRATION_TIMEOUT ?= 20m
TOPOLOGY_SMOKE_TIMEOUT ?= 45m
IMAGE_COMPATIBILITY_TIMEOUT ?= 40m
IMAGE_VARIANT ?= official
IMAGE_LAYOUT ?= all
IMAGE_PLATFORM ?=
SCENARIO_MULTICLUSTER_TOPOLOGY_TIMEOUT ?= 90m
SCENARIO_CEPHFS_REMOVAL_TIMEOUT ?= 90m
SCENARIO_RBD_RECEIVERS_TIMEOUT ?= 90m
SCENARIO_MULTICLUSTER_GROUP ?= all
SCENARIO_CEPHFS_REMOVAL_CASE ?= all
SCENARIO_RGW_SYNC_GROUP ?= all
SCENARIO_TOPOLOGY_EXTENSION_CASE ?= all
SCENARIO_CEPHFS_FIXTURE_CASE ?= all
SCENARIO_IMAGE_LAYOUT ?= all

TOPOLOGY_TESTS = ^Test(MonitorManagerTopology|MonitorRollingReplacement|ManagerLifecycle|CephFSMDSScaleTopology|CephFSMDSScaleStandbyReplayTopology|CephFSMultiActiveStandbyFailoverAndFilesystems|CephFSStandbyReplayFailover|RGWTopology|InitialClusterComposition)$$
TOPOLOGY_EXTENSION_TESTS = ^Test(SeparateClusterNetworksAndInterruptions|FiveMonitorQuorumAndNetworkRecovery|(MultiCluster|HostNetwork)(RBDMirrorDaemonTopology|CephFSMirrorDaemonRebalanceTopology|RGWInitialZonegroupsTopology|RGWZonegroupsAndRemovalTopology)|MultiCluster(RBDPeerNetworkInterruption|RGWPeerNetworkTopology))$$
MULTICLUSTER_TOPOLOGY_TESTS = ^Test(HostNetwork(MultiCluster|MonitorPortConflictRetry|RGWEndpoints|RBDSnapshotMirror|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology)|MultiCluster(TopologySnapshotsHonorBusyOwners|MonitorBootstrapRefresh|RBDSnapshotMirror|RBDJournalMirrorFailback|RBDSnapshotFanout|RBDBackup|RBDPeerLifecycle|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology|RGWMetadataMasterFailover))$$
SCENARIO_CEPHFS_REMOVAL_TESTS = ^TestMultiCluster(CephFSPeerRemovalDrain|CephFSDirectoryRemovalRelease|CephFSOriginalProcessQuiescence|CephFSOriginalProcessQuiescenceRecovery|CephFSDirectoryAdditionIntent)$$
SCENARIO_RBD_RECEIVERS_TESTS = ^TestMultiClusterRBDReceiverReadiness$$

# Keep the aggregate selectors for local runs. CI selects disjoint shards so
# unrelated scenarios do not consume each other's Go process timeout.
MULTICLUSTER_TOPOLOGY_TESTS_all = $(MULTICLUSTER_TOPOLOGY_TESTS)
MULTICLUSTER_TOPOLOGY_TESTS_infra = ^Test(HostNetwork(MultiCluster|MonitorPortConflictRetry)|MultiCluster(MonitorBootstrapRefresh|TopologySnapshotsHonorBusyOwners))$$
MULTICLUSTER_TOPOLOGY_TESTS_rbd = ^Test(HostNetworkRBDSnapshotMirror|MultiCluster(RBDSnapshotMirror|RBDJournalMirrorFailback|RBDSnapshotFanout|RBDBackup|RBDPeerLifecycle))$$
MULTICLUSTER_TOPOLOGY_TESTS_cephfs = ^Test(HostNetwork|MultiCluster)CephFS(SnapshotMirrorAndBackup|ManagerTopology)$$
MULTICLUSTER_TOPOLOGY_TESTS_rgw = ^Test(HostNetwork(RGWEndpoints|RGWMultisite|RGWThreeZoneTopology)|MultiCluster(RGWMultisite|RGWThreeZoneTopology|RGWMetadataMasterFailover))$$
MULTICLUSTER_TOPOLOGY_TESTS_rgw-endpoints-host = ^TestHostNetworkRGWEndpoints$$
MULTICLUSTER_TOPOLOGY_TESTS_rgw-multisite-bridge = ^TestMultiClusterRGWMultisite$$
MULTICLUSTER_TOPOLOGY_TESTS_rgw-multisite-host = ^TestHostNetworkRGWMultisite$$
MULTICLUSTER_TOPOLOGY_TESTS_rgw-three-zone-bridge = ^TestMultiClusterRGWThreeZoneTopology$$
MULTICLUSTER_TOPOLOGY_TESTS_rgw-three-zone-host = ^TestHostNetworkRGWThreeZoneTopology$$
MULTICLUSTER_TOPOLOGY_TESTS_rgw-master-failover = ^TestMultiClusterRGWMetadataMasterFailover$$
TOPOLOGY_EXTENSION_TESTS_all = $(TOPOLOGY_EXTENSION_TESTS)
TOPOLOGY_EXTENSION_TESTS_network-interruption = ^TestSeparateClusterNetworksAndInterruptions$$
TOPOLOGY_EXTENSION_TESTS_five-monitors = ^TestFiveMonitorQuorumAndNetworkRecovery$$
TOPOLOGY_EXTENSION_TESTS_rbd-mirror-bridge = ^TestMultiClusterRBDMirrorDaemonTopology$$
TOPOLOGY_EXTENSION_TESTS_rbd-mirror-host = ^TestHostNetworkRBDMirrorDaemonTopology$$
TOPOLOGY_EXTENSION_TESTS_cephfs-mirror-bridge = ^TestMultiClusterCephFSMirrorDaemonRebalanceTopology$$
TOPOLOGY_EXTENSION_TESTS_cephfs-mirror-host = ^TestHostNetworkCephFSMirrorDaemonRebalanceTopology$$
TOPOLOGY_EXTENSION_TESTS_rgw-initial-bridge = ^TestMultiClusterRGWInitialZonegroupsTopology$$
TOPOLOGY_EXTENSION_TESTS_rgw-initial-host = ^TestHostNetworkRGWInitialZonegroupsTopology$$
TOPOLOGY_EXTENSION_TESTS_rgw-removal-bridge = ^TestMultiClusterRGWZonegroupsAndRemovalTopology$$
TOPOLOGY_EXTENSION_TESTS_rgw-removal-host = ^TestHostNetworkRGWZonegroupsAndRemovalTopology$$
TOPOLOGY_EXTENSION_TESTS_rbd-peer-network = ^TestMultiClusterRBDPeerNetworkInterruption$$
TOPOLOGY_EXTENSION_TESTS_rgw-peer-network = ^TestMultiClusterRGWPeerNetworkTopology$$
CEPHFS_REMOVAL_TESTS_all = $(SCENARIO_CEPHFS_REMOVAL_TESTS)
CEPHFS_REMOVAL_TESTS_peer-drain = ^TestMultiClusterCephFSPeerRemovalDrain$$
CEPHFS_REMOVAL_TESTS_directory-release = ^TestMultiClusterCephFSDirectoryRemovalRelease$$
CEPHFS_REMOVAL_TESTS_process-quiescence = ^TestMultiClusterCephFSOriginalProcessQuiescence$$
CEPHFS_REMOVAL_TESTS_process-recovery = ^TestMultiClusterCephFSOriginalProcessQuiescenceRecovery$$
CEPHFS_REMOVAL_TESTS_directory-intent = ^TestMultiClusterCephFSDirectoryAdditionIntent$$

# The default clears inherited image overrides and exercises ceph.DefaultImage.
# CI can explicitly select prepared role images without changing module defaults.
# Required native clients use the selected control/all image and its runtime
# contract; full client-fixtures remains an explicit optional target.
ifeq ($(SCENARIO_IMAGE_LAYOUT),roles)
$(foreach name,CEPH_TEST_IMAGE CEPH_TEST_OSD_IMAGE CEPH_TEST_RGW_IMAGE CEPH_TEST_MDS_IMAGE,$(if $(strip $($(name))),,$(error $(name) is required for SCENARIO_IMAGE_LAYOUT=roles)))
SCENARIO_TEST_ENV = env CGO_ENABLED=0
else ifeq ($(SCENARIO_IMAGE_LAYOUT),all)
SCENARIO_TEST_ENV = env -u CEPH_TEST_IMAGE -u CEPH_TEST_OSD_IMAGE -u CEPH_TEST_RGW_IMAGE -u CEPH_TEST_MDS_IMAGE CGO_ENABLED=0
else
$(error Unknown SCENARIO_IMAGE_LAYOUT)
endif

# Required fixture profiles verify the selected control/all image as the native
# RBD consumer, including its required cryptsetup executable and libraries.
# Vault is an external KMS fixture; preserve an explicitly selected registry
# image and let the backend recipe select its default when unset.
SCENARIO_FIXTURE_TEST_ENV = env -u CEPH_TEST_RBD_CLIENT_IMAGE $(SCENARIO_TEST_ENV)
SCENARIO_FIXTURE_TAGS = integration,auth,features,topology,hostnetwork,multicluster
SCENARIO_CLUSTER_FIXTURE_TESTS = ^Test(ClientIdentities|CephFSSubvolumes|ConfigurationOverrides|OSDPolicies|OSDRemovalLifecycle|CephFSSubvolumeSnapshotsAndClones|RGWPlacementStorageClasses|HostNetworkRGWPlacementStorageClasses|RGWPlacementRealmStorageClasses)$$
SCENARIO_CEPHFS_FIXTURE_TESTS = ^Test(CephFSDynamicDataPools|CephFSCloneCancellationAndPartialCleanup|CephFSQuiesceCheckpoints|CephFSSubvolumeClientAuthorization|CephFSPins|CephFSRetainedSnapshotAndMetadataRecipe|CephFSAdditionalErasureCodedDataPool|HostNetworkCephFSFilesystem)$$
CEPHFS_FIXTURE_TESTS_all = $(SCENARIO_CEPHFS_FIXTURE_TESTS)
CEPHFS_FIXTURE_TESTS_data-pools = ^TestCephFSDynamicDataPools$$
CEPHFS_FIXTURE_TESTS_clone-cancellation = ^TestCephFSCloneCancellationAndPartialCleanup$$
CEPHFS_FIXTURE_TESTS_quiesce = ^TestCephFSQuiesceCheckpoints$$
CEPHFS_FIXTURE_TESTS_authorization = ^TestCephFSSubvolumeClientAuthorization$$
CEPHFS_FIXTURE_TESTS_pins = ^TestCephFSPins$$
CEPHFS_FIXTURE_TESTS_retained-snapshot = ^TestCephFSRetainedSnapshotAndMetadataRecipe$$
CEPHFS_FIXTURE_TESTS_ec-data-pool = ^TestCephFSAdditionalErasureCodedDataPool$$
CEPHFS_FIXTURE_TESTS_host-filesystem = ^TestHostNetworkCephFSFilesystem$$
SCENARIO_RADOS_FIXTURE_TESTS = ^Test(ClientFencing|MGRModules|RADOSClientFixtures|NativePoolReplacement)$$
SCENARIO_RBD_FIXTURE_TESTS = ^Test(RBDClientFeatures|RBDAutomaticSnapshotSchedule|MultiClusterRBDMirrorScopeAndNamespaces|MultiClusterRBDFailback|MultiClusterRBDSplitBrainResync|HostNetworkRBDLifecycle)$$
SCENARIO_RGW_FIXTURE_TESTS = ^Test(RGWUserPlacementPolicy|HostNetworkRGWUserPlacementPolicy|RGWTenantsAndAccounts|HostNetworkRGWTenantsAndAccounts|RGWBucketMaintenance|RGWS3ClientFeatures|RGWNativeTLS|RGWProtocolBackends|RGWAdminRecordsAndRateLimit|HostNetworkHTTPTransportPreservesSignedRequest|RGWBackendSTSFormContentTypeIsSigned|RGWBackendRoleCleanupRefusesForeignPolicy|RGWBackendAuditProofRequiresCompletedVaultTransactions|RGWBackendStatusProbeReceivesBoundedContext)$$
SCENARIO_RGW_SYNC_FIXTURE_TESTS = ^Test(MultiClusterRGWSelectivePolicy|(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|AccountRootSync))$$
SCENARIO_RGW_TRANSLATION_FIXTURE_TESTS = ^Test(HostNetwork)?MultiClusterRGWSyncTranslationFiltering$$/(tag_owner_class|tenant_system_user_isolation)$$
RGW_SYNC_FIXTURE_TESTS_policy = ^Test(MultiClusterRGWSelectivePolicy|(HostNetwork)?MultiClusterRGWOwnedSyncPolicy)$$
RGW_SYNC_FIXTURE_TESTS_account = ^Test(HostNetwork)?MultiClusterRGWAccountRootSync$$
RGW_SYNC_FIXTURE_TESTS_translation = $(SCENARIO_RGW_TRANSLATION_FIXTURE_TESTS)

.PHONY: topology-extensions
.PHONY: cluster-features
.PHONY: cluster-feature-extensions
.PHONY: client-fixtures
.PHONY: rgw-s3-fixtures rgw-protocol-fixtures rgw-admin-fixtures rgw-sync-fixtures

rgw-s3-fixtures:
	CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=$(RGW_CLIENT_FIXTURES_TIMEOUT) -run '^TestRGW(BucketMaintenance|S3ClientFeatures|NativeTLS)$$' ./internal/integration

rgw-protocol-fixtures:
	CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=$(RGW_CLIENT_FIXTURES_TIMEOUT) -run '^TestRGWProtocolBackends$$' ./internal/integration

rgw-admin-fixtures:
	CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=$(RGW_CLIENT_FIXTURES_TIMEOUT) -run '^TestRGWAdminRecordsAndRateLimit$$' ./internal/integration

rgw-sync-fixtures:
	CGO_ENABLED=0 go test -tags=integration,features,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^Test(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|SyncTranslationFiltering|AccountRootSync)$$' ./internal/integration

# Default-image coverage: keep strict data/permission/checkpoint assertions, and
# select independent supported children without executing the two native defects.
scenario-rgw-sync-supported:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,features,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^Test(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|AccountRootSync)$$' ./internal/integration
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,features,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^Test(HostNetwork)?MultiClusterRGWSyncTranslationFiltering$$/(tag_owner_class|tenant_system_user_isolation)$$' ./internal/integration

# Explicit strict regression gate. Original 20.2.4 has native priority and source
# user-authorization defects here; a caller may supply an existing patched RGW
# image via CEPH_TEST_RGW_IMAGE. This target builds/publishes no image and has no
# expected-failure conversion. The original full rgw-sync-fixtures is retained.
rgw-sync-native-regressions:
	CGO_ENABLED=0 go test -mod=readonly -tags=integration,features,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^Test(HostNetwork)?MultiClusterRGWSyncTranslationFiltering$$/(priority_tags_owner_class|ordinary_user_denial_grant)$$' ./internal/integration

client-fixtures:
	CGO_ENABLED=0 go test -tags=integration,features,multicluster -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '^Test(ClientFencing|MGRModules|RADOSClientFixtures|NativePoolReplacement|CephFSDynamicDataPools|CephFSCloneCancellationAndPartialCleanup|CephFSQuiesceCheckpoints|CephFSSubvolumeClientAuthorization|CephFSPins|CephFSRetainedSnapshotAndMetadataRecipe|(HostNetwork)?RGWUserPlacementPolicy|(HostNetwork)?RGWTenantsAndAccounts|RGWBucketMaintenance|RGWS3ClientFeatures|RGWNativeTLS|RGWProtocolBackends|RGWAdminRecordsAndRateLimit|(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|SyncTranslationFiltering|AccountRootSync)|RBDClientFeatures|RBDAutomaticSnapshotSchedule|MultiClusterRBDMirrorScopeAndNamespaces)$$' ./internal/integration

cluster-features:
	CGO_ENABLED=0 go test -tags=integration,auth,features -count=1 -v -timeout=$(CLUSTER_FEATURES_TIMEOUT) -run '^Test(PoolPolicies|ClientIdentities|RBDNamespaces|CephFSSubvolumes|(HostNetwork)?RGWUserAdministration)$$' ./internal/integration

cluster-feature-extensions:
	CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=$(CLUSTER_FEATURE_EXTENSIONS_TIMEOUT) -run '^Test(ConfigurationOverrides|OSDPolicies|CephFSSubvolumeSnapshotsAndClones|(HostNetwork)?RGWPlacementStorageClasses|RGWPlacementRealmStorageClasses)$$' ./internal/integration

test:
	CGO_ENABLED=0 go test -mod=readonly ./...

# Go host checks. Tag compilation does not execute Docker or native clients.
# Image builder checks belong to ceph-testcontainers-images.
check:
	$(MAKE) test
	$(MAKE) race
	$(MAKE) vet
	$(MAKE) tag-compile

race:
	CGO_ENABLED=1 go test -mod=readonly -race ./...

tag-compile:
	CGO_ENABLED=0 go test -mod=readonly -tags=integration,auth,features,multicluster,topology,hostnetwork,goceph,diagnostics -run '^$$' ./...

integration:
	CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=$(INTEGRATION_TIMEOUT) ./internal/integration

# Required scenarios consume selected existing images; the default is Quay all.
# No image producer is required. Tests and clusters execute sequentially.
scenario-default:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration -count=1 -v -timeout=$(INTEGRATION_TIMEOUT) ./internal/integration

# Optional diagnostic collection proof; excluded from the established 101 tests.
scenario-diagnostics:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,diagnostics -count=1 -v -timeout=$(INTEGRATION_TIMEOUT) -run '^Test(ClusterDiagnostics|PartialClusterDiagnostics)$$' ./internal/integration

topology-smoke:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology,multicluster -count=1 -v -timeout=$(TOPOLOGY_SMOKE_TIMEOUT) -run '^Test(MonitorManagerTopology|MultiClusterRGWMultisite)$$' ./internal/integration

# Major topology profiles run every scenario sequentially within each process.
# Separate bounded CI jobs use these profiles; no image producer is required.
scenario-topology:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology -count=1 -v -timeout=$(TOPOLOGY_TIMEOUT) -run '$(TOPOLOGY_TESTS)' ./internal/integration

scenario-multicluster-topology:
	@test -n '$(MULTICLUSTER_TOPOLOGY_TESTS_$(SCENARIO_MULTICLUSTER_GROUP))' || { echo 'Unknown SCENARIO_MULTICLUSTER_GROUP' >&2; exit 1; }
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology,hostnetwork,multicluster -count=1 -v -timeout=$(SCENARIO_MULTICLUSTER_TOPOLOGY_TIMEOUT) -run '$(MULTICLUSTER_TOPOLOGY_TESTS_$(SCENARIO_MULTICLUSTER_GROUP))' ./internal/integration

# Retained peer/directory removal and registration recovery are
# isolated from the general multicluster budget; every bridge/host case runs.
scenario-cephfs-removal:
	@test -n '$(CEPHFS_REMOVAL_TESTS_$(SCENARIO_CEPHFS_REMOVAL_CASE))' || { echo 'Unknown SCENARIO_CEPHFS_REMOVAL_CASE' >&2; exit 1; }
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology,hostnetwork,multicluster -count=1 -v -timeout=$(SCENARIO_CEPHFS_REMOVAL_TIMEOUT) -run '$(CEPHFS_REMOVAL_TESTS_$(SCENARIO_CEPHFS_REMOVAL_CASE))' ./internal/integration

# Receiver readiness and observed snapshot checkpoints use a separate budget.
# Every scope and bridge/host case uses the explicitly selected scenario layout.
scenario-rbd-receivers:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,multicluster -count=1 -v -timeout=$(SCENARIO_RBD_RECEIVERS_TIMEOUT) -run '$(SCENARIO_RBD_RECEIVERS_TESTS)' ./internal/integration

scenario-topology-extensions:
	@test -n '$(TOPOLOGY_EXTENSION_TESTS_$(SCENARIO_TOPOLOGY_EXTENSION_CASE))' || { echo 'Unknown SCENARIO_TOPOLOGY_EXTENSION_CASE' >&2; exit 1; }
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology,hostnetwork,multicluster -count=1 -v -timeout=$(TOPOLOGY_EXTENSIONS_TIMEOUT) -run '$(TOPOLOGY_EXTENSION_TESTS_$(SCENARIO_TOPOLOGY_EXTENSION_CASE))' ./internal/integration

# Supported cluster/client fixtures consume the selected default/role images.
# These profiles run every planned case, including bridge/host child scenarios.
scenario-cluster-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLUSTER_FEATURES_TIMEOUT) -run '$(SCENARIO_CLUSTER_FIXTURE_TESTS)' ./internal/integration

scenario-cephfs-fixtures:
	@test -n '$(CEPHFS_FIXTURE_TESTS_$(SCENARIO_CEPHFS_FIXTURE_CASE))' || { echo 'Unknown SCENARIO_CEPHFS_FIXTURE_CASE' >&2; exit 1; }
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(CEPHFS_FIXTURE_TESTS_$(SCENARIO_CEPHFS_FIXTURE_CASE))' ./internal/integration

scenario-rados-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(SCENARIO_RADOS_FIXTURE_TESTS)' ./internal/integration

scenario-rbd-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(SCENARIO_RBD_FIXTURE_TESTS)' ./internal/integration

scenario-rgw-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(SCENARIO_RGW_FIXTURE_TESTS)' ./internal/integration

# Supported translation children stay separate from strict optional native
# priority/source-authorization regressions in rgw-sync-native-regressions.
scenario-rgw-sync-fixtures:
ifeq ($(SCENARIO_RGW_SYNC_GROUP),all)
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '$(SCENARIO_RGW_SYNC_FIXTURE_TESTS)' ./internal/integration
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '$(SCENARIO_RGW_TRANSLATION_FIXTURE_TESTS)' ./internal/integration
else
	@test -n '$(RGW_SYNC_FIXTURE_TESTS_$(SCENARIO_RGW_SYNC_GROUP))' || { echo 'Unknown SCENARIO_RGW_SYNC_GROUP' >&2; exit 1; }
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '$(RGW_SYNC_FIXTURE_TESTS_$(SCENARIO_RGW_SYNC_GROUP))' ./internal/integration
endif

# Consume existing client/runner images prepared by the caller.
# CEPH_TEST_GOCEPH_CLIENT_IMAGE and CEPH_TEST_GOCEPH_RUNNER_IMAGE are required.
scenario-goceph-linux:
	$(SCENARIO_TEST_ENV) python3 internal/integration/goceph/run.py

# Module-level representative compatibility for supplied all or role images.
# Image checker quick/full is independent; this target never builds images.
image-compatibility:
	CGO_ENABLED=0 go test -mod=readonly -tags=integration,topology,multicluster -count=1 -v -timeout=$(IMAGE_COMPATIBILITY_TIMEOUT) -run '^Test(ClusterLifecycle|RBDLifecycle|CephFSFilesystem|RGWS3|ManagerLifecycle|MultiCluster(RBDBackup|RBDSnapshotMirror|CephFSSnapshotMirrorAndBackup|RGWMultisite))$$' ./internal/integration

# Select existing official/GHCR images, freeze their IDs, then run the same Go tests.
image-matrix:
	python3 .github/scripts/run_image_matrix.py --variant "$(IMAGE_VARIANT)" --layout "$(IMAGE_LAYOUT)" $(if $(IMAGE_PLATFORM),--platform "$(IMAGE_PLATFORM)",)

topology:
	CGO_ENABLED=0 go test -tags=integration,topology -count=1 -v -timeout=$(TOPOLOGY_TIMEOUT) -run '$(TOPOLOGY_TESTS)' ./internal/integration

topology-extensions:
	CGO_ENABLED=0 go test -tags=integration,topology,multicluster,hostnetwork -count=1 -v -timeout=$(TOPOLOGY_EXTENSIONS_TIMEOUT) -run '$(TOPOLOGY_EXTENSION_TESTS)' ./internal/integration

# Execute supplied client/runner images on the Docker Linux host.
# The nested probe module remains separate from this library's dependencies.
goceph-linux:
	python3 internal/integration/goceph/run.py

hostnetwork:
	CGO_ENABLED=0 go test -tags=integration,hostnetwork -count=1 -v -timeout=$(HOSTNETWORK_TIMEOUT) -run '^TestHostNetwork' ./internal/integration

hostnetwork-multicluster:
	CGO_ENABLED=0 go test -tags=integration,hostnetwork,multicluster -count=1 -v -timeout=$(HOSTNETWORK_TIMEOUT) -run '^TestHostNetwork(RBDSnapshotMirror|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology)$$' ./internal/integration

multicluster:
	CGO_ENABLED=0 go test -tags=integration,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^TestMultiCluster' ./internal/integration

vet:
	CGO_ENABLED=0 go vet -mod=readonly ./...

# Zero-initial mirror construction uses its own sequential bridge/host budget.
.PHONY: scenario-mirror-initial-daemons
SCENARIO_MIRROR_INITIAL_DAEMONS_TIMEOUT ?= 150m
SCENARIO_MIRROR_INITIAL_DAEMONS_TESTS = ^TestMultiClusterNoInitialMirrorDaemons$$

scenario-mirror-initial-daemons:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,multicluster -count=1 -v -timeout=$(SCENARIO_MIRROR_INITIAL_DAEMONS_TIMEOUT) -run '$(SCENARIO_MIRROR_INITIAL_DAEMONS_TESTS)' ./internal/integration

# Existing namespace bindings use a separate shared-pool bridge/host budget.
.PHONY: scenario-rbd-namespaces
SCENARIO_RBD_NAMESPACES_TIMEOUT ?= 90m
SCENARIO_RBD_NAMESPACES_TESTS = ^TestMultiClusterRBDNamespaceBinding$$

scenario-rbd-namespaces:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,multicluster -count=1 -v -timeout=$(SCENARIO_RBD_NAMESPACES_TIMEOUT) -run '$(SCENARIO_RBD_NAMESPACES_TESTS)' ./internal/integration

# Bootstrap without initial storage has a separate bridge/host lifecycle budget.
.PHONY: scenario-storage-bootstrap
SCENARIO_STORAGE_BOOTSTRAP_TIMEOUT ?= 80m
SCENARIO_STORAGE_BOOTSTRAP_TESTS = ^TestNoInitialOSDTopology$$

scenario-storage-bootstrap:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology -count=1 -failfast -v -timeout=$(SCENARIO_STORAGE_BOOTSTRAP_TIMEOUT) -run '$(SCENARIO_STORAGE_BOOTSTRAP_TESTS)' ./internal/integration

# Bootstrap without an initial manager has a separate bridge/host lifecycle budget.
.PHONY: scenario-manager-bootstrap
SCENARIO_MANAGER_BOOTSTRAP_TIMEOUT ?= 80m
SCENARIO_MANAGER_BOOTSTRAP_TESTS = ^TestNoInitialManagerTopology$$

scenario-manager-bootstrap:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology -count=1 -failfast -v -timeout=$(SCENARIO_MANAGER_BOOTSTRAP_TIMEOUT) -run '$(SCENARIO_MANAGER_BOOTSTRAP_TESTS)' ./internal/integration

# Scoped image replay observations have an independent shared-owner lifecycle budget.
.PHONY: scenario-rbd-namespace-observation
SCENARIO_RBD_NAMESPACE_OBSERVATION_TIMEOUT ?= 90m
SCENARIO_RBD_NAMESPACE_OBSERVATION_TESTS = ^TestMultiClusterRBDNamespaceImageObservation$$

scenario-rbd-namespace-observation:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,multicluster -count=1 -failfast -v -timeout=$(SCENARIO_RBD_NAMESPACE_OBSERVATION_TIMEOUT) -run '$(SCENARIO_RBD_NAMESPACE_OBSERVATION_TESTS)' ./internal/integration

# Bootstrap without initial MDS uses a separate bridge/host lifecycle budget.
.PHONY: scenario-mds-bootstrap
SCENARIO_MDS_BOOTSTRAP_TIMEOUT ?= 50m
SCENARIO_MDS_BOOTSTRAP_TESTS = ^TestNoInitialMDSTopology$$

scenario-mds-bootstrap:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology -count=1 -failfast -v -timeout=$(SCENARIO_MDS_BOOTSTRAP_TIMEOUT) -run '$(SCENARIO_MDS_BOOTSTRAP_TESTS)' ./internal/integration

# Stopped owned MDS retirement/replacement has its own bridge/host budget.
.PHONY: scenario-mds-replacement
SCENARIO_MDS_REPLACEMENT_TIMEOUT ?= 50m
SCENARIO_MDS_REPLACEMENT_TESTS = ^TestStoppedMDSRetirementTopology$$

scenario-mds-replacement:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology -count=1 -failfast -v -timeout=$(SCENARIO_MDS_REPLACEMENT_TIMEOUT) -run '$(SCENARIO_MDS_REPLACEMENT_TESTS)' ./internal/integration

# Recover a failed sole MDS with a new owned member under an isolated budget.
.PHONY: scenario-last-mds-replacement
SCENARIO_LAST_MDS_REPLACEMENT_TIMEOUT ?= 50m
SCENARIO_LAST_MDS_REPLACEMENT_TESTS = ^TestLastMDSReplacementTopology$$

scenario-last-mds-replacement:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology -count=1 -failfast -v -timeout=$(SCENARIO_LAST_MDS_REPLACEMENT_TIMEOUT) -run '$(SCENARIO_LAST_MDS_REPLACEMENT_TESTS)' ./internal/integration
