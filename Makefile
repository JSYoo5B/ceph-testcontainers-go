.PHONY: test integration topology hostnetwork hostnetwork-multicluster multicluster goceph-linux vet image-compatibility image-matrix
.PHONY: check race tag-compile scenario-default topology-smoke scenario-rgw-sync-supported rgw-sync-native-regressions
.PHONY: scenario-topology scenario-multicluster-topology scenario-topology-extensions
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

TOPOLOGY_TESTS = ^Test(MonitorManagerTopology|MonitorRollingReplacement|ManagerLifecycle|CephFSMDSScaleTopology|CephFSMDSScaleStandbyReplayTopology|CephFSMultiActiveStandbyFailoverAndFilesystems|CephFSStandbyReplayFailover|RGWTopology|InitialClusterComposition)$$
TOPOLOGY_EXTENSION_TESTS = ^Test(SeparateClusterNetworksAndInterruptions|FiveMonitorQuorumAndNetworkRecovery|(MultiCluster|HostNetwork)(RBDMirrorDaemonTopology|CephFSMirrorDaemonRebalanceTopology|RGWInitialZonegroupsTopology|RGWZonegroupsAndRemovalTopology)|MultiCluster(RBDPeerNetworkInterruption|RGWPeerNetworkTopology))$$
MULTICLUSTER_TOPOLOGY_TESTS = ^Test(HostNetwork(MultiCluster|MonitorPortConflictRetry|RGWEndpoints|RBDSnapshotMirror|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology)|MultiCluster(MonitorBootstrapRefresh|RBDSnapshotMirror|RBDJournalMirrorFailback|RBDSnapshotFanout|RBDBackup|RBDPeerLifecycle|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology|RGWMetadataMasterFailover))$$

# These targets exercise ceph.DefaultImage directly. Clear component image
# overrides even when inherited from a local custom-image session. General
# integration/feature targets below continue to honor those overrides.
# Consumer-only overrides (e.g. the cryptsetup RBD image) are outside the required
# baseline; full client-fixtures remains an explicit optional target.
SCENARIO_TEST_ENV = env -u CEPH_TEST_IMAGE -u CEPH_TEST_OSD_IMAGE -u CEPH_TEST_RGW_IMAGE -u CEPH_TEST_MDS_IMAGE CGO_ENABLED=0

# Required fixture profiles also select the default native RBD consumer
# and the default real Vault backend, independent of custom-image sessions.
SCENARIO_FIXTURE_TEST_ENV = env -u CEPH_TEST_RBD_CLIENT_IMAGE -u CEPH_TEST_VAULT_IMAGE $(SCENARIO_TEST_ENV)
SCENARIO_FIXTURE_TAGS = integration,auth,features,topology,hostnetwork,multicluster
SCENARIO_CLUSTER_FIXTURE_TESTS = ^Test(ClientIdentities|CephFSSubvolumes|ConfigurationOverrides|OSDPolicies|OSDRemovalLifecycle|CephFSSubvolumeSnapshotsAndClones|RGWPlacementStorageClasses|HostNetworkRGWPlacementStorageClasses|RGWPlacementRealmStorageClasses)$$
SCENARIO_CEPHFS_FIXTURE_TESTS = ^Test(CephFSDynamicDataPools|CephFSCloneCancellationAndPartialCleanup|CephFSQuiesceCheckpoints|CephFSSubvolumeClientAuthorization|CephFSPins|CephFSRetainedSnapshotAndMetadataRecipe|CephFSAdditionalErasureCodedDataPool|HostNetworkCephFSFilesystem)$$
SCENARIO_RADOS_FIXTURE_TESTS = ^Test(ClientFencing|MGRModules|RADOSClientFixtures|NativePoolReplacement)$$
SCENARIO_RBD_FIXTURE_TESTS = ^Test(RBDClientFeatures|RBDAutomaticSnapshotSchedule|MultiClusterRBDMirrorScopeAndNamespaces|MultiClusterRBDFailback|MultiClusterRBDSplitBrainResync|HostNetworkRBDLifecycle)$$
SCENARIO_RGW_FIXTURE_TESTS = ^Test(RGWUserPlacementPolicy|HostNetworkRGWUserPlacementPolicy|RGWTenantsAndAccounts|HostNetworkRGWTenantsAndAccounts|RGWBucketMaintenance|RGWS3ClientFeatures|RGWNativeTLS|RGWProtocolBackends|RGWAdminRecordsAndRateLimit|HostNetworkHTTPTransportPreservesSignedRequest|RGWBackendSTSFormContentTypeIsSigned|RGWBackendRoleCleanupRefusesForeignPolicy|RGWBackendAuditProofRequiresCompletedVaultTransactions|RGWBackendStatusProbeReceivesBoundedContext)$$
SCENARIO_RGW_SYNC_FIXTURE_TESTS = ^Test(MultiClusterRGWSelectivePolicy|(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|AccountRootSync))$$
SCENARIO_RGW_TRANSLATION_FIXTURE_TESTS = ^Test(HostNetwork)?MultiClusterRGWSyncTranslationFiltering$$/(tag_owner_class|tenant_system_user_isolation)$$

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

# Required runtime baseline consumes the pinned default image; no slim/.deb/native
# image producer is a prerequisite. Tests and clusters execute sequentially.
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
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology,hostnetwork,multicluster -count=1 -v -timeout=$(SCENARIO_MULTICLUSTER_TOPOLOGY_TIMEOUT) -run '$(MULTICLUSTER_TOPOLOGY_TESTS)' ./internal/integration

scenario-topology-extensions:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration,topology,hostnetwork,multicluster -count=1 -v -timeout=$(TOPOLOGY_EXTENSIONS_TIMEOUT) -run '$(TOPOLOGY_EXTENSION_TESTS)' ./internal/integration

# Supported cluster/client fixtures consume the pinned default server directly.
# These profiles run every planned case, including bridge/host child scenarios.
scenario-cluster-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLUSTER_FEATURES_TIMEOUT) -run '$(SCENARIO_CLUSTER_FIXTURE_TESTS)' ./internal/integration

scenario-cephfs-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(SCENARIO_CEPHFS_FIXTURE_TESTS)' ./internal/integration

scenario-rados-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(SCENARIO_RADOS_FIXTURE_TESTS)' ./internal/integration

scenario-rbd-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(SCENARIO_RBD_FIXTURE_TESTS)' ./internal/integration

scenario-rgw-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(SCENARIO_RGW_FIXTURE_TESTS)' ./internal/integration

# Supported translation children stay separate from strict optional native
# priority/source-authorization regressions in rgw-sync-native-regressions.
scenario-rgw-sync-fixtures:
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '$(SCENARIO_RGW_SYNC_FIXTURE_TESTS)' ./internal/integration
	$(SCENARIO_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(SCENARIO_FIXTURE_TAGS) -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '$(SCENARIO_RGW_TRANSLATION_FIXTURE_TESTS)' ./internal/integration

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
