.PHONY: test integration topology hostnetwork hostnetwork-multicluster multicluster goceph-linux vet
.PHONY: check race tag-compile quay-default topology-smoke rgw-sync-fixtures-quay rgw-sync-native-regressions
.PHONY: quay-topology quay-multicluster-topology quay-topology-extensions
.PHONY: quay-cluster-fixtures quay-cephfs-fixtures quay-rados-fixtures quay-rbd-fixtures quay-rgw-fixtures quay-rgw-sync-fixtures quay-goceph-linux

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
QUAY_MULTICLUSTER_TOPOLOGY_TIMEOUT ?= 90m

TOPOLOGY_TESTS = ^Test(MonitorManagerTopology|ManagerLifecycle|CephFSMDSScaleTopology|CephFSMDSScaleStandbyReplayTopology|CephFSMultiActiveStandbyFailoverAndFilesystems|CephFSStandbyReplayFailover|RGWTopology|InitialClusterComposition)$$
TOPOLOGY_EXTENSION_TESTS = ^Test(SeparateClusterNetworksAndInterruptions|FiveMonitorQuorumAndNetworkRecovery|(MultiCluster|HostNetwork)(RBDMirrorDaemonTopology|CephFSMirrorDaemonRebalanceTopology|RGWInitialZonegroupsTopology|RGWZonegroupsAndRemovalTopology)|MultiCluster(RBDPeerNetworkInterruption|RGWPeerNetworkTopology))$$
MULTICLUSTER_TOPOLOGY_TESTS = ^Test(HostNetwork(MultiCluster|MonitorPortConflictRetry|RGWEndpoints|RBDSnapshotMirror|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology)|MultiCluster(RBDSnapshotMirror|RBDJournalMirrorFailback|RBDSnapshotFanout|RBDBackup|RBDPeerLifecycle|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology|RGWMetadataMasterFailover))$$

# These targets exercise ceph.DefaultImage directly. Clear daemon/mirror image
# overrides even when inherited from a local custom-image session. General
# integration/feature targets below continue to honor those overrides.
# Consumer-only overrides (e.g. the cryptsetup RBD image) are outside the required
# baseline; full client-fixtures remains an explicit optional target.
QUAY_TEST_ENV = env -u CEPH_TEST_IMAGE -u CEPH_TEST_OSD_IMAGE -u CEPH_TEST_RGW_IMAGE -u CEPH_TEST_MDS_IMAGE -u CEPH_TEST_MIRROR_IMAGE CGO_ENABLED=0

# Required fixture profiles also select the original Quay native RBD consumer
# and the default real Vault backend, independent of custom-image sessions.
QUAY_FIXTURE_TEST_ENV = env -u CEPH_TEST_RBD_CLIENT_IMAGE -u CEPH_TEST_VAULT_IMAGE $(QUAY_TEST_ENV)
QUAY_FIXTURE_TAGS = integration,auth,features,topology,hostnetwork,multicluster
QUAY_CLUSTER_FIXTURE_TESTS = ^Test(ClientIdentities|CephFSSubvolumes|ConfigurationOverrides|OSDPolicies|CephFSSubvolumeSnapshotsAndClones|RGWPlacementStorageClasses|HostNetworkRGWPlacementStorageClasses|RGWPlacementRealmStorageClasses)$$
QUAY_CEPHFS_FIXTURE_TESTS = ^Test(CephFSDynamicDataPools|CephFSCloneCancellationAndPartialCleanup|CephFSQuiesceCheckpoints|CephFSSubvolumeClientAuthorization|CephFSPins|CephFSRetainedSnapshotAndMetadataRecipe|CephFSAdditionalErasureCodedDataPool|HostNetworkCephFSFilesystem)$$
QUAY_RADOS_FIXTURE_TESTS = ^Test(ClientFencing|MGRModules|RADOSClientFixtures|NativePoolReplacement)$$
QUAY_RBD_FIXTURE_TESTS = ^Test(RBDClientFeatures|RBDAutomaticSnapshotSchedule|MultiClusterRBDMirrorScopeAndNamespaces|MultiClusterRBDFailback|MultiClusterRBDSplitBrainResync|HostNetworkRBDLifecycle)$$
QUAY_RGW_FIXTURE_TESTS = ^Test(RGWUserPlacementPolicy|HostNetworkRGWUserPlacementPolicy|RGWTenantsAndAccounts|HostNetworkRGWTenantsAndAccounts|RGWBucketMaintenance|RGWS3ClientFeatures|RGWNativeTLS|RGWProtocolBackends|RGWAdminRecordsAndRateLimit|HostNetworkHTTPTransportPreservesSignedRequest|RGWBackendSTSFormContentTypeIsSigned|RGWBackendRoleCleanupRefusesForeignPolicy|RGWBackendAuditProofRequiresCompletedVaultTransactions|RGWBackendStatusProbeReceivesBoundedContext)$$
QUAY_RGW_SYNC_FIXTURE_TESTS = ^Test(MultiClusterRGWSelectivePolicy|(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|AccountRootSync))$$
QUAY_RGW_TRANSLATION_FIXTURE_TESTS = ^Test(HostNetwork)?MultiClusterRGWSyncTranslationFiltering$$/(tag_owner_class|tenant_system_user_isolation)$$

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

# Original Quay coverage: keep strict data/permission/checkpoint assertions, and
# select independent supported children without executing the two native defects.
rgw-sync-fixtures-quay:
	$(QUAY_TEST_ENV) go test -mod=readonly -tags=integration,features,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^Test(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|AccountRootSync)$$' ./internal/integration
	$(QUAY_TEST_ENV) go test -mod=readonly -tags=integration,features,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^Test(HostNetwork)?MultiClusterRGWSyncTranslationFiltering$$/(tag_owner_class|tenant_system_user_isolation)$$' ./internal/integration

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
	CGO_ENABLED=0 go test -mod=readonly -tags=integration,auth,features,multicluster,topology,hostnetwork,goceph -run '^$$' ./...

integration:
	CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=$(INTEGRATION_TIMEOUT) ./internal/integration

# Required runtime baseline consumes the pinned Quay image; no slim/.deb/native
# image producer is a prerequisite. Tests and clusters execute sequentially.
quay-default:
	$(QUAY_TEST_ENV) go test -mod=readonly -tags=integration -count=1 -v -timeout=$(INTEGRATION_TIMEOUT) ./internal/integration

topology-smoke:
	$(QUAY_TEST_ENV) go test -mod=readonly -tags=integration,topology,multicluster -count=1 -v -timeout=$(TOPOLOGY_SMOKE_TIMEOUT) -run '^Test(MonitorManagerTopology|MultiClusterRGWMultisite)$$' ./internal/integration

# Major topology profiles run every scenario sequentially within each process.
# Separate bounded CI jobs use these profiles; no image producer is required.
quay-topology:
	$(QUAY_TEST_ENV) go test -mod=readonly -tags=integration,topology -count=1 -v -timeout=$(TOPOLOGY_TIMEOUT) -run '$(TOPOLOGY_TESTS)' ./internal/integration

quay-multicluster-topology:
	$(QUAY_TEST_ENV) go test -mod=readonly -tags=integration,topology,hostnetwork,multicluster -count=1 -v -timeout=$(QUAY_MULTICLUSTER_TOPOLOGY_TIMEOUT) -run '$(MULTICLUSTER_TOPOLOGY_TESTS)' ./internal/integration

quay-topology-extensions:
	$(QUAY_TEST_ENV) go test -mod=readonly -tags=integration,topology,hostnetwork,multicluster -count=1 -v -timeout=$(TOPOLOGY_EXTENSIONS_TIMEOUT) -run '$(TOPOLOGY_EXTENSION_TESTS)' ./internal/integration

# Supported cluster/client fixtures consume the pinned Quay server directly.
# These profiles run every planned case, including bridge/host child scenarios.
quay-cluster-fixtures:
	$(QUAY_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(QUAY_FIXTURE_TAGS) -count=1 -v -timeout=$(CLUSTER_FEATURES_TIMEOUT) -run '$(QUAY_CLUSTER_FIXTURE_TESTS)' ./internal/integration

quay-cephfs-fixtures:
	$(QUAY_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(QUAY_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(QUAY_CEPHFS_FIXTURE_TESTS)' ./internal/integration

quay-rados-fixtures:
	$(QUAY_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(QUAY_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(QUAY_RADOS_FIXTURE_TESTS)' ./internal/integration

quay-rbd-fixtures:
	$(QUAY_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(QUAY_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(QUAY_RBD_FIXTURE_TESTS)' ./internal/integration

quay-rgw-fixtures:
	$(QUAY_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(QUAY_FIXTURE_TAGS) -count=1 -v -timeout=$(CLIENT_FIXTURES_TIMEOUT) -run '$(QUAY_RGW_FIXTURE_TESTS)' ./internal/integration

# Supported translation children stay separate from strict optional native
# priority/source-authorization regressions in rgw-sync-native-regressions.
quay-rgw-sync-fixtures:
	$(QUAY_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(QUAY_FIXTURE_TAGS) -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '$(QUAY_RGW_SYNC_FIXTURE_TESTS)' ./internal/integration
	$(QUAY_FIXTURE_TEST_ENV) go test -mod=readonly -tags=$(QUAY_FIXTURE_TAGS) -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '$(QUAY_RGW_TRANSLATION_FIXTURE_TESTS)' ./internal/integration

# Consume existing client/runner images prepared by ceph-testcontainers-images.
# CEPH_TEST_GOCEPH_CLIENT_IMAGE and CEPH_TEST_GOCEPH_RUNNER_IMAGE are required.
quay-goceph-linux:
	$(QUAY_TEST_ENV) python3 internal/integration/goceph/run.py

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
