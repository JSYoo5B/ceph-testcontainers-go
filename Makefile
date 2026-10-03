.PHONY: test integration topology hostnetwork hostnetwork-multicluster multicluster goceph-linux vet slim-image slim-smoke slim-integration slim-images slim-images-verify slim-images-multicluster slim-images-deb slim-images-deb-verify slim-test

SLIM_IMAGE ?= ceph-testcontainers:20.2.4-slim
CEPH_SOURCE_IMAGE ?= quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
SLIM_REPOSITORY ?= ceph-testcontainers
CEPH_DEB_DIRECTORY ?= artifacts/debs
CEPH_DEB_BASE_IMAGE ?= ubuntu:24.04
CEPH_DEB_TAG ?= local-deb
MULTICLUSTER_TIMEOUT ?= 60m
HOSTNETWORK_TIMEOUT ?= 40m
TOPOLOGY_TIMEOUT ?= 40m
TOPOLOGY_EXTENSIONS_TIMEOUT ?= 90m
CLUSTER_FEATURES_TIMEOUT ?= 40m
CLUSTER_FEATURE_EXTENSIONS_TIMEOUT ?= 40m

.PHONY: topology-extensions
.PHONY: cluster-features
.PHONY: cluster-feature-extensions

cluster-features:
	CGO_ENABLED=0 go test -tags=integration,auth,features -count=1 -v -timeout=$(CLUSTER_FEATURES_TIMEOUT) -run '^Test(PoolPolicies|ClientIdentities|RBDNamespaces|CephFSSubvolumes|(HostNetwork)?RGWUserAdministration)$$' ./internal/integration

cluster-feature-extensions:
	CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=$(CLUSTER_FEATURE_EXTENSIONS_TIMEOUT) -run '^Test(ConfigurationOverrides|OSDPolicies|CephFSSubvolumeSnapshotsAndClones|(HostNetwork)?RGWPlacementStorageClasses|RGWPlacementRealmStorageClasses)$$' ./internal/integration

test:
	CGO_ENABLED=0 go test ./...

integration:
	CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=20m ./internal/integration

topology:
	CGO_ENABLED=0 go test -tags=integration,topology -count=1 -v -timeout=$(TOPOLOGY_TIMEOUT) -run '^Test(MonitorManagerTopology|ManagerLifecycle|CephFSMDSScaleTopology|CephFSMDSScaleStandbyReplayTopology|CephFSMultiActiveStandbyFailoverAndFilesystems|CephFSStandbyReplayFailover|RGWTopology|InitialClusterComposition)$$' ./internal/integration

topology-extensions:
	CGO_ENABLED=0 go test -tags=integration,topology,multicluster,hostnetwork -count=1 -v -timeout=$(TOPOLOGY_EXTENSIONS_TIMEOUT) -run '^Test(SeparateClusterNetworksAndInterruptions|FiveMonitorQuorumAndNetworkRecovery|(MultiCluster|HostNetwork)(RBDMirrorDaemonTopology|CephFSMirrorDaemonRebalanceTopology|RGWInitialZonegroupsTopology|RGWZonegroupsAndRemovalTopology)|MultiCluster(RBDPeerNetworkInterruption|RGWPeerNetworkTopology))$$' ./internal/integration

# All tests and native go-ceph clients execute on the Docker Linux host.
# The fixture's nested module does not add go-ceph to this library's deps.
goceph-linux:
	python3 internal/integration/goceph/run.py

hostnetwork:
	CGO_ENABLED=0 go test -tags=integration,hostnetwork -count=1 -v -timeout=$(HOSTNETWORK_TIMEOUT) -run '^TestHostNetwork' ./internal/integration

hostnetwork-multicluster:
	CGO_ENABLED=0 go test -tags=integration,hostnetwork,multicluster -count=1 -v -timeout=$(HOSTNETWORK_TIMEOUT) -run '^TestHostNetwork(RBDSnapshotMirror|CephFSSnapshotMirrorAndBackup|CephFSManagerTopology|RGWMultisite|RGWThreeZoneTopology)$$' ./internal/integration

multicluster:
	CGO_ENABLED=0 go test -tags=integration,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^TestMultiCluster' ./internal/integration

vet:
	go vet ./...

slim-image:
	docker build --network=none -t $(SLIM_IMAGE) image/slim

slim-smoke:
	docker run --rm -i --entrypoint /bin/sh $(SLIM_IMAGE) < image/slim/smoke.sh

slim-integration:
	CEPH_TEST_IMAGE=$(SLIM_IMAGE) CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=20m ./internal/integration

slim-images:
	python3 image/slim/build.py --source-image "$(CEPH_SOURCE_IMAGE)" --repository "$(SLIM_REPOSITORY)"

slim-images-verify:
	python3 image/slim/build.py --source-image "$(CEPH_SOURCE_IMAGE)" --repository "$(SLIM_REPOSITORY)" --integration

slim-images-multicluster:
	python3 image/slim/build.py --source-image "$(CEPH_SOURCE_IMAGE)" --repository "$(SLIM_REPOSITORY)" --multicluster

slim-images-deb:
	python3 image/slim/build.py --deb-directory "$(CEPH_DEB_DIRECTORY)" --base-image "$(CEPH_DEB_BASE_IMAGE)" --repository "$(SLIM_REPOSITORY)" --tag "$(CEPH_DEB_TAG)"

slim-images-deb-verify:
	python3 image/slim/build.py --deb-directory "$(CEPH_DEB_DIRECTORY)" --base-image "$(CEPH_DEB_BASE_IMAGE)" --repository "$(SLIM_REPOSITORY)" --tag "$(CEPH_DEB_TAG)" --integration

slim-test:
	python3 -m unittest discover -s image/slim/tests -v
