.PHONY: test test-all check race vet lint tag-compile vulncheck integration hostnetwork multicluster
.PHONY: scenario scenario-default scenario-goceph-linux goceph-linux image-compatibility image-matrix

MULTICLUSTER_TIMEOUT ?= 60m
HOSTNETWORK_TIMEOUT ?= 40m
INTEGRATION_TIMEOUT ?= 20m
ALL_TEST_TIMEOUT ?= 180m
IMAGE_COMPATIBILITY_TIMEOUT ?= 40m
IMAGE_VARIANT ?= official
IMAGE_LAYOUT ?= all
IMAGE_PLATFORM ?=
SCENARIO_IMAGE_LAYOUT ?= all
SCENARIO_CATEGORY ?=
SCENARIO_BATCH ?=
SCENARIO_DIRECTORY ?= artifacts/scenario-$(SCENARIO_CATEGORY)-$(SCENARIO_BATCH)

# The default clears inherited image overrides and exercises ceph.DefaultImage.
# CI can explicitly select prepared role images without changing module defaults.
ifeq ($(SCENARIO_IMAGE_LAYOUT),roles)
$(foreach name,CEPH_TEST_ESSENTIAL_IMAGE CEPH_TEST_CLIENT_IMAGE CEPH_TEST_CEPHFS_IMAGE CEPH_TEST_RBD_AUX_IMAGE CEPH_TEST_RGW_IMAGE,$(if $(strip $($(name))),,$(error $(name) is required for SCENARIO_IMAGE_LAYOUT=roles)))
SCENARIO_TEST_ENV = env CGO_ENABLED=0
else ifeq ($(SCENARIO_IMAGE_LAYOUT),all)
SCENARIO_TEST_ENV = env -u CEPH_TEST_IMAGE -u CEPH_TEST_ESSENTIAL_IMAGE -u CEPH_TEST_CLIENT_IMAGE -u CEPH_TEST_CEPHFS_IMAGE -u CEPH_TEST_RBD_AUX_IMAGE -u CEPH_TEST_RGW_IMAGE CGO_ENABLED=0
else
$(error Unknown SCENARIO_IMAGE_LAYOUT)
endif

test:
	CGO_ENABLED=0 go test -mod=readonly ./...

# Normal supported runtime tests and host unit tests. SDK consumers and strict
# native upstream regressions require their additional explicit opt-in tags.
test-all:
	CGO_ENABLED=0 go test -mod=readonly -tags=all -count=1 -v -timeout=$(ALL_TEST_TIMEOUT) ./...

# Go host checks. Tag compilation does not execute Docker or native clients.
# Image builder checks belong to ceph-testcontainers-images.
check:
	$(MAKE) test
	$(MAKE) race
	$(MAKE) vet
	$(MAKE) tag-compile

race:
	CGO_ENABLED=1 go test -mod=readonly -race ./...

# The first line checks, without Docker, the import, seam and declaration
# boundary of test-only code (docs/contributing/testing.md#test-only-code); it
# does not exclude that code from ordinary compilation. The second checks that
# each test double declares its class (docs/contributing/test-double-inventory.md).
tag-compile:
	CGO_ENABLED=0 go run -mod=readonly ./internal/tools/testsupportcheck
	CGO_ENABLED=0 go run -mod=readonly ./internal/tools/testdoublecheck
	CGO_ENABLED=0 go test -mod=readonly -tags=integration -run '^$$' ./...
	CGO_ENABLED=0 go test -mod=readonly -tags=all -run '^$$' ./...
	CGO_ENABLED=0 go test -mod=readonly -tags=all,goceph,native_regression,role_images,ci_optional,ci_batch_role_routing,ci_batch_role_mirror_refresh -run '^$$' ./...
	CGO_ENABLED=0 go test -mod=readonly -tags=all,cephfs_fuse -run '^$$' ./...

integration:
	CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=$(INTEGRATION_TIMEOUT) ./internal/integration

# Required scenarios consume selected existing images; the default is the pruned aggregate.
# No image producer is required. Tests and clusters execute sequentially.
scenario-default:
	$(SCENARIO_TEST_ENV) go test -mod=readonly -tags=integration -count=1 -v -timeout=$(INTEGRATION_TIMEOUT) ./internal/integration

# One CI batch, with the tags, budget and pass rules that CI uses. The tag
# runner selects its tests from the build tags and metadata of the test
# sources, so no test list is kept here; `python3 .github/scripts/tag_scenarios.py
# plan` lists the categories and batches.
scenario:
	@test -n '$(SCENARIO_CATEGORY)' || { echo 'SCENARIO_CATEGORY is required' >&2; exit 1; }
	@test -n '$(SCENARIO_BATCH)' || { echo 'SCENARIO_BATCH is required' >&2; exit 1; }
	$(SCENARIO_TEST_ENV) python3 .github/scripts/tag_scenarios.py run --category '$(SCENARIO_CATEGORY)' --batch '$(SCENARIO_BATCH)' --directory '$(SCENARIO_DIRECTORY)'

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

# Execute supplied client/runner images on the Docker Linux host.
# The nested probe module remains separate from this library's dependencies.
goceph-linux:
	python3 internal/integration/goceph/run.py

hostnetwork:
	CGO_ENABLED=0 go test -tags=integration,hostnetwork -count=1 -v -timeout=$(HOSTNETWORK_TIMEOUT) -run '^TestHostNetwork' ./internal/integration

multicluster:
	CGO_ENABLED=0 go test -tags=integration,multicluster -count=1 -v -timeout=$(MULTICLUSTER_TIMEOUT) -run '^TestMultiCluster' ./internal/integration

vet:
	CGO_ENABLED=0 go vet -mod=readonly ./...

# Go vulnerability check, as in the Code checks workflow. govulncheck's JSON
# mode exits 0 even with findings, so .github/scripts/check_vulns.py applies
# the policy: a vulnerable symbol or package that the code reaches fails, one
# only in a required module is listed. The scanner is built for the host with
# the current toolchain, and each scan loads the packages of its platform and
# tags. The nested go-ceph probe module, which needs cgo, gets a module scan.
GOVULNCHECK ?= golang.org/x/vuln/cmd/govulncheck@v1.9.0
VULNCHECK_DIR ?= artifacts/vulncheck
VULNCHECK_SCANS ?= linux/amd64: linux/amd64:all linux/amd64:all,goceph,native_regression linux/arm64: darwin/arm64:

vulncheck:
	mkdir -p $(VULNCHECK_DIR)/bin
	GOBIN=$(abspath $(VULNCHECK_DIR)/bin) GOFLAGS= go install $(GOVULNCHECK)
	status=0; for scan in $(VULNCHECK_SCANS); do \
		platform=$${scan%%:*}; tags=$${scan#*:}; name=$$(echo "$$platform-$${tags:-default}" | tr '/,' '--'); \
		echo "== govulncheck $$platform tags=$${tags:-none}"; \
		GOOS=$${platform%/*} GOARCH=$${platform#*/} CGO_ENABLED=0 GOWORK=off GOFLAGS=-mod=readonly \
			$(VULNCHECK_DIR)/bin/govulncheck -format json -test $${tags:+-tags=$$tags} ./... \
			> $(VULNCHECK_DIR)/$$name.json || status=1; \
		python3 .github/scripts/check_vulns.py $(VULNCHECK_DIR)/$$name.json || status=1; \
	done; \
	echo "== govulncheck module scan of internal/integration/goceph/probe"; \
	(cd internal/integration/goceph/probe && GOOS=linux GOARCH=amd64 CGO_ENABLED=1 GOWORK=off GOFLAGS=-mod=readonly \
		$(abspath $(VULNCHECK_DIR)/bin/govulncheck) -format json -scan module) > $(VULNCHECK_DIR)/goceph-probe-module.json || status=1; \
	python3 .github/scripts/check_vulns.py $(VULNCHECK_DIR)/goceph-probe-module.json || status=1; \
	exit $$status

# golangci-lint v2.15.0 with .golangci.yml, as in the Code checks workflow.
lint:
	golangci-lint run --timeout=15m ./...
