.PHONY: test integration vet slim-image slim-smoke slim-integration slim-images slim-images-verify

SLIM_IMAGE ?= ceph-testcontainers:20.2.4-slim
CEPH_SOURCE_IMAGE ?= quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
SLIM_REPOSITORY ?= ceph-testcontainers

test:
	CGO_ENABLED=0 go test ./...

integration:
	CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=20m ./...

vet:
	go vet ./...

slim-image:
	docker build --network=none -t $(SLIM_IMAGE) image/slim

slim-smoke:
	docker run --rm -i --entrypoint /bin/sh $(SLIM_IMAGE) < image/slim/smoke.sh

slim-integration:
	CEPH_TEST_IMAGE=$(SLIM_IMAGE) CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=20m ./...

slim-images:
	python3 image/slim/build.py --source-image "$(CEPH_SOURCE_IMAGE)" --repository "$(SLIM_REPOSITORY)"

slim-images-verify:
	python3 image/slim/build.py --source-image "$(CEPH_SOURCE_IMAGE)" --repository "$(SLIM_REPOSITORY)" --integration
