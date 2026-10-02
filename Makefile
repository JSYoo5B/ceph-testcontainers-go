.PHONY: test integration vet slim-image slim-smoke slim-integration

SLIM_IMAGE ?= ceph-testcontainers:20.2.4-slim

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
