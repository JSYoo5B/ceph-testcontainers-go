.PHONY: test integration vet

test:
	CGO_ENABLED=0 go test ./...

integration:
	CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=20m ./...

vet:
	go vet ./...
