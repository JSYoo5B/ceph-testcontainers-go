package ceph

import (
	"slices"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

func TestIdleEntrypointKeepsSleepAndChainsInit(t *testing.T) {
	req := testcontainers.GenericContainerRequest{}
	if err := hostContainerCustomizer("127.0.0.1")(&req); err != nil {
		t.Fatal(err)
	}
	if err := WithIdleEntrypoint()(&req); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(req.Entrypoint, []string{"sleep"}) || !slices.Equal(req.Cmd, []string{"infinity"}) {
		t.Fatalf("idle command changed: %v %v", req.Entrypoint, req.Cmd)
	}
	host := container.HostConfig{}
	req.HostConfigModifier(&host)
	if host.Init == nil || !*host.Init || host.NetworkMode != "host" {
		t.Fatalf("init did not chain with the earlier host modifier: %+v", host)
	}
}
