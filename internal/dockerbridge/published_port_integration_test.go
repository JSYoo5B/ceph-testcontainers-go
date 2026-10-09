//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_sdk_network))))

//ci: timeout=15m job-timeout=25

package dockerbridge_test

import (
	"context"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/dockerbridge"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// A removable peer must not become the gateway for a retained published port.
// Verify real fresh HTTP connections, not just unchanged Docker port metadata.
func TestRecoverableBridgePublishedPort(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := docker.Close(); err != nil {
			t.Errorf("close published-port regression Docker client: %v", err)
		}
	})
	newBridge := func() *testcontainers.DockerNetwork {
		t.Helper()
		bridge, err := dockerbridge.New(ctx)
		if bridge != nil {
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				if err := bridge.Remove(cleanupCtx); err != nil {
					t.Errorf("remove owned published-port regression bridge: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatalf("create published-port regression bridge: %v", err)
		}
		if bridge == nil || bridge.ID == "" || bridge.Name == "" {
			t.Fatal("published-port regression bridge lacks native identity")
		}
		return bridge
	}
	public, peer := newBridge(), newBridge()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          "busybox:1.37.0",
			Entrypoint:     []string{"/bin/sh", "-c"},
			Cmd:            []string{"mkdir -p /www && printf '%s\\n' 'stable-local-http' > /www/index.html && exec httpd -f -p 8080 -h /www"},
			ExposedPorts:   []string{"8080/tcp"},
			Networks:       []string{public.Name, peer.Name},
			NetworkAliases: map[string][]string{public.Name: {"retained-http"}, peer.Name: {"removable-http-peer"}},
			EndpointSettingsModifier: func(endpoints map[string]*dockernetwork.EndpointSettings) {
				if endpoint := endpoints[public.Name]; endpoint != nil {
					endpoint.GwPriority = 1
				}
			},
			WaitingFor: wait.ForHTTP("/index.html").WithPort("8080/tcp"),
		},
		Started: true,
	})
	if ctr != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := ctr.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate owned published-port regression container: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatalf("start published-port regression container: %v", err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	checkHTTP := func(phase string) {
		t.Helper()
		current, err := ctr.PortEndpoint(ctx, "8080/tcp", "http")
		if err != nil || current != endpoint {
			t.Fatalf("%s changed the published endpoint: %q %v", phase, current, err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/index.html", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s fresh HTTP GET at the original published endpoint: %v", phase, err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || string(body) != "stable-local-http\n" {
			t.Fatalf("%s HTTP response: status=%d body=%q read=%v close=%v", phase, response.StatusCode, body, readErr, closeErr)
		}
	}
	inspect := func() mobycl.ContainerInspectResult {
		t.Helper()
		result, err := docker.ContainerInspect(ctx, ctr.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if result.Container.State == nil || !result.Container.State.Running || result.Container.State.Pid <= 0 || result.Container.NetworkSettings == nil {
			t.Fatal("published-port regression container lost its native running process")
		}
		return result
	}
	before := inspect()
	local, remote := before.Container.NetworkSettings.Networks[public.Name], before.Container.NetworkSettings.Networks[peer.Name]
	if local == nil || remote == nil || !local.IPAddress.IsValid() || !remote.IPAddress.IsValid() || local.GwPriority != 1 || remote.GwPriority != 0 {
		t.Fatal("public/peer endpoints do not have the required addresses and gateway priorities")
	}
	checkHTTP("before peer interruption")
	cut, err := ceph.InterruptNetwork(ctx, ctr, peer.Name)
	if cut != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := cut.Restore(cleanupCtx); err != nil {
				t.Errorf("restore published-port regression peer endpoint during cleanup: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if cut == nil || cut.OriginalAddress != remote.IPAddress {
		t.Fatal("interruption does not identify the original peer endpoint")
	}
	for _, phase := range []string{"peer isolated", "peer restored", "idempotent restore"} {
		if phase != "peer isolated" {
			if err := cut.Restore(ctx); err != nil {
				t.Fatalf("%s: %v", phase, err)
			}
		}
		state := inspect()
		kept := state.Container.NetworkSettings.Networks[public.Name]
		if state.Container.State.Pid != before.Container.State.Pid || kept == nil || kept.IPAddress != local.IPAddress || kept.GwPriority != local.GwPriority || !slices.Equal(kept.Aliases, local.Aliases) {
			t.Fatalf("%s changed the public endpoint or native process", phase)
		}
		actualPeer := state.Container.NetworkSettings.Networks[peer.Name]
		if phase == "peer isolated" {
			if actualPeer != nil {
				t.Fatal("interrupted peer endpoint remains attached")
			}
		} else if actualPeer == nil || actualPeer.IPAddress != remote.IPAddress || actualPeer.GwPriority != remote.GwPriority || !slices.Equal(actualPeer.Aliases, remote.Aliases) {
			t.Fatalf("%s changed the original peer IP, aliases or gateway priority", phase)
		}
		checkHTTP(phase)
	}
	t.Log("fresh HTTP GETs at the same published endpoint passed before, during and after peer interruption; public/peer IPs, aliases, priorities and native process preserved")
}
