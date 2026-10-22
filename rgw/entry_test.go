package rgw

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

var _ testcontainers.Container = (*Gateway)(nil)

func TestGatewayOptionsRejectInvalidCompositionBeforeRuntime(t *testing.T) {
	for _, test := range []struct {
		name, want string
		opts       []testcontainers.ContainerCustomizer
	}{
		{"duplicate initial gateways", "duplicated", []testcontainers.ContainerCustomizer{WithGateways(Config{Name: "a"}, Config{Name: "a"})}},
		{"invalid initial gateway", "", []testcontainers.ContainerCustomizer{WithGateways(Config{Name: "invalid/name"})}},
		{"blank image", "image", []testcontainers.ContainerCustomizer{WithImage(" \t")}},
		{"no initial OSDs/zero-last", "", []testcontainers.ContainerCustomizer{WithGateways(), ceph.WithNoInitialOSDs()}},
		{"no initial OSDs/zero-first", "", []testcontainers.ContainerCustomizer{ceph.WithNoInitialOSDs(), WithGateways()}},
		{"no initial managers/zero-last", "no initial", []testcontainers.ContainerCustomizer{WithGateways(), ceph.WithNoInitialManagers()}},
		{"no initial managers/zero-first", "no initial", []testcontainers.ContainerCustomizer{ceph.WithNoInitialManagers(), WithGateways()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var customizers atomic.Int32
			opts := append(test.opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
			// Validation runs before the context is consulted, so a canceled
			// context separates a rejection from an attempt to start.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			cluster, err := ceph.Run(ctx, ceph.DefaultImage, opts...)
			if cluster != nil || err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), test.want) || customizers.Load() != 0 {
				t.Fatalf("invalid gateway composition reached runtime: cluster=%v err=%v customizers=%d", cluster, err, customizers.Load())
			}
		})
	}
}

func TestDefaultGatewayAppliesOnlyWithoutExplicitGateways(t *testing.T) {
	for _, tc := range []struct {
		opts []ceph.Option
		want string
	}{
		{[]ceph.Option{defaultGateway()}, ""},
		{[]ceph.Option{WithGateways(Config{Name: "s3"}), defaultGateway()}, "s3"},
	} {
		services, err := servicehook.TestApply(tc.opts...)
		if err != nil || len(services) != 1 {
			t.Fatalf("bootstraps: %v %v", services, err)
		}
		if b, ok := services[0].(*rgwBootstrap); !ok || len(b.configs) != 1 || b.configs[0].Name != tc.want {
			t.Fatalf("gateways = %+v, want %q", services[0], tc.want)
		}
	}
}

func TestEntryPointsRejectMissingCluster(t *testing.T) {
	ctx := t.Context()
	if _, err := Start(ctx, nil, Config{}); err == nil {
		t.Fatal("nil cluster started a gateway")
	}
	if err := Remove(ctx, nil, "a"); err == nil {
		t.Fatal("nil cluster removed a gateway")
	}
	if _, err := GatewaysContext(ctx, nil); err == nil {
		t.Fatal("nil cluster listed gateways")
	}
	if Gateways(nil) != nil {
		t.Fatal("nil cluster reported gateways")
	}
}

func TestSerializedGatewayMutationQueuedDeadline(t *testing.T) {
	operations := map[string]func(context.Context, *ceph.Container) error{
		"Remove": func(ctx context.Context, c *ceph.Container) error { return Remove(ctx, c, "queued") },
		"StartBridge": func(ctx context.Context, c *ceph.Container) error {
			_, err := Start(ctx, c, Config{Name: "queued"})
			return err
		},
		"StartHost": func(ctx context.Context, c *ceph.Container) error {
			gateway, err := Start(ctx, c, Config{SkipUserCreation: true})
			if gateway != nil {
				t.Error("busy host owner returned a gateway")
			}
			return err
		},
		"GatewaysContext": func(ctx context.Context, c *ceph.Container) error { _, err := GatewaysContext(ctx, c); return err },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			control := &clustertest.ContextLockRuntime{}
			cluster := testCluster(control, servicehook.TestConfig{StartupTimeout: time.Second, HostNetwork: name == "StartHost", PublicAddress: "127.0.0.1"}, nil)
			clustertest.AssertContextLockQueued(t, servicehook.TestState(cluster).Mutex, func(ctx context.Context) error { return operation(ctx, cluster) })
			if control.Execs.Load() != 0 || servicehook.TestState(cluster).PortLeases != 0 || len(servicehook.Services(cluster)) != 0 || len(gateways(cluster).byName) != 0 {
				t.Fatal("queued caller created containers, port leases or gateway inventory")
			}
		})
	}
}

func TestGatewaySnapshotsCopyOwnedDescriptors(t *testing.T) {
	x, y := &Gateway{GatewayName: "x"}, &Gateway{GatewayName: "y"}
	cluster := testCluster(nil, servicehook.TestConfig{}, nil)
	gateways(cluster).byName["y"], gateways(cluster).byName["x"] = y, x
	snapshot, err := GatewaysContext(t.Context(), cluster)
	if err != nil || len(snapshot) != 2 || snapshot[0] != x || snapshot[1] != y {
		t.Fatalf("gateway descriptors/order: %v %v", snapshot, err)
	}
	snapshot[0] = nil
	if gateways(cluster).byName["x"] != x || Gateways(cluster)[0] != x {
		t.Fatal("gateway snapshot modified ownership map")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := GatewaysContext(ctx, cluster); !errors.Is(err, context.Canceled) {
		t.Fatalf("expired context listed gateways: %v", err)
	}
	if registry := gateways(cluster); len(registry.Secrets()) != 4 || len(registry.ConfigTargets()) != 0 {
		t.Fatalf("descriptors without containers became config targets: %v", registry.ConfigTargets())
	}
}

func TestContainerShortcutsUseTheEmbeddedCluster(t *testing.T) {
	var missing *Container
	if missing.Gateways() != nil {
		t.Fatal("nil container reported gateways")
	}
	if _, err := missing.S3Endpoint(t.Context()); err == nil {
		t.Fatal("nil container returned an endpoint")
	}
	cluster := &Container{Container: testCluster(nil, servicehook.TestConfig{}, nil)}
	if _, err := cluster.S3Endpoint(t.Context()); err == nil || !strings.Contains(err.Error(), "0 RGW gateways") {
		t.Fatalf("a cluster without gateways returned an endpoint: %v", err)
	}
	only := &Gateway{GatewayName: "default", publicAddress: "192.0.2.10", port: 8000}
	gateways(cluster.Container).byName["default"] = only
	if got := cluster.Gateways(); len(got) != 1 || got[0] != only {
		t.Fatalf("Gateways = %v", got)
	}
	if endpoint, err := cluster.S3Endpoint(t.Context()); err != nil || endpoint != "http://192.0.2.10:8000" {
		t.Fatalf("S3Endpoint = %q, %v", endpoint, err)
	}
	gateways(cluster.Container).byName["other"] = &Gateway{GatewayName: "other"}
	if _, err := cluster.S3Endpoint(t.Context()); err == nil || !strings.Contains(err.Error(), "2 RGW gateways") {
		t.Fatalf("several gateways did not require a choice: %v", err)
	}
}

func TestGatewayImageOptionFillsMissingRoleInRunWithImages(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cluster, err := ceph.RunWithImages(ctx, ceph.ImageSet{Essential: "core", Client: "toolkit"},
		WithImage("service-image"), WithGateways())
	if cluster != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("service image option did not reach canceled admission: %v %v", cluster, err)
	}
}
