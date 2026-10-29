package cephfs

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Every descriptor must keep the complete testcontainers.Container interface.
var (
	_ testcontainers.Container = (*Filesystem)(nil)
	_ testcontainers.Container = (*MDS)(nil)
)

func TestFilesystemOptionsRejectInvalidCompositionBeforeRuntime(t *testing.T) {
	for _, test := range []struct {
		name string
		opts []testcontainers.ContainerCustomizer
	}{
		{"duplicate initial filesystems", []testcontainers.ContainerCustomizer{WithFilesystems(Config{Name: "alpha"}, Config{Name: "alpha"})}},
		{"pool shared with filesystem", []testcontainers.ContainerCustomizer{ceph.WithPools(ceph.PoolConfig{Name: "alpha-data"}), WithFilesystems(Config{Name: "alpha"})}},
		{"invalid captured filesystem", []testcontainers.ContainerCustomizer{WithFilesystems(Config{Name: "bad name"})}},
		{"blank MDS image", []testcontainers.ContainerCustomizer{WithMDSImage("\n")}},
		{"no initial OSDs/zero-last", []testcontainers.ContainerCustomizer{WithFilesystems(), ceph.WithNoInitialOSDs()}},
		{"no initial OSDs/zero-first", []testcontainers.ContainerCustomizer{ceph.WithNoInitialOSDs(), WithFilesystems()}},
		{"no initial managers/zero-last", []testcontainers.ContainerCustomizer{WithFilesystems(), ceph.WithNoInitialManagers()}},
		{"no initial managers/zero-first", []testcontainers.ContainerCustomizer{ceph.WithNoInitialManagers(), WithFilesystems()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var customizers atomic.Int32
			opts := append(test.opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
			// Validation runs before the context is consulted, so a canceled
			// context separates a rejection from an attempt to start.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			cluster, err := ceph.Run(ctx, ceph.DefaultImage, opts...)
			if cluster != nil || err == nil || errors.Is(err, context.Canceled) || customizers.Load() != 0 {
				t.Fatalf("invalid filesystem composition reached runtime: cluster=%v err=%v customizers=%d", cluster, err, customizers.Load())
			}
		})
	}
}

func TestFilesystemCompositionResolvesFinalDefaults(t *testing.T) {
	for _, test := range []struct {
		name string
		opts []ceph.Option
	}{
		{"filesystem then defaults", []ceph.Option{WithFilesystems(Config{Name: "alpha", MetadataPool: ceph.PoolConfig{MinSize: 3}}), ceph.WithPoolDefaults(3, 2)}},
		{"defaults then filesystem", []ceph.Option{ceph.WithPoolDefaults(3, 2), WithFilesystems(Config{Name: "alpha", MetadataPool: ceph.PoolConfig{MinSize: 3}})}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, services, err := testhelper.TestPrepare(3, false, nil, test.opts...)
			if err != nil {
				t.Fatalf("capture prematurely validated unresolved defaults: %v", err)
			}
			if config := preparedFilesystems(services)[0].MetadataPool; config.Replicas != 3 || config.MinSize != 3 {
				t.Fatalf("explicit min_size did not resolve against final replica default: %+v", config)
			}
		})
	}
}

func TestFilesystemCompositionUsesDefaultCRUSHRoot(t *testing.T) {
	layout := []ceph.OSDConfig{{Host: "a"}, {Host: "b"}, {Host: "other-host", Root: "other"}}
	_, services, err := testhelper.TestPrepare(3, false, nil, ceph.WithPoolDefaults(2, 1), ceph.WithInitialOSDs(layout...), WithFilesystems(Config{Name: "alpha"}), ceph.WithDefaultCRUSHRoot("storage"))
	if err != nil {
		t.Fatal(err)
	}
	if fs := preparedFilesystems(services)[0]; fs.MetadataPool.CRUSHRoot != "storage" || fs.DataPool.CRUSHRoot != "storage" {
		t.Fatalf("initial filesystem pools lost the selected CRUSH root: %+v", fs)
	}
}

func TestFilesystemCompositionCopiesCallerConfigs(t *testing.T) {
	ec := &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}
	additional := []ceph.PoolConfig{{Name: "ec", ErasureCode: ec}}
	option := WithFilesystems(Config{Name: "alpha", AdditionalDataPools: additional})
	ec.K, additional[0].Name = 100, "changed"
	_, services, err := testhelper.TestPrepare(3, false, nil, ceph.WithPoolDefaults(3, 2), option, ceph.WithPools(ceph.PoolConfig{Name: "ordinary"}))
	if err != nil {
		t.Fatal(err)
	}
	resolved := preparedFilesystems(services)[0]
	if resolved.MetadataPool.Replicas != 3 || resolved.MetadataPool.MinSize != 2 || resolved.DataPool.Replicas != 3 {
		t.Fatal("initial filesystem pools did not use final cluster defaults")
	}
	if resolved.AdditionalDataPools[0].Name != "ec" || resolved.AdditionalDataPools[0].ErasureCode.K != 2 || resolved.AdditionalDataPools[0].MinSize != 3 {
		t.Fatal("initial config aliases caller buffers or applied replicated min_size to EC")
	}
	_, services, err = testhelper.TestPrepare(1, false, nil, ceph.WithPoolDefaults(1, 1), WithFilesystems())
	if err != nil || preparedFilesystems(services)[0].MetadataPool.Replicas != 1 {
		t.Fatalf("one-OSD filesystem did not resolve to a usable single-copy pool: %v", err)
	}
}

func TestDefaultFilesystemAppliesOnlyWithoutExplicitFilesystems(t *testing.T) {
	for _, tc := range []struct {
		opts []ceph.Option
		want string
	}{
		{[]ceph.Option{defaultFilesystem()}, ""},
		{[]ceph.Option{WithFilesystems(Config{Name: "app"}), defaultFilesystem()}, "app"},
	} {
		services, err := testhelper.TestApply(tc.opts...)
		if err != nil || len(services) != 1 {
			t.Fatalf("bootstraps: %v %v", services, err)
		}
		if configs := preparedFilesystems(services); len(configs) != 1 || configs[0].Name != tc.want {
			t.Fatalf("filesystems = %+v, want %q", configs, tc.want)
		}
	}
}

func TestEntryPointsRejectMissingCluster(t *testing.T) {
	ctx := t.Context()
	if _, err := Start(ctx, nil, Config{}); err == nil {
		t.Fatal("nil cluster started a filesystem")
	}
	if err := (&Filesystem{FilesystemName: "a", name: "a"}).Remove(ctx); err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("nil cluster removed a filesystem: %v", err)
	}
	if Filesystems(nil) != nil {
		t.Fatal("nil cluster reported filesystems")
	}
}

func TestFilesystemRegistryReportsOwnedState(t *testing.T) {
	cluster := testhelper.TestCluster(nil, testhelper.TestConfig{})
	registry := filesystems(cluster)
	if registry.RequiresMGRModule("volumes") || len(registry.ConfigTargets()) != 0 {
		t.Fatal("an empty registry reported owned filesystems")
	}
	mds := &MDS{Container: &poolFixtureContainer{}, ID: "alpha-0"}
	registry.byName["alpha"] = &Filesystem{FilesystemName: "alpha", name: "alpha", roster: mdsRoster{daemons: []*MDS{mds, nil, {ID: "uncreated"}}}}
	if !registry.RequiresMGRModule("volumes") || registry.RequiresMGRModule("mirroring") {
		t.Fatal("owned filesystems did not protect exactly the volumes module")
	}
	if targets := registry.ConfigTargets(); len(targets) != 1 || targets["mds.alpha-0"] != mds.Container {
		t.Fatalf("config targets = %v", targets)
	}
	if filesystems(cluster) != registry {
		t.Fatal("the registry was not retained")
	}
}

func TestContainerFilesystemsUsesTheEmbeddedCluster(t *testing.T) {
	var missing *Container
	if missing.Filesystems() != nil {
		t.Fatal("nil container reported filesystems")
	}
	cluster := &Container{Container: testhelper.TestCluster(nil, testhelper.TestConfig{})}
	beta, alpha := &Filesystem{FilesystemName: "beta", name: "beta"}, &Filesystem{FilesystemName: "alpha", name: "alpha"}
	filesystems(cluster.Container).byName["beta"], filesystems(cluster.Container).byName["alpha"] = beta, alpha
	if got := cluster.Filesystems(); len(got) != 2 || got[0] != alpha || got[1] != beta {
		t.Fatalf("Filesystems = %v", got)
	}
}

func TestFilesystemImageOptionFillsMissingRoleInRunWithImageSet(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(ceph.ImageSet{Essential: "core", Client: "toolkit"})), WithMDSImage("service-image"), WithFilesystems())
	if cluster != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("service image option did not reach canceled admission: %v %v", cluster, err)
	}
}
