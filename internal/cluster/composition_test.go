package cluster

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestInvalidInitialCompositionDoesNotCreateResources(t *testing.T) {
	for _, test := range []struct {
		name string
		opts []testcontainers.ContainerCustomizer
	}{
		{"replicas exceed initial OSDs", []testcontainers.ContainerCustomizer{WithOSDCount(1), WithPools(PoolConfig{Name: "data", Replicas: 3})}},
		{"EC exceeds initial OSDs", []testcontainers.ContainerCustomizer{WithPools(PoolConfig{Name: "data", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}})}},
		{"duplicate initial pools", []testcontainers.ContainerCustomizer{WithPools(PoolConfig{Name: "data"}, PoolConfig{Name: "data"})}},
		{"duplicate initial filesystems", []testcontainers.ContainerCustomizer{WithCephFS(CephFSConfig{Name: "alpha"}, CephFSConfig{Name: "alpha"})}},
		{"duplicate initial gateways", []testcontainers.ContainerCustomizer{WithRGW(RGWConfig{Name: "a"}, RGWConfig{Name: "a"})}},
		{"invalid initial gateway", []testcontainers.ContainerCustomizer{WithRGW(RGWConfig{Name: "invalid/name"})}},
		{"pool shared with filesystem", []testcontainers.ContainerCustomizer{WithPools(PoolConfig{Name: "alpha-data"}), WithCephFS(CephFSConfig{Name: "alpha"})}},
		{"too few CRUSH domains", []testcontainers.ContainerCustomizer{WithInitialOSDs(OSDConfig{Host: "same"}, OSDConfig{Host: "same"}), WithPools(PoolConfig{Name: "data", FailureDomain: "host"})}},
		{"late default invalidates pool", []testcontainers.ContainerCustomizer{WithPools(PoolConfig{Name: "data", Replicas: 1}), WithPoolDefaults(3, 2)}},
		{"empty default root", []testcontainers.ContainerCustomizer{WithDefaultCRUSHRoot("")}},
		{"unsafe default root", []testcontainers.ContainerCustomizer{WithDefaultCRUSHRoot("bad root")}},
		{"unpopulated default root", []testcontainers.ContainerCustomizer{WithInitialOSDs(OSDConfig{Root: "storage"}, OSDConfig{Root: "storage"})}},
		{"default root too small", []testcontainers.ContainerCustomizer{WithInitialOSDs(OSDConfig{Root: "default"}, OSDConfig{Root: "storage"})}},
		{"explicit absent default root", []testcontainers.ContainerCustomizer{WithDefaultCRUSHRoot("absent"), WithInitialOSDs(OSDConfig{Root: "storage"}, OSDConfig{Root: "storage"})}},
		{"invalid captured pool", []testcontainers.ContainerCustomizer{WithPools(PoolConfig{Name: "bad name"})}},
		{"invalid captured filesystem", []testcontainers.ContainerCustomizer{WithCephFS(CephFSConfig{Name: "bad name"})}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster, err := Run(context.Background(), DefaultImage, test.opts...)
			if err == nil || cluster != nil {
				t.Fatalf("invalid initial composition allocated resources: cluster=%v error=%v", cluster, err)
			}
		})
	}
}

func TestCompositionResolvesDefaultsAfterOptionCapture(t *testing.T) {
	for _, test := range []struct {
		name string
		opts []Option
	}{
		{"pool then defaults", []Option{WithPools(PoolConfig{Name: "data", MinSize: 3}), WithPoolDefaults(3, 2)}},
		{"defaults then pool", []Option{WithPoolDefaults(3, 2), WithPools(PoolConfig{Name: "data", MinSize: 3})}},
		{"filesystem then defaults", []Option{WithCephFS(CephFSConfig{Name: "alpha", MetadataPool: PoolConfig{MinSize: 3}}), WithPoolDefaults(3, 2)}},
		{"defaults then filesystem", []Option{WithPoolDefaults(3, 2), WithCephFS(CephFSConfig{Name: "alpha", MetadataPool: PoolConfig{MinSize: 3}})}},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings := options{osds: 3}
			for _, option := range test.opts {
				if err := option(&settings); err != nil {
					t.Fatalf("capture prematurely validated unresolved defaults: %v", err)
				}
			}
			if err := prepareInitialComposition(&settings); err != nil {
				t.Fatal(err)
			}
			var config PoolConfig
			if len(settings.pools) != 0 {
				config = settings.pools[0]
			} else {
				config = settings.filesystems[0].MetadataPool
			}
			if config.Replicas != 3 || config.MinSize != 3 {
				t.Fatalf("explicit min_size did not resolve against final replica default: %+v", config)
			}
		})
	}
}

func TestCompositionDefaultCRUSHRootResolvesOmissionsAndHonorsExplicitRoots(t *testing.T) {
	for _, rootFirst := range []bool{false, true} {
		settings := options{osds: 3, poolReplicas: 2, poolMinSize: 1}
		layout := []OSDConfig{{Host: "a"}, {Host: "b"}, {Host: "other-host", Root: "other"}}
		opts := []Option{WithInitialOSDs(layout...), WithPools(PoolConfig{Name: "ordinary"}), WithCephFS(CephFSConfig{Name: "alpha"})}
		if rootFirst {
			opts = append([]Option{WithDefaultCRUSHRoot("storage")}, opts...)
		} else {
			opts = append(opts, WithDefaultCRUSHRoot("storage"))
		}
		for _, option := range opts {
			if err := option(&settings); err != nil {
				t.Fatal(err)
			}
		}
		if err := prepareInitialComposition(&settings); err != nil {
			t.Fatal(err)
		}
		if settings.initialOSDs[0].Root != "storage" || settings.initialOSDs[1].Root != "storage" || settings.initialOSDs[2].Root != "other" || layout[0].Root != "" {
			t.Fatal("default root resolution changed explicit roots or caller data")
		}
		if settings.pools[0].CRUSHRoot != "storage" || settings.filesystems[0].MetadataPool.CRUSHRoot != "storage" || settings.filesystems[0].DataPool.CRUSHRoot != "storage" {
			t.Fatal("initial pool/filesystem defaults lost the selected CRUSH root")
		}
		if got := resolvePoolDefaults(settings, PoolConfig{CRUSHRoot: "default"}); got.CRUSHRoot != "default" {
			t.Fatal("explicit pool root was replaced by the cluster default")
		}
	}
}

func TestCompositionResolvesFinalDefaultsAndCopiesCallerConfigs(t *testing.T) {
	ec := &ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}
	additional := []PoolConfig{{Name: "ec", ErasureCode: ec}}
	fs := CephFSConfig{Name: "alpha", AdditionalDataPools: additional}
	withFS := WithCephFS(fs)
	withPool := WithPools(PoolConfig{Name: "ordinary"})
	settings := options{osds: 3, poolReplicas: 3, poolMinSize: 2}
	if err := withFS(&settings); err != nil {
		t.Fatal(err)
	}
	if err := withPool(&settings); err != nil {
		t.Fatal(err)
	}
	ec.K, additional[0].Name = 100, "changed"
	if err := prepareInitialComposition(&settings); err != nil {
		t.Fatal(err)
	}
	resolved := settings.filesystems[0]
	if resolved.MetadataPool.Replicas != 3 || resolved.MetadataPool.MinSize != 2 || resolved.DataPool.Replicas != 3 || settings.pools[0].Replicas != 3 {
		t.Fatal("initial pools did not use final cluster defaults")
	}
	if resolved.AdditionalDataPools[0].Name != "ec" || resolved.AdditionalDataPools[0].ErasureCode.K != 2 || resolved.AdditionalDataPools[0].MinSize != 3 {
		t.Fatal("initial config aliases caller buffers or applied replicated min_size to EC")
	}
	settings = options{osds: 1, poolReplicas: 1, poolMinSize: 1}
	if err := WithCephFS()(&settings); err != nil {
		t.Fatal(err)
	}
	if err := prepareInitialComposition(&settings); err != nil || settings.filesystems[0].MetadataPool.Replicas != 1 {
		t.Fatalf("one-OSD filesystem did not resolve to a usable single-copy pool: %v", err)
	}
}
