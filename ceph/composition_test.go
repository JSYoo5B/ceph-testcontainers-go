package ceph

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
		{"replicas exceed initial OSDs", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithOSDCount(1).WithPools(PoolConfig{Name: "data", Replicas: 3}))}},
		{"EC exceeds initial OSDs", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithPools(PoolConfig{Name: "data", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}}))}},
		{"duplicate initial pools", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithPools(PoolConfig{Name: "data"}, PoolConfig{Name: "data"}))}},
		{"too few CRUSH domains", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithInitialOSDs(OSDConfig{Host: "same"}, OSDConfig{Host: "same"}).WithPools(PoolConfig{Name: "data", FailureDomain: "host"}))}},
		{"late default invalidates pool", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithPools(PoolConfig{Name: "data", Replicas: 1}).WithPoolDefaults(3, 2))}},
		{"empty default root", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithDefaultCRUSHRoot(""))}},
		{"unsafe default root", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithDefaultCRUSHRoot("bad root"))}},
		{"unpopulated default root", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithInitialOSDs(OSDConfig{Root: "storage"}, OSDConfig{Root: "storage"}))}},
		{"default root too small", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithInitialOSDs(OSDConfig{Root: "default"}, OSDConfig{Root: "storage"}))}},
		{"explicit absent default root", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithDefaultCRUSHRoot("absent").WithInitialOSDs(OSDConfig{Root: "storage"}, OSDConfig{Root: "storage"}))}},
		{"invalid captured pool", []testcontainers.ContainerCustomizer{WithClusterConfig(ClusterConfig{}.WithPools(PoolConfig{Name: "bad name"}))}},
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
		{"pool then defaults", []Option{WithClusterConfig(ClusterConfig{}.WithPools(PoolConfig{Name: "data", MinSize: 3}).WithPoolDefaults(3, 2))}},
		{"defaults then pool", []Option{WithClusterConfig(ClusterConfig{}.WithPoolDefaults(3, 2).WithPools(PoolConfig{Name: "data", MinSize: 3}))}},
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
			config := settings.pools[0]
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
		opts := []Option{WithClusterConfig(ClusterConfig{}.WithInitialOSDs(layout...).WithPools(PoolConfig{Name: "ordinary"}))}
		if rootFirst {
			opts = append([]Option{WithClusterConfig(ClusterConfig{}.WithDefaultCRUSHRoot("storage"))}, opts...)
		} else {
			opts = append(opts, WithClusterConfig(ClusterConfig{}.WithDefaultCRUSHRoot("storage")))
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
		if settings.pools[0].CRUSHRoot != "storage" {
			t.Fatal("initial pool defaults lost the selected CRUSH root")
		}
		if got := resolvePoolDefaults(settings, PoolConfig{CRUSHRoot: "default"}); got.CRUSHRoot != "default" {
			t.Fatal("explicit pool root was replaced by the cluster default")
		}
	}
}
