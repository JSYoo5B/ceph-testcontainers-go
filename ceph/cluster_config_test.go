package ceph

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func clusterOptions(t *testing.T, clusters ...ClusterConfig) (*options, error) {
	t.Helper()
	o := &options{osds: 2, monitors: 1, managers: 1}
	for _, c := range clusters {
		if err := WithClusterConfig(c)(o); err != nil {
			return o, err
		}
	}
	return o, nil
}

// Zero, one and several values.
func TestClusterConfigZeroOneAndSeveralValues(t *testing.T) {
	o, err := clusterOptions(t, ClusterConfig{})
	if err != nil || o.osds != 2 || o.monitors != 1 || o.managers != 1 || o.pools != nil || o.poolDefaultsSet || o.clusterSet != 0 {
		t.Fatalf("the zero value changed the settings: %+v %v", o, err)
	}
	o, err = clusterOptions(t, ClusterConfig{}.WithMonitorCount(3).WithManagerCount(2).WithOSDCount(4))
	if err != nil || o.monitors != 3 || o.managers != 2 || o.osds != 4 {
		t.Fatalf("chained fields: %+v %v", o, err)
	}
	// Different fields in different values compose in either order, around an empty value.
	for _, order := range [][]ClusterConfig{
		{ClusterConfig{}.WithPoolDefaults(3, 2), ClusterConfig{}, ClusterConfig{}.WithInitialOSDs(OSDConfig{Host: "a"}, OSDConfig{Host: "b"}, OSDConfig{Host: "c"})},
		{ClusterConfig{}.WithInitialOSDs(OSDConfig{Host: "a"}, OSDConfig{Host: "b"}, OSDConfig{Host: "c"}), ClusterConfig{}, ClusterConfig{}.WithPoolDefaults(3, 2)},
	} {
		o, err = clusterOptions(t, order...)
		if err != nil || o.osds != 3 || len(o.initialOSDs) != 3 || o.poolReplicas != 3 || o.poolMinSize != 2 || !o.poolDefaultsSet {
			t.Fatalf("separate values: %+v %v", o, err)
		}
	}
}

// Explicit invalid settings are refused and never read as unspecified.
func TestClusterConfigExplicitInvalidIsRefused(t *testing.T) {
	for name, c := range map[string]ClusterConfig{
		"zero monitors":         ClusterConfig{}.WithMonitorCount(0),
		"zero managers":         ClusterConfig{}.WithManagerCount(0),
		"zero OSDs":             ClusterConfig{}.WithOSDCount(0),
		"negative OSDs":         ClusterConfig{}.WithOSDCount(-1),
		"empty layout":          ClusterConfig{}.WithInitialOSDs(),
		"conflicting host":      ClusterConfig{}.WithInitialOSDs(OSDConfig{Host: "a", Rack: "r1"}, OSDConfig{Host: "a", Rack: "r2"}),
		"empty root":            ClusterConfig{}.WithDefaultCRUSHRoot(""),
		"bad root":              ClusterConfig{}.WithDefaultCRUSHRoot("a b"),
		"zero replicas":         ClusterConfig{}.WithPoolDefaults(0, 1),
		"min size over size":    ClusterConfig{}.WithPoolDefaults(2, 3),
		"valid then invalid":    ClusterConfig{}.WithOSDCount(3).WithOSDCount(0),
		"layout then bad count": ClusterConfig{}.WithInitialOSDs(OSDConfig{}).WithOSDCount(0),
	} {
		if _, err := clusterOptions(t, c); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// An explicit empty pool list is a setting that replaces nothing with nothing.
	o, err := clusterOptions(t, ClusterConfig{}.WithPools())
	if err != nil || o.pools == nil || len(o.pools) != 0 || o.clusterSet&clusterPools == 0 {
		t.Fatalf("an explicit empty pool list was not kept: %+v %v", o, err)
	}
}

// A refused value writes none of its fields.
func TestClusterConfigRefusalChangesNothing(t *testing.T) {
	o, err := clusterOptions(t, ClusterConfig{}.WithMonitorCount(3).WithPools(PoolConfig{Name: "data"}).WithPoolDefaults(0, 1))
	if err == nil || o.monitors != 1 || o.pools != nil || o.clusterSet != 0 {
		t.Fatalf("a refused value changed the settings: %+v %v", o, err)
	}
}

// One owner per field across values, also when both agree; within one value a
// later call replaces the earlier one, including across the methods of one field.
func TestClusterConfigConflictsAreRefused(t *testing.T) {
	for name, pair := range map[string][2]ClusterConfig{
		"monitors":           {ClusterConfig{}.WithMonitorCount(3), ClusterConfig{}.WithMonitorCount(3)},
		"manager count":      {ClusterConfig{}.WithManagerCount(2), ClusterConfig{}.WithManagerCount(1)},
		"managers deferred":  {ClusterConfig{}.WithManagerCount(2), ClusterConfig{}.WithNoInitialManagers()},
		"OSD count":          {ClusterConfig{}.WithOSDCount(1), ClusterConfig{}.WithOSDCount(1)},
		"OSD layout":         {ClusterConfig{}.WithOSDCount(1), ClusterConfig{}.WithInitialOSDs(OSDConfig{})},
		"OSDs deferred":      {ClusterConfig{}.WithNoInitialOSDs(), ClusterConfig{}.WithOSDCount(1)},
		"default CRUSH root": {ClusterConfig{}.WithDefaultCRUSHRoot("a"), ClusterConfig{}.WithDefaultCRUSHRoot("a")},
		"pool defaults":      {ClusterConfig{}.WithPoolDefaults(1, 1), ClusterConfig{}.WithPoolDefaults(2, 1)},
		"pools":              {ClusterConfig{}.WithPools(PoolConfig{Name: "a"}), ClusterConfig{}.WithPools()},
	} {
		if _, err := clusterOptions(t, pair[0], pair[1]); err == nil {
			t.Errorf("%s set by two values was accepted", name)
		}
	}
	for name, test := range map[string]struct {
		c                        ClusterConfig
		osds, managers           int
		noOSDs, noManagers, have bool
	}{
		"count replaces layout": {ClusterConfig{}.WithInitialOSDs(OSDConfig{}, OSDConfig{}, OSDConfig{}).WithOSDCount(1), 1, 1, false, false, false},
		"layout replaces count": {ClusterConfig{}.WithOSDCount(5).WithInitialOSDs(OSDConfig{}), 1, 1, false, false, true},
		"count replaces none":   {ClusterConfig{}.WithNoInitialOSDs().WithOSDCount(3), 3, 1, false, false, false},
		"none replaces count":   {ClusterConfig{}.WithOSDCount(3).WithNoInitialOSDs(), 2, 1, true, false, false},
		"managers replace none": {ClusterConfig{}.WithNoInitialManagers().WithManagerCount(2), 2, 2, false, false, false},
		"none replaces count 2": {ClusterConfig{}.WithManagerCount(3).WithNoInitialManagers(), 2, 1, false, true, false},
	} {
		o, err := clusterOptions(t, test.c)
		if err != nil || o.osds != test.osds || o.managers != test.managers || o.noInitialOSDs != test.noOSDs || o.noInitialManagers != test.noManagers || (o.initialOSDs != nil) != test.have {
			t.Errorf("%s: %+v %v", name, o, err)
		}
	}
}

// Derived values and the settings never share the caller's slices.
func TestClusterConfigDerivedValuesAreIndependent(t *testing.T) {
	layout := []OSDConfig{{Host: "a"}, {Host: "b"}}
	pools := []PoolConfig{{Name: "data", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}}}
	base := ClusterConfig{}.WithInitialOSDs(layout...).WithPools(pools...)
	derived := base.WithPools(PoolConfig{Name: "other"})
	layout[0].Host, pools[0].Name, pools[0].ErasureCode.K = "changed", "changed", 9
	o, err := clusterOptions(t, base)
	if err != nil || o.initialOSDs[0].Host != "a" || o.pools[0].Name != "data" || o.pools[0].ErasureCode.K != 2 {
		t.Fatalf("the base value shared the caller's slices: %+v %v", o, err)
	}
	o.pools[0].ErasureCode.K = 7
	again, err := clusterOptions(t, base)
	if err != nil || again.pools[0].ErasureCode.K != 2 {
		t.Fatalf("the settings shared the value's pools: %+v %v", again, err)
	}
	o, err = clusterOptions(t, derived)
	if err != nil || len(o.pools) != 1 || o.pools[0].Name != "other" || len(o.initialOSDs) != 2 {
		t.Fatalf("derived value: %+v %v", o, err)
	}
}

func TestClusterConfigIsNotACustomizer(t *testing.T) {
	if _, ok := any(ClusterConfig{}).(testcontainers.ContainerCustomizer); ok {
		t.Fatal("ClusterConfig must enter Run through WithClusterConfig")
	}
	if reflect.TypeOf(WithClusterConfig(ClusterConfig{})) != reflect.TypeOf(Option(nil)) {
		t.Fatal("WithClusterConfig must be a ceph.Option")
	}
}

// Run refuses a conflicting or invalid value before any customizer or effect,
// and a valid one keeps the other groups' settings.
func TestRunClusterConfigRefusesBeforeEffectsAndKeepsOtherSettings(t *testing.T) {
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"conflict":         {WithClusterConfig(ClusterConfig{}.WithOSDCount(1)), WithClusterConfig(ClusterConfig{}.WithOSDCount(1))},
		"invalid":          {WithClusterConfig(ClusterConfig{}.WithMonitorCount(0))},
		"pool on none":     {WithClusterConfig(ClusterConfig{}.WithNoInitialOSDs().WithPools(PoolConfig{Name: "data"}))},
		"layout too small": {WithClusterConfig(ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(3, 2))},
	} {
		var customizers atomic.Int32
		opts = append(opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		cluster, err := Run(ctx, DefaultImage, opts...)
		if cluster != nil || !errors.Is(err, ErrInvalidRequest) || customizers.Load() != 0 {
			t.Errorf("%s: cluster=%v err=%v customizers=%d", name, cluster, err, customizers.Load())
		}
	}
	var effective *options
	capture := Option(func(o *options) error { effective = o; return nil })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Run(ctx, DefaultImage, WithClusterConfig(ClusterConfig{}.WithOSDCount(3).WithPoolDefaults(3, 2)), WithOSDStorage(OSDStorage{}.WithBlockSize(128<<20)),
		WithNetworkConfig(NetworkConfig{}.WithSeparateClusterNetwork()), capture)
	if !errors.Is(err, context.Canceled) || effective == nil || effective.osds != 3 || effective.poolReplicas != 3 || effective.blockSize != 128<<20 || !effective.separateClusterNetwork {
		t.Fatalf("a setting was lost: %+v %v", effective, err)
	}
}
