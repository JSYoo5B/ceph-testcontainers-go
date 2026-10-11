package cluster

import (
	"context"
	"testing"
)

func TestServiceDefaultsApplyOnlyWithoutExplicitConfiguration(t *testing.T) {
	settings := options{}
	for _, option := range []Option{DefaultCephFS()} {
		if err := option(&settings); err != nil {
			t.Fatal(err)
		}
	}
	if fs := testFilesystems(settings); len(fs) != 1 || fs[0].Name != "" {
		t.Fatalf("defaults were not selected: %+v", settings)
	}
	explicit := options{}
	for _, option := range []Option{WithCephFS(CephFSConfig{Name: "app"}), DefaultCephFS()} {
		if err := option(&explicit); err != nil {
			t.Fatal(err)
		}
	}
	if fs := testFilesystems(explicit); len(fs) != 1 || fs[0].Name != "app" {
		t.Fatalf("defaults replaced explicit services: %+v", explicit)
	}
}

func TestServiceEntryPointsRejectMissingCluster(t *testing.T) {
	ctx := context.Background()
	if _, err := StartCephFS(ctx, nil, CephFSConfig{}); err == nil {
		t.Fatal("nil cluster started a filesystem")
	}
	if Filesystems(nil) != nil {
		t.Fatal("nil cluster reported services")
	}
}
