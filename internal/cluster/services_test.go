package cluster

import (
	"context"
	"testing"
)

func TestServiceDefaultsApplyOnlyWithoutExplicitConfiguration(t *testing.T) {
	settings := options{}
	for _, option := range []Option{DefaultCephFS(), DefaultRGW()} {
		if err := option(&settings); err != nil {
			t.Fatal(err)
		}
	}
	if fs, gw := testFilesystems(settings), testGateways(settings); len(fs) != 1 || fs[0].Name != "" || len(gw) != 1 {
		t.Fatalf("defaults were not selected: %+v", settings)
	}
	explicit := options{}
	for _, option := range []Option{WithCephFS(CephFSConfig{Name: "app"}), WithRGW(RGWConfig{Name: "s3"}), DefaultCephFS(), DefaultRGW()} {
		if err := option(&explicit); err != nil {
			t.Fatal(err)
		}
	}
	if fs, gw := testFilesystems(explicit), testGateways(explicit); len(fs) != 1 || fs[0].Name != "app" || gw[0].Name != "s3" {
		t.Fatalf("defaults replaced explicit services: %+v", explicit)
	}
}

func TestServiceEntryPointsRejectMissingCluster(t *testing.T) {
	ctx := context.Background()
	if _, err := StartCephFS(ctx, nil, CephFSConfig{}); err == nil {
		t.Fatal("nil cluster started a filesystem")
	}
	if _, err := StartRGW(ctx, nil, RGWConfig{}); err == nil {
		t.Fatal("nil cluster started a gateway")
	}
	if err := RemoveRGW(ctx, nil, "a"); err == nil {
		t.Fatal("nil cluster removed a gateway")
	}
	if _, err := GatewaysContext(ctx, nil); err == nil {
		t.Fatal("nil cluster listed gateways")
	}
	if Filesystems(nil) != nil || Gateways(nil) != nil {
		t.Fatal("nil cluster reported services")
	}
}
