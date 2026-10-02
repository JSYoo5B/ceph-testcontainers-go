package federation

import (
	"context"
	"testing"
)

func TestRunRBDMirrorRejectsMissingClusters(t *testing.T) {
	mirror, err := RunRBDMirror(context.Background(), "example/ceph:version", RBDMirrorConfig{Pool: "rbd"})
	if err == nil || mirror != nil {
		t.Fatalf("missing cluster pair must fail before resource allocation: mirror=%v error=%v", mirror, err)
	}
}

func TestRBDMirrorConfigDefaultsAndRejectsAmbiguousPool(t *testing.T) {
	config, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: " rbd "})
	if err != nil || config.Pool != "rbd" || config.SourceSite != "source" || config.DestinationSite != "destination" {
		t.Fatalf("unexpected normalized configuration: %+v error=%v", config, err)
	}
	for _, pool := range []string{"", " ", "-rbd", "rbd/namespace", "rbd images"} {
		t.Run(pool, func(t *testing.T) {
			if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: pool}); err == nil {
				t.Fatalf("ambiguous pool %q accepted", pool)
			}
		})
	}
	if _, err := normalizeRBDMirrorConfig(RBDMirrorConfig{Pool: "rbd", SourceSite: "same", DestinationSite: "same"}); err == nil {
		t.Fatal("identical source and destination site names accepted")
	}
}
