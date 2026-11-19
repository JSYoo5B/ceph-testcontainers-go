//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"regexp"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"
)

func TestVerificationRestartNoDownWarning(t *testing.T) {
	expectation := restartNoDownWarning()
	if expectation.Daemon != "mon.a" || expectation.Event != finalize.LogClusterWarning ||
		expectation.Required || expectation.MaxCount != 0 {
		t.Fatalf("restart warning must be optional for the MON and admit repeated deliveries: %+v", expectation)
	}
	pattern := regexp.MustCompile(expectation.Pattern)
	const cluster = "cluster 2026-10-26T10:00:02.000000+0000 mon.a (mon.0) 88 : cluster [WRN] "
	const daemon = "2026-10-26T10:00:02.000+0000 7f3a1c0e5640  0 log_channel(cluster) log [WRN] : "
	const warning = "Health check failed: nodown flag(s) set (OSDMAP_FLAGS)"
	for _, tc := range []struct {
		name string
		line string
		want bool
	}{
		{"delayed cluster warning", cluster + warning, true},
		{"daemon copy", daemon + warning, true},
		{"unrelated warning", cluster + "Health check failed: 1 osds down (OSD_DOWN)", false},
		{"additional flag", cluster + "Health check failed: nodown,noout flag(s) set (OSDMAP_FLAGS)", false},
		{"quoted setup warning", cluster + "unrelated warning quoted: " + cluster + warning, false},
		{"error severity", "cluster 2026-10-26T10:00:02.000000+0000 mon.a (mon.0) 88 : cluster [ERR] " + warning, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := pattern.MatchString(tc.line); got != tc.want {
				t.Fatalf("restart warning matched = %t, want %t for %q", got, tc.want, tc.line)
			}
		})
	}
}
