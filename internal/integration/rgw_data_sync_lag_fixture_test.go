//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestRGWDataSyncLagComparesShardMarkers covers the shard comparison that
// TestMultiClusterRGWZoneLinkPathCut logs while zone b catches up, with the
// marker shapes of an Umbrella run that did not catch up: shards whose
// destination marker differs from the source's data log are reported, empty
// source shards are not, and outputs that do not cover the same shards once
// are refused.
func TestRGWDataSyncLagComparesShardMarkers(t *testing.T) {
	const (
		m512 = "00000000000000000000:00000000000000000512"
		m665 = "00000000000000000000:00000000000000000665"
	)
	datalog := `[{"marker":"","last_update":"0.000000"},{"marker":"` + m512 + `","last_update":"2026-11-09 14:35:41.0Z"},{"marker":"` + m665 + `","last_update":"2026-11-09 14:35:41.0Z"},{"marker":"` + m512 + `","last_update":"2026-11-09 14:35:00.0Z"}]`
	marker := func(key, status, marker string) string {
		return `{"key":` + key + `,"val":{"status":"` + status + `","marker":"` + marker + `","next_step_marker":"","total_entries":0,"pos":0,"timestamp":"0.000000"}}`
	}
	status := func(shards string, markers ...string) string {
		return `{"sync_status":{"info":{"status":"sync","num_shards":` + shards + `,"mode":"","instance_id":1},"markers":[` + strings.Join(markers, ",") + `]}}`
	}
	// Shard 0 has no entries at the source, whatever the destination holds
	// (a trimmed log keeps the destination's last marker).
	lag, err := harness.RGWDataSyncLag([]byte(datalog), []byte(status("4",
		marker("0", "incremental-sync", m512), marker("1", "incremental-sync", ""),
		marker("3", "incremental-sync", m512), marker("2", "incremental-sync", m512))))
	want := []harness.RGWShardLag{{Shard: 1, Source: m512, Status: "incremental-sync"}, {Shard: 2, Source: m665, Destination: m512, Status: "incremental-sync"}}
	if err != nil || !slices.Equal(lag, want) {
		t.Fatalf("lag %+v, %v, want %+v", lag, err, want)
	}
	if lag, err := harness.RGWDataSyncLag([]byte(datalog), []byte(status("4",
		marker("0", "incremental-sync", ""), marker("1", "incremental-sync", m512), marker("2", "incremental-sync", m665), marker("3", "incremental-sync", m512)))); err != nil || len(lag) != 0 {
		t.Fatalf("caught-up shards reported %+v, %v", lag, err)
	}
	for name, out := range map[string]string{
		"fewer markers":      status("4", marker("0", "", ""), marker("1", "", ""), marker("2", "", "")),
		"shard count":        status("3", marker("0", "", ""), marker("1", "", ""), marker("2", "", ""), marker("3", "", "")),
		"no shard count":     `{"sync_status":{"info":{},"markers":[]}}`,
		"duplicated shard":   status("4", marker("0", "", ""), marker("1", "", ""), marker("1", "", ""), marker("3", "", "")),
		"shard out of range": status("4", marker("0", "", ""), marker("1", "", ""), marker("2", "", ""), marker("4", "", "")),
		"missing value":      status("4", marker("0", "", ""), marker("1", "", ""), marker("2", "", ""), `{"key":3}`),
		"not JSON":           "ERROR: no such source zone",
	} {
		if lag, err := harness.RGWDataSyncLag([]byte(datalog), []byte(out)); err == nil {
			t.Errorf("%s: accepted as %+v", name, lag)
		}
	}
	all := status("4", marker("0", "", ""), marker("1", "", ""), marker("2", "", ""), marker("3", "", ""))
	for name, out := range map[string]string{
		"missing marker": `[{"marker":""},{"marker":"` + m512 + `"},{"last_update":"0"},{"marker":""}]`,
		"not a list":     `{"marker":""}`,
	} {
		if lag, err := harness.RGWDataSyncLag([]byte(out), []byte(all)); err == nil {
			t.Errorf("%s: accepted as %+v", name, lag)
		}
	}
}
