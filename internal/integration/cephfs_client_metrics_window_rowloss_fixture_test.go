//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

// These controls feed synthetic sample series to judgeClientMetricsWindow in
// which a selected row, or the whole metrics section, disappears after the
// workload and may reappear. A lost row is unavailable evidence, so the window
// is inconclusive with an exact reason and a later covering sample never
// promotes it. They reuse the helpers of the window fixture and are not native
// counter-loss reproductions.

func TestClientMetricsWindowRowLoss(t *testing.T) {
	base := sample(1, 2, tot(0, 0), tot(0, 0))
	covered := func() clientMetricsSample { return sample(30, 31, tot(1, 131072), tot(0, 0)) }
	noSection := func(start, end float64) clientMetricsSample {
		return clientMetricsSample{Start: at(start), End: at(end)}
	}
	const (
		sectionLost = "row identity lost after workload: sample 1 has no metrics section"
		writerLost  = "row identity lost after workload: sample 1 lacks the writer client.4101 row"
		idleLost    = "row identity lost after workload: sample 1 lacks the idle client.4102 row"
	)
	cases := []struct {
		name   string
		w      clientMetricsWindow
		want   clientMetricsVerdict
		reason string
	}{
		// Loss between the acknowledgement and the first covering sample.
		{"section lost then both rows covering", window(base, noSection(25, 26), covered()), clientMetricsInconclusive, sectionLost},
		{"writer row lost then both rows covering", window(base, sample(25, 26, nil, tot(0, 0)), covered()), clientMetricsInconclusive, writerLost},
		{"idle row lost then both rows covering", window(base, sample(25, 26, tot(0, 0), nil), covered()), clientMetricsInconclusive, idleLost},
		{"section lost then partial then covering", window(base, noSection(25, 26), sample(28, 29, tot(0, 0), tot(0, 0)), covered()), clientMetricsInconclusive, sectionLost},
		// Loss after a covering sample.
		{"section lost after a covering sample", window(base, sample(25, 26, tot(1, 131072), tot(0, 0)), noSection(30, 31)), clientMetricsInconclusive,
			"row identity lost after workload: sample 2 has no metrics section"},
		{"writer row lost after a covering sample", window(base, sample(25, 26, tot(1, 131072), tot(0, 0)), sample(30, 31, nil, tot(0, 0))), clientMetricsInconclusive,
			"row identity lost after workload: sample 2 lacks the writer client.4101 row"},
		{"idle row lost after a covering sample", window(base, sample(25, 26, tot(1, 131072), tot(0, 0)), sample(30, 31, tot(1, 131072), nil)), clientMetricsInconclusive,
			"row identity lost after workload: sample 2 lacks the idle client.4102 row"},
		{"loss then reappearance after a covering sample", window(base, sample(25, 26, tot(1, 131072), tot(0, 0)), noSection(30, 31), sample(35, 36, tot(1, 131072), tot(0, 0))), clientMetricsInconclusive,
			"row identity lost after workload: sample 2 has no metrics section"},
		// Positive evidence from a present row still wins over a lost row.
		{"idle writes after a lost row stay contradicted", window(base, noSection(25, 26), sample(30, 31, tot(1, 131072), tot(1, 4096))), clientMetricsContradicted, "idle client row carries writes"},

		// Paired controls: rows present throughout, monotonic.
		{"control rows present throughout", window(base, sample(25, 26, tot(1, 131072), tot(0, 0)), sample(30, 31, tot(1, 131072), tot(0, 0))), clientMetricsAttributed,
			"writer row covers the acknowledged write"},
		{"control partial report then covering", window(base, sample(25, 26, tot(0, 0), tot(0, 0)), covered()), clientMetricsAttributed,
			"writer row covers the acknowledged write"},
		// Samples outside the acknowledgement-to-deadline window are not part
		// of the continuity check.
		{"control section absent during the workload", window(base, noSection(12, 13), sample(25, 26, tot(1, 131072), tot(0, 0))), clientMetricsAttributed,
			"writer row covers the acknowledged write"},
		{"control section absent after the deadline", window(base, sample(25, 26, tot(1, 131072), tot(0, 0)), noSection(61, 62)), clientMetricsAttributed,
			"writer row covers the acknowledged write"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := judgeClientMetricsWindow(c.w)
			if got.Verdict != c.want || got.Reason != c.reason {
				t.Fatalf("verdict %d reason %q, want %d reason %q", got.Verdict, got.Reason, c.want, c.reason)
			}
		})
	}
}
