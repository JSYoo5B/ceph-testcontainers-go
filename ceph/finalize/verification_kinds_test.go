package finalize

import "testing"

// Every log event kind that has a pattern is tried by the classifier,
// wherever it stands in the table; a kind added without a sample here fails.
func TestEveryPatternKindIsClassified(t *testing.T) {
	samples := map[LogEvent]string{
		LogAssertion:   "2026-11-27T04:00:00.000+0000 7f1b8c0a1640 -1 ./src/osd/OSD.cc: 42: FAILED ceph_assert(ok)",
		LogFatalSignal: "2026-11-27T04:00:00.000+0000 7f1b8c0a1640 -1 *** Caught signal (Aborted) **",
	}
	tried := 0
	for _, kind := range verificationLogEvents {
		if kind.pattern == nil {
			continue
		}
		tried++
		sample, ok := samples[kind.event]
		if !ok {
			t.Errorf("no sample line for the pattern kind %s", kind.event)
			continue
		}
		if got, _, _, _ := classifyVerificationLine(sample); got != kind.event {
			t.Errorf("%s sample classified as %q", kind.event, got)
		}
	}
	if tried != len(samples) {
		t.Errorf("%d pattern kinds, %d samples", tried, len(samples))
	}
}
