//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"
)

// Admin socket output from TestMapPropagationReceipt runs, reduced to the
// fields that the parsers read; v19.2.5 and v20.2.4 printed the same values.
// mon.a's mon_status after MON d joined, and osd.0's status.
const (
	monStatusFourMembersSample = `{"name": "a", "state": "leader", "election_epoch": 16, "quorum": [0, 1, 2, 3], "monmap": {"epoch": 4, "mons": [{"name": "a"}, {"name": "b"}, {"name": "c"}, {"name": "d"}]}}`
	osdStatusSample            = `{"whoami": 0, "state": "active", "oldest_map": "1", "newest_map": "14"}`
)

func TestParseMonmapView(t *testing.T) {
	view, err := parseMonmapView([]byte(monStatusFourMembersSample))
	if err != nil || view.Name != "a" || view.State != "leader" || view.ElectionEpoch != 16 || view.Epoch != 4 ||
		!slices.Equal(view.Members, []string{"a", "b", "c", "d"}) || len(view.Quorum) != 4 {
		t.Fatalf("%+v %v", view, err)
	}
	// Members are compared sorted, whatever order the MonMap lists them in.
	reordered := strings.Replace(monStatusFourMembersSample, `{"name": "a"}, {"name": "b"}`, `{"name": "b"}, {"name": "a"}`, 1)
	if view, err := parseMonmapView([]byte(reordered)); err != nil || !slices.Equal(view.Members, []string{"a", "b", "c", "d"}) {
		t.Fatalf("reordered: %+v %v", view, err)
	}
	for _, malformed := range []string{`{}`, `{"name": "a"}`, `{"name": "a", "monmap": {"epoch": 0, "mons": [{"name": "a"}]}}`,
		`{"name": "a", "monmap": {"epoch": 4, "mons": []}}`, `[]`, ``} {
		if view, err := parseMonmapView([]byte(malformed)); err == nil {
			t.Errorf("%q parsed: %+v", malformed, view)
		}
	}
}

func TestParseOSDMapView(t *testing.T) {
	view, err := parseOSDMapView([]byte(osdStatusSample))
	if err != nil || view.WhoAmI != 0 || view.OldestMap != 1 || view.NewestMap != 14 {
		t.Fatalf("%+v %v", view, err)
	}
	if view, err := parseOSDMapView([]byte(`{"whoami": 1, "oldest_map": 1, "newest_map": 15}`)); err != nil || view.NewestMap != 15 {
		t.Fatalf("plain numbers: %+v %v", view, err)
	}
	for _, malformed := range []string{`{}`, `{"whoami": 0}`, `{"whoami": 0, "oldest_map": "1", "newest_map": "x"}`, `[]`} {
		if view, err := parseOSDMapView([]byte(malformed)); err == nil {
			t.Errorf("%q parsed: %+v", malformed, view)
		}
	}
}
