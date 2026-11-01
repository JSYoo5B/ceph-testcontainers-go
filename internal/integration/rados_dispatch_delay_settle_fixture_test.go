//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"testing"
)

func TestDispatchDelaySettleNeedsTheBeaconInOSDXInfo(t *testing.T) {
	const before = `{"epoch":15,"osd_xinfo":[{"osd":0,"down_stamp":"0.000000","last_purged_snaps_scrub":"0.000000","dead_epoch":0}]}`
	const after = `{"epoch":16,"osd_xinfo":[{"osd":0,"down_stamp":"0.000000","last_purged_snaps_scrub":"2026-10-31T14:15:02.486211+0000","dead_epoch":0}]}`
	// The beacon epoch grows the dump by exactly the zero time replaced by a
	// timestamp, the 23 bytes that the failure records show.
	if len(after)-len(before) != 23 {
		t.Fatalf("the beacon epoch grows the dump by %d bytes", len(after)-len(before))
	}
	for _, c := range []struct {
		dump     string
		epoch    uint32
		beaconed bool
	}{{before, 15, false}, {after, 16, true}} {
		epoch, beaconed, err := parseBeaconedOSDMap([]byte(c.dump), 0)
		if err != nil || epoch != c.epoch || beaconed != c.beaconed {
			t.Fatalf("%s: e%d beaconed=%t err=%v", c.dump, epoch, beaconed, err)
		}
	}
	for _, dump := range []string{
		`{"osd_xinfo":[{"osd":0,"last_purged_snaps_scrub":"0.000000"}]}`,
		`{"epoch":16,"osd_xinfo":[{"osd":1,"last_purged_snaps_scrub":"2026-10-31T14:15:02.486211+0000"}]}`,
		`{"epoch":16,"osd_xinfo":[{"osd":0}]}`,
		`{"epoch":16}`,
		`not json`,
	} {
		if _, beaconed, err := parseBeaconedOSDMap([]byte(dump), 0); err == nil || beaconed {
			t.Fatalf("%s: beaconed=%t err=%v, want an error", dump, beaconed, err)
		}
	}
}

func TestDispatchDelaySettleListsEveryPGOfTheOSD(t *testing.T) {
	pgs, err := parsePGIDs([]byte(`{"pg_ready":true,"pg_stats":[{"pgid":"1.0","state":"active+clean"},{"pgid":"2.0","state":"active+clean"}]}`))
	if err != nil || !slices.Equal(pgs, []string{"1.0", "2.0"}) {
		t.Fatalf("pgs %v, err %v", pgs, err)
	}
	for _, out := range []string{`{"pg_ready":true,"pg_stats":[]}`, `{"pg_ready":true}`, `{"pg_stats":[{"state":"active+clean"}]}`, `[]`} {
		if pgs, err := parsePGIDs([]byte(out)); err == nil {
			t.Fatalf("%s: pgs %v, want an error", out, pgs)
		}
	}
}
