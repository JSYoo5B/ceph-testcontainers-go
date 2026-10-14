//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

// Fields of "pg query" saved from TestOSDRecoveryAndBackfillReceipts on
// quay.io/ceph/ceph:v20.2.4, while recovery was held: osd.0 had stopped at
// 16'20 for the log case and at 19'25 for the backfill case. Only the fields
// that pgQuery reads are kept; the values are native.
const (
	logRecoveryQuerySample = `{"state": "active+recovering+degraded", "up": [1, 0], "acting": [1, 0], "info": {"last_update": "19'25", "last_complete": "19'25", "log_tail": "0'0", "stats": {"stat_sum": {"num_objects": 25, "num_objects_recovered": 0, "num_objects_degraded": 5}}}, "peer_info": [{"peer": "0", "last_update": "19'25", "last_complete": "16'20", "last_backfill": "MAX"}], "recovery_state": [{"name": "Started/Primary/Active", "recovery_progress": {"backfill_targets": []}}, {"name": "Started"}]}`
	backfillQuerySample    = `{"state": "active+undersized+degraded+remapped+backfilling", "up": [1, 0], "acting": [1], "info": {"last_update": "26'85", "last_complete": "26'85", "log_tail": "26'74", "stats": {"stat_sum": {"num_objects": 85, "num_objects_recovered": 5, "num_objects_degraded": 85}}}, "peer_info": [{"peer": "0", "last_update": "26'85", "last_complete": "26'85", "last_backfill": "MIN"}], "recovery_state": [{"name": "Started/Primary/Active", "recovery_progress": {"backfill_targets": ["0"]}}, {"name": "Started"}]}`
)

func TestPGRecoveryPath(t *testing.T) {
	logStop, backfillStop := eversion{16, 20}, eversion{19, 25}
	for name, test := range map[string]struct {
		sample  string
		stopped eversion
		want    string
	}{
		"log":      {logRecoveryQuerySample, logStop, "log"},
		"backfill": {backfillQuerySample, backfillStop, "backfill"},
	} {
		query, err := parsePGQuery([]byte(test.sample))
		if err != nil {
			t.Fatal(name, err)
		}
		if path, err := pgRecoveryPath(query, 0, test.stopped); err != nil || path != test.want {
			t.Errorf("%s: path %q, %v", name, path, err)
		}
	}
	if query, _ := parsePGQuery([]byte(backfillQuerySample)); query.Info.Stats.StatSum.Recovered != 5 || query.Info.Stats.StatSum.Objects != 85 {
		t.Errorf("stat_sum = %+v", query.Info.Stats.StatSum)
	}

	// Each case breaks one condition of the native report.
	for name, test := range map[string]struct {
		sample, old, new string
		stopped          eversion
	}{
		"log replica left the acting set":     {logRecoveryQuerySample, `"acting": [1, 0]`, `"acting": [1]`, logStop},
		"log with a backfill target":          {logRecoveryQuerySample, `"backfill_targets": []`, `"backfill_targets": ["0"]`, logStop},
		"log peer backfills":                  {logRecoveryQuerySample, `"last_backfill": "MAX"`, `"last_backfill": "MIN"`, logStop},
		"log no longer covers the replica":    {logRecoveryQuerySample, `"log_tail": "0'0"`, `"log_tail": "17'21"`, logStop},
		"log replica already complete":        {logRecoveryQuerySample, `"last_complete": "16'20"`, `"last_complete": "19'25"`, logStop},
		"log clean state":                     {logRecoveryQuerySample, `active+recovering+degraded`, `active+clean`, logStop},
		"backfill replica acting":             {backfillQuerySample, `"acting": [1]`, `"acting": [1, 0]`, backfillStop},
		"backfill of another OSD":             {backfillQuerySample, `"backfill_targets": ["0"]`, `"backfill_targets": ["2"]`, backfillStop},
		"backfill peer already backfilled":    {backfillQuerySample, `"last_backfill": "MIN"`, `"last_backfill": "MAX"`, backfillStop},
		"backfill while the log still covers": {backfillQuerySample, `"log_tail": "26'74"`, `"log_tail": "19'25"`, backfillStop},
		"backfill state without backfill":     {backfillQuerySample, `+backfilling`, `+recovering`, backfillStop},
		"replica is no peer":                  {logRecoveryQuerySample, `"peer": "0"`, `"peer": "2"`, logStop},
		"nothing written since the stop":      {logRecoveryQuerySample, `"last_update": "19'25", "last_complete": "19'25"`, `"last_update": "16'20", "last_complete": "16'20"`, logStop},
	} {
		changed := strings.Replace(test.sample, test.old, test.new, 1)
		if changed == test.sample {
			t.Fatalf("%s: %q not in the sample", name, test.old)
		}
		query, err := parsePGQuery([]byte(changed))
		if err != nil {
			t.Fatal(name, err)
		}
		if path, err := pgRecoveryPath(query, 0, test.stopped); err == nil {
			t.Errorf("%s: classified as %q", name, path)
		}
	}
	for name, data := range map[string]string{
		"not JSON":     `{`,
		"no state":     `{"info": {"last_update": "1'1", "log_tail": "0'0"}}`,
		"bad eversion": strings.Replace(logRecoveryQuerySample, `"log_tail": "0'0"`, `"log_tail": "0"`, 1),
	} {
		query, err := parsePGQuery([]byte(data))
		if err == nil {
			if _, err := pgRecoveryPath(query, 0, logStop); err == nil {
				t.Errorf("%s accepted", name)
			}
		}
	}
}

func TestExpectedRecoveryDigest(t *testing.T) {
	// recoveryObjects writes name*64 and reads in name order: seed-10 sorts
	// before seed-2.
	if expectedRecoveryDigest("seed:11") == expectedRecoveryDigest("seed:10") || expectedRecoveryDigest("a:1", "b:1") != expectedRecoveryDigest("b:1", "a:1") {
		t.Fatal("digest does not follow the object set in name order")
	}
}
