//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Native output saved from TestSeparateClusterNetworksAndInterruptions on
// quay.io/ceph/ceph:v20.2.4: the held object's map and the primary's
// dump_ops_in_flight while the replica's cluster endpoint was cut.
const (
	heldOSDMapSample = `{"epoch":17,"pool":"tc-topology","pool_id":2,"objname":"held","raw_pgid":"2.100489ea","pgid":"2.0","up":[1,0],"up_primary":1,"acting":[1,0],"acting_primary":1}`
	heldOpsSample    = `{
    "ops": [
        {
            "description": "osd_op(client.4215.0:1 2.0 2:57912008:::held:head [writefull 0~16384 in=16384b] snapc 0=[] ondisk+write+known_if_redirected+supports_pool_eio e17)",
            "initiated_at": "2026-10-14T14:41:27.913520+0000",
            "age": 10.020297920999999,
            "duration": 10.020322588000001,
            "continuous": false,
            "type_data": {
                "flag_point": "waiting for sub ops",
                "client_info": {
                    "client": "client.4215",
                    "client_addr": "172.20.0.6:0/1696013070",
                    "tid": 1
                },
                "events": [
                    {"event": "initiated", "time": "2026-10-14T14:41:27.913520+0000", "duration": 0},
                    {"event": "header_read", "time": "2026-10-14T14:41:27.913520+0000", "duration": 0},
                    {"event": "throttled", "time": "2026-10-14T14:41:27.913522+0000", "duration": 1.6249999999999999e-06},
                    {"event": "all_read", "time": "2026-10-14T14:41:27.913553+0000", "duration": 3.1207999999999997e-05},
                    {"event": "dispatched", "time": "2026-10-14T14:41:27.913556+0000", "duration": 3.625e-06},
                    {"event": "queued_for_pg", "time": "2026-10-14T14:41:27.913569+0000", "duration": 1.2334000000000001e-05},
                    {"event": "reached_pg", "time": "2026-10-14T14:41:27.913639+0000", "duration": 7.0749999999999999e-05},
                    {"event": "started", "time": "2026-10-14T14:41:27.913921+0000", "duration": 0.00028200000000000002},
                    {"event": "waiting for subops from 0", "time": "2026-10-14T14:41:27.914074+0000", "duration": 0.00015266600000000001},
                    {"event": "op_commit", "time": "2026-10-14T14:41:27.916315+0000", "duration": 0.0022412090000000001}
                ]
            }
        }
    ],
    "num_ops": 1
}`
)

func TestHeldReplicationMapping(t *testing.T) {
	mapping, err := harness.ParseHeldMapping([]byte(heldOSDMapSample), "tc-topology", "held")
	if err != nil {
		t.Fatal(err)
	}
	if mapping.Epoch != 17 || mapping.PoolID != 2 || mapping.PGID != "2.0" || !slices.Equal(mapping.Acting, []int{1, 0}) || mapping.ActingPrimary != 1 || mapping.Replica() != 0 {
		t.Fatalf("mapping = %+v", mapping)
	}
	for name, test := range map[string]struct{ data, pool, object string }{
		"another object":       {heldOSDMapSample, "tc-topology", "other"},
		"another pool":         {heldOSDMapSample, "other", "held"},
		"one acting OSD":       {strings.Replace(heldOSDMapSample, `"acting":[1,0]`, `"acting":[1]`, 1), "tc-topology", "held"},
		"primary not acting":   {strings.Replace(heldOSDMapSample, `"acting_primary":1`, `"acting_primary":2`, 1), "tc-topology", "held"},
		"up differs":           {strings.Replace(heldOSDMapSample, `"up":[1,0]`, `"up":[1,2]`, 1), "tc-topology", "held"},
		"up primary differs":   {strings.Replace(heldOSDMapSample, `"up_primary":1`, `"up_primary":0`, 1), "tc-topology", "held"},
		"no epoch":             {strings.Replace(heldOSDMapSample, `"epoch":17`, `"epoch":0`, 1), "tc-topology", "held"},
		"not an osd map reply": {`[]`, "tc-topology", "held"},
	} {
		if _, err := harness.ParseHeldMapping([]byte(test.data), test.pool, test.object); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	later := mapping
	later.Epoch = 18
	if !later.SamePlacement(mapping) {
		t.Error("an unrelated new epoch changed the placement")
	}
	for name, change := range map[string]func(*harness.HeldMapping){
		"acting":  func(m *harness.HeldMapping) { m.Acting = []int{0, 1} },
		"primary": func(m *harness.HeldMapping) { m.ActingPrimary = 0 },
		"PG":      func(m *harness.HeldMapping) { m.PGID = "2.1" },
		"pool":    func(m *harness.HeldMapping) { m.PoolID = 3 },
	} {
		changed := mapping
		changed.Acting = slices.Clone(mapping.Acting)
		change(&changed)
		if changed.SamePlacement(mapping) {
			t.Errorf("a changed %s kept the placement", name)
		}
	}
}

func TestHeldReplicationOp(t *testing.T) {
	op, found, err := harness.FindHeldOp([]byte(heldOpsSample), "held")
	if err != nil || !found {
		t.Fatalf("held op not found: %v", err)
	}
	if op.Request() != "client.4215.0:1" || op.WaitingFor() != "0" || op.TypeData.FlagPoint != "waiting for sub ops" || len(op.Events()) != 10 {
		t.Fatalf("held op = request %q, waiting for %q, %+v", op.Request(), op.WaitingFor(), op.TypeData)
	}
	committed := strings.Replace(heldOpsSample, `{"event": "op_commit"`, `{"event": "sub_op_commit_rec from 0"`, 1)
	for name, data := range map[string]string{
		"replica committed": committed,
		"another flag":      strings.Replace(heldOpsSample, `"flag_point": "waiting for sub ops"`, `"flag_point": "commit sent; apply or cleanup"`, 1),
		"not a client op":   strings.Replace(heldOpsSample, `osd_op(client.4215.0:1`, `osd_repop(client.4215.0:1`, 1),
		"no ops":            `{"ops": [], "num_ops": 0}`,
	} {
		if _, found, err := harness.FindHeldOp([]byte(data), "held"); err != nil || found {
			t.Errorf("%s: found=%t err=%v", name, found, err)
		}
	}
	if _, found, err := harness.FindHeldOp([]byte(heldOpsSample), "hel"); err != nil || found {
		t.Errorf("a prefix of the object name matched: found=%t err=%v", found, err)
	}
	if _, _, err := harness.FindHeldOp([]byte(`{"ops": {}}`), "held"); err == nil {
		t.Error("malformed dump accepted")
	}
	if waiting := (harness.HeldOp{}).WaitingFor(); waiting != "" {
		t.Errorf("an op without the event waits for %q", waiting)
	}
}
