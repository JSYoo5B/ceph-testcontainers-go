//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

// Cluster log lines from TestOSDHeartbeatFailure runs on
// quay.io/ceph/ceph:v20.2.4, where osd.2's cluster network was cut and the
// map that marked it down was e17.
const osdHeartbeatClusterLog = `2026-10-16T16:19:10.449022+0000 mon.a (mon.0) 142 : cluster [INF] osd.2 failed (root=default,host=osd-2) (2 reporters from different host after 23.732306 >= grace 20.000000)
2026-10-16T16:19:11.293074+0000 mon.a (mon.0) 143 : cluster [WRN] Health check failed: 1 osds down (OSD_DOWN)
2026-10-16T16:19:11.293118+0000 mon.a (mon.0) 144 : cluster [WRN] Health check failed: 1 host (1 osds) down (OSD_HOST_DOWN)
2026-10-16T16:19:11.314228+0000 osd.2 (osd.2) 4 : cluster [WRN] Monitor daemon marked osd.2 down, but it is still running
2026-10-16T16:19:11.314536+0000 mon.a (mon.0) 146 : cluster [INF] osd.2 marked itself dead as of e17
`

func TestParseOSDHeartbeatRecords(t *testing.T) {
	failure, ok := parseOSDFailure(osdHeartbeatClusterLog, 2)
	if !ok || failure.Reporters != 2 || failure.Subtree != "host" || failure.After != 23.732306 || failure.Grace != 20 {
		t.Fatalf("failure record: %+v ok=%t", failure, ok)
	}
	if dead, running, ok := parseOSDMarkedDead(osdHeartbeatClusterLog, 2); !ok || !running || dead != 17 {
		t.Fatalf("dead record: e%d running=%t ok=%t", dead, running, ok)
	}
	// Another OSD's records, or none, are no record of osd.2; osd.12 does
	// not match osd.2 either.
	other := strings.ReplaceAll(osdHeartbeatClusterLog, "osd.2", "osd.12")
	if _, ok := parseOSDFailure(other, 2); ok {
		t.Error("osd.12's failure matched osd.2")
	}
	if _, running, ok := parseOSDMarkedDead(other, 2); ok || running {
		t.Error("osd.12's records matched osd.2")
	}
	// A failure that the MON reports at a warning level, or with a malformed
	// count, is not this record.
	for _, line := range []string{
		strings.Replace(osdHeartbeatClusterLog, "cluster [INF] osd.2 failed", "cluster [WRN] osd.2 failed", 1),
		strings.Replace(osdHeartbeatClusterLog, "(2 reporters", "(two reporters", 1),
	} {
		if failure, ok := parseOSDFailure(line, 2); ok {
			t.Errorf("parsed %+v", failure)
		}
	}
	// The last record wins when an OSD is marked down twice.
	twice := osdHeartbeatClusterLog + strings.ReplaceAll(strings.ReplaceAll(osdHeartbeatClusterLog, "23.732306", "41.5"), "e17", "e21")
	if failure, _ := parseOSDFailure(twice, 2); failure.After != 41.5 {
		t.Errorf("last failure: %+v", failure)
	}
	if dead, _, _ := parseOSDMarkedDead(twice, 2); dead != 21 {
		t.Errorf("last dead epoch: %d", dead)
	}
}

func TestParseOSDMap(t *testing.T) {
	dump, err := parseOSDMap([]byte(`{"epoch":18,"osds":[{"osd":2,"uuid":"1e7c1933-1274-45ac-84d1-ebd7cf000036","up":0,"in":1,"up_from":13,"down_at":17,"state":["exists"]}]}`))
	if err != nil || dump.Epoch != 18 {
		t.Fatalf("%+v %v", dump, err)
	}
	if osd, ok := dump.osd(2); !ok || osd.Up != 0 || osd.UpFrom != 13 || osd.DownAt != 17 {
		t.Fatalf("osd.2: %+v ok=%t", osd, ok)
	}
	if _, ok := dump.osd(1); ok {
		t.Error("found osd.1")
	}
	for _, malformed := range []string{`{}`, `{"epoch":1}`, `{"osds":[]}`, `[]`} {
		if _, err := parseOSDMap([]byte(malformed)); err == nil {
			t.Errorf("%s parsed", malformed)
		}
	}
}
