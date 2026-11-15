//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

func TestInfluxLineParse(t *testing.T) {
	point, err := parseInfluxLine(`ceph_pool_stats,fsid=5f1c,pool_id=2,pool_name=tg\ probe\,x\=y,type_instance=wr value=7i 1763190000123456789`)
	if err != nil {
		t.Fatal(err)
	}
	if point.Measurement != "ceph_pool_stats" || point.Tags["pool_name"] != "tg probe,x=y" || point.Tags["pool_id"] != "2" || point.Fields["value"] != int64(7) || point.Timestamp != 1763190000123456789 {
		t.Fatalf("parsed %+v", point)
	}
	typed, err := parseInfluxLine(`m,a=b f=1.5,g="x y,z",h=True,i=-3i 1`)
	if err != nil || typed.Fields["f"] != 1.5 || typed.Fields["g"] != "x y,z" || typed.Fields["h"] != true || typed.Fields["i"] != int64(-3) {
		t.Fatalf("typed fields %+v %v", typed, err)
	}
	for _, line := range []string{
		"ceph_cluster_stats,fsid=5f1c value=1i",                  // no timestamp
		"ceph_cluster_stats,fsid=5f1c value=1i 17 extra",         // trailing part
		"ceph_cluster_stats,fsid value=1i 17",                    // tag without value
		"ceph_cluster_stats,fsid=5f1c value 17",                  // field without value
		"ceph_cluster_stats,fsid=5f1c value=1x 17",               // not a number
		"ceph_cluster_stats,fsid=5f1c value=\"open 17",           // unterminated string
		"ceph_cluster_stats,fsid=5f1c value=1i -5",               // negative timestamp
		",fsid=5f1c value=1i 17",                                 // no measurement
		"ceph_pool_stats,fsid=5f1c,pool_name=tg probe value=1 2", // unescaped space
	} {
		if point, err := parseInfluxLine(line); err == nil {
			t.Errorf("%q parsed as %+v", line, point)
		}
	}
}

func TestTelegrafBatches(t *testing.T) {
	line := func(ts, measurement, tags, value string) string {
		return measurement + ",fsid=5f1c" + tags + " value=" + value + " " + ts
	}
	records := []telegrafRecord{
		{Time: 10.0, Proto: "udp", Peer: "10.0.0.3", Unit: 1, Line: line("100", "ceph_cluster_stats", ",type_instance=num_pools", "2i")},
		{Time: 10.1, Proto: "udp", Peer: "10.0.0.3", Unit: 2, Line: line("100", "ceph_pool_stats", ",pool_id=2,pool_name=tg-probe,type_instance=wr", "7i")},
		{Time: 10.2, Proto: "udp", Peer: "10.0.0.3", Unit: 3, Line: line("100", "ceph_daemon_stats", ",ceph_daemon=osd.0,type_instance=osd.op_w", "7i")},
		{Time: 15.0, Proto: "udp", Peer: "10.0.0.4", Unit: 4, Line: line("200", "ceph_cluster_stats", ",type_instance=num_pools", "2i")},
	}
	batches, err := telegrafBatches(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || batches[0].Timestamp != 100 || len(batches[0].Points) != 3 || batches[0].Received != 10.0 || len(batches[0].Units) != 3 {
		t.Fatalf("batches %+v", batches)
	}
	if peer, err := batches[1].Peer(); err != nil || peer != "10.0.0.4" {
		t.Fatalf("peer %q %v", peer, err)
	}
	if err := checkTelegrafBatch(batches[0], "5f1c"); err != nil {
		t.Fatal(err)
	}
	if wr, err := batches[0].Value("ceph_pool_stats", map[string]string{"pool_id": "2", "type_instance": "wr"}); err != nil || wr != 7 {
		t.Fatalf("pool writes %d %v", wr, err)
	}
	// A pass of another cluster, or one without the pool and daemon
	// measurements, is not a pass of this cluster.
	if err := checkTelegrafBatch(batches[0], "other"); err == nil || !strings.Contains(err.Error(), `want "other"`) {
		t.Fatalf("foreign fsid accepted: %v", err)
	}
	if err := checkTelegrafBatch(batches[1], "5f1c"); err == nil || !strings.Contains(err.Error(), "no ceph_pool_stats") {
		t.Fatalf("partial batch accepted: %v", err)
	}
	if _, err := batches[0].Value("ceph_pool_stats", map[string]string{"pool_id": "3"}); err == nil {
		t.Fatal("absent pool has a value")
	}
	for name, bad := range map[string][]telegrafRecord{
		"duplicate line":  {records[0], records[0]},
		"mixed transport": {records[0], {Time: 10.1, Proto: "tcp", Peer: "10.0.0.3", Unit: 9, Line: records[1].Line}},
		"malformed line":  {{Proto: "udp", Line: "ceph_cluster_stats value=1i"}},
	} {
		if batches, err := telegrafBatches(bad); err == nil {
			t.Errorf("%s: grouped as %+v", name, batches)
		}
	}
	mixed, err := telegrafBatches([]telegrafRecord{records[0], {Time: 10.1, Proto: "udp", Peer: "10.0.0.4", Unit: 2, Line: records[1].Line}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mixed[0].Peer(); err == nil {
		t.Fatal("a batch from two senders has one peer")
	}
}
