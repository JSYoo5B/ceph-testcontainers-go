//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
	"time"
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
		{Time: 10.0, Proto: "udp", Peer: "10.0.0.3", Unit: 1, Lines: []string{line("100", "ceph_cluster_stats", ",type_instance=num_pools", "2i")}},
		{Time: 10.1, Proto: "udp", Peer: "10.0.0.3", Unit: 2, Lines: []string{line("100", "ceph_pool_stats", ",pool_id=2,pool_name=tg-probe,type_instance=wr", "7i")}},
		{Time: 10.2, Proto: "udp", Peer: "10.0.0.3", Unit: 3, Lines: []string{line("100", "ceph_daemon_stats", ",ceph_daemon=osd.0,type_instance=osd.op_w", "7i")}},
		{Time: 15.0, Proto: "udp", Peer: "10.0.0.4", Unit: 4, Lines: []string{line("200", "ceph_cluster_stats", ",type_instance=num_pools", "2i")}},
	}
	batches, err := telegrafBatches(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || batches[0].Timestamp != 100 || len(batches[0].Points) != 3 || batches[0].Received != 10.0 || batches[0].LastReceived != 10.2 || len(batches[0].Units) != 3 {
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
		"mixed transport": {records[0], {Time: 10.1, Proto: "tcp", Peer: "10.0.0.3", Unit: 9, Lines: records[1].Lines}},
		"malformed line":  {{Proto: "udp", Lines: []string{"ceph_cluster_stats value=1i"}}},
	} {
		if batches, err := telegrafBatches(bad); err == nil {
			t.Errorf("%s: grouped as %+v", name, batches)
		}
	}
	mixed, err := telegrafBatches([]telegrafRecord{records[0], {Time: 10.1, Proto: "udp", Peer: "10.0.0.4", Unit: 2, Lines: records[1].Lines}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mixed[0].Peer(); err == nil {
		t.Fatal("a batch from two senders has one peer")
	}
}

func TestTelegrafUDPQuietWindowUsesLatestArrival(t *testing.T) {
	records := []telegrafRecord{
		{Time: 10, Proto: "udp", Unit: 1, Lines: []string{"ceph_cluster_stats,fsid=5f1c value=1i 100"}},
		{Time: 13, Proto: "udp", Unit: 2, Lines: []string{"ceph_cluster_stats,fsid=5f1c value=1i 200"}},
		{Time: 14, Proto: "udp", Unit: 3, Lines: []string{"ceph_daemon_stats,fsid=5f1c value=2i 100"}},
	}
	batches, err := telegrafBatches(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || batches[0].Received != 10 || batches[0].LastReceived != 14 {
		t.Fatalf("interleaved batches: %+v", batches)
	}
	now := time.Unix(15, 0)
	if batches[0].ready(now) || !batches[1].ready(now) || !batches[0].ready(now.Add(time.Second)) {
		t.Fatalf("quiet window credited a later timestamp or missed the last arrival: %+v", batches)
	}
}

func TestTelegrafBatchChecksTransportFraming(t *testing.T) {
	lines := []string{"ceph_cluster_stats,fsid=5f1c value=1i 100", "ceph_pool_stats,fsid=5f1c value=1i 100", "ceph_daemon_stats,fsid=5f1c value=1i 100"}
	for _, tc := range []struct {
		name    string
		records []telegrafRecord
		wantErr string
	}{
		{"one TCP connection", []telegrafRecord{{Proto: "tcp", Unit: 1, Lines: lines}}, ""},
		{"several TCP connections", []telegrafRecord{{Proto: "tcp", Unit: 1, Lines: lines[:1]}, {Proto: "tcp", Unit: 2, Lines: lines[1:]}}, "want one"},
		{"several lines in one UDP datagram", []telegrafRecord{{Proto: "udp", Unit: 1, Lines: lines}}, "want one line per datagram"},
		{"multiple timestamps in one TCP connection", []telegrafRecord{{Proto: "tcp", Unit: 1, Lines: append(append([]string(nil), lines...), "ceph_cluster_stats,fsid=5f1c value=1i 200")}}, "mixes timestamps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batches, err := telegrafBatches(tc.records)
			if err == nil {
				if len(batches) != 1 {
					t.Fatalf("batches: %+v", batches)
				}
				err = checkTelegrafBatch(batches[0], "5f1c")
			}
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("framing: got %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestTelegrafReceiverReportsOnlyFramedUnits(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	// Drive the actual receiver functions with controlled datagram and
	// stream reads for framing, clean EOF and connection reset.
	prelude := `import json, sys, threading
_lock = threading.Lock()
def record(entry):
    print(json.dumps(entry), flush=True)
`
	control := `
class Socket:
    def __init__(self, *reads):
        self.reads = iter(reads)
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def recv(self, _):
        value = next(self.reads)
        if isinstance(value, OSError):
            raise value
        return value
    def recvfrom(self, limit):
        return self.recv(limit), ('192.0.2.1', 12345)
line = b'ceph_cluster_stats,fsid=5f1c value=1i 100'
receive_udp(Socket(line + b'\n', line, OSError('socket closed')))
read_tcp(Socket(line[:12], line[12:] + b'\n', b''), '192.0.2.1')
read_tcp(Socket(line, b''), '192.0.2.1')
read_tcp(Socket(line + b'\n', ConnectionResetError('peer reset')), '192.0.2.1')
`
	script := strings.TrimSuffix(telegrafReceiverScript, "serve_receiver()\n")
	out, err := exec.CommandContext(t.Context(), python, "-I", "-c", prelude+script+control, "0", "0").CombinedOutput()
	if err != nil {
		t.Fatalf("telegraf receiver: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 5 {
		t.Fatalf("receiver recorded %d units, want 5: %s", len(lines), out)
	}
	for i, want := range []struct {
		proto string
		err   string
	}{
		{"udp", ""},
		{"udp", "unterminated"},
		{"tcp", ""},
		{"tcp", "unterminated"},
		{"tcp", "peer reset"},
	} {
		var got telegrafRecord
		if err := json.Unmarshal([]byte(lines[i]), &got); err != nil {
			t.Fatal(err)
		}
		if got.Proto != want.proto || (want.err == "" && got.Error != "") ||
			(want.err != "" && !strings.Contains(got.Error, want.err)) {
			t.Errorf("unit %d: %+v, want %s with error %q", i, got, want.proto, want.err)
		}
		if want.err == "" && (len(got.Lines) != 1 || !strings.HasSuffix(got.Lines[0], " 100")) {
			t.Errorf("unit %d lost its complete line: %+v", i, got)
		}
		if want.err != "" {
			if len(got.Lines) != 0 {
				t.Errorf("unit %d published data from a failed unit: %+v", i, got)
			}
			if _, err := telegrafBatches([]telegrafRecord{got}); err == nil || !strings.Contains(err.Error(), want.err) {
				t.Errorf("unit %d did not carry failure into the batch judge: %v", i, err)
			}
		}
	}
}
