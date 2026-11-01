//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"slices"
	"testing"
)

// Lines of a Tentacle 20.2.4 run (issue 389).
const (
	logAckOSDLog = `2026-11-01T00:07:48.886+0000 ffffa68c3f00 10 log_client  log_queue is 5 last_log 5 sent 3 num 5 unsent 2 sending 2
2026-11-01T00:07:48.886+0000 ffffa68c3f00 10 log_client  will send 2026-11-01T00:07:48.323283+0000 osd.0 (osd.0) 4 : cluster [DBG] purged_snaps scrub starts
2026-11-01T00:07:48.886+0000 ffffa68c3f00 10 log_client  will send 2026-11-01T00:07:48.323308+0000 osd.0 (osd.0) 5 : cluster [DBG] purged_snaps scrub ok
`
	logAckPeonLog = `2026-11-01T00:08:12.146+0000 ffff8c719a80 10 mon.b@1(peon) e3 forward_request 198 request log(1 entries from seq 34 at 2026-11-01T00:08:11.686534+0000) features 4541880224203014143
2026-11-01T00:08:12.147+0000 ffff8c719a80 10 mon.b@1(peon) e3 handle_route tid 198 null
2026-11-01T00:08:12.981+0000 ffff8c719a80 10 mon.b@1(peon) e3 handle_route tid 198 log(last 34)
2026-11-01T00:08:12.981+0000 ffff8c719a80 10 mon.b@1(peon) e3  don't have routed request tid 198
2026-11-01T00:08:13.691+0000 ffff8c719a80 10 mon.b@1(peon) e3 forward_request 199 request monmgrreport(gid 4161, 0 checks, 0 progress events) features 4541880224203014143
2026-11-01T00:08:13.691+0000 ffff8c719a80 10 mon.b@1(peon) e3 handle_route tid 199 null
`
)

func TestReadLogClientProgress(t *testing.T) {
	if got := readLogClientProgress([]byte(logAckOSDLog), "osd.0"); got != (logClientProgress{Sent: 5}) {
		t.Fatalf("unacknowledged entries: %+v", got)
	}
	acked := logAckOSDLog + "2026-11-01T00:07:49.101+0000 ffffa68c3f00 10 log_client handle_log_ack log(last 5)\n"
	if got := readLogClientProgress([]byte(acked), "osd.0"); got != (logClientProgress{Sent: 5, Acked: 5, Acks: 1}) {
		t.Fatalf("acknowledged entries: %+v", got)
	}
	if got := readLogClientProgress([]byte(logAckOSDLog), "osd.1"); got.Sent != 0 {
		t.Fatalf("another daemon's entries were counted: %+v", got)
	}
}

func TestLostRoutedAcks(t *testing.T) {
	if got := lostRoutedAcks([]byte(logAckPeonLog)); !slices.Equal(got, []string{"198:34"}) {
		t.Fatalf("lost acks %v", got)
	}
	delivered := "2026-11-01T00:08:12.981+0000 ffff8c719a80 10 mon.b@1(peon) e3 handle_route tid 198 log(last 34)\n"
	if got := lostRoutedAcks([]byte(delivered)); len(got) != 0 {
		t.Fatalf("a delivered ack was reported lost: %v", got)
	}
}

func TestOSDMonSessions(t *testing.T) {
	listings := map[string][]byte{
		"a": []byte(`[{"name":"mon.1","entity_name":"","con_type":"mon"},{"name":"osd.1","entity_name":"osd.1","con_type":"osd","global_id":14125}]`),
		"b": []byte(`[{"name":"osd.0","entity_name":"osd.0","con_type":"osd","global_id":14128},{"name":"mgr.4161","entity_name":"mgr.a","con_type":"mgr"}]`),
	}
	got, err := osdMonSessions(listings)
	if err != nil || len(got) != 2 || got["osd.0"] != (logAckSessionID{"b", 14128}) || got["osd.1"] != (logAckSessionID{"a", 14125}) {
		t.Fatalf("sessions %v: %v", got, err)
	}
	if logAckRole("a", "a") != "leader" || logAckRole("b", "a") != "peon" {
		t.Fatal("roles")
	}
	// A new session with the same MON is another layout.
	renewed := map[string][]byte{"a": listings["a"], "b": []byte(`[{"name":"osd.0","entity_name":"osd.0","con_type":"osd","global_id":14126}]`)}
	if again, err := osdMonSessions(renewed); err != nil || fmt.Sprint(again) == fmt.Sprint(got) {
		t.Fatalf("a renewed session gave the same layout: %v %v", again, err)
	}
	listings["c"] = []byte(`[{"name":"osd.0","entity_name":"osd.0","con_type":"osd"}]`)
	if _, err := osdMonSessions(listings); err == nil {
		t.Fatal("an OSD with two sessions was accepted")
	}
}

// An OSD on peon b that hunts, is acknowledged by the leader a, and returns to
// b: the MonClient lines show it although the final layout looks unchanged.
const logAckRehuntLog = `2026-11-01T00:09:01.100+0000 ffffa68c3f00 10 monclient: ms_handle_reset current mon v2:172.19.0.4:3300/0
2026-11-01T00:09:01.100+0000 ffffa68c3f00 10 monclient: _reopen_session rank -1
2026-11-01T00:09:01.101+0000 ffffa68c3f00 10 monclient: picked mon.a con 0xaaaa addr [v2:172.19.0.2:3300/0,v1:172.19.0.2:6789/0]
2026-11-01T00:09:01.120+0000 ffffa68c3f00  1 monclient: found mon.a
2026-11-01T00:09:01.300+0000 ffffa68c3f00 10 log_client handle_log_ack log(last 5)
2026-11-01T00:09:02.000+0000 ffffa68c3f00 10 monclient: _reopen_session rank -1
2026-11-01T00:09:02.010+0000 ffffa68c3f00  1 monclient: found mon.b
`

func TestMonClientTransitions(t *testing.T) {
	if got := monClientTransitions([]byte(logAckOSDLog)); len(got) != 0 {
		t.Fatalf("a steady session reported transitions: %v", got)
	}
	if got := monClientTransitions([]byte(logAckRehuntLog)); len(got) != 6 {
		t.Fatalf("a peon -> leader -> peon transition: %v", got)
	}
}

func TestRoutedLogAck(t *testing.T) {
	forwarded := `2026-11-01T00:08:12.146+0000 ffff8c719a80 10 mon.b@1(peon).log v49 preprocess_query log(2 entries from seq 4 at 2026-11-01T00:08:11.686534+0000) from osd.0 172.19.0.8:0/1
2026-11-01T00:08:12.146+0000 ffff8c719a80 10 mon.b@1(peon) e3 forward_request 198 request log(2 entries from seq 4 at 2026-11-01T00:08:11.686534+0000) features 4541880224203014143
2026-11-01T00:08:12.147+0000 ffff8c719a80 10 mon.b@1(peon) e3 handle_route tid 198 null
`
	delivered := forwarded + "2026-11-01T00:08:12.981+0000 ffff8c719a80 10 mon.b@1(peon) e3 handle_route tid 198 log(last 5)\n"
	if tid, why := routedLogAck([]byte(delivered), "osd.0", 5); tid != "198" || why != "" {
		t.Fatalf("a routed ack: %q %q", tid, why)
	}
	discarded := delivered + "2026-11-01T00:08:12.981+0000 ffff8c719a80 10 mon.b@1(peon) e3  don't have routed request tid 198\n"
	if tid, _ := routedLogAck([]byte(discarded), "osd.0", 5); tid != "" {
		t.Fatal("a discarded ack was bound")
	}
	if tid, _ := routedLogAck([]byte(delivered), "osd.0", 6); tid != "" {
		t.Fatal("an ack below the sent sequence was bound")
	}
	if tid, _ := routedLogAck([]byte(delivered), "osd.2", 5); tid != "" {
		t.Fatal("another OSD's route was bound")
	}
	// Acknowledged by the leader after a rehunt: nothing routed through b.
	if tid, why := routedLogAck([]byte(forwarded), "osd.0", 5); tid != "" || why == "" {
		t.Fatalf("an ack that never came through the peon: %q %q", tid, why)
	}
}
