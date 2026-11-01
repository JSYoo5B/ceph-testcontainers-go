//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
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
		"b": []byte(`[{"name":"osd.0","entity_name":"osd.0","con_type":"osd"},{"name":"mgr.4161","entity_name":"mgr.a","con_type":"mgr"}]`),
	}
	got, err := osdMonSessions(listings)
	if err != nil || len(got) != 2 || got["osd.0"] != "b" || got["osd.1"] != "a" {
		t.Fatalf("sessions %v: %v", got, err)
	}
	if logAckRole("a", "a") != "leader" || logAckRole("b", "a") != "peon" {
		t.Fatal("roles")
	}
	listings["c"] = []byte(`[{"name":"osd.0","entity_name":"osd.0","con_type":"osd"}]`)
	if _, err := osdMonSessions(listings); err == nil {
		t.Fatal("an OSD with two sessions was accepted")
	}
}
