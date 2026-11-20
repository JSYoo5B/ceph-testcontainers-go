package harness

import (
	"net/netip"
	"testing"
)

func TestRBDAbsenceRequiresCompleteObservation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		check     func([]byte) error
		good, bad []string
	}{
		{"snapshots", func(b []byte) error { return RBDRequireSnapshotAbsent(b, "loser") },
			[]string{`[]`, `[{"name":"winner"}]`},
			[]string{`null`, `{}`, `[{}]`, `[null]`, `[{"name":null}]`, `[{"name":"loser"}]`, `[{"name":"winner"},{}]`}},
		{"peers", RBDRequireNoPeers,
			[]string{`{"mode":"image","peers":[]}`, `{"mode":"disabled"}`, `{"mode":"pool","peers":[]}`},
			[]string{`{}`, `null`, `{"peers":null}`, `{"mode":"image"}`, `{"mode":"image","peers":null}`, `{"mode":"disabled","peers":null}`, `{"mode":"image","peers":[{"uuid":"peer"}]}`}},
		{"unmirrored", func(b []byte) error { return RBDRequireUnmirrored(b, "pool/ns/image") },
			[]string{`{"name":"image","id":"abc"}`, `{"name":"image","id":"abc","mirroring":{"state":"disabled"}}`},
			[]string{`{}`, `null`, `{"name":"image"}`, `{"name":"other","id":"abc"}`, `{"name":"image","id":"abc","mirroring":null}`, `{"name":"image","id":"abc","mirroring":{}}`, `{"name":"image","id":"abc","mirroring":{"state":"enabled"}}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, input := range tc.good {
				if err := tc.check([]byte(input)); err != nil {
					t.Fatalf("valid %s: %v", input, err)
				}
			}
			for _, input := range tc.bad {
				if err := tc.check([]byte(input)); err == nil {
					t.Fatalf("accepted incomplete or present %s", input)
				}
			}
		})
	}
}

func TestRBDHistoryRequiresDescriptionsBeforeExcludingOperations(t *testing.T) {
	objects := map[string]bool{"rbd_data.1.0000": true}
	for _, input := range []string{`{}`, `null`, `{"ops":null}`, `{"ops":[null]}`, `{"ops":[{}]}`, `{"ops":[{"description":null}]}`, `{"ops":[{"description":""}]}`, `{"ops":[{"description":"  "}]}`} {
		if _, err := RBDDataReadsInHistory([]byte(input), objects); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	for _, input := range []string{`{"ops":[]}`, `{"ops":[{"description":"osd_op(client.1 rbd_data.2.0000 [read 0~1])"}]}`, `{"ops":[{"description":"osd_op(client.1 rbd_data.1.0000 [write 0~1])"}]}`, `{"ops":[{"description":"pg_update_log"}]}`} {
		if reads, err := RBDDataReadsInHistory([]byte(input), objects); err != nil || len(reads) != 0 {
			t.Fatalf("valid unrelated %s: %v, %v", input, reads, err)
		}
	}
	input := `{"ops":[{"description":"osd_op(client.1 rbd_data.1.0000 [read 0~1])","initiated_at":"now"}]}`
	if reads, err := RBDDataReadsInHistory([]byte(input), objects); err != nil || len(reads) != 1 {
		t.Fatalf("positive: %v %v", reads, err)
	}
}

func TestOSDLogBetweenMarkersCutsAtBothMarkerReplies(t *testing.T) {
	const open, close = "tc-reply-open-1", "tc-reply-close-1"
	line := func(peer, tid, object, ops, result string) string {
		return "2026-11-20T01:02:46.408+0000 7f -- [v2:10.0.0.2:6802/7,v1:10.0.0.2:6803/7] --> " + peer + " -- osd_op_reply(" + tid + " " + object + " [" + ops + "] v0'0 uv5 ondisk = " + result + ") -- 0x1 con 0x2\n"
	}
	enoent := "-2 ((2) No such file or directory)"
	before := line("10.0.0.9:0/40", "26", "rbd_data.1.0000", "sparse-read 0~4194304 out=4194304b", "0")
	opened := line("10.0.0.9:0/41", "3", open, "stat", enoent)
	read := line("10.0.0.9:0/42", "27", "rbd_data.1.0001", "sparse-read 0~4194304 out=4194304b", "0")
	similar := line("10.0.0.9:0/43", "4", close+"0", "stat", enoent)
	closed := line("10.0.0.9:0/44", "5", close, "stat", enoent)
	after := line("10.0.0.9:0/45", "28", "rbd_data.1.0002", "sparse-read 0~4194304 out=4194304b", "0")
	for _, log := range []string{"", before + opened + read, before + read + closed, opened + read + similar} {
		if _, _, found, err := OSDLogBetweenMarkers([]byte(log), open, close); err != nil || found {
			t.Fatalf("judged %q without both markers: %t, %v", log, found, err)
		}
	}
	window, client, found, err := OSDLogBetweenMarkers([]byte(before+opened+read+similar+closed+after), open, close)
	if err != nil || !found || client != netip.MustParseAddr("10.0.0.9") || string(window) != read+similar {
		t.Fatalf("cut %q at %v: %t, %v", window, client, found, err)
	}
	for name, log := range map[string]string{
		"close before open": closed + opened,
		"another client":    opened + line("10.0.0.8:0/46", "6", close, "stat", enoent),
		"broken address":    opened + "x -- a --> not-an-address -- osd_op_reply(7 " + close + " [stat] v0'0 uv0 ondisk = -2) -- 0x5\n",
	} {
		if _, _, _, err := OSDLogBetweenMarkers([]byte(log), open, close); err == nil {
			t.Fatalf("%s: accepted %q", name, log)
		}
	}
}
