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

func TestOSDLogBeforeMarkerCutsAtTheMarkerReply(t *testing.T) {
	const marker = "tc-reply-marker-1"
	read := "2026-11-20T01:02:46.408+0000 7f -- [v2:10.0.0.2:6802/7,v1:10.0.0.2:6803/7] --> 10.0.0.9:0/41 -- osd_op_reply(27 rbd_data.1.0000 [sparse-read 0~4194304 out=4194304b] v0'0 uv5 ondisk = 0) -- 0x1 con 0x2\n"
	other := "2026-11-20T01:02:46.409+0000 7f -- [v2:10.0.0.2:6802/7,v1:10.0.0.2:6803/7] --> 10.0.0.9:0/43 -- osd_op_reply(3 tc-reply-marker-10 [stat] v0'0 uv0 ondisk = -2 ((2) No such file or directory)) -- 0x3 con 0x4\n"
	reply := "2026-11-20T01:02:46.410+0000 7f -- [v2:10.0.0.2:6802/7,v1:10.0.0.2:6803/7] --> 10.0.0.9:0/42 -- osd_op_reply(7 tc-reply-marker-1 [stat] v0'0 uv0 ondisk = -2 ((2) No such file or directory)) -- 0x5 con 0x6\n"
	late := "2026-11-20T01:02:46.411+0000 7f -- [v2:10.0.0.2:6802/7,v1:10.0.0.2:6803/7] --> 10.0.0.9:0/44 -- osd_op_reply(28 rbd_data.1.0001 [sparse-read 0~4194304 out=4194304b] v0'0 uv5 ondisk = 0) -- 0x7 con 0x8\n"
	for _, window := range []string{"", read, read + other} {
		if _, _, found, err := OSDLogBeforeMarker([]byte(window), marker); err != nil || found {
			t.Fatalf("found the marker in %q: %t, %v", window, found, err)
		}
	}
	before, client, found, err := OSDLogBeforeMarker([]byte(read+other+reply+late), marker)
	if err != nil || !found || client != netip.MustParseAddr("10.0.0.9") || string(before) != read+other {
		t.Fatalf("cut %q at %v: %t, %v", before, client, found, err)
	}
	broken := "x -- a --> not-an-address -- osd_op_reply(7 tc-reply-marker-1 [stat] v0'0 uv0 ondisk = -2) -- 0x5\n"
	if _, _, _, err := OSDLogBeforeMarker([]byte(broken), marker); err == nil {
		t.Fatal("accepted a marker reply without a client address")
	}
}
