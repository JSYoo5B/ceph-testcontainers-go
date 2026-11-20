package harness

import "testing"

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

func TestOSDHistoryNamesObjectRequiresTheExactObject(t *testing.T) {
	const marker = "tc-history-barrier-1"
	for _, input := range []string{`{}`, `null`, `{"ops":null}`, `{"ops":[{}]}`, `{"ops":[{"description":" "}]}`} {
		if _, err := OSDHistoryNamesObject([]byte(input), marker); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	for _, input := range []string{
		`{"ops":[]}`,
		`{"ops":[{"description":"osd_op(client.1 2.0 2:abc:::tc-history-barrier-10:head [stat])"}]}`,
		`{"ops":[{"description":"osd_op(client.1 2.0 2:abc:::tc-history-barrier-1x:head [stat])"}]}`,
		`{"ops":[{"description":"pg_update_log tc-history-barrier-1"}]}`,
	} {
		if seen, err := OSDHistoryNamesObject([]byte(input), marker); err != nil || seen {
			t.Fatalf("another object in %s: %t, %v", input, seen, err)
		}
	}
	input := `{"ops":[{"description":"osd_op(client.1 rbd_data.1.0000 [read 0~1])","initiated_at":"t0"},{"description":"osd_op(client.1.0:7 2.0 2:abc:::tc-history-barrier-1:head [stat] snapc 0=[] ondisk+read+known_if_redirected e20)"}]}`
	if seen, err := OSDHistoryNamesObject([]byte(input), marker); err != nil || !seen {
		t.Fatalf("marker: %t, %v", seen, err)
	}
}
