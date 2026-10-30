//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"strings"
	"testing"
)

// Reports in the dualStackActor format for TestDualStackFamilyCut's osd-cut
// read of objects[1]: pending while the cut holds, and done after Restore.
const (
	familyCutPendingReport = `{"id": "osd-cut", "done": false, "instance": 4211, "sockets": []}`
	familyCutJoinedReport  = `{"id": "osd-cut", "done": true, "instance": 4211, "sockets": [], "errno": 0, "sha": "%s", "ms": 41237.5}`
)

func familyCutReport(t *testing.T, format, sha string) actorResult {
	t.Helper()
	var result actorResult
	if err := json.Unmarshal([]byte(strings.Replace(format, "%s", sha, 1)), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFamilyCutJoinChecksPendingRead(t *testing.T) {
	const object, other = "obj-osd1", "obj-osd0"
	want := map[string]string{object: strings.Repeat("a", 64), other: strings.Repeat("b", 64)}
	read := map[string]any{"id": "osd-cut", "op": "read", "name": object, "wait": 8}
	join := map[string]any{"id": "osd-cut", "op": "join", "wait": 90}

	for _, tc := range []struct {
		name, sha, errPart string
	}{
		{"exact bytes", want[object], ""},
		{"another object's bytes", want[other], "SHA-256"},
		{"other bytes", strings.Repeat("c", 64), "SHA-256"},
		{"no hash", "", "SHA-256"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := actorRequests{}
			if hash, err := started.Check(read, familyCutReport(t, familyCutPendingReport, ""), false, want); err != nil || hash != "" {
				t.Fatalf("pending read: %q, %v", hash, err)
			}
			hash, err := started.Check(join, familyCutReport(t, familyCutJoinedReport, tc.sha), true, want)
			if tc.errPart == "" {
				if err != nil || hash != "osd-cut:sha256("+object+")="+want[object] {
					t.Fatalf("joined read: %q, %v", hash, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.errPart) || !strings.Contains(err.Error(), object) {
				t.Fatalf("joined read with SHA-256 %q: %q, %v; want an error naming %s", tc.sha, hash, err, object)
			}
		})
	}
}

func TestFamilyCutJoinNeedsStartedOperation(t *testing.T) {
	join := map[string]any{"id": "osd-cut", "op": "join", "wait": 90}
	if _, err := (actorRequests{}).Check(join, familyCutReport(t, familyCutJoinedReport, ""), true, nil); err == nil {
		t.Fatal("a join without a started operation was accepted")
	}
	// A pending MON command joined after Restore has no bytes to compare.
	started := actorRequests{}
	mon := map[string]any{"id": "mon-cut", "op": "mon", "wait": 8}
	if _, err := started.Check(mon, actorResult{ID: "mon-cut"}, false, nil); err != nil {
		t.Fatal(err)
	}
	if hash, err := started.Check(map[string]any{"id": "mon-cut", "op": "join", "wait": 90}, actorResult{ID: "mon-cut", Done: true}, true, nil); err != nil || hash != "" {
		t.Fatalf("joined mon command: %q, %v", hash, err)
	}
}
