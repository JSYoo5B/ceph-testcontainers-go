//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"testing"
)

func TestJournalPayloadRequiresTheFinalObjectAndPurgeObservations(t *testing.T) {
	size := uint64(12)
	rec := jbRecord{Start: 20, PayloadEnd: 28, ObjectSize: 16, PayloadSHA256: "matching"}
	good := func() journalPayloadView {
		return journalPayloadView{SHA256: "matching", LastObjectSize: &size, Beyond: map[string]bool{"200.00000002": false}}
	}
	if err := checkJournalPayload(good(), rec); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*journalPayloadView){
		"stale suffix":            func(v *journalPayloadView) { n := uint64(17); v.LastObjectSize = &n },
		"truncated final object":  func(v *journalPayloadView) { n := uint64(11); v.LastObjectSize = &n },
		"unobserved final object": func(v *journalPayloadView) { v.LastObjectSize = nil },
		"unobserved purge":        func(v *journalPayloadView) { v.Beyond = nil },
		"surviving next object":   func(v *journalPayloadView) { v.Beyond["200.00000002"] = true },
		"short payload":           func(v *journalPayloadView) { n := 1; v.Short = &n },
		"altered payload":         func(v *journalPayloadView) { v.SHA256 = "different" },
	} {
		v := good()
		mutate(&v)
		if err := checkJournalPayload(v, rec); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// An aligned end leaves a full final object and two following objects to purge.
	aligned := rec
	aligned.PayloadEnd = 32
	n := uint64(16)
	v := good()
	v.LastObjectSize = &n
	v.Beyond["200.00000003"] = false
	if err := checkJournalPayload(v, aligned); err != nil {
		t.Fatal(err)
	}
	delete(v.Beyond, "200.00000003")
	if err := checkJournalPayload(v, aligned); err == nil {
		t.Fatal("missing second aligned purge observation accepted")
	}
}

// This tiny RADOS substitute exposes only read/stat on synthetic objects. It
// runs the actual producer so matching payload hashes cannot hide a stale suffix.
const journalPayloadObjectControl = `import json, sys, types
objects = {k: bytes.fromhex(v) for k,v in json.loads(sys.argv[1]).items()}
script, end = sys.argv[2], sys.argv[3]
class Missing(Exception): pass
class Store:
    def __init__(self, **kw): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def open_ioctx(self, pool): return self
    def read(self, name, length, offset=0):
        if name not in objects: raise Missing(name)
        return objects[name][offset:offset+length]
    def stat(self, name):
        if name not in objects: raise Missing(name)
        return len(objects[name]), 0
rados = types.ModuleType("rados")
rados.Rados, rados.ObjectNotFound = Store, Missing
sys.modules["rados"] = rados
sys.argv = ["payload", "metadata", "20", end, "16"]
exec(script)
`

func TestJournalPayloadObserverDetectsStaleFinalObjectSuffix(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	for _, tc := range []struct {
		name      string
		end       int
		suffix    string
		next      bool
		wantError bool
	}{
		{name: "partial clean", end: 28},
		{name: "partial stale suffix", end: 28, suffix: "STALE", wantError: true},
		{name: "aligned clean", end: 32},
		{name: "aligned stale suffix", end: 32, suffix: "STALE", wantError: true},
		{name: "surviving purge object", end: 28, next: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte("PAYLOAD!MORE"[:tc.end-20])
			object := append(append([]byte("HEAD"), payload...), []byte(tc.suffix)...)
			objects := map[string]string{"200.00000001": fmt.Sprintf("%x", object)}
			if tc.next {
				objects["200.00000002"] = "01"
			}
			input, _ := json.Marshal(objects)
			out, err := exec.CommandContext(t.Context(), python, "-I", "-c", journalPayloadObjectControl, string(input), journalPayload, strconv.Itoa(tc.end)).CombinedOutput()
			if err != nil {
				t.Fatalf("producer: %v: %s", err, out)
			}
			var v journalPayloadView
			if err := json.Unmarshal(out, &v); err != nil {
				t.Fatal(err)
			}
			rec := jbRecord{Start: 20, PayloadEnd: uint64(tc.end), ObjectSize: 16, PayloadSHA256: jbSum(payload)}
			// In the stale cases these are exactly the old oracle's passing conditions.
			if v.SHA256 != rec.PayloadSHA256 {
				t.Fatal("payload changed in tail control")
			}
			if err := checkJournalPayload(v, rec); (err != nil) != tc.wantError {
				t.Fatalf("tail decision %v, want error=%t; %+v", err, tc.wantError, v)
			}
		})
	}
}
