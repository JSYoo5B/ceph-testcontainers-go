package blockio

import (
	"os/exec"
	"testing"
)

func TestParseRefusesIncompleteControlEvidence(t *testing.T) {
	for _, sample := range []struct{ action, raw string }{
		{"observe", `{"action":"observe","name":"owned"}`},
		{"observe", `{"action":"observe","name":"owned","exists":null}`},
		{"observe", `{"action":"observe","name":"owned","exists":false,"error":"permission denied"}`},
		{"observe", `{"action":"observe","name":"owned","exists":true,"state":"ACTIVE","uuid":"owned","open":0,"event":1,"major":253,"minor":1,"live":"0 8 error","in_flight":[0,0]}`},
		{"observe", `{"action":"observe","name":"owned","exists":true,"state":"ACTIVE","uuid":"owned","open":0,"event":1,"major":253,"minor":1,"live":"0 8 error","inactive":null,"in_flight":[0,null]}`},
		{"settled", `{"action":"settled","name":"owned","id":"step","done":false}`},
		{"settled", `{"action":"settled","name":"owned","id":"step","done":false,"started":null}`},
		{"settled", `{"action":"settled","name":"owned","id":"step","done":true,"error":""}`},
		{"settled", `{"action":"settled","name":"owned","id":"step","done":true,"rc":null,"error":""}`},
		{"loopid", `{"action":"loopid","name":"owned","file":"/tc-blk/owned","inode":1}`},
		{"create", `{"action":"create","name":"owned","created":null,"error":""}`},
	} {
		t.Run(sample.action+sample.raw, func(t *testing.T) {
			if _, err := Parse([]byte(sample.raw), sample.action); err == nil {
				t.Fatal("incomplete evidence was accepted")
			}
		})
	}
}

func TestParseKeepsUnknownStepAndExplicitAbsenceDistinct(t *testing.T) {
	for _, tc := range []struct {
		value   string
		started bool
	}{{"false", false}, {"true", true}} {
		pending, err := Parse([]byte(`{"action":"settled","name":"owned","id":"step","done":false,"started":`+tc.value+`}`), "settled")
		if err != nil || pending.Done || pending.Started != tc.started {
			t.Fatalf("pending: %+v %v", pending, err)
		}
	}
	gone, err := Parse([]byte(`{"action":"observe","name":"owned","exists":false}`), "observe")
	if err != nil || gone.Exists {
		t.Fatalf("absence: %+v %v", gone, err)
	}
	live, err := Parse([]byte(`{"action":"observe","name":"owned","exists":true,"state":"ACTIVE","uuid":"owned","open":0,"event":1,"major":253,"minor":1,"live":"0 8 error","inactive":null,"in_flight":[0,0]}`), "observe")
	if err != nil || live.Live == nil || live.Inactive != nil {
		t.Fatalf("live: %+v %v", live, err)
	}
}

func TestKeeperScriptsCompile(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python 3 is required for keeper syntax validation")
	}
	for name, script := range map[string]string{"control": Script, "test-control": TestScript} {
		t.Run(name, func(t *testing.T) {
			if out, err := exec.Command(python, "-I", "-c", "import sys; compile(sys.argv[1], 'keeper', 'exec')", script).CombinedOutput(); err != nil {
				t.Fatalf("%v: %s", err, out)
			}
		})
	}
}

func TestKeeperStepLaunchReceipts(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python 3 is required for keeper receipt validation")
	}
	if out, err := exec.CommandContext(t.Context(), python, "-I", "testdata/step_launch.py", Script, t.TempDir()).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}
