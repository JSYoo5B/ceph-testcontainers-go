package blockio

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// runProbe runs ProbeScript with PATH set to a directory of the given
// executables, each a shell script with the given body.
func runProbe(t *testing.T, tools map[string]string) Probe {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not available")
	}
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh is not available")
	}
	dir := t.TempDir()
	for name, body := range tools {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!"+shell+"\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(python, "-I", "-c", ProbeScript)
	cmd.Env = []string{"PATH=" + dir}
	raw, err := cmd.Output()
	if err != nil {
		t.Fatalf("probe script: %v", err)
	}
	probe, err := ParseProbe(raw)
	if err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	return probe
}

func TestProbeScriptReportsMissingToolsAndSkipsHostGates(t *testing.T) {
	probe := runProbe(t, map[string]string{"sh": "exit 0", "sleep": "exit 0", "losetup": "exit 0"})
	tools := probe.Gates[0]
	if tools.State != "unavailable" || tools.Kind != "image" || !slices.Equal(tools.Missing, []string{"dmsetup", "blockdev"}) {
		t.Fatalf("tools gate: %+v", tools)
	}
	for _, gate := range probe.Gates[1:] {
		if gate.State != "not-run" {
			t.Fatalf("gate after a missing tool ran: %+v", gate)
		}
	}
	if len(probe.Targets) != 0 {
		t.Fatalf("targets without a probe: %v", probe.Targets)
	}
}

func TestProbeScriptReportsUnusableToolAsImageCapability(t *testing.T) {
	probe := runProbe(t, map[string]string{"sh": "exit 0", "sleep": "exit 0", "dmsetup": "exit 0", "blockdev": "exit 0", "losetup": "echo broken >&2; exit 3"})
	if tools := probe.Gates[0]; tools.State != "unavailable" || tools.Kind != "image" || !strings.Contains(tools.Error, "losetup exited 3") {
		t.Fatalf("tools gate: %+v", tools)
	}
}

// With every tool present, the host gates run on the test host. Whatever
// it registers, each gate is observed, conclusively unavailable or unknown,
// and a gate after one that is not observed is not run.
func TestProbeScriptRunsHostGatesInOrder(t *testing.T) {
	ok := "exit 0"
	probe := runProbe(t, map[string]string{"sh": ok, "sleep": ok, "losetup": ok, "blockdev": ok, "dmsetup": `[ "$1" = targets ] && printf 'linear v1.4.0\nerror v1.6.0\n'; exit 0`})
	if probe.Gates[0].State != "observed" {
		t.Fatalf("tools gate: %+v", probe.Gates[0])
	}
	stopped := false
	for _, gate := range probe.Gates[1:] {
		if stopped && gate.State != "not-run" {
			t.Fatalf("gate after a failed gate ran: %+v", probe.Gates)
		}
		stopped = stopped || gate.State != "observed"
	}
	if !stopped && !slices.Equal(probe.Targets, []string{"linear", "error"}) {
		t.Fatalf("targets: %v", probe.Targets)
	}
}

func TestParseProbeRefusesIncompleteOutput(t *testing.T) {
	gates := func(states ...string) string {
		var parts []string
		for i, state := range states {
			kind := ""
			if state == "unavailable" {
				kind = "environment"
			}
			parts = append(parts, `{"key":"`+string(ProbeGates[i])+`","state":"`+state+`","kind":"`+kind+`"}`)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	for name, raw := range map[string]string{
		"not json":             `targets: linear`,
		"missing gate":         `{"gates":` + gates("observed", "observed", "observed") + `,"targets":[]}`,
		"unknown state":        `{"gates":` + gates("observed", "observed", "observed", "maybe") + `,"targets":[]}`,
		"reordered":            `{"gates":[{"key":"drivers","state":"observed"},{"key":"tools","state":"observed"},{"key":"control","state":"observed"},{"key":"targets","state":"observed"}],"targets":[]}`,
		"unavailable no kind":  `{"gates":[{"key":"tools","state":"unavailable"},{"key":"drivers","state":"not-run"},{"key":"control","state":"not-run"},{"key":"targets","state":"not-run"}],"targets":[]}`,
		"targets not observed": `{"gates":` + gates("observed", "observed", "observed", "unknown") + `,"targets":["linear"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseProbe([]byte(raw)); err == nil {
				t.Fatal("incomplete probe output was accepted")
			}
		})
	}
	if _, err := ParseProbe([]byte(`{"gates":` + gates("observed", "observed", "observed", "observed") + `,"targets":["linear"]}`)); err != nil {
		t.Fatal(err)
	}
}
