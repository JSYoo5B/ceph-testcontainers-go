package blockio

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// GateKey names one gate of the probe.
type GateKey string

// The gates of a probe.
const (
	GateTools   GateKey = "tools"
	GateDrivers GateKey = "drivers"
	GateControl GateKey = "control"
	GateTargets GateKey = "targets"
)

// ProbeGates are the gate keys of a probe, in the order the probe checks
// them.
var ProbeGates = []GateKey{GateTools, GateDrivers, GateControl, GateTargets}

// GateState is the conclusion of one gate.
type GateState string

// The states of a gate. A gate after one that is not observed is not run.
const (
	GateObserved    GateState = "observed"
	GateUnavailable GateState = "unavailable"
	GateUnknown     GateState = "unknown"
	GateNotRun      GateState = "not-run"
)

// GateKind is the error category of an unavailable gate: a missing tool of
// the helper image, or a missing engine, kernel or privilege prerequisite.
type GateKind string

// The kinds of an unavailable gate.
const (
	GateImage       GateKind = "image"
	GateEnvironment GateKind = "environment"
)

// ProbeTools are the executables that the probe requires in a helper image.
var ProbeTools = []string{"sh", "sleep", "dmsetup", "losetup", "blockdev"}

// ProbeSource defines the Python functions that check the block I/O
// prerequisites of a helper container: probe_tools(command) checks the image
// tools and probe_host(command) checks the device-mapper and loop drivers,
// their control devices and the registered targets. command(*argv) runs a
// program and raises on a nonzero exit. The keeper's probe and the preflight
// helper share them, so both read the same prerequisites the same way.
//
// Each gate is a dict with key, state, kind (for a gate that is not
// observed), error and missing; the keys, states, kinds and tools are those
// of this package, bound to the Python names _GATE_TOOLS, _OBSERVED, _IMAGE,
// _TOOLS and so on. A failed dmsetup targets is unknown: it cannot prove
// that a target is absent. The drivers are checked before dmsetup runs, so
// dmsetup cannot try to load dm_mod.
var ProbeSource = probeNames() + `
def _gate(key, state, kind="", error="", missing=None):
    return {"key": key, "state": state, "kind": kind, "error": error, "missing": missing or []}
def probe_tools(command):
    missing=[p for p in _TOOLS if _shutil.which(p) is None]
    if missing: return [_gate(_GATE_TOOLS,_UNAVAILABLE,_IMAGE,"missing executables: "+", ".join(missing),missing)]
    try:
        command("losetup","--version");command("blockdev","--version");command("dmsetup","help")
    except Exception as e: return [_gate(_GATE_TOOLS,_UNAVAILABLE,_IMAGE,str(e))]
    return [_gate(_GATE_TOOLS,_OBSERVED)]
def probe_host(command):
    gates=[]
    def rest(*keys):
        return gates+[_gate(k,_NOT_RUN) for k in keys], []
    if not _os.path.exists("/sys/class/misc/device-mapper") or not _os.path.exists("/sys/class/misc/loop-control"):
        gates.append(_gate(_GATE_DRIVERS,_UNAVAILABLE,_ENVIRONMENT,"device-mapper or loop driver is not registered"))
        return rest(_GATE_CONTROL,_GATE_TARGETS)
    gates.append(_gate(_GATE_DRIVERS,_OBSERVED))
    if not all(_os.path.exists(p) and _stat.S_ISCHR(_os.stat(p).st_mode) and _os.access(p,_os.R_OK|_os.W_OK) for p in ("/dev/mapper/control","/dev/loop-control")):
        gates.append(_gate(_GATE_CONTROL,_UNAVAILABLE,_ENVIRONMENT,"host control devices are not accessible"))
        return rest(_GATE_TARGETS)
    gates.append(_gate(_GATE_CONTROL,_OBSERVED))
    try: targets=[line.split()[0] for line in command("dmsetup","targets").splitlines() if line.strip()]
    except Exception as e:
        gates.append(_gate(_GATE_TARGETS,_UNKNOWN,_ENVIRONMENT,str(e)))
        return gates, []
    gates.append(_gate(_GATE_TARGETS,_OBSERVED))
    return gates, targets
`

// ProbeScript runs ProbeSource in a helper container and prints one JSON
// object: the gates in order and the registered targets.
var ProbeScript = ProbeSource + `
import json as _json, subprocess as _subprocess
def _command(*args):
    p=_subprocess.run(args,capture_output=True,text=True,timeout=20)
    if p.returncode: raise RuntimeError("%s exited %d: %s"%(args[0],p.returncode,p.stderr[-400:]))
    return p.stdout.strip()
_gates=probe_tools(_command);_targets=[]
if _gates[0]["state"]==_OBSERVED:
    _host,_targets=probe_host(_command);_gates+=_host
else: _gates+=[_gate(k,_NOT_RUN) for k in (_GATE_DRIVERS,_GATE_CONTROL,_GATE_TARGETS)]
print(_json.dumps({"gates":_gates,"targets":_targets}),flush=True)
`

// probeNames binds the probe's Python names to this package's constants.
func probeNames() string {
	quote := func(values ...string) string {
		quoted := make([]string, len(values))
		for i, value := range values {
			quoted[i] = strconv.Quote(value)
		}
		return strings.Join(quoted, ", ")
	}
	return fmt.Sprintf(`
import os as _os, shutil as _shutil, stat as _stat
_TOOLS = (%s,)
_GATE_TOOLS, _GATE_DRIVERS, _GATE_CONTROL, _GATE_TARGETS = %s
_OBSERVED, _UNAVAILABLE, _UNKNOWN, _NOT_RUN = %s
_IMAGE, _ENVIRONMENT = %s
`, quote(ProbeTools...),
		quote(string(GateTools), string(GateDrivers), string(GateControl), string(GateTargets)),
		quote(string(GateObserved), string(GateUnavailable), string(GateUnknown), string(GateNotRun)),
		quote(string(GateImage), string(GateEnvironment)))
}

// Gate is one gate of a probe.
type Gate struct {
	Key     GateKey   `json:"key"`
	State   GateState `json:"state"`
	Kind    GateKind  `json:"kind"`
	Error   string    `json:"error"`
	Missing []string  `json:"missing"`
}

// Probe is the parsed output of ProbeScript.
type Probe struct {
	Gates   []Gate   `json:"gates"`
	Targets []string `json:"targets"`
}

// Gate returns the gate named key, or a zero Gate when the probe has none.
func (p Probe) Gate(key GateKey) Gate {
	for _, gate := range p.Gates {
		if gate.Key == key {
			return gate
		}
	}
	return Gate{}
}

// ParseProbe parses the output of ProbeScript. It refuses output that does
// not name every gate once, in order, with a known state, or that lists
// targets without an observed targets gate.
func ParseProbe(raw []byte) (Probe, error) {
	var probe Probe
	if err := json.Unmarshal(raw, &probe); err != nil {
		return Probe{}, fmt.Errorf("invalid probe output: %w", err)
	}
	if len(probe.Gates) != len(ProbeGates) {
		return Probe{}, fmt.Errorf("probe output has %d gates, want %d", len(probe.Gates), len(ProbeGates))
	}
	for i, gate := range probe.Gates {
		if gate.Key != ProbeGates[i] {
			return Probe{}, fmt.Errorf("probe gate %d is %q, want %q", i, gate.Key, ProbeGates[i])
		}
		if !slices.Contains([]GateState{GateObserved, GateUnavailable, GateUnknown, GateNotRun}, gate.State) {
			return Probe{}, fmt.Errorf("probe gate %s has unknown state %q", gate.Key, gate.State)
		}
		if gate.State == GateUnavailable && gate.Kind != GateImage && gate.Kind != GateEnvironment {
			return Probe{}, fmt.Errorf("probe gate %s is unavailable without a kind", gate.Key)
		}
	}
	if probe.Gate(GateTargets).State != GateObserved && len(probe.Targets) != 0 {
		return Probe{}, fmt.Errorf("probe output lists targets without observing them")
	}
	return probe, nil
}
