package blockio

import (
	"encoding/json"
	"fmt"
	"slices"
)

// ProbeSource defines the Python functions that check the block I/O
// prerequisites of a helper container: probe_tools(command) checks the image
// tools and probe_host(command) checks the device-mapper and loop drivers,
// their control devices and the registered targets. command(*argv) runs a
// program and raises on a nonzero exit. The keeper's probe and the preflight
// helper share them, so both read the same prerequisites the same way.
//
// Each gate is a dict with key, state (observed, unavailable, unknown or
// not-run), kind (image or environment for a gate that is not observed),
// error and missing. A gate after one that is not observed is not run. A
// failed dmsetup targets is unknown: it cannot prove that a target is absent.
// The drivers are checked before dmsetup runs, so dmsetup cannot try to load
// dm_mod.
const ProbeSource = `
import os as _os, shutil as _shutil, stat as _stat
def _gate(key, state, kind="", error="", missing=None):
    return {"key": key, "state": state, "kind": kind, "error": error, "missing": missing or []}
def probe_tools(command):
    missing=[p for p in ("sh","sleep","dmsetup","losetup","blockdev") if _shutil.which(p) is None]
    if missing: return [_gate("tools","unavailable","image","missing executables: "+", ".join(missing),missing)]
    try:
        command("losetup","--version");command("blockdev","--version");command("dmsetup","help")
    except Exception as e: return [_gate("tools","unavailable","image",str(e))]
    return [_gate("tools","observed")]
def probe_host(command):
    gates=[]
    def rest(*keys):
        return gates+[_gate(k,"not-run") for k in keys], []
    if not _os.path.exists("/sys/class/misc/device-mapper") or not _os.path.exists("/sys/class/misc/loop-control"):
        gates.append(_gate("drivers","unavailable","environment","device-mapper or loop driver is not registered"))
        return rest("control","targets")
    gates.append(_gate("drivers","observed"))
    if not all(_os.path.exists(p) and _stat.S_ISCHR(_os.stat(p).st_mode) and _os.access(p,_os.R_OK|_os.W_OK) for p in ("/dev/mapper/control","/dev/loop-control")):
        gates.append(_gate("control","unavailable","environment","host control devices are not accessible"))
        return rest("targets")
    gates.append(_gate("control","observed"))
    try: targets=[line.split()[0] for line in command("dmsetup","targets").splitlines() if line.strip()]
    except Exception as e:
        gates.append(_gate("targets","unknown","environment",str(e)))
        return gates, []
    gates.append(_gate("targets","observed"))
    return gates, targets
`

// ProbeScript runs ProbeSource in a helper container and prints one JSON
// object: the gates in order and the registered targets.
const ProbeScript = ProbeSource + `
import json as _json, subprocess as _subprocess
def _command(*args):
    p=_subprocess.run(args,capture_output=True,text=True,timeout=20)
    if p.returncode: raise RuntimeError("%s exited %d: %s"%(args[0],p.returncode,p.stderr[-400:]))
    return p.stdout.strip()
_gates=probe_tools(_command);_targets=[]
if _gates[0]["state"]=="observed":
    _host,_targets=probe_host(_command);_gates+=_host
else: _gates+=[_gate(k,"not-run") for k in ("drivers","control","targets")]
print(_json.dumps({"gates":_gates,"targets":_targets}),flush=True)
`

// ProbeGates are the gate keys of a probe, in order.
var ProbeGates = []string{"tools", "drivers", "control", "targets"}

// Gate is one gate of a probe.
type Gate struct {
	Key     string   `json:"key"`
	State   string   `json:"state"`
	Kind    string   `json:"kind"`
	Error   string   `json:"error"`
	Missing []string `json:"missing"`
}

// Probe is the parsed output of ProbeScript.
type Probe struct {
	Gates   []Gate   `json:"gates"`
	Targets []string `json:"targets"`
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
		if !slices.Contains([]string{"observed", "unavailable", "unknown", "not-run"}, gate.State) {
			return Probe{}, fmt.Errorf("probe gate %s has unknown state %q", gate.Key, gate.State)
		}
		if gate.State == "unavailable" && gate.Kind != "image" && gate.Kind != "environment" {
			return Probe{}, fmt.Errorf("probe gate %s is unavailable without a kind", gate.Key)
		}
	}
	if probe.Gates[len(probe.Gates)-1].State != "observed" && len(probe.Targets) != 0 {
		return Probe{}, fmt.Errorf("probe output lists targets without observing them")
	}
	return probe, nil
}
