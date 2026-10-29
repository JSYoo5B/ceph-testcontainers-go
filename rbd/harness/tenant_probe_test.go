package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// probeStubs model the rados and rbd bindings: one pool with the rbd
// application whose namespace_list behaves as STUB_NAMESPACES says.
const probeStubRados = `
class Error(Exception): pass
class ObjectNotFound(Error): pass
class TimedOut(Error): pass
class _IoCtx:
    def application_list(self): return ["rbd"]
    def close(self): pass
class Rados:
    def __init__(self, **kw): self.down = False
    def connect(self, timeout=0): pass
    def list_pools(self): return ["rbd-pool"]
    def open_ioctx(self, pool): return _IoCtx()
    def shutdown(self):
        open(__import__("os").environ["STUB_SHUTDOWN"], "w").write("1")
`

const probeStubRBD = `
import os, rados
class Error(Exception): pass
class ObjectNotFound(Error): pass
class RBD:
    def namespace_list(self, ioctx):
        mode = os.environ["STUB_NAMESPACES"]
        if mode == "timeout": raise rados.TimedOut("timed out")
        if mode == "rbd-error": raise Error("transient")
        if mode == "missing": raise ObjectNotFound("no rbd_namespace")
        return [n for n in mode.split(",") if n]
`

func runProbeScript(t *testing.T, mode string) (string, error) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	dir := t.TempDir()
	for name, body := range map[string]string{"rados.py": probeStubRados, "rbd.py": probeStubRBD} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	shutdown := filepath.Join(dir, "shutdown")
	cmd := exec.CommandContext(t.Context(), python, "-c", tenantProbeScript)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir, "STUB_NAMESPACES="+mode, "STUB_SHUTDOWN="+shutdown)
	out, err := cmd.Output()
	if _, statErr := os.Stat(shutdown); statErr != nil {
		t.Errorf("mode %s: the script did not shut the cluster handle down", mode)
	}
	return strings.TrimSpace(string(out)), err
}

func TestTenantProbeScriptReportsNamespaces(t *testing.T) {
	for mode, want := range map[string]string{
		"t1,t2": `{"rbd-pool": ["t1", "t2"]}`,
		"":      `{"rbd-pool": []}`,
	} {
		out, err := runProbeScript(t, mode)
		if err != nil || out != want {
			t.Errorf("mode %q: out=%s err=%v, want %s", mode, out, err, want)
		}
	}
}

// A namespace list that could not be read must fail the probe rather than
// read as an empty pool, or the baseline and release checks would accept it.
func TestTenantProbeScriptFailsWhenNamespacesCannotBeListed(t *testing.T) {
	for _, mode := range []string{"timeout", "rbd-error", "missing"} {
		out, err := runProbeScript(t, mode)
		if err == nil {
			t.Errorf("mode %s: the probe succeeded with %s", mode, out)
		}
	}
}
