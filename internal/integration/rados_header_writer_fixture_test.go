//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"os/exec"
	"strings"
	"testing"
)

// radosHeaderWriterHarness runs radosHeaderWriterScript against a stub cli
// that records each call and answers from the case's usage text and write
// outcome.
const radosHeaderWriterHarness = `import os,subprocess,sys,tempfile,types
usage,outcome=sys.argv[1],sys.argv[2]
writes=[]
def cli(*args,ok=True):
    if args[0]=='--help':
        return types.SimpleNamespace(returncode=1,stdout=usage.encode(),stderr=b'')
    writes.append([a if isinstance(a,str) else a.decode() for a in args])
    if outcome=='timeout': raise subprocess.TimeoutExpired('rados',60)
    code={'ok':0,'eio':5,'eperm':13}[outcome]
    assert not ok or code==0,('rados',code,b'failed')
    return types.SimpleNamespace(returncode=code,stdout=b'',stderr=b'failed')
` + radosHeaderWriterScript + `
try:
    via=set_header('omap',b'header')
    print('OK',via,len(writes),'--input-file' in writes[0])
except BaseException as e:
    print('FAIL',type(e).__name__,len(writes))
`

func TestRADOSHeaderWriterFallsBackOnlyOnAMissingOption(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	const tentacle = "   setomapheader <obj-name> <val | --input-file file>\n"
	const squid = "   setomapheader <obj-name> <val>   Set the value of the object map header of <obj-name>\n"
	for _, test := range []struct{ name, usage, outcome, want string }{
		{"supported", tentacle, "ok", "OK input-file 1 True"},
		{"unsupported option", squid, "ok", "OK argument 1 False"},
		// A failed or uncertain write is reported, never retried as a
		// second write through the argument form.
		{"I/O error", tentacle, "eio", "FAIL AssertionError 1"},
		{"permission denied", tentacle, "eperm", "FAIL AssertionError 1"},
		{"timeout", tentacle, "timeout", "FAIL TimeoutExpired 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			out, err := exec.CommandContext(t.Context(), python, "-c", radosHeaderWriterHarness, test.usage, test.outcome).CombinedOutput()
			if err != nil {
				t.Fatalf("harness: %v: %s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}
