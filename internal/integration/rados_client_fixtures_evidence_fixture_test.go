//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"os/exec"
	"strings"
	"testing"
)

func TestRADOSFixtureComparisonRequiresMismatchAndSuccessfulMatch(t *testing.T) {
	start := strings.Index(radosClientFixtureProbe, "    rejected=False")
	end := strings.Index(radosClientFixtureProbe[start:], "    with rados.WriteOpCtx() as op:\n        op.execute") + start
	if start < 0 || end < start {
		t.Fatal("comparison probe not found")
	}
	comparison := radosClientFixtureProbe[start:end]
	for _, outcome := range []string{"mismatch", "permission", "transport", "accepted"} {
		t.Run(outcome, func(t *testing.T) {
			preamble := `import types,sys
class Error(Exception):
 def __init__(self,errno): self.errno=errno
class Op:
 def __enter__(self): return self
 def __exit__(self,*args): pass
 def cmpext(self,value,offset): self.value=value
 def write_full(self,value): pass
 def set_xattr(self,*args): pass
rados=types.SimpleNamespace(Error=Error,MAX_ERRNO=4095,WriteOpCtx=Op)
payload=b'atomic-client-fixture'
class IO:
 def read(self,*args): return payload
 def get_xattr(self,*args): return b'original'
 def operate_write_op(self,op,*args):
  if op.value==payload:
   print('match completed')
   return
  value={'mismatch':4095,'permission':13,'transport':5,'accepted':0}[sys.argv[1]]
  if value: raise Error(value)
io=IO()
if True:
`
			output, err := exec.Command("python3", "-c", preamble+comparison, outcome).CombinedOutput()
			if outcome == "mismatch" {
				if err != nil || strings.TrimSpace(string(output)) != "match completed" {
					t.Fatalf("valid comparison controls failed: %q %v", output, err)
				}
			} else if err == nil {
				t.Fatalf("unrelated result %s satisfied mismatch evidence", outcome)
			}
		})
	}
}
