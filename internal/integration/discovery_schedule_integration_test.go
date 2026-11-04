//go:build integration && ci_optional && ci_batch_discovery_schedule

//ci: timeout=7m job-timeout=15 image-roles=essential,client

package integration_test

import (
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
	"time"
)

// TestDiscoveryTrashScheduleBoundary records native acceptance/refusal with
// empty and populated policies. A characterized old success is evidence, not
// an assertion that malformed input is valid or a support guarantee.
func TestDiscoveryTrashScheduleBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.MustCeph(t, ctx, cluster, "mgr", "module", "enable", "rbd_support")
	script := `import subprocess,json

def run(label,args):
 p=subprocess.run(args,capture_output=True,text=True,timeout=30)
 print(json.dumps(dict(case=label,argv=args,code=p.returncode,out=p.stdout.strip(),err=p.stderr.strip())),flush=True)
 return p
run('version',['ceph','--version'])
base=['rbd','trash','purge','schedule']
p=run('empty-before',base+['ls','-R','--format','json'])
assert p.returncode==0 and json.loads(p.stdout)==[]
run('empty-invalid-interval',base+['remove','dummy'])
run('empty-invalid-start',base+['remove','1d','dummy'])
p=run('empty-after',base+['ls','-R','--format','json']);assert p.returncode==0 and json.loads(p.stdout)==[]
p=run('add',base+['add','2d']);assert p.returncode==0
try:
 before=run('present-before',base+['ls','-R','--format','json']);assert before.returncode==0
 run('present-invalid-interval',base+['remove','dummy'])
 run('present-invalid-start',base+['remove','1d','dummy'])
 after=run('present-after',base+['ls','-R','--format','json']);assert after.returncode==0 and json.loads(before.stdout)==json.loads(after.stdout)
finally:
 p=run('cleanup',base+['remove','2d']);assert p.returncode==0
p=run('restored',base+['ls','-R','--format','json']);assert p.returncode==0 and json.loads(p.stdout)==[]
`
	t.Log(harness.ExecOutput(t, ctx, client, "python3", "-c", script))
}
