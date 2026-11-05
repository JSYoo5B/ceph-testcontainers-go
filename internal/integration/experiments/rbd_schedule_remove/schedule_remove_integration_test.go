//go:build all || (integration && (!ci || (ci_topology && (!ci_batch || ci_batch_discovery_schedule))))

//ci: timeout=7m job-timeout=15 image-roles=essential,client

package rbd_schedule_remove_test

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestDiscoveryTrashScheduleBoundary records native acceptance/refusal of
// malformed schedule removal with empty and populated policies, for both
// rbd_support schedule handlers (trash purge and mirror snapshot), which share
// one removal implementation. A characterized old success is evidence, not an
// assertion that malformed input is valid or a support guarantee. With a
// schedule present every observed build refuses the removal and keeps the
// policy, so that refusal is asserted.
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
for prefix,base in (('',['rbd','trash','purge','schedule']),('mirror-',['rbd','mirror','snapshot','schedule'])):
 p=run(prefix+'empty-before',base+['ls','-R','--format','json'])
 assert p.returncode==0 and json.loads(p.stdout)==[]
 run(prefix+'empty-invalid-interval',base+['remove','dummy'])
 run(prefix+'empty-invalid-start',base+['remove','1d','dummy'])
 p=run(prefix+'empty-after',base+['ls','-R','--format','json']);assert p.returncode==0 and json.loads(p.stdout)==[]
 p=run(prefix+'add',base+['add','2d']);assert p.returncode==0
 try:
  before=run(prefix+'present-before',base+['ls','-R','--format','json']);assert before.returncode==0 and json.loads(before.stdout)!=[]
  for case,argv in (('present-invalid-interval',['remove','dummy']),('present-invalid-start',['remove','1d','dummy'])):
   p=run(prefix+case,base+argv);assert p.returncode!=0,prefix+case+' accepted malformed input with a schedule present'
  after=run(prefix+'present-after',base+['ls','-R','--format','json']);assert after.returncode==0 and json.loads(before.stdout)==json.loads(after.stdout)
 finally:
  p=run(prefix+'cleanup',base+['remove','2d']);assert p.returncode==0
 p=run(prefix+'restored',base+['ls','-R','--format','json']);assert p.returncode==0 and json.loads(p.stdout)==[]
`
	t.Log(harness.ExecOutput(t, ctx, client, "python3", "-c", script))
}
