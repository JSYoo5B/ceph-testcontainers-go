//go:build all || integration

package integration_test

import (
	"testing"

	"github.com/containerd/errdefs"
)

func requireNetworkAbsent(t testing.TB, name string, err error) {
	t.Helper()
	if !errdefs.IsNotFound(err) {
		t.Fatalf("bridge %s removal was not confirmed: %v", name, err)
	}
}

// Only ESRCH proves that the exact PID no longer exists. Permission errors
// and other failed observations must fail the cancellation scenario.
const requireProcessAbsent = `import os,sys
pid=int(sys.argv[1])
if pid<=0: raise ValueError('invalid process identity')
try:
 os.kill(pid,0)
except ProcessLookupError:
 pass
else:
 raise RuntimeError('process remains present')
`

const listSupervisedMarkers = `import os,sys
try:
 names=os.listdir(sys.argv[1])
except FileNotFoundError:
 names=[]
for name in sorted(names): print(name)
`

// An empty or unreadable stat is not an absent process.
const readProcessStat = `import sys
try:
 with open('/proc/'+str(int(sys.argv[1]))+'/stat') as source:
  data=source.read()
except FileNotFoundError:
 print('absent')
else:
 if not data.strip(): raise RuntimeError('empty process stat')
 print(data)
`

// Enumerate the complete directory before filtering the lease's names.
// A removed parent is valid absence; access failures are not empty listings.
const cacheLeaseFiles = `import os,sys
socket=sys.argv[1]
parent,base=os.path.dirname(socket),os.path.basename(socket)
try:
 names=os.listdir(parent)
except FileNotFoundError:
 names=[]
selected=[base] if base in names else []
for suffix in ('.asok','.pid'):
 selected.extend(sorted(name for name in names if name.startswith('immutable-object-cache-') and name.endswith(suffix)))
for name in selected:
 if len(sys.argv)>2 and sys.argv[2]=='pid':
  if name.endswith('.pid'):
   with open(os.path.join(parent,name)) as source: print(source.read())
 else:
  print(name)
`
