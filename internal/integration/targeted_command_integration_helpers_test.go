//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
)

// targetedReply is one command sent through librados to one OSD or PG: the
// signed return code librados reports, the status string, and the reply
// buffer's length, SHA-256 and, when it is JSON, its decoded value.
type targetedReply struct {
	Target string          `json:"target"`
	Ret    int             `json:"ret"`
	Status string          `json:"status"`
	Bytes  int             `json:"bytes"`
	SHA256 string          `json:"sha256"`
	JSON   json.RawMessage `json:"json"`
}

// targetedCLI is one "ceph tell" run: its exit status and the first line of
// its standard error.
type targetedCLI struct {
	Argv   []string `json:"argv"`
	Exit   int      `json:"exit"`
	Stderr string   `json:"stderr"`
}

// osdStatusIdentity is the identity part of an OSD's "status" reply.
type osdStatusIdentity struct {
	ClusterFSID string `json:"cluster_fsid"`
	OSDFSID     string `json:"osd_fsid"`
	WhoAmI      int    `json:"whoami"`
	State       string `json:"state"`
	NewestMap   int    `json:"newest_map"`
}

// parseOSDStatusIdentity reads an OSD's "status" reply. The epochs are JSON
// numbers or, on some releases, strings.
func parseOSDStatusIdentity(data []byte) (osdStatusIdentity, error) {
	var raw struct {
		ClusterFSID string      `json:"cluster_fsid"`
		OSDFSID     string      `json:"osd_fsid"`
		WhoAmI      int         `json:"whoami"`
		State       string      `json:"state"`
		NewestMap   json.Number `json:"newest_map"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return osdStatusIdentity{}, err
	}
	newest, err := raw.NewestMap.Int64()
	if err != nil {
		return osdStatusIdentity{}, fmt.Errorf("newest_map %q: %w", raw.NewestMap, err)
	}
	return osdStatusIdentity{ClusterFSID: raw.ClusterFSID, OSDFSID: raw.OSDFSID, WhoAmI: raw.WhoAmI, State: raw.State,
		NewestMap: int(newest)}, nil
}

// pgQueryIdentity is the identity part of a PG's "query" reply.
type pgQueryIdentity struct {
	PGID   string `json:"pgid"`
	Epoch  int    `json:"epoch"`
	Up     []int  `json:"up"`
	Acting []int  `json:"acting"`
	State  string `json:"state"`
}

func parsePGQueryIdentity(data []byte) (pgQueryIdentity, error) {
	var raw struct {
		Epoch  int    `json:"epoch"`
		State  string `json:"state"`
		Up     []int  `json:"up"`
		Acting []int  `json:"acting"`
		Info   struct {
			PGID string `json:"pgid"`
		} `json:"info"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return pgQueryIdentity{}, err
	}
	if raw.Info.PGID == "" {
		return pgQueryIdentity{}, fmt.Errorf("pg query names no pgid: %.200s", data)
	}
	return pgQueryIdentity{PGID: raw.Info.PGID, Epoch: raw.Epoch, Up: raw.Up, Acting: raw.Acting, State: raw.State}, nil
}

// targetedCommandProbe sends commands to OSDs and PGs through librados as
// the identity in ceph.conf's keyring, each with its own timeout, and prints
// one JSON object of targetedReply values. Each request is
// "name=kind:target:prefix", kind being osd or pg; a target may name an OSD
// or a PG that does not exist. It shuts its client down.
const targetedCommandProbe = `import hashlib,json,sys,rados
keyring,name,timeout=sys.argv[1],sys.argv[2],int(sys.argv[3])
client=rados.Rados(conffile='/etc/ceph/ceph.conf',name=name,conf={'keyring':keyring,'rados_osd_op_timeout':str(timeout),'rados_mon_op_timeout':'30'})
client.connect(timeout=30)
try:
    result={}
    for request in sys.argv[4:]:
        label,spec=request.split('=',1)
        kind,target,prefix=spec.split(':',2)
        if kind=='osd':
            command=json.dumps({'prefix':prefix,'format':'json'})
            ret,out,status=client.osd_command(int(target),command,b'',timeout=timeout)
        else:
            # The PG's OSD reads the target from the command as well.
            command=json.dumps({'prefix':prefix,'pgid':target,'format':'json'})
            ret,out,status=client.pg_command(target,command,b'',timeout=timeout)
        try:
            decoded=json.loads(out) if out else None
        except ValueError:
            decoded=None
        result[label]={'target':'%s.%s'%(kind,target),'ret':ret,'status':status[:300],'bytes':len(out),'sha256':hashlib.sha256(out).hexdigest(),'json':decoded}
    print(json.dumps(result))
finally:
    client.shutdown()
`

// targetedCLIProbe runs each argv (a JSON list) with the ceph CLI and
// prints the exit status and first standard-error line of each.
const targetedCLIProbe = `import json,subprocess,sys
result=[]
for argv in json.loads(sys.argv[1]):
    r=subprocess.run(argv,capture_output=True,text=True,timeout=120)
    lines=[l for l in r.stderr.splitlines() if l.strip()]
    result.append({'argv':argv,'exit':r.returncode,'stderr':lines[-1][:300] if lines else ''})
print(json.dumps(result))
`
