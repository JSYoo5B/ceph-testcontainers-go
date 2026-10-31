//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSRealmAdmin is the independent administrator: a mount of its own, in
// another container than the actor. Arguments: mode, filesystem, directory,
// snapshot name, file name. Mode "snap" makes the directory snapshot, "read"
// prints the file's bytes from HEAD and from the snapshot (a failed read
// prints its errno), and "rmsnap" removes the snapshot, where an absent one
// counts as removed.
const cephFSRealmAdmin = `
import cephfs, json, os, sys
mode, filesystem, directory, snap, leaf = sys.argv[1:6]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=filesystem.encode())
def slurp(path):
    try:
        stat = fs.stat(path.encode())
        fd = fs.open(path.encode(), os.O_RDONLY, 0)
        data = fs.read(fd, 0, stat.st_size + 1)
        fs.close(fd)
        return {"size": stat.st_size, "data": data.hex()}
    except cephfs.Error as error:
        return {"errno": getattr(error, "errno", None) or -1}
try:
    snapdir = directory + "/.snap/" + snap
    out = {"client": fs.get_instance_id()}
    if mode == "snap":
        fs.mkdir(snapdir.encode(), 0o755)
        out["dir_inode"] = fs.stat(directory.encode()).st_ino
    elif mode == "read":
        out["head"] = slurp(directory + "/" + leaf)
        out["snap"] = slurp(snapdir + "/" + leaf)
    elif mode == "rmsnap":
        try:
            fs.rmdir(snapdir.encode())
            out["removed"] = True
        except cephfs.ObjectNotFound:
            out["removed"] = False
    print(json.dumps(out))
finally:
    fs.unmount()
    fs.shutdown()
`

type cephFSRealmAdminRead struct {
	Size  int64  `json:"size"`
	Data  string `json:"data"`
	Errno int    `json:"errno"`
}

type cephFSRealmAdminOut struct {
	Client   uint64                `json:"client"`
	DirInode uint64                `json:"dir_inode"`
	Removed  bool                  `json:"removed"`
	Head     *cephFSRealmAdminRead `json:"head"`
	Snap     *cephFSRealmAdminRead `json:"snap"`
}

func runCephFSRealmAdmin(ctx context.Context, cluster *ceph.Container, toolkit testcontainers.Container, mode, filesystem, directory, snap, leaf string) (cephFSRealmAdminOut, error) {
	runCtx, stop := context.WithTimeout(ctx, 2*time.Minute)
	defer stop()
	res, err := cluster.RunNativeCommand(runCtx, toolkit, ceph.NativeCommand{
		Argv: []string{"python3", "-c", cephFSRealmAdmin, mode, filesystem, directory, snap, leaf}, Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "CephFS snapshot administrator",
	})
	if err != nil || res.ExitCode != 0 || !res.Complete() {
		return cephFSRealmAdminOut{}, fmt.Errorf("administrator %s: exit=%d err=%v stderr=%.1024s", mode, res.ExitCode, err, res.Stderr)
	}
	var out cephFSRealmAdminOut
	if err := json.Unmarshal([]byte(harness.LastJSONLine(string(res.Stdout))), &out); err != nil {
		return cephFSRealmAdminOut{}, fmt.Errorf("administrator %s output: %v: %.512s", mode, err, res.Stdout)
	}
	return out, nil
}

// queryClientDumpCache returns one complete dump_cache of the client behind
// asok, which libcephfs prints from a single locked pass over its inode map.
func queryClientDumpCache(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok string) ([]byte, error) {
	queryCtx, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	res, err := cluster.RunNativeCommand(queryCtx, client, ceph.NativeCommand{
		Argv: []string{"ceph", "--admin-daemon", asok, "dump_cache"}, Timeout: 30 * time.Second,
		MaxStdout: 1 << 20, MaxStderr: 16 << 10, Label: "client dump_cache",
	})
	if err != nil || res.ExitCode != 0 || !res.Complete() {
		return nil, fmt.Errorf("client dump_cache: exit=%d err=%v stderr=%.512s", res.ExitCode, err, res.Stderr)
	}
	return res.Stdout, nil
}
