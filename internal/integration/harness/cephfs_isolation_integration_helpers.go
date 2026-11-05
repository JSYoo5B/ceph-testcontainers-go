//go:build all || integration

package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// The two filesystems of the CephFS multi-filesystem scenarios.
const (
	CephFSIsolationFSA = "iso-a"
	CephFSIsolationFSB = "iso-b"
)

// CephFSIsolationJSON runs a Python probe in client and decodes the last line that
// is a JSON object. libcephfs writes a refused mount to stderr, which the exec
// output interleaves.
func CephFSIsolationJSON(t *testing.T, ctx context.Context, client testcontainers.Container, script string, args ...string) map[string]any {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	argv := append([]string{"python3", "-c", script}, args...)
	code, reader, err := client.Exec(execCtx, argv, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("native probe %v: exit=%d error=%v output=%s", args, code, err, out)
	}
	var last []byte
	for _, line := range bytes.Split(out, []byte("\n")) {
		if line = bytes.TrimSpace(line); bytes.HasPrefix(line, []byte("{")) {
			last = line
		}
	}
	result := map[string]any{}
	if err := json.Unmarshal(last, &result); err != nil {
		t.Fatalf("probe output %s: %v", out, err)
	}
	t.Logf("native probe: %s", last)
	return result
}

// CephFSIsolationPreamble opens libcephfs handles for the admin or for one
// principal, and names an errno the way the probes report it.
const CephFSIsolationPreamble = `import cephfs, errno, json, os, sys, threading
deadline = threading.Timer(90, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
def handle(user=None, keyring=None):
    fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id=user) if user else cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf')
    if keyring:
        fs.conf_set('keyring', keyring)
    fs.conf_set('client_mount_timeout', '20')
    return fs
def code(error):
    return errno.errorcode.get(abs(error.args[0]), str(error.args[0]))
def attempt(operation):
    try:
        operation()
        return "ok"
    except cephfs.Error as error:
        return code(error)
def read(fs, path):
    try:
        fd = fs.open(path.encode(), 'r')
    except cephfs.Error as error:
        return code(error)
    try:
        return fs.read(fd, 0, 64).decode()
    finally:
        fs.close(fd)
def write(fs, path, data):
    fd = fs.open(path.encode(), 'w', 0o644)
    fs.write(fd, data.encode(), 0)
    fs.fsync(fd, 0)
    fs.close(fd)
`

// CephFSIsolationSeed gives each filesystem a world-writable /shared with a known
// seed file, so POSIX permissions never refuse a create.
const CephFSIsolationSeed = CephFSIsolationPreamble + `
for name in sys.argv[1:]:
    fs = handle()
    fs.mount(filesystem_name=name.encode())
    fs.mkdir(b'/shared', 0o777)
    fs.chmod(b'/shared', 0o777)
    write(fs, '/shared/seed', 'seed-of-' + name)
    fs.unmount()
    fs.shutdown()
print(json.dumps({"seeded": sys.argv[1:]}))
`
