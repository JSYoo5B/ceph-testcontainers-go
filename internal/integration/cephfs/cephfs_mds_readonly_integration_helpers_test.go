//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// cephFSReadOnlyProbeScript mounts a new libcephfs client and reads the
// seeded file. In phase seed it first writes the file; in every phase it then
// opens the file for writing, writes and fsyncs, and asserts the errno:
// EROFS (30) in phase readonly, none otherwise. It prints the phase, the
// write errno and whether the read was exact.
const cephFSReadOnlyProbeScript = `import cephfs, json, os, sys
f = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf')
f.mount(filesystem_name=sys.argv[1].encode())
mode = sys.argv[2]
path = '/readonly-fault'
payload = b'readonly-retained-bytes'
if mode == 'seed':
    fd = f.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    f.write(fd, payload, 0)
    f.fsync(fd, False)
    f.close(fd)
fd = f.open(path, os.O_RDONLY)
assert f.read(fd, 0, len(payload)) == payload
f.close(fd)
errno = 0
try:
    fd = f.open(path, os.O_WRONLY)
    f.write(fd, payload, 0)
    f.fsync(fd, False)
    f.close(fd)
except cephfs.Error as e:
    errno = abs(e.args[0])
assert errno == (30 if mode == 'readonly' else 0), (mode, errno)
f.shutdown()
print(json.dumps(dict(phase=mode, write_errno=errno, read_exact=True)))
`

// cephFSReadOnlyProbe is the line the probe script prints.
type cephFSReadOnlyProbe struct {
	Phase      string `json:"phase"`
	WriteErrno int    `json:"write_errno"`
	ReadExact  bool   `json:"read_exact"`
}

// testCephFSMDSReadOnlyFault starts one owned active MDS and one owned
// standby and seeds a file through a fresh libcephfs client, which also
// writes it again: the control that writes succeed before the fault.
//
// MDS.ForceReadOnly is applied to the owned daemon that MDSStatus reports on
// rank 0.
// The independent oracles are a new client mount, which must read the
// seeded bytes exactly and get EROFS on its write, the MON health, read by
// the test itself, which must name that daemon and rank under
// MDS_READ_ONLY, and MDSStatus, which must still show the same GID on rank
// 0: the server is read-only, not gone. WaitReplaced must not succeed while
// that process lives.
//
// Recovery is explicit: the active MDS is SIGKILLed and its GID failed, so
// the owned standby takes rank 0. WaitReplaced must return the standby's
// GID, MDSStatus must agree, and a newly mounted client must read the same
// bytes and write successfully. The health must drop MDS_READ_ONLY.
//
// It proves the fault on one process and its end by replacement on a
// single-rank filesystem. It does not cover a client that held a session
// with the read-only process, a real metadata-pool failure or other ranks.
func testCephFSMDSReadOnlyFault(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := startCephFSStandbyFailover(t, ctx)
	probe := func(phase string) cephFSReadOnlyProbe {
		t.Helper()
		out := harness.ExecOutput(t, ctx, m.Client, "python3", "-c", cephFSReadOnlyProbeScript, m.FS.FilesystemName, phase)
		lines := strings.Split(strings.TrimSpace(out), "\n")
		var result cephFSReadOnlyProbe
		if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil || result.Phase != phase || !result.ReadExact {
			t.Fatalf("probe %s printed %q: %v", phase, out, err)
		}
		return result
	}
	readOnlyHealth := func(daemon cephfs.MDSStatus) bool {
		t.Helper()
		health, err := harness.ObserveCluster(m.Cluster).HealthDetails(ctx)
		if err != nil {
			t.Fatal(err)
		}
		prefix := "mds." + daemon.Name + "(mds." + strconv.Itoa(daemon.Rank) + "):"
		for _, detail := range health.Checks["MDS_READ_ONLY"].Details {
			if strings.HasPrefix(detail, prefix) {
				return true
			}
		}
		return false
	}

	seeded := probe("seed")
	if seeded.WriteErrno != 0 || readOnlyHealth(m.MDS) {
		t.Fatalf("before the fault: write errno %d, read-only health %t", seeded.WriteErrno, readOnlyHealth(m.MDS))
	}
	var daemon *cephfs.MDS
	for _, member := range m.FS.MDSs() {
		if member.ID == m.MDS.Name {
			daemon = member
		}
	}
	if daemon == nil {
		t.Fatalf("active mds.%s has no owned daemon: %+v", m.MDS.Name, m.FS.MDSs())
	}
	faulted := m.MDS
	started := time.Now()
	fault, err := daemon.ForceReadOnly(ctx)
	if err != nil {
		t.Fatal(err)
	}
	applied := time.Since(started)
	t.Log(fault)
	if !readOnlyHealth(faulted) {
		t.Fatalf("the health does not report mds.%s read-only after ForceReadOnly returned", faulted.Name)
	}
	if readonly := probe("readonly"); readonly.WriteErrno != 30 {
		t.Fatalf("write on the read-only MDS: errno %d", readonly.WriteErrno)
	}
	status, err := m.FS.MDSStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if same, err := (expect.MDSs{Ranks: []expect.MDSRank{{Rank: 0, Name: faulted.Name, GID: faulted.GID}}, Owned: true, Exact: true}).Evaluate(*status); err != nil || !same.Matched {
		t.Fatalf("the read-only MDS left rank 0: %v, %v (%+v)", same, err, status)
	}
	early, cancelEarly := context.WithTimeout(ctx, 5*time.Second)
	holder, err := fault.WaitReplaced(early)
	cancelEarly()
	if err == nil {
		t.Fatalf("WaitReplaced succeeded while the read-only process holds rank 0: %+v", holder)
	}

	kill := time.Duration(0)
	if err := daemon.Stop(ctx, &kill); err != nil {
		t.Fatal("kill MDS:", err)
	}
	harness.MustCeph(t, ctx, m.Cluster, "mds", "fail", strconv.FormatUint(faulted.GID, 10))
	failed := time.Now()
	holder, err = fault.WaitReplaced(ctx)
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(failed)
	if holder.Name != m.standby.Name || holder.GID != m.standby.GID || holder.GID == faulted.GID || holder.Rank != 0 || !holder.Owned {
		t.Fatalf("WaitReplaced = %+v, want rank 0 on standby mds.%s (GID %d)", holder, m.standby.Name, m.standby.GID)
	}
	status, err = m.FS.MDSStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if moved, err := (expect.MDSs{Ranks: []expect.MDSRank{{Rank: 0, Name: holder.Name, GID: holder.GID}}, Owned: true, Exact: true}).Evaluate(*status); err != nil || !moved.Matched {
		t.Fatalf("MDSStatus disagrees with WaitReplaced: %v, %v (%+v)", moved, err, status)
	}
	if recovered := probe("recovered"); recovered.WriteErrno != 0 {
		t.Fatalf("write after the replacement: errno %d", recovered.WriteErrno)
	}
	seen, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(context.Context) (bool, bool, error) {
		cleared := !readOnlyHealth(faulted) && !readOnlyHealth(holder)
		return cleared, cleared, nil
	})
	if err != nil || !seen.Value {
		t.Fatalf("MDS_READ_ONLY stayed after the replacement: %v", err)
	}
	t.Logf("CEPHFS_MDS_READONLY %s: mds.%s GID %d read-only in %v -> fresh client read exact, write EROFS -> SIGKILL and mds fail -> mds.%s GID %d on rank 0 after %v -> new client read exact and wrote",
		m.Version, faulted.Name, faulted.GID, applied.Round(time.Millisecond), holder.Name, holder.GID, took.Round(time.Millisecond))
}
