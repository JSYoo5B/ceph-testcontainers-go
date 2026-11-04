package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// readOnlyControl models the FSMap, the MDS processes and the health of
// "fixture" (FSMap ID 41, metadata pool 1, default data pool 2). active holds
// the up:active entries, standbys the standby list. force_readonly marks the
// GID read-only; health reports every read-only GID still registered, unless
// silentHealth is set. lostReply applies force_readonly and fails its reply;
// noCommand drops force_readonly from the command list, and status replaces
// the status reply of a GID. beforeDump runs
// before each fs dump with its sequence number.
type readOnlyControl struct {
	*clustertest.CannedCommandContainer
	fsID, metadataPool int64
	maxMDS             int
	active, standbys   []fsbase.MDSInfo
	readOnly           map[uint64]bool
	silentHealth       bool
	lostReply          bool
	noCommand          bool
	status             map[uint64]string
	dumps              int
	beforeDump         func(int)
}

func (ctr *readOnlyControl) daemon(gid uint64) (fsbase.MDSInfo, bool) {
	for _, info := range slices.Concat(ctr.active, ctr.standbys) {
		if info.GID == gid {
			return info, true
		}
	}
	return fsbase.MDSInfo{}, false
}

func (ctr *readOnlyControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	command := strings.Join(clustertest.ModuleArgs(args), " ")
	ctr.Fail, ctr.FailCode, ctr.FailReason = "", 0, ""
	if command == "fs dump --format json" {
		ctr.dumps++
		if ctr.beforeDump != nil {
			ctr.beforeDump(ctr.dumps)
		}
	}
	info := map[string]any{}
	for _, active := range ctr.active {
		info[fmt.Sprintf("gid_%d", active.GID)] = map[string]any{"gid": active.GID, "name": active.Name, "rank": active.Rank, "state": active.State, "join_fscid": -1}
	}
	var standbys []map[string]any
	for _, standby := range ctr.standbys {
		standbys = append(standbys, map[string]any{"gid": standby.GID, "name": standby.Name, "rank": -1, "state": "up:standby", "join_fscid": ctr.fsID})
	}
	dump, _ := json.Marshal(map[string]any{"epoch": 9, "standbys": standbys, "filesystems": []any{map[string]any{"id": ctr.fsID,
		"mdsmap": map[string]any{"epoch": 8, "fs_name": "fixture", "max_mds": ctr.maxMDS, "metadata_pool": ctr.metadataPool, "data_pools": []int64{2}, "info": info}}}})
	ctr.Output["fs dump --format json"] = string(dump)
	var details []map[string]string
	for _, active := range ctr.active {
		if ctr.readOnly[active.GID] && !ctr.silentHealth {
			details = append(details, map[string]string{"message": fmt.Sprintf("mds.%s(mds.%d): MDS in read-only mode", active.Name, active.Rank)})
		}
	}
	checks := map[string]any{}
	if len(details) > 0 {
		checks["MDS_READ_ONLY"] = map[string]any{"severity": "HEALTH_WARN", "detail": details}
	}
	health, _ := json.Marshal(map[string]any{"status": "HEALTH_WARN", "checks": checks})
	ctr.Output["health detail --format json"] = string(health)
	var gid uint64
	var verb string
	if _, err := fmt.Sscanf(command, "tell mds.%d %s", &gid, &verb); err == nil {
		daemon, ok := ctr.daemon(gid)
		switch {
		case !ok:
			ctr.Fail, ctr.FailCode, ctr.FailReason = command, 2, fmt.Sprintf("Error ENOENT: problem getting command descriptions from mds.%d", gid)
		case verb == "status" && ctr.status[gid] != "":
			ctr.Output[command] = ctr.status[gid]
		case verb == "status":
			ctr.Output[command] = fmt.Sprintf(`{"id":%d,"whoami":%d,"state":%q,"fs_name":"fixture","sysinfo":{"ceph_version_short":"20.2.4"}}`, gid, daemon.Rank, daemon.State)
		case verb == "get_command_descriptions":
			commands := `{"cmd000":{"sig":["status"],"help":""},"cmd001":{"sig":["flush",{"name":"journal"}],"help":""}`
			if !ctr.noCommand {
				commands += `,"cmd002":{"sig":["force_readonly"],"help":"Force MDS to read-only mode"}`
			}
			ctr.Output[command] = commands + "}"
		case verb == mdsReadOnlyCommand:
			ctr.readOnly[gid] = true
			if ctr.lostReply {
				ctr.Fail = command
			}
		}
	}
	return ctr.CannedCommandContainer.Exec(ctx, args, opts...)
}

// forced lists the force_readonly commands sent.
func (ctr *readOnlyControl) forced() []string {
	var commands []string
	for _, call := range ctr.Calls {
		if command := strings.Join(call, " "); strings.HasSuffix(command, " "+mdsReadOnlyCommand) {
			commands = append(commands, command)
		}
	}
	return commands
}

// takeover moves the rank to the standby with gid, as a failover does, and
// drops the faulted process from the map.
func (ctr *readOnlyControl) takeover(gid uint64) {
	index := slices.IndexFunc(ctr.standbys, func(info fsbase.MDSInfo) bool { return info.GID == gid })
	standby := ctr.standbys[index]
	ctr.standbys = slices.Delete(ctr.standbys, index, index+1)
	ctr.active = []fsbase.MDSInfo{{Name: standby.Name, GID: standby.GID, Rank: 0, State: "up:active"}}
}

var readOnlyTarget = MDSStatus{Name: "fixture-0", Rank: 0, GID: 4199, State: "up:active", Owned: true}

// readOnlyFixture is "fixture" with the owned active fixture-0 (GID 4199)
// and the owned standby fixture-1 (GID 4203). Their handles carry the
// creation identity a confirmed startup records, and their containers are
// registered as the cluster's MDS services.
func readOnlyFixture(t *testing.T) (*Filesystem, *readOnlyControl, *MDS, *MDS) {
	t.Helper()
	ctr := &readOnlyControl{CannedCommandContainer: &clustertest.CannedCommandContainer{Output: map[string]string{}}, fsID: 41, metadataPool: 1, maxMDS: 1,
		active:   []fsbase.MDSInfo{{Name: "fixture-0", GID: 4199, Rank: 0, State: "up:active"}},
		standbys: []fsbase.MDSInfo{{Name: "fixture-1", GID: 4203, Rank: -1, State: "up:standby"}},
		readOnly: map[uint64]bool{}}
	cluster := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: 5 * time.Second, OSDs: 1})
	fs := newFilesystem(cluster, Config{Name: "fixture", MetadataPool: ceph.PoolConfig{Name: "metadata"}, DataPool: ceph.PoolConfig{Name: "data"}, ActiveMDS: 1, StandbyMDS: 1}, nil)
	fs.base().ReplaceNative(&fsbase.NativeIdentity{ID: 41, MetadataPool: 1, DefaultPool: 2, Attachments: make(map[string]*fsbase.PoolAttachment)})
	filesystems(cluster).byName = map[string]*Filesystem{"fixture": fs}
	services := map[string]testcontainers.Container{}
	for index, name := range []string{"fixture-0", "fixture-1"} {
		cid := strings.Repeat(string(rune('a'+index)), 64)
		process := &stoppedMDSDaemon{cid: cid, state: container.State{Status: "running", Running: true, Pid: 9}}
		daemon := &MDS{Container: process, ID: name, FilesystemName: "fixture"}
		daemon.identity = &cephFSMDSIdentity{fleet: fs.fleet(), native: fs.base().Native(), descriptor: daemon, container: process, name: name, cid: cid, confirmed: true}
		fs.mds.roster.daemons = append(fs.mds.roster.daemons, daemon)
		services["mds."+name] = process
	}
	setServices(cluster, services)
	fs.Container = fs.mds.roster.daemons[0].Container
	return fs, ctr, fs.mds.roster.daemons[0], fs.mds.roster.daemons[1]
}

func TestCephFSMDSReadOnlyTargetsTheGIDConfirmsAndWaitsForAReplacement(t *testing.T) {
	_, ctr, active, _ := readOnlyFixture(t)
	copied := *active
	fault, err := copied.ForceReadOnly(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := ctr.forced(); !slices.Equal(got, []string{"tell mds.4199 force_readonly"}) {
		t.Fatalf("force_readonly commands = %q, want one to the captured GID", got)
	}
	if s := fault.state; s.fsID != 41 || s.metadataPool != 1 || s.defaultPool != 2 || s.target != readOnlyTarget {
		t.Fatalf("captured identity = %+v", *s)
	}
	if report := fault.String(); !strings.Contains(report, "mds.fixture-0 (GID 4199, rank 0)") || !strings.Contains(report, "no inverse") {
		t.Fatalf("report = %q", report)
	}
	// Until the process is replaced the wait does not succeed, and its error
	// says what ends the fault.
	short, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := fault.WaitReplaced(short); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "fail or restart the daemon") {
		t.Fatalf("WaitReplaced before a replacement: %v", err)
	}
	ctr.takeover(4203)
	shared := *fault
	holder, err := shared.WaitReplaced(t.Context())
	if err != nil || holder != (MDSStatus{Name: "fixture-1", Rank: 0, GID: 4203, State: "up:active", Owned: true}) {
		t.Fatalf("WaitReplaced = %+v, %v", holder, err)
	}
	if got := ctr.forced(); len(got) != 1 {
		t.Fatalf("the wait sent force_readonly: %q", got)
	}
}

// A restarted daemon keeps its name and gets a new GID; while the faulted
// GID is still registered, or the rank is not active, the wait goes on.
func TestCephFSMDSReadOnlyWaitAcceptsARestartUnderANewGID(t *testing.T) {
	_, ctr, active, _ := readOnlyFixture(t)
	fault, err := active.ForceReadOnly(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	start := ctr.dumps
	ctr.beforeDump = func(n int) {
		switch n - start {
		case 1: // the faulted process is a standby now
			ctr.active, ctr.standbys = nil, append(ctr.standbys, fsbase.MDSInfo{Name: "fixture-0", GID: 4199, State: "up:standby"})
		case 2: // gone, the rank replays on the new process
			ctr.standbys = ctr.standbys[:1]
			ctr.active = []fsbase.MDSInfo{{Name: "fixture-0", GID: 5000, Rank: 0, State: "up:replay"}}
		case 3:
			ctr.active[0].State = "up:active"
		}
	}
	holder, err := fault.WaitReplaced(t.Context())
	if err != nil || holder.Name != "fixture-0" || holder.GID != 5000 || ctr.dumps-start < 3 {
		t.Fatalf("WaitReplaced = %+v, %v after %d reads", holder, err, ctr.dumps-start)
	}
}

// A replacement that reports MDS_READ_ONLY itself is not a recovery.
func TestCephFSMDSReadOnlyWaitRefusesAReadOnlyReplacement(t *testing.T) {
	_, ctr, active, _ := readOnlyFixture(t)
	fault, err := active.ForceReadOnly(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctr.takeover(4203)
	ctr.readOnly[4203] = true
	short, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if holder, err := fault.WaitReplaced(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a read-only replacement was accepted: %+v, %v", holder, err)
	}
}

func TestCephFSMDSReadOnlyRefusesInvalidTargetsBeforeAnyChange(t *testing.T) {
	for name, test := range map[string]struct {
		target  func(active, standby *MDS) *MDS
		edit    func(*Filesystem, *readOnlyControl)
		invalid bool
	}{
		"nil handle":         {target: func(*MDS, *MDS) *MDS { return nil }, invalid: true},
		"no identity":        {target: func(*MDS, *MDS) *MDS { return &MDS{ID: "fixture-0", FilesystemName: "fixture"} }, invalid: true},
		"standby":            {target: func(_, standby *MDS) *MDS { return standby }, invalid: true},
		"edited handle":      {target: func(active, _ *MDS) *MDS { edited := *active; edited.ID = "fixture-1"; return &edited }, invalid: true},
		"edited filesystem":  {edit: func(fs *Filesystem, _ *readOnlyControl) { fs.FilesystemName = "other" }, invalid: true},
		"removed identity":   {target: func(active, _ *MDS) *MDS { active.identity.removed = true; return active }, invalid: true},
		"unconfirmed start":  {target: func(active, _ *MDS) *MDS { active.identity.confirmed = false; return active }, invalid: true},
		"foreign rank owner": {edit: func(_ *Filesystem, ctr *readOnlyControl) { ctr.active[0].Name = "foreign" }, invalid: true},
		"two ranks": {edit: func(_ *Filesystem, ctr *readOnlyControl) {
			ctr.maxMDS = 2
			ctr.active = append(ctr.active, fsbase.MDSInfo{Name: "fixture-1", GID: 4203, Rank: 1, State: "up:active"})
			ctr.standbys = nil
		}, invalid: true},
		"read-only outside": {edit: func(_ *Filesystem, ctr *readOnlyControl) { ctr.readOnly[4199] = true }, invalid: true},
		// A process that answers as another daemon is a failed check of
		// native state, not an invalid request.
		"process mismatch": {edit: func(_ *Filesystem, ctr *readOnlyControl) {
			ctr.status = map[uint64]string{4199: `{"id":4199,"whoami":0,"state":"up:active","fs_name":"other"}`}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			fs, ctr, active, standby := readOnlyFixture(t)
			if test.edit != nil {
				test.edit(fs, ctr)
			}
			target := active
			if test.target != nil {
				target = test.target(active, standby)
			}
			fault, err := target.ForceReadOnly(t.Context())
			if fault != nil || err == nil || len(ctr.forced()) != 0 {
				t.Fatalf("fault=%v error=%v forced=%q", fault, err, ctr.forced())
			}
			if errors.Is(err, ceph.ErrInvalidRequest) != test.invalid {
				t.Fatalf("refusal class: %v", err)
			}
		})
	}
}

func TestCephFSMDSReadOnlyRefusesATerminatedClusterAndARemovedFilesystem(t *testing.T) {
	fs, ctr, active, standby := readOnlyFixture(t)
	fault, err := active.ForceReadOnly(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	delete(filesystems(fs.base().Cluster()).byName, "fixture")
	if _, err := fault.WaitReplaced(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("WaitReplaced on a removed filesystem: %v", err)
	}
	if again, err := standby.ForceReadOnly(t.Context()); again != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a removed filesystem admitted a fault: %v, %v", again, err)
	}
	filesystems(fs.base().Cluster()).byName["fixture"] = fs
	testhelper.TestSetClosed(fs.base().Cluster(), true)
	if _, err := fault.WaitReplaced(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "terminated") {
		t.Fatalf("WaitReplaced on a terminated cluster: %v", err)
	}
	calls := len(ctr.Calls)
	if again, err := active.ForceReadOnly(t.Context()); again != nil || !errors.Is(err, ceph.ErrInvalidRequest) || len(ctr.Calls) != calls {
		t.Fatalf("a terminated cluster admitted a fault: %v, %v, calls %q", again, err, ctr.Calls[calls:])
	}
	var unavailable *MDSReadOnlyFault
	if _, err := unavailable.WaitReplaced(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil fault: %v", err)
	}
}

// A second fault of the same process returns the existing record, and an
// outside edit of what the fault depends on (the rank's holder, the
// filesystem identity) ends the wait with a refusal.
func TestCephFSMDSReadOnlyOwnershipAndOutsideEdits(t *testing.T) {
	_, ctr, active, _ := readOnlyFixture(t)
	fault, err := active.ForceReadOnly(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	again, err := active.ForceReadOnly(t.Context())
	if again == nil || again.state != fault.state || !errors.Is(err, ceph.ErrInvalidRequest) || len(ctr.forced()) != 1 {
		t.Fatalf("second fault = %v, %v, forced %q", again, err, ctr.forced())
	}
	ctr.standbys = nil
	ctr.active = []fsbase.MDSInfo{{Name: "foreign", GID: 6000, Rank: 0, State: "up:active"}}
	if _, err := fault.WaitReplaced(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "does not own") {
		t.Fatalf("a foreign rank holder: %v", err)
	}
	ctr.active = []fsbase.MDSInfo{{Name: "fixture-1", GID: 4203, Rank: 0, State: "up:active"}}
	ctr.fsID = 42
	if _, err := fault.WaitReplaced(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "recreated") {
		t.Fatalf("a recreated filesystem: %v", err)
	}
}

// A lost reply or an unconfirmed state returns the fault with the error, and
// the fault still waits for the replacement.
func TestCephFSMDSReadOnlyKeepsTheFaultAfterAnUncertainApply(t *testing.T) {
	_, ctr, active, _ := readOnlyFixture(t)
	ctr.lostReply = true
	fault, err := active.ForceReadOnly(t.Context())
	if fault == nil || err == nil || !errors.Is(err, ceph.ErrNativeFailure) {
		t.Fatalf("lost reply: fault=%v error=%v", fault, err)
	}
	if again, err := active.ForceReadOnly(t.Context()); again == nil || again.state != fault.state || err == nil {
		t.Fatalf("the uncertain fault is not tracked: %v, %v", again, err)
	}
	ctr.takeover(4203)
	if holder, err := fault.WaitReplaced(t.Context()); err != nil || holder.GID != 4203 {
		t.Fatalf("WaitReplaced after a lost reply = %+v, %v", holder, err)
	}

	_, ctr, active, _ = readOnlyFixture(t)
	ctr.silentHealth = true
	short, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	fault, err = active.ForceReadOnly(short)
	if fault == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "did not report MDS_READ_ONLY") {
		t.Fatalf("unconfirmed state: fault=%v error=%v", fault, err)
	}
	ctr.takeover(4203)
	if holder, err := fault.WaitReplaced(t.Context()); err != nil || holder.GID != 4203 {
		t.Fatalf("WaitReplaced after an unconfirmed apply = %+v, %v", holder, err)
	}
}

func TestCephFSMDSReadOnlyRefusesADaemonWithoutTheCommand(t *testing.T) {
	_, ctr, active, _ := readOnlyFixture(t)
	ctr.noCommand = true
	fault, err := active.ForceReadOnly(t.Context())
	var unsupported *ceph.OperationError
	if fault != nil || !errors.As(err, &unsupported) || unsupported.Kind != ceph.ErrUnsupported || unsupported.Capability != "mds force_readonly" || unsupported.Version != "20.2.4" || unsupported.Target != "mds.fixture-0" {
		t.Fatalf("fault=%v error=%#v", fault, err)
	}
	if len(ctr.forced()) != 0 {
		t.Fatalf("an unsupported daemon was faulted: %q", ctr.forced())
	}
}

func TestCephFSMDSReadOnlyWaitsForItsLocksWithTheCallersContext(t *testing.T) {
	for _, gate := range []string{"setup", "owner"} {
		t.Run(gate, func(t *testing.T) {
			fs, ctr, active, _ := readOnlyFixture(t)
			assertCephFSContextAdmissionGate(t, fs.base().Cluster(), gate, func(ctx context.Context) error {
				_, err := active.ForceReadOnly(ctx)
				return err
			})
			if len(ctr.Calls) != 0 {
				t.Fatalf("a queued fault reached the cluster: %q", ctr.Calls)
			}
			assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
		})
	}
	fs, ctr, active, standby := readOnlyFixture(t)
	fault, err := active.ForceReadOnly(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	calls := len(ctr.Calls)
	assertCephFSContextAdmissionGate(t, fs.base().Cluster(), "owner", func(ctx context.Context) error {
		_, err := fault.WaitReplaced(ctx)
		return err
	})
	if len(ctr.Calls) != calls {
		t.Fatalf("a queued wait reached the cluster: %q", ctr.Calls[calls:])
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if again, err := standby.ForceReadOnly(canceled); again != nil || !errors.Is(err, context.Canceled) || len(ctr.Calls) != calls {
		t.Fatalf("a canceled caller reached the cluster: %v, %v", again, err)
	}
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
}

func TestParseMDSReadOnlyHealthAndCommands(t *testing.T) {
	daemon := MDSStatus{Name: "a", Rank: 0}
	health := `{"status":"HEALTH_WARN","checks":{"MDS_READ_ONLY":{"severity":"HEALTH_WARN","summary":{"message":"1 MDSs are read only","count":1},"detail":[{"message":"mds.a(mds.0): MDS in read-only mode"}],"muted":false}},"mutes":[]}`
	if reported, err := parseMDSReadOnlyHealth([]byte(health), daemon); err != nil || !reported {
		t.Fatalf("native health = %t, %v", reported, err)
	}
	for _, other := range []MDSStatus{{Name: "ab", Rank: 0}, {Name: "a", Rank: 1}} {
		if reported, err := parseMDSReadOnlyHealth([]byte(health), other); err != nil || reported {
			t.Fatalf("%+v matched another daemon's detail: %t, %v", other, reported, err)
		}
	}
	if reported, err := parseMDSReadOnlyHealth([]byte(`{"status":"HEALTH_OK","checks":{},"mutes":[]}`), daemon); err != nil || reported {
		t.Fatalf("healthy = %t, %v", reported, err)
	}
	for _, bad := range []string{`{}`, `[]`, `x`} {
		if _, err := parseMDSReadOnlyHealth([]byte(bad), daemon); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
	commands := `{"cmd000":{"sig":["0"],"help":""},"cmd001":{"sig":["force_readonly"],"help":"Force MDS to read-only mode"},"cmd002":{"sig":["flush",{"name":"x"}]}}`
	if offered, err := mdsOffersCommand([]byte(commands), "force_readonly"); err != nil || !offered {
		t.Fatalf("offered = %t, %v", offered, err)
	}
	if offered, err := mdsOffersCommand([]byte(commands), "flush"); err != nil || offered {
		t.Fatalf("a longer signature matched: %t, %v", offered, err)
	}
	for _, bad := range []string{`{}`, `[]`, `x`} {
		if _, err := mdsOffersCommand([]byte(bad), "force_readonly"); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
}
