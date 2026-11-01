package session

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// sessionFixture keeps the two MDSMap limits and answers fs get with them.
type sessionFixture struct {
	*clustertest.CannedCommandContainer
	fsID               int64
	timeout, autoclose int
}

func (ctr *sessionFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.Output["fs get app --format json"] = fmt.Sprintf(`{"id":%d,"mdsmap":{"session_timeout":%d,"session_autoclose":%d}}`, ctr.fsID, ctr.timeout, ctr.autoclose)
	code, reader, err := ctr.CannedCommandContainer.Exec(ctx, args, opts...)
	command := strings.Join(clustertest.ModuleArgs(args), " ")
	if code == 0 && err == nil {
		var value int
		if _, scanErr := fmt.Sscanf(command, "fs set app session_timeout %d", &value); scanErr == nil {
			ctr.timeout = value
		} else if _, scanErr := fmt.Sscanf(command, "fs set app session_autoclose %d", &value); scanErr == nil {
			ctr.autoclose = value
		}
	}
	return code, reader, err
}

func (ctr *sessionFixture) mutations() []string {
	var commands []string
	for _, call := range ctr.Calls {
		if command := strings.Join(call, " "); strings.HasPrefix(command, "fs set") {
			commands = append(commands, command)
		}
	}
	return commands
}

// sessionCore is the core of the registered filesystem "app" (FSMap ID 7)
// over a synthetic cluster, with the registry state cephfs would inject.
type sessionCore struct {
	*fsbase.Core
	setupMu      *sync.Mutex
	descriptor   string
	unregistered bool
}

func sessionFilesystem(t *testing.T) (*sessionCore, *sessionFixture) {
	t.Helper()
	ctr := &sessionFixture{CannedCommandContainer: &clustertest.CannedCommandContainer{Output: map[string]string{}}, fsID: 7, timeout: 60, autoclose: 300}
	cluster := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: 1})
	fs := &sessionCore{setupMu: &sync.Mutex{}, descriptor: "app"}
	fs.Core = fsbase.NewCore(fsbase.Params{Cluster: cluster, Name: "app", DescriptorName: &fs.descriptor, SetupMu: fs.setupMu,
		Live: func(*fsbase.Core) bool { return !fs.unregistered },
		ActiveRanks: func(context.Context) (*fsbase.ActiveRanks, error) {
			return &fsbase.ActiveRanks{FilesystemID: 7, MaxMDS: 2, Active: []fsbase.ActiveRank{{Rank: 0, State: "up:active", Owned: true}, {Rank: 1, State: "up:active", Owned: true}}}, nil
		}})
	fs.ReplaceNative(&fsbase.NativeIdentity{ID: 7})
	return fs, ctr
}

func TestCephFSSessionTimeoutsRefuseInvalidValuesBeforeCommands(t *testing.T) {
	for _, requested := range []Limits{
		{},
		{Timeout: 29 * time.Second, Autoclose: time.Minute},
		{Timeout: time.Minute, Autoclose: 29 * time.Second},
		{Timeout: 30*time.Second + time.Millisecond, Autoclose: time.Minute},
		{Timeout: time.Minute, Autoclose: 1 << 62},
	} {
		fs, ctr := sessionFilesystem(t)
		if change, err := overrideTimeouts(t.Context(), fs.Core, requested); change != nil || err == nil || len(ctr.Calls) != 0 {
			t.Fatalf("%+v: handle=%v error=%v calls=%q", requested, change, err, ctr.Calls)
		}
	}
}

// Real Ceph cannot fail only the second of the two native commands or swap
// the filesystem ID under one name between calls, so these use the fake.
func TestCephFSSessionTimeoutsRefuseRecreatedFilesystem(t *testing.T) {
	fs, ctr := sessionFilesystem(t)
	change, err := overrideTimeouts(t.Context(), fs.Core, Limits{Timeout: 30 * time.Second, Autoclose: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctr.fsID = 8
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("Restore on recreated filesystem error = %v", err)
	}
	if got := ctr.mutations(); len(got) != 2 {
		t.Fatalf("recreated filesystem was changed: %q", got)
	}
}

func TestCephFSSessionTimeoutsReportPartialApply(t *testing.T) {
	fs, ctr := sessionFilesystem(t)
	ctr.Fail = "fs set app session_autoclose 30"
	change, err := overrideTimeouts(t.Context(), fs.Core, Limits{Timeout: 30 * time.Second, Autoclose: 30 * time.Second})
	if change == nil || err == nil {
		t.Fatalf("partial apply returned handle=%v error=%v", change, err)
	}
	ctr.Fail = ""
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ctr.timeout != 60 || ctr.autoclose != 300 {
		t.Fatalf("restore left timeout=%d autoclose=%d", ctr.timeout, ctr.autoclose)
	}
}

func TestParseSessions(t *testing.T) {
	data := `[{"id":4239,"entity":{"name":{"type":"client","num":4239},"addr":{"type":"any","addr":"172.19.0.7:0","nonce":4208389369}},` +
		`"state":"open","num_caps":2,"client_metadata":{"entity_id":"admin","hostname":"fb95a494726c","pid":"41","root":"/"}}]`
	sessions, err := parseSessions([]byte(data), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := Session{Rank: 1, ID: 4239, Address: "172.19.0.7:0/4208389369", State: "open", EntityID: "admin", Hostname: "fb95a494726c", Root: "/", PID: "41", Caps: 2}
	if len(sessions) != 1 || sessions[0] != want {
		t.Fatalf("sessions = %+v", sessions)
	}
	if sessions, err := parseSessions([]byte(`[]`), 0); err != nil || len(sessions) != 0 {
		t.Fatalf("empty list = %+v, %v", sessions, err)
	}
	for _, bad := range []string{`{}`, `null`, `[{"id":1,"state":"open"}]`} {
		if _, err := parseSessions([]byte(bad), 0); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
}

func TestListReadsEveryActiveRank(t *testing.T) {
	fs, ctr := sessionFilesystem(t)
	for rank, id := range []int{11, 12} {
		ctr.Output[fmt.Sprintf("tell mds.app:%d session ls --format json", rank)] = fmt.Sprintf(`[{"id":%d,"entity":{"addr":{"addr":"172.19.0.7:0","nonce":1}},"state":"open"}]`, id)
	}
	sessions, err := list(t.Context(), fs.Core)
	if err != nil || len(sessions) != 2 || sessions[0].Rank != 0 || sessions[0].ID != 11 || sessions[1].Rank != 1 || sessions[1].ID != 12 {
		t.Fatalf("sessions = %+v, %v", sessions, err)
	}
	fstest.AssertAdmissionReleased(t, fs.Cluster(), fs.setupMu)
}

// The session admission keeps its refusal text for every predicate it owns,
// does not recheck cancellation after the setup gate but returns a canceled
// context as is, and holds no gate after a refusal.
func TestBeginRefusesAnEditedDescriptor(t *testing.T) {
	const refusal = "CephFS filesystem must be an active filesystem created by this cluster"
	for _, change := range []string{"descriptor", "native", "unregistered", "closed"} {
		t.Run(change, func(t *testing.T) {
			fs, ctr := sessionFilesystem(t)
			switch change {
			case "descriptor":
				fs.descriptor = "sibling"
			case "native":
				fs.ReplaceNative(nil)
			case "unregistered":
				fs.unregistered = true
			case "closed":
				testhelper.TestSetClosed(fs.Cluster(), true)
			}
			if _, _, done, err := begin(t.Context(), fs.Core); err == nil || err.Error() != refusal || done == nil {
				t.Fatalf("session refusal changed: %v", err)
			} else {
				done()
			}
			if len(ctr.Calls) != 0 {
				t.Fatalf("a refused admission ran commands: %v", ctr.Calls)
			}
			fstest.AssertAdmissionReleased(t, fs.Cluster(), fs.setupMu)
		})
	}
	fs, _ := sessionFilesystem(t)
	_, id, done, err := begin(t.Context(), fs.Core)
	if err != nil || id != 7 {
		t.Fatalf("an intact descriptor was refused or lost the FSMap id: %d %v", id, err)
	}
	done()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := begin(canceled, fs.Core); err != context.Canceled {
		t.Fatalf("session admission did not return the cancellation: %v", err)
	}
	fstest.AssertAdmissionReleased(t, fs.Cluster(), fs.setupMu)
}

// One override per filesystem: a second is refused until the first is
// restored, another filesystem of the cluster is independent, and a restored
// handle stays restored after its cluster is terminated.
func TestOverrideTimeoutsKeepsOneOverridePerFilesystem(t *testing.T) {
	fs, ctr := sessionFilesystem(t)
	requested := Limits{Timeout: 30 * time.Second, Autoclose: 30 * time.Second}
	change, err := overrideTimeouts(t.Context(), fs.Core, requested)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := overrideTimeouts(t.Context(), fs.Core, requested); second != nil || err == nil {
		t.Fatal("a second override of the same filesystem was accepted")
	}
	other := fsbase.NewCore(fsbase.Params{Cluster: fs.Cluster(), Name: "other"})
	if overridesOf(other).byCore[other] != nil || overridesOf(fs.Core).byCore[fs.Core] != change.state {
		t.Fatal("the override is not kept per filesystem")
	}
	copied := *change
	if err := copied.Restore(t.Context()); err != nil || ctr.timeout != 60 || ctr.autoclose != 300 {
		t.Fatalf("a copy did not restore the shared override: %v", err)
	}
	if overridesOf(fs.Core).byCore[fs.Core] != nil {
		t.Fatal("a restored override is still tracked")
	}
	testhelper.TestSetClosed(fs.Cluster(), true)
	if err := change.Restore(t.Context()); err != nil {
		t.Fatalf("a confirmed restore did not stay complete: %v", err)
	}
}
