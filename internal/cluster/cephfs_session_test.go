package cluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// sessionFixture keeps the two MDSMap limits and answers fs get with them.
type sessionFixture struct {
	*poolFixtureContainer
	fsID               int64
	timeout, autoclose int
}

func (ctr *sessionFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.output["fs get app --format json"] = fmt.Sprintf(`{"id":%d,"mdsmap":{"session_timeout":%d,"session_autoclose":%d}}`, ctr.fsID, ctr.timeout, ctr.autoclose)
	code, reader, err := ctr.poolFixtureContainer.Exec(ctx, args, opts...)
	command := strings.Join(monitorQuorumTestModuleArgs(args), " ")
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
	for _, call := range ctr.calls {
		if command := strings.Join(call, " "); strings.HasPrefix(command, "fs set") {
			commands = append(commands, command)
		}
	}
	return commands
}

func sessionFilesystem(t *testing.T) (*CephFSContainer, *sessionFixture) {
	t.Helper()
	ctr := &sessionFixture{poolFixtureContainer: &poolFixtureContainer{output: map[string]string{}}, fsID: 7, timeout: 60, autoclose: 300}
	cluster := poolFixtureCluster(ctr, 1)
	config, err := normalizeCephFSConfig(CephFSConfig{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	fs := &CephFSContainer{FilesystemName: "app", cluster: cluster, config: config, nativeIdentity: &cephFSNativeIdentity{id: 7}}
	cluster.filesystems = map[string]*CephFSContainer{"app": fs}
	return fs, ctr
}

func TestCephFSSessionTimeoutsRefuseInvalidValuesBeforeCommands(t *testing.T) {
	for _, requested := range []CephFSSessionTimeouts{
		{},
		{Timeout: 29 * time.Second, Autoclose: time.Minute},
		{Timeout: time.Minute, Autoclose: 29 * time.Second},
		{Timeout: 30*time.Second + time.Millisecond, Autoclose: time.Minute},
		{Timeout: time.Minute, Autoclose: 1 << 62},
	} {
		fs, ctr := sessionFilesystem(t)
		if change, err := fs.TemporarySessionTimeouts(t.Context(), requested); change != nil || err == nil || len(ctr.calls) != 0 {
			t.Fatalf("%+v: handle=%v error=%v calls=%q", requested, change, err, ctr.calls)
		}
	}
}

// Real Ceph cannot fail only the second of the two native commands or swap
// the filesystem ID under one name between calls, so these use the fake.
func TestCephFSSessionTimeoutsRefuseRecreatedFilesystem(t *testing.T) {
	fs, ctr := sessionFilesystem(t)
	change, err := fs.TemporarySessionTimeouts(t.Context(), CephFSSessionTimeouts{Timeout: 30 * time.Second, Autoclose: 30 * time.Second})
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
	ctr.fail = "fs set app session_autoclose 30"
	change, err := fs.TemporarySessionTimeouts(t.Context(), CephFSSessionTimeouts{Timeout: 30 * time.Second, Autoclose: 30 * time.Second})
	if change == nil || err == nil {
		t.Fatalf("partial apply returned handle=%v error=%v", change, err)
	}
	ctr.fail = ""
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ctr.timeout != 60 || ctr.autoclose != 300 {
		t.Fatalf("restore left timeout=%d autoclose=%d", ctr.timeout, ctr.autoclose)
	}
}

func TestParseCephFSSessions(t *testing.T) {
	data := `[{"id":4239,"entity":{"name":{"type":"client","num":4239},"addr":{"type":"any","addr":"172.19.0.7:0","nonce":4208389369}},` +
		`"state":"open","num_caps":2,"client_metadata":{"entity_id":"admin","hostname":"fb95a494726c","pid":"41","root":"/"}}]`
	sessions, err := parseCephFSSessions([]byte(data), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := CephFSSession{Rank: 1, ID: 4239, Address: "172.19.0.7:0/4208389369", State: "open", EntityID: "admin", Hostname: "fb95a494726c", Root: "/", PID: "41", Caps: 2}
	if len(sessions) != 1 || sessions[0] != want {
		t.Fatalf("sessions = %+v", sessions)
	}
	if sessions, err := parseCephFSSessions([]byte(`[]`), 0); err != nil || len(sessions) != 0 {
		t.Fatalf("empty list = %+v, %v", sessions, err)
	}
	for _, bad := range []string{`{}`, `null`, `[{"id":1,"state":"open"}]`} {
		if _, err := parseCephFSSessions([]byte(bad), 0); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
}
