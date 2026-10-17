//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// fakeMonCluster models the native state a MON replacement changes: the
// monmap and its epoch, the quorum, each MON's own state, which MON holds each
// warm daemon's session and each daemon's debug_monc. topology models the
// cluster's topology mutex: AddMonitor and RemoveMonitor hold it until they
// return, and admin socket and diagnostics reads need it to start.
type fakeMonCluster struct {
	topology chan struct{}
	mu       sync.Mutex
	epoch    int
	election int
	members  []string
	quorum   map[string]bool
	probing  map[string]bool
	paused   map[string]bool
	// stallJoin never lets the named MON join; lagQuorum lets AddMonitor
	// return but keeps the MON out of quorum.
	stallJoin, lagQuorum map[string]bool
	session              map[string]string
	strand               map[string]bool
	debug                map[string]string
	started              map[string]string
	// restartOnAdd restarts the named warm daemon when a MON is added.
	restartOnAdd string
	adds         map[string]int
	removes      []string
	diagnostics  []error
}

func newFakeMonCluster() *fakeMonCluster {
	f := &fakeMonCluster{
		topology: make(chan struct{}, 1), epoch: 3, election: 10, members: []string{"a", "b", "c"},
		quorum: map[string]bool{"a": true, "b": true, "c": true}, probing: map[string]bool{}, paused: map[string]bool{},
		stallJoin: map[string]bool{}, lagQuorum: map[string]bool{}, strand: map[string]bool{},
		session: map[string]string{"mgr.x": "a", "osd.0": "b", "mds.fs-a": "c"},
		debug:   map[string]string{"mgr.x": "1/5", "osd.0": "0/10"},
		started: map[string]string{"mgr.x": "t0", "osd.0": "t0"},
		adds:    map[string]int{},
	}
	return f
}

func (f *fakeMonCluster) lock(ctx context.Context) error {
	select {
	case f.topology <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeMonCluster) unlock() { <-f.topology }

func (f *fakeMonCluster) AddMonitor(ctx context.Context, name string) (*ceph.MonitorContainer, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	defer f.unlock()
	f.mu.Lock()
	f.adds[name]++
	f.members = append(f.members, name)
	f.epoch++
	f.probing[name] = true
	if f.restartOnAdd != "" {
		f.started[f.restartOnAdd] = "t1"
	}
	stall, lag := f.stallJoin[name], f.lagQuorum[name]
	f.mu.Unlock()
	mon := &ceph.MonitorContainer{DaemonName: name, Container: &fakeDaemonContainer{cluster: f, daemon: "mon." + name}}
	if stall {
		<-ctx.Done()
		return mon, fmt.Errorf("join monitor %s to quorum: %w", name, ctx.Err())
	}
	if !lag {
		f.mu.Lock()
		delete(f.probing, name)
		f.quorum[name] = true
		f.election += 2
		f.mu.Unlock()
	}
	return mon, nil
}

func (f *fakeMonCluster) RemoveMonitor(ctx context.Context, name string) error {
	if err := f.lock(ctx); err != nil {
		return err
	}
	defer f.unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removes = append(f.removes, name)
	f.members = slices.DeleteFunc(f.members, func(member string) bool { return member == name })
	delete(f.quorum, name)
	f.epoch++
	f.election += 2
	for entity, mon := range f.session {
		if mon != name {
			continue
		}
		// A daemon whose MON left hunts the remaining quorum unless it is
		// stranded, as a refused reconnect leaves it.
		f.session[entity] = ""
		if !f.strand[entity] {
			for _, member := range f.members {
				if f.quorum[member] {
					f.session[entity] = member
					break
				}
			}
		}
	}
	return nil
}

func (f *fakeMonCluster) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error) {
	if err := ctx.Err(); err != nil {
		return ceph.QuorumStatus{}, err
	}
	f.mu.Lock()
	type addr struct {
		Type string `json:"type"`
		Addr string `json:"addr"`
	}
	type mon struct {
		Name        string `json:"name"`
		PublicAddrs struct {
			Addrvec []addr `json:"addrvec"`
		} `json:"public_addrs"`
	}
	value := struct {
		QuorumNames []string `json:"quorum_names"`
		MonMap      struct {
			FSID string `json:"fsid"`
			Mons []mon  `json:"mons"`
		} `json:"monmap"`
	}{}
	value.MonMap.FSID = "fsid-1"
	for i, name := range f.members {
		if f.quorum[name] {
			value.QuorumNames = append(value.QuorumNames, name)
		}
		m := mon{Name: name}
		m.PublicAddrs.Addrvec = []addr{{Type: "v2", Addr: fmt.Sprintf("10.0.0.%d:3300", i+2)}}
		value.MonMap.Mons = append(value.MonMap.Mons, m)
	}
	f.mu.Unlock()
	raw, _ := json.Marshal(value)
	var status ceph.QuorumStatus
	return status, json.Unmarshal(raw, &status)
}

func (f *fakeMonCluster) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	f.unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	kind, id, _ := strings.Cut(daemon, ".")
	if kind == "mon" && !slices.Contains(f.members, id) {
		return nil, fmt.Errorf("daemon %s is not owned by this cluster", daemon)
	}
	if f.paused[daemon] {
		f.mu.Unlock()
		<-ctx.Done()
		f.mu.Lock()
		return nil, fmt.Errorf("admin socket of %s: %w", daemon, ctx.Err())
	}
	command := strings.Join(args, " ")
	switch {
	case command == "help":
		commands := map[string][]string{"mon": {"mon_status", "sessions"}, "mgr": {"mgr_status"}, "osd": {"status"}}[kind]
		help := map[string]string{"help": "", "config get": "", "config set": ""}
		for _, name := range commands {
			help[name] = ""
		}
		return json.Marshal(help)
	case command == "mon_status":
		return f.monStatus(id)
	case command == "sessions":
		var list []map[string]any
		for entity, mon := range f.session {
			if mon == id {
				kind, _, _ := strings.Cut(entity, ".")
				list = append(list, map[string]any{"name": kind + ".4100", "entity_name": entity, "global_id": 4100, "con_type": kind, "open": true, "authenticated": true, "caps": map[string]string{"text": "allow *"}})
			}
		}
		return json.Marshal(list)
	case command == "status" && kind == "osd":
		return json.Marshal(map[string]any{"cluster_fsid": "fsid-1", "whoami": 0, "state": "active", "newest_map": "9"})
	case command == "mgr_status" && kind == "mgr":
		return json.Marshal(map[string]any{"mgrmap_epoch": 5, "initialized": true})
	case len(args) == 3 && args[0] == "config" && args[1] == "get":
		value := map[string]string{"mon_host": "[v2:10.0.0.2:3300]", "auth_exit_on_failure": "-1", "auth_mon_ticket_ttl": "259200.000000",
			"auth_service_ticket_ttl": "3600.000000", "mon_client_hunt_interval": "3.000000"}[args[2]]
		if args[2] == "debug_monc" {
			value = f.debug[daemon]
		}
		return json.Marshal(map[string]string{args[2]: value})
	case len(args) == 4 && args[0] == "config" && args[1] == "set" && args[2] == "debug_monc":
		f.debug[daemon] = args[3]
		return []byte(`{"success": ""}`), nil
	}
	return nil, fmt.Errorf("admin socket of %s: unknown command %q", daemon, command)
}

// monStatus answers as the MON itself would: a joining MON knows the monmap
// it was given but is still probing, outside the quorum.
func (f *fakeMonCluster) monStatus(id string) ([]byte, error) {
	type member struct {
		Rank int    `json:"rank"`
		Name string `json:"name"`
	}
	value := map[string]any{"name": id, "rank": slices.Index(f.members, id), "election_epoch": f.election, "quorum": []int{}}
	var mons []member
	var quorum []int
	for rank, name := range f.members {
		mons = append(mons, member{Rank: rank, Name: name})
		if f.quorum[name] {
			quorum = append(quorum, rank)
		}
	}
	value["monmap"] = map[string]any{"epoch": f.epoch, "mons": mons}
	switch {
	case f.probing[id]:
		value["state"] = "probing"
	case f.quorum[id] && quorum[0] == slices.Index(f.members, id):
		value["state"], value["quorum"] = "leader", quorum
	default:
		value["state"], value["quorum"] = "peon", quorum
	}
	return json.Marshal(value)
}

func (f *fakeMonCluster) CollectDiagnostics(ctx context.Context, _ ceph.DiagnosticsConfig) (*ceph.DiagnosticsReport, error) {
	// The real collection snapshots the inventory under the topology mutex.
	err := f.lock(ctx)
	if err == nil {
		f.unlock()
	}
	f.mu.Lock()
	f.diagnostics = append(f.diagnostics, err)
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &ceph.DiagnosticsReport{SchemaVersion: 1, Complete: true, Artifacts: []ceph.DiagnosticArtifact{{Kind: "logs", Name: "mon.e", Data: "mon.e@-1(probing)"}}}, nil
}

func (f *fakeMonCluster) Monitors() []*ceph.MonitorContainer {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []*ceph.MonitorContainer
	for _, name := range f.members {
		result = append(result, &ceph.MonitorContainer{DaemonName: name, Container: &fakeDaemonContainer{cluster: f, daemon: "mon." + name}})
	}
	slices.SortFunc(result, func(a, b *ceph.MonitorContainer) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return result
}

func (f *fakeMonCluster) Managers() []*ceph.ManagerContainer {
	return []*ceph.ManagerContainer{{DaemonName: "x", Container: &fakeDaemonContainer{cluster: f, daemon: "mgr.x"}}}
}

func (f *fakeMonCluster) OSDs() []*ceph.OSDContainer {
	return []*ceph.OSDContainer{{ID: 0, Container: &fakeDaemonContainer{cluster: f, daemon: "osd.0"}}}
}

// fakeDaemonContainer reports the generation that its cluster records.
type fakeDaemonContainer struct {
	testcontainers.Container
	cluster *fakeMonCluster
	daemon  string
}

func (c *fakeDaemonContainer) GetContainerID() string { return "cid-" + c.daemon + "-0123456789" }

func (c *fakeDaemonContainer) State(context.Context) (*container.State, error) {
	c.cluster.mu.Lock()
	defer c.cluster.mu.Unlock()
	return &container.State{Running: true, StartedAt: c.cluster.started[c.daemon]}, nil
}

// fakeReporter records what a run logs and how it fails, in order.
type fakeReporter struct{ entries []string }

func (r *fakeReporter) Helper() {}

func (r *fakeReporter) Logf(format string, args ...any) {
	r.entries = append(r.entries, "log "+fmt.Sprintf(format, args...))
}

func (r *fakeReporter) Fatalf(format string, args ...any) {
	r.entries = append(r.entries, "fatal "+fmt.Sprintf(format, args...))
}

var fakeMonReplacementBounds = monReplacementBounds{
	Operation: 300 * time.Millisecond, Quorum: 300 * time.Millisecond, QuorumInterval: time.Millisecond,
	Query: 50 * time.Millisecond, Snapshot: time.Second, Bundle: 3 * time.Second,
}

var fakeMonReplacementPairs = [][2]string{{"a", "d"}, {"b", "e"}, {"c", "f"}}

func TestMonReplacementRecordsEveryPhase(t *testing.T) {
	f := newFakeMonCluster()
	f.strand["osd.0"] = true
	r := newMonReplacementRecorder("source", f, fakeMonReplacementBounds)
	reporter := &fakeReporter{}
	runMonReplacement(reporter, t.Context(), r, fakeMonReplacementPairs)
	if len(reporter.entries) != 1 || !strings.HasPrefix(reporter.entries[0], "log MON_REPLACEMENT_RECEIPT token="+r.Token+" cluster=source phases=6 ") {
		t.Fatalf("reported %q", reporter.entries)
	}
	receipt := reporter.entries[0]
	t.Log(receipt)
	epoch := r.Baseline.monMapEpoch()
	for i, p := range r.Phases {
		if p.Error != "" || p.After == nil || p.Seq != i+1 || p.OperationEnded.Before(p.Started) || p.Ended.Before(p.OperationEnded) {
			t.Fatalf("phase %d: %+v", i, p)
		}
		if next := p.After.monMapEpoch(); next != epoch+1 {
			t.Fatalf("phase %q: monmap epoch %d after %d", p.Name, next, epoch)
		}
		epoch++
	}
	if names := []string{r.Phases[0].Name, r.Phases[1].Name, r.Phases[5].Name}; !slices.Equal(names, []string{"add d", "remove a", "remove c"}) {
		t.Fatalf("phase names %v", names)
	}
	// Each MON reports its own view: the quorum it is part of, by name.
	last := r.Phases[5].After
	if !slices.Equal(last.Quorum, []string{"d", "e", "f"}) || len(last.Monitors) != 3 || !slices.Equal(last.Monitors[0].Quorum, []string{"d", "e", "f"}) || len(last.Incomplete) != 0 {
		t.Fatalf("final snapshot %+v", last)
	}
	// The MGR followed the MONs; the stranded OSD's session ended with a.
	for _, want := range []string{"mgr.x:a>b>c>d,", "osd.0:b>-", "mds.fs-a:c>d", "debug_monc_restored=2/2 incomplete=0", "remove-c="} {
		if !strings.Contains(receipt, want) {
			t.Fatalf("receipt lacks %q: %s", want, receipt)
		}
	}
	if f.debug["mgr.x"] != "1/5" || f.debug["osd.0"] != "0/10" {
		t.Fatalf("debug_monc not restored exactly: %v", f.debug)
	}
	if got := r.Baseline.Warm[0].Config; got["auth_exit_on_failure"] != "-1" || got["debug_monc"] != "1/5" {
		t.Fatalf("baseline settings %v", got)
	}
}

func TestMonReplacementJoinTimeoutLogsBundleBeforeFailing(t *testing.T) {
	f := newFakeMonCluster()
	f.stallJoin["e"] = true
	// mon.c's socket hangs throughout, as a paused MON's does, and the MGR
	// restarts during the replacement.
	f.paused["mon.c"] = true
	f.restartOnAdd = "mgr.x"
	r := newMonReplacementRecorder("destination", f, fakeMonReplacementBounds, ceph.DiagnosticsContainer{Role: "mirror", Name: "rbd-mirror"})
	reporter := &fakeReporter{}
	started := time.Now()
	runMonReplacement(reporter, t.Context(), r, fakeMonReplacementPairs)
	elapsed := time.Since(started)
	// The old test called t.Fatal with the AddMonitor error alone; the
	// bundle must be logged first, and the failure must stay the original.
	if len(reporter.entries) != 2 || !strings.HasPrefix(reporter.entries[0], "log MON_REPLACEMENT_BUNDLE ") || !strings.HasPrefix(reporter.entries[1], "fatal ") {
		t.Fatalf("reported %q", reporter.entries)
	}
	if !strings.Contains(reporter.entries[1], `phase 3 "add e": join monitor e to quorum: context deadline exceeded`) {
		t.Fatalf("failure %s", reporter.entries[1])
	}
	var bundle monReplacementBundle
	if err := json.Unmarshal([]byte(strings.TrimPrefix(reporter.entries[0], "log MON_REPLACEMENT_BUNDLE ")), &bundle); err != nil {
		t.Fatal(err)
	}
	// No retry and no further membership change after the failure.
	if f.adds["e"] != 1 || f.adds["f"] != 0 || !slices.Equal(f.removes, []string{"a"}) {
		t.Fatalf("adds %v removes %v", f.adds, f.removes)
	}
	if bundle.Token != r.Token || bundle.Failed.Name != "add e" || !strings.HasPrefix(bundle.Failed.Handle, "mon.e container=") || len(bundle.Phases) != 3 {
		t.Fatalf("failed phase %+v", bundle.Failed)
	}
	// The joining MON is reached after AddMonitor returned: it has the new
	// monmap and is still probing outside the quorum.
	var joining *monLocalStatus
	for i, mon := range bundle.Failure.Monitors {
		if mon.Name == "e" {
			joining = &bundle.Failure.Monitors[i]
		}
	}
	if joining == nil || joining.State != "probing" || joining.MonMapEpoch != 6 || len(joining.Quorum) != 0 || !slices.Contains(joining.Members, "e") {
		t.Fatalf("joining MON %+v", joining)
	}
	// The paused MON is an incomplete observation, not a status.
	if !slices.ContainsFunc(bundle.Failure.Incomplete, func(s string) bool { return strings.HasPrefix(s, "mon.c mon_status: ") }) {
		t.Fatalf("incomplete %v", bundle.Failure.Incomplete)
	}
	if bundle.Diagnostics == nil || bundle.DiagnosticsError != "" || len(f.diagnostics) != 1 || f.diagnostics[0] != nil {
		t.Fatalf("diagnostics %+v error=%q calls=%v", bundle.Diagnostics, bundle.DiagnosticsError, f.diagnostics)
	}
	// The OSD's window is restored exactly; the restarted MGR's is not
	// written into its new process.
	debug := map[string]string{}
	for _, window := range bundle.Debug {
		debug[window.Daemon] = window.Restored
	}
	if debug["osd.0"] != "ok" || debug["mgr.x"] != "skipped: container generation changed" || f.debug["osd.0"] != "0/10" || f.debug["mgr.x"] != monReplacementDebugLevel {
		t.Fatalf("debug windows %v native %v", debug, f.debug)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("bounded run took %s", elapsed)
	}
}

func TestMonReplacementQuorumBoundFailsThePhase(t *testing.T) {
	f := newFakeMonCluster()
	f.lagQuorum["d"] = true
	r := newMonReplacementRecorder("source", f, fakeMonReplacementBounds)
	reporter := &fakeReporter{}
	runMonReplacement(reporter, t.Context(), r, fakeMonReplacementPairs)
	if len(reporter.entries) != 2 || !strings.Contains(reporter.entries[1], `phase 1 "add d": quorum [a b c d] after add d: not observed within 300ms`) {
		t.Fatalf("reported %q", reporter.entries)
	}
	if len(r.Phases) != 1 || r.Phases[0].QuorumProbes < 2 || len(f.removes) != 0 || f.adds["d"] != 1 {
		t.Fatalf("phases %+v removes %v", r.Phases, f.removes)
	}
	var bundle monReplacementBundle
	if err := json.Unmarshal([]byte(strings.TrimPrefix(reporter.entries[0], "log MON_REPLACEMENT_BUNDLE ")), &bundle); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(bundle.Failure.Quorum, []string{"a", "b", "c"}) || !slices.Equal(bundle.Failure.MonMap, []string{"a", "b", "c", "d"}) {
		t.Fatalf("failure snapshot %+v", bundle.Failure)
	}
	if f.debug["mgr.x"] != "1/5" || f.debug["osd.0"] != "0/10" {
		t.Fatalf("debug %v", f.debug)
	}
}
