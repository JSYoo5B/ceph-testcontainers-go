//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
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
	// noCLI names the daemons reached in their own containers whose image
	// has no ceph CLI; logs holds each container's output.
	noCLI map[string]bool
	logs  map[string][]string
	// restartOnAdd restarts the named warm daemon when a MON is added.
	restartOnAdd string
	adds         map[string]int
	removes      []string
	diagnostics  []error
	// statusEdits rewrites the named MON's mon_status response; partial
	// lists MONs whose descriptor has no container; stateErr fails the
	// named container's inspection; restartOnStatus restarts the named MON
	// while it answers mon_status. statusQueries counts the queries.
	statusEdits     map[string]func(map[string]any)
	partial         map[string]bool
	stateErr        map[string]bool
	restartOnStatus map[string]bool
	statusQueries   map[string]int
}

func newFakeMonCluster() *fakeMonCluster {
	f := &fakeMonCluster{
		topology: make(chan struct{}, 1), epoch: 3, election: 10, members: []string{"a", "b", "c"},
		quorum: map[string]bool{"a": true, "b": true, "c": true}, probing: map[string]bool{}, paused: map[string]bool{},
		stallJoin: map[string]bool{}, lagQuorum: map[string]bool{}, strand: map[string]bool{},
		session: map[string]string{"mgr.x": "a", "osd.0": "b", "mds.fs-a": "c"},
		debug:   map[string]string{"mgr.x": "1/5", "osd.0": "0/10", "mds.fs-a": "1/5"},
		started: map[string]string{"mgr.x": "t0", "osd.0": "t0", "mds.fs-a": "t0"},
		adds:    map[string]int{}, noCLI: map[string]bool{}, logs: map[string][]string{},
		statusEdits: map[string]func(map[string]any){}, partial: map[string]bool{}, stateErr: map[string]bool{},
		restartOnStatus: map[string]bool{}, statusQueries: map[string]int{},
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
		f.logs[entity] = append(f.logs[entity], "monclient(hunting): hunting for new mon")
		// A daemon whose MON left hunts the remaining quorum unless it is
		// stranded, as a refused reconnect leaves it.
		f.session[entity] = ""
		if f.strand[entity] {
			f.logs[entity] = append(f.logs[entity], "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied")
		} else {
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
		commands := map[string][]string{"mon": {"mon_status", "sessions"}, "mgr": {"mgr_status"}, "osd": {"status"}, "mds": {"status"}}[kind]
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
	case command == "status" && kind == "mds":
		state := "up:active"
		if f.session[daemon] == "" {
			state = "up:active (laggy)"
		}
		return json.Marshal(map[string]any{"cluster_fsid": "fsid-1", "whoami": 0, "id": 4200, "want_state": "up:active", "state": state, "mdsmap_epoch": 7})
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
	f.statusQueries[id]++
	if f.restartOnStatus["mon."+id] {
		f.started["mon."+id] = "t1"
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
	value["monmap"] = map[string]any{"epoch": f.epoch, "fsid": "fsid-1", "mons": mons}
	switch {
	case f.probing[id]:
		value["state"] = "probing"
	case f.quorum[id] && quorum[0] == slices.Index(f.members, id):
		value["state"], value["quorum"] = "leader", quorum
	default:
		value["state"], value["quorum"] = "peon", quorum
	}
	if edit := f.statusEdits[id]; edit != nil {
		edit(value)
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
		mon := &ceph.MonitorContainer{DaemonName: name}
		if !f.partial[name] {
			mon.Container = &fakeDaemonContainer{cluster: f, daemon: "mon." + name}
		}
		result = append(result, mon)
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

// The context getters, like the real ones, list membership under the
// topology mutex and stop waiting for it when ctx ends.
func (f *fakeMonCluster) MonitorsContext(ctx context.Context) ([]*ceph.MonitorContainer, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	defer f.unlock()
	return f.Monitors(), nil
}

func (f *fakeMonCluster) ManagersContext(ctx context.Context) ([]*ceph.ManagerContainer, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	defer f.unlock()
	return f.Managers(), nil
}

func (f *fakeMonCluster) OSDsContext(ctx context.Context) ([]*ceph.OSDContainer, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	defer f.unlock()
	return f.OSDs(), nil
}

// fakeDaemonContainer reports the generation that its cluster records.
type fakeDaemonContainer struct {
	testcontainers.Container
	cluster *fakeMonCluster
	daemon  string
}

func (c *fakeDaemonContainer) GetContainerID() string { return "cid-" + c.daemon + "-0123456789" }

func (c *fakeDaemonContainer) Logs(context.Context) (io.ReadCloser, error) {
	c.cluster.mu.Lock()
	defer c.cluster.mu.Unlock()
	return io.NopCloser(strings.NewReader(strings.Join(append([]string{"unrelated line"}, c.cluster.logs[c.daemon]...), "\n") + "\n")), nil
}

func (c *fakeDaemonContainer) State(context.Context) (*container.State, error) {
	c.cluster.mu.Lock()
	defer c.cluster.mu.Unlock()
	if c.cluster.stateErr[c.daemon] {
		return nil, errors.New("inspect: connection refused")
	}
	started, ok := c.cluster.started[c.daemon]
	if !ok {
		started = "t0"
	}
	return &container.State{Running: true, StartedAt: started}, nil
}

// fakeMDS adds the cluster's MDS to r as a daemon reached in its own
// container, through the fake's admin socket unless its image has no CLI.
func fakeMDS(f *fakeMonCluster, r *monReplacementRecorder) *monReplacementRecorder {
	r.execSocket = func(ctx context.Context, d monReplacementExecDaemon, args ...string) ([]byte, error) {
		f.mu.Lock()
		noCLI := f.noCLI[d.Daemon]
		f.mu.Unlock()
		if noCLI {
			return nil, fmt.Errorf("admin socket of %s: %w (exit 127)", d.Daemon, errMonReplacementNoCLI)
		}
		return f.AdminSocket(ctx, d.Daemon, args...)
	}
	return r.withDaemons(monReplacementExecDaemon{Daemon: "mds.fs-a", Container: &fakeDaemonContainer{cluster: f, daemon: "mds.fs-a"}, Socket: "/var/run/ceph/ceph-mds.fs-a.asok"})
}

// fakeReporter records what a run logs and how it fails, in order.
type fakeReporter struct {
	entries []string
	failed  bool
}

func (r *fakeReporter) Failed() bool { return r.failed }

func (r *fakeReporter) Helper() {}

func (r *fakeReporter) Logf(format string, args ...any) {
	r.entries = append(r.entries, "log "+fmt.Sprintf(format, args...))
}

func (r *fakeReporter) Fatalf(format string, args ...any) {
	r.failed = true
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
	r := fakeMDS(f, newMonReplacementRecorder("source", f, fakeMonReplacementBounds))
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
	for _, want := range []string{"mgr.x:a>b>c>d,", "osd.0:b>-", "mds.fs-a:c>d", "debug_monc_open=3/3 incomplete=0", "remove-c="} {
		if !strings.Contains(receipt, want) {
			t.Fatalf("receipt lacks %q: %s", want, receipt)
		}
	}
	// The windows stay open after the replacement, for the rest of the test.
	for _, daemon := range []string{"mgr.x", "osd.0", "mds.fs-a"} {
		if f.debug[daemon] != monReplacementDebugLevel {
			t.Fatalf("debug_monc of %s closed early: %v", daemon, f.debug)
		}
	}
	mds := r.Phases[5].After.Warm[2]
	if mds.Daemon != "mds.fs-a" || mds.Native["state"] != "up:active" || mds.Config["mon_host"] == "" {
		t.Fatalf("MDS snapshot %+v", mds)
	}
	finishMonReplacement(reporter, r)
	if len(reporter.entries) != 2 || reporter.entries[1] != "log MON_REPLACEMENT_DEBUG token="+r.Token+" cluster=source debug_monc_restored=3/3" {
		t.Fatalf("reported %q", reporter.entries)
	}
	if f.debug["mgr.x"] != "1/5" || f.debug["osd.0"] != "0/10" || f.debug["mds.fs-a"] != "1/5" {
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
	r := fakeMDS(f, newMonReplacementRecorder("destination", f, fakeMonReplacementBounds, ceph.DiagnosticsContainer{Role: "mirror", Name: "rbd-mirror"}))
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
	// Its response reports the requested name and the quorum's FSID, so the
	// state above is attributed to it, with the payload kept beside it.
	if bundle.Failure.FSID != "fsid-1" || joining.Identity == nil || joining.Identity.Attribution != monStatusMatched ||
		joining.Identity.ContainerID != "cid-mon.e-0123456789" || joining.Identity.Generation != "unchanged" ||
		joining.Payload == nil || !strings.Contains(joining.Payload.Data, `"state":"probing"`) {
		t.Fatalf("joining MON identity %+v payload %+v", joining.Identity, joining.Payload)
	}
	// The paused MON is an incomplete observation, not a status.
	if !slices.ContainsFunc(bundle.Failure.Incomplete, func(s string) bool { return strings.HasPrefix(s, "mon.c mon_status: ") }) {
		t.Fatalf("incomplete %v", bundle.Failure.Incomplete)
	}
	// Its expired query keeps an explicit reason and no payload digest.
	for _, mon := range bundle.Failure.Monitors {
		if mon.Name == "c" && (mon.Identity.Attribution != monStatusUnknown || mon.Payload.Digest != "" || mon.Payload.Bytes != 0 ||
			len(mon.Payload.Incomplete) != 1 || !strings.Contains(mon.Payload.Incomplete[0], "context deadline exceeded") || mon.State != "") {
			t.Fatalf("paused MON %+v payload %+v", mon, mon.Payload)
		}
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
	if debug["osd.0"] != "ok" || debug["mds.fs-a"] != "ok" || debug["mgr.x"] != "skipped: container generation changed" || f.debug["osd.0"] != "0/10" || f.debug["mgr.x"] != monReplacementDebugLevel {
		t.Fatalf("debug windows %v native %v", debug, f.debug)
	}
	// The additional container without a handle is an incomplete trace.
	if len(bundle.Traces) != 4 || bundle.Traces[2].Daemon != "mds.fs-a" || bundle.Traces[3].Error != "no container" {
		t.Fatalf("traces %+v", bundle.Traces)
	}
	// A cleanup after the bundle does not collect again; it only reports the
	// windows, which the bundle already closed.
	finishMonReplacement(reporter, r)
	if len(reporter.entries) != 3 || reporter.entries[2] != "log MON_REPLACEMENT_DEBUG token="+r.Token+" cluster=destination debug_monc_restored=2/3 debug_issues=mgr.x:skipped: container generation changed" || len(f.diagnostics) != 1 {
		t.Fatalf("reported %q", reporter.entries)
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

// A failure later in the test, after a replacement that succeeded, still
// leaves a bundle with the warm daemons' MON client traces. This is the shape
// of the original host failure: the source MDS's MON session was lost and a
// later CephFS observation timed out.
func TestMonReplacementLaterFailureLogsBundleFromCleanup(t *testing.T) {
	f := newFakeMonCluster()
	f.strand["mds.fs-a"] = true
	r := fakeMDS(f, newMonReplacementRecorder("source", f, fakeMonReplacementBounds))
	reporter := &fakeReporter{}
	runMonReplacement(reporter, t.Context(), r, fakeMonReplacementPairs)
	if len(reporter.entries) != 1 || !strings.Contains(reporter.entries[0], "mds.fs-a:c>-") {
		t.Fatalf("reported %q", reporter.entries)
	}
	reporter.Fatalf("observe CephFS snapshot: context deadline exceeded")
	finishMonReplacement(reporter, r)
	if len(reporter.entries) != 3 || !strings.HasPrefix(reporter.entries[2], "log MON_REPLACEMENT_BUNDLE ") {
		t.Fatalf("reported %q", reporter.entries)
	}
	var bundle monReplacementBundle
	if err := json.Unmarshal([]byte(strings.TrimPrefix(reporter.entries[2], "log MON_REPLACEMENT_BUNDLE ")), &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Failed.Name != "after replacement" || bundle.Failed.Seq != 7 || !bundle.Failed.Started.Equal(r.Phases[5].Ended) || len(bundle.Phases) != 6 {
		t.Fatalf("failed phase %+v", bundle.Failed)
	}
	var mds *monReplacementTrace
	for i := range bundle.Traces {
		if bundle.Traces[i].Daemon == "mds.fs-a" {
			mds = &bundle.Traces[i]
		}
	}
	if mds == nil || mds.Matched != 2 || !strings.Contains(mds.Lines[1], "(13) Permission denied") {
		t.Fatalf("MDS trace %+v", mds)
	}
	// The stranded MDS is still running and reports itself laggy on its own
	// socket; no MON lists a session for it.
	for _, warm := range bundle.Failure.Warm {
		if warm.Daemon == "mds.fs-a" && warm.Native["state"] != "up:active (laggy)" {
			t.Fatalf("MDS failure snapshot %+v", warm)
		}
	}
	if slices.ContainsFunc(bundle.Failure.Sessions, func(s monReplacementSession) bool { return s.Name == "mds.fs-a" }) {
		t.Fatalf("sessions %+v", bundle.Failure.Sessions)
	}
	if f.debug["mds.fs-a"] != "1/5" || f.debug["mgr.x"] != "1/5" || len(f.diagnostics) != 1 {
		t.Fatalf("debug %v diagnostics %v", f.debug, f.diagnostics)
	}
}

// An MDS image without the ceph CLI leaves the MDS's observations incomplete
// and opens no window on it; the replacement itself is unaffected.
func TestMonReplacementMDSWithoutCLIIsIncomplete(t *testing.T) {
	f := newFakeMonCluster()
	f.noCLI["mds.fs-a"] = true
	r := fakeMDS(f, newMonReplacementRecorder("source", f, fakeMonReplacementBounds))
	reporter := &fakeReporter{}
	runMonReplacement(reporter, t.Context(), r, fakeMonReplacementPairs)
	if len(reporter.entries) != 1 || !strings.Contains(reporter.entries[0], "debug_monc_open=2/3 debug_issues=mds.fs-a:help: admin socket of mds.fs-a: the daemon's image has no ceph CLI") {
		t.Fatalf("reported %q", reporter.entries)
	}
	if !slices.ContainsFunc(r.Phases[0].After.Incomplete, func(s string) bool { return strings.HasPrefix(s, "mds.fs-a status: help: ") }) {
		t.Fatalf("incomplete %v", r.Phases[0].After.Incomplete)
	}
	finishMonReplacement(reporter, r)
	if reporter.entries[1] != "log MON_REPLACEMENT_DEBUG token="+r.Token+" cluster=source debug_monc_restored=2/2" || f.debug["mds.fs-a"] != "1/5" {
		t.Fatalf("reported %q debug %v", reporter.entries, f.debug)
	}
}

// heldTopologyMonCluster lists membership through a real cluster value whose
// topology mutex a test holds; every other observation is the fake's.
type heldTopologyMonCluster struct {
	*fakeMonCluster
	real *ceph.Container
}

func (c heldTopologyMonCluster) MonitorsContext(ctx context.Context) ([]*ceph.MonitorContainer, error) {
	return c.real.MonitorsContext(ctx)
}

func (c heldTopologyMonCluster) ManagersContext(ctx context.Context) ([]*ceph.ManagerContainer, error) {
	return c.real.ManagersContext(ctx)
}

func (c heldTopologyMonCluster) OSDsContext(ctx context.Context) ([]*ceph.OSDContainer, error) {
	return c.real.OSDsContext(ctx)
}

// holdTopology holds the real cluster's topology mutex until the test ends.
func holdTopology(t *testing.T) *ceph.Container {
	t.Helper()
	real := servicehook.TestCluster(nil, servicehook.TestConfig{OSDs: 1})
	servicehook.Lock(real)
	t.Cleanup(func() { servicehook.Unlock(real) })
	return real
}

// awaitWhileHeld fails the test unless work returns while real's topology
// mutex is still held, so that no enumeration waited for its release.
func awaitWhileHeld[T any](t *testing.T, real *ceph.Container, limit time.Duration, work func() T) T {
	t.Helper()
	done := make(chan T, 1)
	go func() { done <- work() }()
	select {
	case result := <-done:
		if servicehook.TryLock(real) {
			servicehook.Unlock(real)
			t.Fatal("the topology mutex was released before the collection returned")
		}
		return result
	case <-time.After(limit):
		t.Fatalf("collection did not return within %s while the topology mutex was held", limit)
		panic("unreachable")
	}
}

var heldTopologyBounds = monReplacementBounds{
	Operation: 300 * time.Millisecond, Quorum: 200 * time.Millisecond, QuorumInterval: 5 * time.Millisecond,
	Query: 20 * time.Millisecond, Snapshot: time.Second, Bundle: 3 * time.Second,
}

// A failure bundle collected while another operation holds the topology
// mutex returns within its bounds, before the mutex is released. Membership
// it cannot list is incomplete, a debug window whose daemon it cannot
// identify stays explicitly unrestored, and the observations that need no
// membership, such as the MDS reached in its own container, are kept.
func TestMonReplacementCollectionReturnsWhileTopologyIsHeld(t *testing.T) {
	f := newFakeMonCluster()
	real := holdTopology(t)
	r := fakeMDS(f, newMonReplacementRecorder("source", heldTopologyMonCluster{f, real}, heldTopologyBounds))
	r.Debug = []monReplacementDebug{
		{Daemon: "mgr.x", StartedAt: "t0", Prior: "1/5", Set: true, Requested: monReplacementDebugLevel},
		{Daemon: "mds.fs-a", StartedAt: "t0", Prior: "1/5", Set: true, Requested: monReplacementDebugLevel},
	}
	f.debug["mgr.x"], f.debug["mds.fs-a"] = monReplacementDebugLevel, monReplacementDebugLevel
	failed := monReplacementPhase{Seq: 1, Name: "add d", Error: "join monitor d to quorum: context deadline exceeded", Handle: "mon.d"}
	bundle := awaitWhileHeld(t, real, 2*time.Second, func() *monReplacementBundle { return r.collect(t.Context(), failed) })
	if bundle.Failed.Error != failed.Error || bundle.Failed.Handle != "mon.d" {
		t.Fatalf("failed phase %+v", bundle.Failed)
	}
	for _, want := range []string{"list monitors: context deadline exceeded", "list managers: context deadline exceeded", "list osds: context deadline exceeded"} {
		if !slices.Contains(bundle.Failure.Incomplete, want) {
			t.Fatalf("incomplete lacks %q: %v", want, bundle.Failure.Incomplete)
		}
	}
	if !slices.Equal(bundle.Failure.Quorum, []string{"a", "b", "c"}) || len(bundle.Failure.Monitors) != 0 {
		t.Fatalf("failure snapshot %+v", bundle.Failure)
	}
	if len(bundle.Failure.Warm) != 1 || bundle.Failure.Warm[0].Daemon != "mds.fs-a" || bundle.Failure.Warm[0].Native == nil {
		t.Fatalf("warm %+v", bundle.Failure.Warm)
	}
	var owned []string
	for _, trace := range bundle.Traces {
		if trace.Daemon == "owned" {
			owned = append(owned, trace.Error)
		}
	}
	if !slices.Equal(owned, []string{"list managers: context deadline exceeded", "list osds: context deadline exceeded"}) ||
		!slices.ContainsFunc(bundle.Traces, func(trace monReplacementTrace) bool { return trace.Daemon == "mds.fs-a" && trace.Error == "" }) {
		t.Fatalf("traces %+v", bundle.Traces)
	}
	if !strings.HasPrefix(bundle.Debug[0].Restored, "skipped: identity unavailable: list managers: ") || f.debug["mgr.x"] != monReplacementDebugLevel {
		t.Fatalf("mgr window %+v debug %v", bundle.Debug[0], f.debug)
	}
	if bundle.Debug[1].Restored != "ok" || f.debug["mds.fs-a"] != "1/5" {
		t.Fatalf("mds window %+v debug %v", bundle.Debug[1], f.debug)
	}
}

// The quorum wait of a phase and the replacement's own membership list stop
// at their bounds while the topology mutex is held; neither adds a monitor
// it could not account for.
func TestMonReplacementQuorumWaitReturnsWhileTopologyIsHeld(t *testing.T) {
	f := newFakeMonCluster()
	real := holdTopology(t)
	r := newMonReplacementRecorder("source", heldTopologyMonCluster{f, real}, heldTopologyBounds)
	err := awaitWhileHeld(t, real, 2*time.Second, func() error {
		return r.phase(t.Context(), "check", []string{"a", "b", "c"}, func(context.Context) (string, error) { return "", nil })
	})
	if err == nil || !strings.Contains(err.Error(), "quorum [a b c] after check") || len(r.Phases) != 1 || r.Phases[0].QuorumProbes == 0 {
		t.Fatalf("phase %v %+v", err, r.Phases)
	}
	r.Baseline = &monReplacementSnapshot{}
	err = awaitWhileHeld(t, real, 2*time.Second, func() error { return r.replace(t.Context(), fakeMonReplacementPairs) })
	if err == nil || !strings.Contains(err.Error(), "list monitors before the replacement: context deadline exceeded") || len(f.adds) != 0 {
		t.Fatalf("replace %v adds %v", err, f.adds)
	}
}

// monStatusFixture is a mon_status of a joining MON shaped like the
// supported releases' Monitor::get_mon_status: rank -1, probing, outside a
// quorum whose monmap does not list it yet, synchronizing from a provider.
func monStatusFixture(edit func(map[string]any)) []byte {
	value := map[string]any{
		"name": "e", "rank": -1, "state": "probing", "election_epoch": 14, "quorum": []int{},
		"features":       map[string]any{"required_con": "2449958747317026820", "quorum_con": "0"},
		"outside_quorum": []string{}, "extra_probe_peers": []any{},
		"sync_provider": []any{},
		"sync":          map[string]any{"sync_provider": "mon.0 v2:10.0.0.2:3300/0", "sync_cookie": 4294967296, "sync_start_version": 12},
		"monmap": map[string]any{"epoch": 6, "fsid": "fsid-1", "mons": []any{
			map[string]any{"rank": 0, "name": "b"}, map[string]any{"rank": 1, "name": "c"}, map[string]any{"rank": 2, "name": "d"},
		}},
	}
	if edit != nil {
		edit(value)
	}
	raw, _ := json.Marshal(value)
	return raw
}

// A response is the requested MON's own only when its name and monmap FSID
// say so. A wrong or missing identity keeps the payload but no summary.
func TestMonStatusAttributionChecksReportedIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, expectedFSID string
		raw                []byte
		attribution        string
		reason             string
		reportedName       *string
		reportedFSID       *string
	}{
		{name: "matching joining MON", expectedFSID: "fsid-1", raw: monStatusFixture(nil), attribution: monStatusMatched},
		{name: "wrong name", expectedFSID: "fsid-1", raw: monStatusFixture(func(v map[string]any) { v["name"] = "x" }),
			attribution: monStatusMismatched, reason: `reported name "x", requested "e"`},
		{name: "wrong fsid", expectedFSID: "fsid-1", raw: monStatusFixture(func(v map[string]any) { v["monmap"].(map[string]any)["fsid"] = "fsid-2" }),
			attribution: monStatusMismatched, reason: `reported fsid "fsid-2", expected "fsid-1"`},
		{name: "absent name", expectedFSID: "fsid-1", raw: monStatusFixture(func(v map[string]any) { delete(v, "name") }),
			attribution: monStatusUnknown, reason: "the response reports no name"},
		{name: "empty name", expectedFSID: "fsid-1", raw: monStatusFixture(func(v map[string]any) { v["name"] = "" }),
			attribution: monStatusUnknown, reason: "the response reports no name"},
		{name: "absent fsid", expectedFSID: "fsid-1", raw: monStatusFixture(func(v map[string]any) { delete(v["monmap"].(map[string]any), "fsid") }),
			attribution: monStatusUnknown, reason: "the response reports no monmap fsid"},
		{name: "absent monmap", expectedFSID: "fsid-1", raw: monStatusFixture(func(v map[string]any) { delete(v, "monmap") }),
			attribution: monStatusUnknown, reason: "the response reports no monmap fsid"},
		{name: "absent identity", expectedFSID: "fsid-1", raw: []byte(`{"rank": 2, "state": "peon"}`),
			attribution: monStatusUnknown, reason: "the response reports no name; the response reports no monmap fsid"},
		{name: "no fsid baseline", raw: monStatusFixture(nil), attribution: monStatusUnknown, reason: "no cluster fsid was available to compare"},
		{name: "wrong name with absent fsid", expectedFSID: "fsid-1",
			raw:         monStatusFixture(func(v map[string]any) { v["name"] = "x"; delete(v["monmap"].(map[string]any), "fsid") }),
			attribution: monStatusMismatched, reason: `reported name "x", requested "e"; the response reports no monmap fsid`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := attributeMonStatus("e", tc.expectedFSID, tc.raw)
			id := local.Identity
			if local.Name != "e" || id == nil || id.Requested != "e" || id.Attribution != tc.attribution || id.Reason != tc.reason {
				t.Fatalf("status %+v identity %+v", local, id)
			}
			if local.Payload == nil || local.Payload.Data != string(tc.raw) || local.Payload.Bytes != len(tc.raw) || len(local.Payload.Digest) != 64 {
				t.Fatalf("payload %+v", local.Payload)
			}
			if tc.attribution == monStatusMatched {
				// Rank -1, a monmap without the MON and an empty quorum are
				// valid observations of a joining MON.
				if local.Error != "" || local.State != "probing" || local.Rank != -1 || local.MonMapEpoch != 6 || len(local.Quorum) != 0 || slices.Contains(local.Members, "e") {
					t.Fatalf("matched summary %+v", local)
				}
				return
			}
			// The expected name is not replaced by the reported one, and the
			// payload's state is not presented as the candidate's.
			if local.Error != "mon_status not attributed: "+tc.reason || local.State != "" || local.Rank != -1 || local.MonMapEpoch != 0 || local.Members != nil {
				t.Fatalf("unattributed summary %+v", local)
			}
		})
	}
	// Absent and empty reported fields stay distinct.
	absent := attributeMonStatus("e", "fsid-1", monStatusFixture(func(v map[string]any) { delete(v, "name") }))
	empty := attributeMonStatus("e", "fsid-1", monStatusFixture(func(v map[string]any) { v["name"] = "" }))
	wrong := attributeMonStatus("e", "fsid-1", monStatusFixture(func(v map[string]any) { v["name"] = "x"; v["monmap"].(map[string]any)["fsid"] = "fsid-2" }))
	if absent.Identity.ReportedName != nil || empty.Identity.ReportedName == nil || *empty.Identity.ReportedName != "" ||
		*wrong.Identity.ReportedName != "x" || *wrong.Identity.ReportedFSID != "fsid-2" || wrong.Identity.ExpectedFSID != "fsid-1" {
		t.Fatalf("reported identities %+v %+v %+v", absent.Identity, empty.Identity, wrong.Identity)
	}
	raw, _ := json.Marshal(absent)
	if strings.Contains(string(raw), `"reported_name"`) {
		t.Fatalf("absent name serialized: %s", raw)
	}
}

// The retained payload keeps what the summary drops: two synchronization
// sessions with an identical summary stay distinguishable, and an absent sync
// object stays absent. Malformed and oversized responses keep their bounded,
// redacted evidence with an explicit reason.
func TestMonStatusPayloadIsRetainedBoundedAndRedacted(t *testing.T) {
	first := attributeMonStatus("e", "fsid-1", monStatusFixture(nil))
	second := attributeMonStatus("e", "fsid-1", monStatusFixture(func(v map[string]any) {
		v["sync"] = map[string]any{"sync_provider": "mon.1 v2:10.0.0.3:3300/0", "sync_cookie": 8589934592, "sync_start_version": 15}
	}))
	none := attributeMonStatus("e", "fsid-1", monStatusFixture(func(v map[string]any) { delete(v, "sync") }))
	summary := func(l monLocalStatus) monLocalStatus { l.Identity, l.Payload = nil, nil; return l }
	if fmt.Sprint(summary(first)) != fmt.Sprint(summary(second)) || first.Payload.Digest == second.Payload.Digest ||
		!strings.Contains(second.Payload.Data, `"sync_cookie":8589934592`) || strings.Contains(none.Payload.Data, `"sync"`) {
		t.Fatalf("sync sessions %+v %+v %+v", first.Payload, second.Payload, none.Payload)
	}

	malformed := attributeMonStatus("e", "fsid-1", []byte(`{"name":"e","state":"prob`))
	if malformed.Identity.Attribution != monStatusUnknown || malformed.Payload.Data != `{"name":"e","state":"prob` ||
		len(malformed.Payload.Incomplete) != 1 || !strings.HasPrefix(malformed.Payload.Incomplete[0], "malformed: ") || malformed.State != "" {
		t.Fatalf("malformed %+v %+v", malformed, malformed.Payload)
	}

	big := monStatusFixture(func(v map[string]any) { v["extra_probe_peers"] = []string{strings.Repeat("é", monStatusRetainBytes)} })
	oversized := attributeMonStatus("e", "fsid-1", big)
	if oversized.Identity.Attribution != monStatusMatched || oversized.State != "probing" || !oversized.Payload.Truncated ||
		oversized.Payload.Bytes != len(big) || len(oversized.Payload.Data) > monStatusRetainBytes || !utf8.ValidString(oversized.Payload.Data) ||
		len(oversized.Payload.Incomplete) != 1 || !strings.HasPrefix(oversized.Payload.Incomplete[0], "oversized: retained ") {
		t.Fatalf("oversized payload bytes=%d retained=%d %+v", oversized.Payload.Bytes, len(oversized.Payload.Data), oversized.Payload.Incomplete)
	}

	secret := attributeMonStatus("e", "fsid-1", monStatusFixture(func(v map[string]any) { v["key"] = "AQBsecretvalue==" }))
	if strings.Contains(secret.Payload.Data, "AQBsecretvalue") || !strings.Contains(secret.Payload.Data, `"key":"[REDACTED]"`) {
		t.Fatalf("redaction %s", secret.Payload.Data)
	}
}

// In a snapshot, a MON whose response reports another identity keeps the
// payload as unattributed, incomplete evidence. A partial descriptor without
// a container, an inspection that fails and a restart during the query are
// reported as such; each MON is queried once and no membership changes.
func TestMonReplacementSnapshotAttributesMonStatus(t *testing.T) {
	f := newFakeMonCluster()
	f.members, f.quorum = []string{"a", "b", "c", "d", "e", "x"}, map[string]bool{"a": true, "b": true, "c": true}
	f.probing["d"], f.probing["e"] = true, true
	f.statusEdits["b"] = func(v map[string]any) { v["name"] = "x" }
	f.statusEdits["c"] = func(v map[string]any) { v["monmap"].(map[string]any)["fsid"] = "fsid-2" }
	f.statusEdits["d"] = func(v map[string]any) { delete(v, "name"); delete(v["monmap"].(map[string]any), "fsid") }
	f.partial["e"] = true
	f.stateErr["mon.x"] = true
	f.restartOnStatus["mon.a"] = true
	r := newMonReplacementRecorder("source", f, fakeMonReplacementBounds)
	s := r.snapshot(t.Context(), false)
	if s.FSID != "fsid-1" || !slices.Equal(s.Quorum, []string{"a", "b", "c"}) {
		t.Fatalf("quorum %q %v", s.FSID, s.Quorum)
	}
	mons := map[string]monLocalStatus{}
	for _, mon := range s.Monitors {
		mons[mon.Name] = mon
	}
	for name, want := range map[string]string{"a": monStatusMatched, "b": monStatusMismatched, "c": monStatusMismatched, "d": monStatusUnknown, "e": monStatusMatched, "x": monStatusMatched} {
		mon := mons[name]
		if mon.Identity == nil || mon.Identity.Attribution != want || mon.Identity.Requested != name || mon.Identity.ExpectedFSID != "fsid-1" || mon.Identity.FSIDSource != "quorum" || mon.Payload == nil || mon.Payload.Data == "" {
			t.Fatalf("mon.%s %+v identity %+v", name, mon, mon.Identity)
		}
		if f.statusQueries[name] != 1 {
			t.Fatalf("mon.%s queried %d times", name, f.statusQueries[name])
		}
	}
	if b := mons["b"]; b.State != "" || b.Rank != -1 || *b.Identity.ReportedName != "x" {
		t.Fatalf("misnamed response attributed: %+v", b)
	}
	if c := mons["c"]; c.State != "" || *c.Identity.ReportedFSID != "fsid-2" {
		t.Fatalf("foreign fsid attributed: %+v", c)
	}
	if mons["a"].Identity.Generation != "changed" || mons["a"].Identity.StartedBefore != "t0" || mons["a"].Identity.StartedAfter != "t1" ||
		mons["e"].Identity.Generation != "unavailable: no container" || mons["e"].Identity.ContainerID != "" ||
		mons["x"].Identity.Generation != "unavailable: before the query: state: inspect: connection refused; after the query: state: inspect: connection refused" ||
		mons["x"].Identity.ContainerID != "cid-mon.x-0123456789" || mons["d"].Identity.Generation != "unchanged" {
		t.Fatalf("generations a=%+v e=%+v x=%+v", mons["a"].Identity, mons["e"].Identity, mons["x"].Identity)
	}
	for _, want := range []string{
		`mon.b mon_status: mon_status not attributed: reported name "x", requested "b"`,
		`mon.c mon_status: mon_status not attributed: reported fsid "fsid-2", expected "fsid-1"`,
		"mon.d mon_status: mon_status not attributed: the response reports no name; the response reports no monmap fsid",
		"mon.a generation: changed during mon_status from t0 to t1",
		"mon.e generation: unavailable: no container",
	} {
		if !slices.Contains(s.Incomplete, want) {
			t.Fatalf("incomplete lacks %q: %q", want, s.Incomplete)
		}
	}
	if len(f.adds) != 0 || len(f.removes) != 0 {
		t.Fatalf("membership changed: adds %v removes %v", f.adds, f.removes)
	}
}

// Without a quorum FSID in its own snapshot, a response is compared with the
// baseline's, and without either it stays unattributed.
func TestMonReplacementSnapshotFSIDBaseline(t *testing.T) {
	f := newFakeMonCluster()
	r := newMonReplacementRecorder("source", quorumlessMonCluster{f}, fakeMonReplacementBounds)
	s := r.snapshot(t.Context(), false)
	if s.FSID != "" || s.QuorumError == "" || len(s.Monitors) != 3 || s.Monitors[0].Identity.Attribution != monStatusUnknown ||
		s.Monitors[0].Identity.Reason != "no cluster fsid was available to compare" || s.Monitors[0].State != "" {
		t.Fatalf("snapshot without a baseline %+v %+v", s, s.Monitors[0].Identity)
	}
	r.Baseline = &monReplacementSnapshot{FSID: "fsid-1"}
	s = r.snapshot(t.Context(), false)
	if id := s.Monitors[0].Identity; id.Attribution != monStatusMatched || id.ExpectedFSID != "fsid-1" || id.FSIDSource != "baseline quorum" || s.Monitors[0].State != "leader" {
		t.Fatalf("snapshot with a baseline %+v", id)
	}
}

// quorumlessMonCluster fails every quorum query.
type quorumlessMonCluster struct{ *fakeMonCluster }

func (quorumlessMonCluster) QuorumStatus(context.Context) (ceph.QuorumStatus, error) {
	return ceph.QuorumStatus{}, errors.New("quorum_status: timed out")
}
