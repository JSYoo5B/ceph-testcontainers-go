//go:build all || integration

package cephfs_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// debugMSConfig is a configuration file that has every MON, MGR, OSD and MDS
// log each msgr2 session that becomes READY (debug_ms 1). Clients keep their
// default, so the fixture's CLI output is unchanged.
func debugMSConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ceph.conf")
	if err := os.WriteFile(path, []byte("[mon]\ndebug_ms = 1\n[mgr]\ndebug_ms = 1\n[osd]\ndebug_ms = 1\n[mds]\ndebug_ms = 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// messengerDaemon is one owned daemon and the container whose log it writes.
type messengerDaemon struct {
	Name string
	Ctr  testcontainers.Container
}

// messengerDaemons lists the cluster's MONs, MGRs, OSDs and the filesystem's
// MDSs, in that order.
func messengerDaemons(t *testing.T, m *harness.CephFSSingleMDS) []messengerDaemon {
	t.Helper()
	var daemons []messengerDaemon
	for _, mon := range clusteraccess.Monitors(t, m.Cluster) {
		daemons = append(daemons, messengerDaemon{"mon." + mon.DaemonName, mon.Container})
	}
	for _, mgr := range clusteraccess.Managers(t, m.Cluster) {
		daemons = append(daemons, messengerDaemon{"mgr." + mgr.DaemonName, mgr.Container})
	}
	for _, osd := range clusteraccess.OSDs(t, m.Cluster) {
		daemons = append(daemons, messengerDaemon{fmt.Sprintf("osd.%d", osd.ID), osd.Container})
	}
	for _, mds := range m.FS.MDSs() {
		daemons = append(daemons, messengerDaemon{"mds." + mds.ID, mds.Container})
	}
	return daemons
}

// generation names the process a container's log belongs to: the container
// and the time Docker last started it.
func generation(t *testing.T, ctx context.Context, ctr testcontainers.Container) string {
	t.Helper()
	inspect, err := ctr.Inspect(ctx)
	if err != nil || inspect.State == nil {
		t.Fatalf("inspect %s: %v", ctr.GetContainerID(), err)
	}
	return ctr.GetContainerID() + "@" + inspect.State.StartedAt
}

// logMark is the number of complete lines a daemon's log had: the lines after
// it are the window's. The last element of a split log can be an unfinished
// line, so it belongs to the window.
func logMark(t *testing.T, ctx context.Context, ctr testcontainers.Container) int {
	t.Helper()
	return len(harness.ContainerLogLines(t, ctx, ctr)) - 1
}

// tellMessengers returns the connections and self reports of every messenger
// of daemon through "ceph tell <daemon> messenger dump".
func tellMessengers(t *testing.T, ctx context.Context, cluster *ceph.Container, daemon string) ([]harness.MessengerConnection, []harness.MessengerSelf) {
	t.Helper()
	data, err := cluster.Ceph(ctx, "tell", daemon, "messenger", "dump")
	if err != nil {
		t.Fatalf("%s messenger dump: %v", daemon, err)
	}
	names, err := harness.ParseMessengerCatalog(data)
	if err != nil {
		t.Fatalf("%s messenger dump: %v", daemon, err)
	}
	var connections []harness.MessengerConnection
	var selves []harness.MessengerSelf
	for _, name := range names {
		data, err := cluster.Ceph(ctx, "tell", daemon, "messenger", "dump", name)
		if err != nil {
			t.Fatalf("%s messenger dump %s: %v", daemon, name, err)
		}
		c, err := harness.ParseMessengerConnections(data)
		if err != nil {
			t.Fatalf("%s messenger dump %s: %v", daemon, name, err)
		}
		self, err := harness.ParseMessengerSelf(data)
		if err != nil {
			t.Fatalf("%s messenger dump %s: %v", daemon, name, err)
		}
		connections, selves = append(connections, c...), append(selves, self)
	}
	return connections, selves
}

// fsDumpMDSAddrs returns each MDS's advertised address vector from "fs
// dump", for the filesystems' ranks and the standbys alike.
func fsDumpMDSAddrs(t *testing.T, ctx context.Context, cluster *ceph.Container) map[string][]harness.MessengerAddress {
	t.Helper()
	data, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal("fs dump:", err)
	}
	type info struct {
		Name  string `json:"name"`
		Addrs struct {
			Addrvec []harness.MessengerAddress `json:"addrvec"`
		} `json:"addrs"`
	}
	var dump struct {
		Standbys    []info `json:"standbys"`
		Filesystems []struct {
			MDSMap struct {
				Info map[string]info `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		t.Fatalf("fs dump: %v: %.400s", err, data)
	}
	result := map[string][]harness.MessengerAddress{}
	for _, s := range dump.Standbys {
		result[s.Name] = s.Addrs.Addrvec
	}
	for _, fs := range dump.Filesystems {
		for _, i := range fs.MDSMap.Info {
			result[i.Name] = i.Addrs.Addrvec
		}
	}
	return result
}

// v2SessionsDaemon is one daemon's part of the v2 sessions receipt.
type v2SessionsDaemon struct {
	Generation string         `json:"generation"`
	Lines      int            `json:"lines"`
	Ready      int            `json:"ready"`
	Peers      map[string]int `json:"peers"`
	Problems   []string       `json:"problems,omitempty"`
	Dumped     int            `json:"dumped_ready"`
	Listening  []string       `json:"listening_messengers,omitempty"`
}

// testCephFSMessengerV2Sessions starts a CephFS cluster without Messenger v1
// whose MONs, MGRs, OSDs and MDS log their READY sessions, mounts a seed
// client, and requires of every owned daemon that it advertises and binds v2
// only and that every READY session it logged is msgr2.1. With messenger
// dump each daemon's READY connections must be msgr2.1 as well, and each
// messenger that listens must advertise v2 addresses only.
func testCephFSMessengerV2Sessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx, ceph.WithConfigFile(debugMSConfig(t)))
	helper := harness.ToolkitContainer(t, m.Cluster)
	dump := harness.CapabilitySupported(t, ctx, m.Cluster, harness.CapabilityMessengerDump)
	receipt := map[string]any{"native_version": m.Version, "messenger_dump": dump}
	daemons := map[string]*v2SessionsDaemon{}
	receipt["daemons"] = daemons
	defer func() {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("MESSENGER_V2_SESSIONS_RECEIPT %s", data)
	}()

	var seed cephFSMetricsFile
	if err := json.Unmarshal([]byte(harness.LastJSONLine(harness.ExecOutput(t, ctx, helper, "python3", "-c", cephFSMetricsSeed, m.FS.FilesystemName, "/v2-sessions"))), &seed); err != nil || seed.Client == 0 {
		t.Fatalf("seed: %+v, %v", seed, err)
	}
	receipt["seed_client"] = seed.Client

	addrs := fsDumpMDSAddrs(t, ctx, m.Cluster)
	receipt["mds_addrs"] = addrs
	for _, mds := range m.FS.MDSs() {
		vector := addrs[mds.ID]
		if len(vector) == 0 || slices.ContainsFunc(vector, func(a harness.MessengerAddress) bool { return a.Type != "v2" }) {
			t.Fatalf("mds.%s advertises %+v in fs dump, not only v2", mds.ID, vector)
		}
	}
	for option, want := range map[string]string{"ms_bind_msgr1": "false", "ms_bind_msgr2": "true"} {
		if got := m.DaemonConfig(t, ctx, option); got != want {
			t.Fatalf("mds.%s %s = %q, want %q", m.MDS.Name, option, got, want)
		}
	}

	var problems []string
	for _, d := range messengerDaemons(t, m) {
		lines := harness.ContainerLogLines(t, ctx, d.Ctr)
		entry := &v2SessionsDaemon{Generation: generation(t, ctx, d.Ctr), Lines: len(lines), Peers: map[string]int{}}
		daemons[d.Name] = entry
		binding := harness.ReadyRevisionBinding{Daemon: d.Name, Generation: entry.Generation, Capture: fmt.Sprintf("container log lines 0-%d", len(lines))}
		observations := harness.ParseReadyRevisionLines(binding, lines)
		entry.Ready = len(observations)
		for _, o := range observations {
			entry.Peers[o.PeerType]++
		}
		entry.Problems = harness.ReadyRevision1Problems(observations)
		if d.Name == "mds."+m.MDS.Name && !slices.ContainsFunc(observations, func(o harness.ReadyRevisionObservation) bool {
			return o.PeerType == "client" && o.PeerID == strconv.FormatUint(seed.Client, 10) && o.Revision == harness.ReadyRevision1
		}) {
			entry.Problems = append(entry.Problems, fmt.Sprintf("no msgr2.1 READY session of the seed client.%d", seed.Client))
		}
		if dump {
			connections, selves := tellMessengers(t, ctx, m.Cluster, d.Name)
			for _, c := range connections {
				if !c.Ready() {
					continue
				}
				entry.Dumped++
				if !c.Revision1 {
					entry.Problems = append(entry.Problems, fmt.Sprintf("dumped %s connection to %s.%d is not msgr2.1", c.Messenger, c.PeerType, c.PeerID))
				}
			}
			for _, self := range selves {
				if self.ListenSockets == 0 {
					continue
				}
				entry.Listening = append(entry.Listening, self.Name)
				if len(self.Addrs) == 0 || slices.ContainsFunc(self.Addrs, func(a harness.MessengerAddress) bool { return a.Type != "v2" }) {
					entry.Problems = append(entry.Problems, fmt.Sprintf("messenger %s listens with %+v, not only v2", self.Name, self.Addrs))
				}
			}
			if entry.Dumped == 0 {
				entry.Problems = append(entry.Problems, "messenger dump has no READY connection")
			}
		}
		for _, problem := range entry.Problems {
			problems = append(problems, d.Name+": "+problem)
		}
	}
	// The MDS's own sessions reach every other daemon type and the client.
	mds := daemons["mds."+m.MDS.Name]
	if mds == nil {
		t.Fatalf("mds.%s is not an owned daemon", m.MDS.Name)
	}
	for _, peer := range []string{"mon", "mgr", "osd", "client"} {
		if mds.Peers[peer] == 0 {
			problems = append(problems, fmt.Sprintf("mds.%s logged no READY session with a %s", m.MDS.Name, peer))
		}
	}
	receipt["problems"] = problems
	if len(problems) != 0 {
		t.Fatalf("v2 sessions: %s", strings.Join(problems, "; "))
	}
}

// mdsContainer returns the owned container of the active MDS.
func mdsContainer(t *testing.T, m *harness.CephFSSingleMDS) *cephfs.MDS {
	t.Helper()
	for _, member := range m.FS.MDSs() {
		if member.ID == m.MDS.Name {
			return member
		}
	}
	t.Fatalf("mds.%s is not an owned daemon", m.MDS.Name)
	return nil
}

// cephFSReconnectActor mounts, creates the file and writes and fsyncs record
// 0, then publishes receipt 1 with its native client ID. Behind the "inject"
// gate a worker thread writes, fsyncs and stats record after record at its
// offset while the main thread sets ms_inject_socket_failures to the given
// rate for 2 s and back to 0; the worker goes on for 3 s more and stops at
// its first error. Receipt 2 carries the number of acknowledged records, the
// first error and the file size the fd reports. Record k is 4096 bytes of
// k%251. The fd closes after the "finish" gate and receipt 3 follows.
// Arguments: filesystem, work directory, token, path, the injection rate.
const cephFSReconnectActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
import threading
path, rate = sys.argv[4].encode(), sys.argv[5]
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.mount(filesystem_name=name.encode())
fd = fs.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
fs.write(fd, bytes([0]) * 4096, 0)
fs.fsync(fd, 0)
publish(1, client=fs.get_instance_id(), count=1)
gate("inject")
state = dict(count=1, error="")
stop = threading.Event()
def work():
    while not stop.is_set():
        k = state["count"]
        try:
            written = fs.write(fd, bytes([k % 251]) * 4096, k * 4096)
            fs.fsync(fd, 0)
            fs.stat(path)
        except Exception as e:
            state["error"] = repr(e)
            return
        if written != 4096:
            state["error"] = "short write %d" % written
            return
        state["count"] = k + 1
worker = threading.Thread(target=work)
worker.start()
fs.conf_set("ms_inject_socket_failures", rate)
time.sleep(2)
fs.conf_set("ms_inject_socket_failures", "0")
time.sleep(3)
stop.set()
worker.join(60)
if worker.is_alive():
    sys.exit("the workload did not stop")
publish(2, client=fs.get_instance_id(), count=state["count"], error=state["error"], size=fs.fstat(fd).st_size)
gate("finish")
fs.close(fd)
fs.unmount()
fs.shutdown()
publish(3, closed=True)
`

// reconnectRecords is the content of count records of cephFSReconnectActor.
func reconnectRecords(count int64) []byte {
	var data bytes.Buffer
	for k := range count {
		data.Write(bytes.Repeat([]byte{byte(k % 251)}, 4096))
	}
	return data.Bytes()
}

// injectedFailures counts the socket failures that the client's messenger
// logged, by the policy of the connection.
func injectedFailures(stderr []byte) (lossless, lossy int) {
	for _, line := range strings.Split(string(stderr), "\n") {
		if !strings.Contains(line, "injecting socket failure") {
			continue
		}
		switch {
		case strings.Contains(line, " l=0)"):
			lossless++
		case strings.Contains(line, " l=1)"):
			lossy++
		}
	}
	return lossless, lossy
}

// testCephFSMessengerReconnect runs the reconnect recipe on one active MDS
// whose MDS and OSDs log READY sessions:
//
//  1. A libcephfs actor mounts, writes and fsyncs one record and publishes
//     its client ID; its MDS session is open and, with messenger dump, its
//     lossless MDS connection is READY with connect_seq 0.
//  2. The daemons' log lines so far are the baseline. Behind the gate the
//     actor injects socket failures into its own messenger for 2 s while it
//     goes on writing, fsyncing and statting records, and stops 3 s later.
//  3. The MDS's READY lines of the client in the window must be reconnects
//     of the one lossless session, the OSDs' must be new lossy sessions, and
//     with messenger dump the MDS's connection must be the same object with
//     the last logged connect_seq. The MDS session stays open under one MDS
//     generation and an independent client reads every acknowledged record.
//
// The receipt is logged on every outcome.
func testCephFSMessengerReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	m := harness.StartCephFSSingleMDS(t, ctx, ceph.WithConfigFile(debugMSConfig(t)))
	helper := harness.ToolkitContainer(t, m.Cluster)
	dump := harness.CapabilitySupported(t, ctx, m.Cluster, harness.CapabilityMessengerDump)
	mds := mdsContainer(t, m)
	osds := clusteraccess.OSDs(t, m.Cluster)
	const path, rate = "/messenger-reconnect", "40"

	receipt := map[string]any{"native_version": m.Version, "messenger_dump": dump, "mds": m.MDS, "rate": rate, "window": "2s injected, 3s after"}
	var lossless, lossy, dumped reconnectOutcome
	defer func() {
		receipt["lossless"], receipt["lossy"] = lossless, lossy
		if dump {
			receipt["dumped"] = dumped
		}
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("MESSENGER_RECONNECT_RECEIPT %s", data)
	}()

	actor := harness.StartCephFSActor(t, ctx, m.Cluster, m.Client, cephFSReconnectActor, m.FS.FilesystemName, "/tmp/messenger-reconnect", path, rate)
	first := actor.Receipt(t, ctx, 1)
	client := first.Client
	receipt["client"] = client
	if client == 0 {
		t.Fatalf("actor identity: %+v", first)
	}
	if state := string(harness.CephFSRawSession(t, ctx, m.Cluster, m.MDS.Name, client)["state"]); state != `"open"` {
		t.Fatalf("client.%d session before the window: %s", client, state)
	}
	var mdsBefore []harness.MessengerConnection
	osdBefore := map[string][]harness.MessengerConnection{}
	if dump {
		mdsBefore, _ = tellMessengers(t, ctx, m.Cluster, "mds."+m.MDS.Name)
		for _, osd := range osds {
			osdBefore[fmt.Sprintf("osd.%d", osd.ID)], _ = tellMessengers(t, ctx, m.Cluster, fmt.Sprintf("osd.%d", osd.ID))
		}
	}
	mdsMark := logMark(t, ctx, mds)
	osdMarks := make([]int, len(osds))
	for i, osd := range osds {
		osdMarks[i] = logMark(t, ctx, osd)
	}

	actor.Open(t, ctx, "inject")
	worked := actor.Receipt(t, ctx, 2)
	receipt["records"], receipt["actor_error"], receipt["size"] = worked.Count, worked.Error, worked.Size
	if worked.Client != client || worked.PID != first.PID || worked.Error != "" || worked.Count < 2 || worked.Size != worked.Count*4096 {
		t.Fatalf("the workload under injected failures: %+v", worked)
	}
	var mdsAfter []harness.MessengerConnection
	osdAfter := map[string][]harness.MessengerConnection{}
	if dump {
		mdsAfter, _ = tellMessengers(t, ctx, m.Cluster, "mds."+m.MDS.Name)
		for _, osd := range osds {
			osdAfter[fmt.Sprintf("osd.%d", osd.ID)], _ = tellMessengers(t, ctx, m.Cluster, fmt.Sprintf("osd.%d", osd.ID))
		}
	}
	session := harness.CephFSRawSession(t, ctx, m.Cluster, m.MDS.Name, client)
	receipt["session_after"] = harness.CephFSRawSessionSummary(session)
	status, err := m.FS.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 || status.Active[0].GID != m.MDS.GID {
		t.Fatalf("MDS status after the window: %+v, %v", status, err)
	}
	if state := string(session["state"]); state != `"open"` {
		t.Fatalf("client.%d session after the window: %s", client, state)
	}

	split := func(ctr testcontainers.Container, mark int) (before, window []harness.ReadySession) {
		lines := harness.ContainerLogLines(t, ctx, ctr)
		var unusable []string
		before, unusable = harness.ParseReadySessions(lines[:mark])
		window, more := harness.ParseReadySessions(lines[mark:])
		for _, line := range append(unusable, more...) {
			if strings.Contains(line, "entity=client."+strconv.FormatUint(client, 10)+" ") {
				t.Fatalf("an unusable READY line of the client: %s", line)
			}
		}
		return before, window
	}
	mdsBase, mdsWindow := split(mds, mdsMark)
	var osdBase, osdWindow []harness.ReadySession
	for i, osd := range osds {
		before, window := split(osd, osdMarks[i])
		osdBase, osdWindow = append(osdBase, before...), append(osdWindow, window...)
	}
	lossless = judgeLosslessReconnect(mdsBase, mdsWindow, client)
	lossy = judgeLossyResets(osdBase, osdWindow, client)
	if dump && lossless.Verdict == reconnectObserved {
		dumped = judgeDumpedReconnect(mdsBefore, mdsAfter, client, lossless.Sessions[len(lossless.Sessions)-1])
		// A reset lossy session is a new connection object on an OSD.
		if dumped.Verdict == reconnectObserved {
			replaced := 0
			for daemon, after := range osdAfter {
				for _, c := range after {
					known := slices.ContainsFunc(osdBefore[daemon], func(b harness.MessengerConnection) bool { return b.ConnID == c.ConnID })
					if c.PeerType == "client" && c.GlobalID == client && c.Ready() && !known {
						replaced++
					}
				}
			}
			receipt["dumped_new_osd_connections"] = replaced
			if replaced == 0 {
				dumped = reconnectOutcome{Reason: "no OSD dumps a new READY connection of the client"}
			}
		}
	}

	out := harness.ExecOutput(t, ctx, helper, "python3", "-c", cephFSMetricsReader, m.FS.FilesystemName, path, strconv.FormatInt(worked.Size, 10))
	var read cephFSMetricsFile
	want := sha256.Sum256(reconnectRecords(worked.Count))
	if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &read); err != nil || read.Client == 0 || read.Client == client ||
		int64(read.Size) != worked.Size || read.SHA256 != hex.EncodeToString(want[:]) {
		t.Fatalf("independent readback: %s, %v", out, err)
	}
	receipt["independent_read"] = read

	actor.Open(t, ctx, "finish")
	if closed := actor.Receipt(t, ctx, 3); !closed.Closed {
		t.Fatalf("actor did not close: %+v", closed)
	}
	actor.Wait(t, ctx)
	injectedLossless, injectedLossy := injectedFailures(actor.Result.Stderr)
	receipt["injected_lossless"], receipt["injected_lossy"] = injectedLossless, injectedLossy

	for _, check := range []struct {
		name    string
		outcome reconnectOutcome
	}{{"lossless MDS session", lossless}, {"lossy OSD sessions", lossy}} {
		if check.outcome.Verdict != reconnectObserved {
			t.Errorf("%s %s: %s", check.name, check.outcome.Verdict, check.outcome.Reason)
		}
	}
	if dump && dumped.Verdict != reconnectObserved {
		t.Errorf("messenger dump %s: %s", dumped.Verdict, dumped.Reason)
	}
	if injectedLossless == 0 || injectedLossy == 0 {
		t.Errorf("the client logged %d lossless and %d lossy injected socket failures", injectedLossless, injectedLossy)
	}
}
