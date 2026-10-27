//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_correlated_outage))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
)

const (
	outageLedgerPool   = "tc-outage-ledger"
	outageBaselinePool = "tc-outage-baseline"
)

// TestCorrelatedLogicalHostOutageLedger stops the MON, the active MGR and the
// OSD of one logical host, one after the other, while a retained librados
// client outside that host keeps writing unique objects into a ledger. The
// hosts are labels on one container engine: this is a correlated stop of three
// daemons, not an atomic host loss, and it says nothing about a physical host,
// power, switch or disk failure. Three MONs lose one member, so quorum
// survives; the boundary with two lost domains belongs to a separate scenario.
func TestCorrelatedLogicalHostOutageLedger(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
	defer cancel()
	hosts := []string{"host-a", "host-b", "host-c"}
	layout := make([]ceph.OSDConfig, 0, len(hosts))
	for i, host := range hosts {
		layout = append(layout, ceph.OSDConfig{Host: host, Rack: "rack-" + string(rune('a'+i)), Root: "tc-storage", DeviceClass: "fast"})
	}
	cluster, client := newServiceCluster(t, ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithStartupTimeout(3*time.Minute),
		ceph.WithInitialOSDs(layout...), ceph.WithPoolDefaults(3, 2), ceph.WithDefaultCRUSHRoot("tc-storage"))
	for _, pg := range []struct {
		name string
		num  int
	}{{outageLedgerPool, 1}, {outageBaselinePool, 8}} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pg.name, PGNum: pg.num, Application: "rados", Replicas: 3, MinSize: 2,
			FailureDomain: "host", CRUSHRoot: "tc-storage", DeviceClass: "fast"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// Map 1: native CRUSH placement of the OSDs, read from Ceph.
	osds := clusteraccess.OSDs(t, cluster)
	hostByOSD := map[int]string{}
	for _, osd := range osds {
		var location struct {
			OSD   int `json:"osd"`
			CRUSH struct {
				Host string `json:"host"`
			} `json:"crush_location"`
		}
		outageJSON(t, ctx, cluster, &location, "osd", "find", strconv.Itoa(osd.ID))
		if location.OSD != osd.ID || location.CRUSH.Host != osd.Placement().Host || !slices.Contains(hosts, location.CRUSH.Host) {
			t.Fatalf("osd.%d native CRUSH host %q differs from its requested %q", osd.ID, location.CRUSH.Host, osd.Placement().Host)
		}
		hostByOSD[osd.ID] = location.CRUSH.Host
	}
	if len(osds) != 3 || len(clusteraccess.Monitors(t, cluster)) != 3 || len(clusteraccess.Managers(t, cluster)) != 2 {
		t.Fatalf("membership: %d OSDs, %d MONs, %d MGRs", len(osds), len(clusteraccess.Monitors(t, cluster)), len(clusteraccess.Managers(t, cluster)))
	}
	states, err := cluster.OSDStates(ctx)
	if err != nil || len(states) != 3 {
		t.Fatalf("initial OSD identities: %v %+v", err, states)
	}
	for _, state := range states {
		if !state.Up || !state.In || state.UUID == "" {
			t.Fatalf("initial OSD identity: %+v", state)
		}
	}
	acting := func(object string) []int {
		var mapping struct {
			Acting []int `json:"acting"`
		}
		outageJSON(t, ctx, cluster, &mapping, "osd", "map", outageLedgerPool, object)
		return mapping.Acting
	}
	// The ledger pool has one PG, so every ledger object is in it. All replicas
	// must lie in three different logical hosts before anything is stopped.
	span := map[string]bool{}
	for _, id := range acting("op-probe") {
		span[hostByOSD[id]] = true
	}
	if len(acting("op-probe")) != 3 || len(span) != 3 {
		t.Fatalf("ledger PG replicas %v do not span three hosts", acting("op-probe"))
	}

	// Map 2: outage membership, which also places MONs and MGRs. Nothing in
	// Ceph says where they live; this manifest is the simulation's claim.
	var quorum struct {
		Leader string   `json:"quorum_leader_name"`
		Epoch  int      `json:"election_epoch"`
		Names  []string `json:"quorum_names"`
	}
	outageJSON(t, ctx, cluster, &quorum, "quorum_status")
	managerBefore, err := cluster.ManagerStatus(ctx)
	if err != nil || !managerBefore.Available || len(managerBefore.Standbys) != 1 || len(quorum.Names) != 3 {
		t.Fatalf("initial MON/MGR identities: %+v %+v %v", quorum, managerBefore, err)
	}
	standbyName := managerBefore.Standbys[0].Name
	owned := map[string]bool{}
	var members []outageMember
	monitors, managers := clusteraccess.Monitors(t, cluster), clusteraccess.Managers(t, cluster)
	for i, mon := range monitors {
		owned[mon.GetContainerID()] = true
		name := mon.DaemonName
		members = append(members, outageMember{Role: "mon", Name: name, Host: hosts[i], Container: mon.GetContainerID(), daemon: outageContainer{mon.Container},
			current: func() string {
				for _, m := range clusteraccess.Monitors(t, cluster) {
					if m.DaemonName == name {
						return m.GetContainerID()
					}
				}
				return ""
			}})
	}
	managerHost := map[string]string{managerBefore.ActiveName: hosts[0], standbyName: hosts[1]}
	for _, mgr := range managers {
		owned[mgr.GetContainerID()] = true
		name := mgr.DaemonName
		host, ok := managerHost[name]
		if !ok {
			t.Fatalf("manager %s is neither the active nor the standby identity", name)
		}
		members = append(members, outageMember{Role: "mgr", Name: name, Host: host, Container: mgr.GetContainerID(), daemon: outageContainer{mgr.Container},
			current: func() string {
				for _, m := range clusteraccess.Managers(t, cluster) {
					if m.DaemonName == name {
						return m.GetContainerID()
					}
				}
				return ""
			}})
	}
	for _, osd := range osds {
		owned[osd.GetContainerID()] = true
		id := osd.ID
		members = append(members, outageMember{Role: "osd", Name: strconv.Itoa(id), Host: hostByOSD[id], Container: osd.GetContainerID(), daemon: outageContainer{osd.Container},
			current: func() string {
				for _, o := range clusteraccess.OSDs(t, cluster) {
					if o.ID == id {
						return o.GetContainerID()
					}
				}
				return ""
			}})
	}
	manifest, err := newOutageManifest(hosts, members, owned)
	if err != nil {
		t.Fatal(err)
	}
	if owned[client.GetContainerID()] || owned[clusteraccess.ControlContainer(t, cluster).GetContainerID()] {
		t.Fatal("the client or control container is inside the outage membership")
	}
	hostA := manifest.onHost(hosts[0])
	for _, host := range hosts {
		roles := map[string]int{}
		for _, m := range manifest.onHost(host) {
			roles[m.Role]++
		}
		if roles["mon"] != 1 || roles["osd"] != 1 || roles["mgr"] > 1 {
			t.Fatalf("host %s membership %v", host, roles)
		}
	}
	if len(hostA) != 3 {
		t.Fatalf("host-a membership: %+v", hostA)
	}
	var monA, mgrA, osdA outageMember
	for _, m := range hostA {
		switch m.Role {
		case "mon":
			monA = m
		case "mgr":
			mgrA = m
		case "osd":
			osdA = m
		}
	}
	if mgrA.Name != managerBefore.ActiveName || monA.Name == "" || osdA.Name == "" {
		t.Fatalf("host-a is missing a role or lacks the active MGR: %+v", hostA)
	}
	osdAID, _ := strconv.Atoi(osdA.Name)
	start := time.Now()
	// The stops run OSD, MGR, MON and the restores run in reverse, so the MON
	// starts first. The primary MON is pinned to its bridge address, and Docker
	// hands a released address to whichever container starts first: a native
	// run that started the OSD and MGR before the MON failed with "Address
	// already in use". That explanation is an inference from the library's
	// fixed-address handling and the order of the calls in the log; the
	// occupant of the address was not measured.
	plan := newOutagePlan(func() time.Duration { return time.Since(start) }, []outageMember{osdA, mgrA, monA})
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
		defer stop()
		outageFinish(cleanup, plan, t.Logf, t.Errorf)
	})
	version := staticFacts(cluster).CephVersion
	platform := strings.TrimSpace(string(topologyExecOutput(t, ctx, client, "uname", "-sm")))
	manifestJSON, _ := json.Marshal(manifest)
	t.Logf("CORRELATED_OUTAGE_MANIFEST ceph=%s platform=%s essential_image=%s client_image=%s plan=osd.%s,mgr.%s,mon.%s election_leader=%s election_epoch=%d active_mgr_gid=%d manifest=%s",
		version, platform, imageID(t, ctx, cluster.Container), imageID(t, ctx, client), osdA.Name, mgrA.Name, monA.Name, quorum.Leader, quorum.Epoch, managerBefore.ActiveGID, manifestJSON)

	// Baseline: seed the placement pool, then run the retained client.
	execCommand(t, ctx, client, "python3", "-c", hostPlacementClientScript, outageBaselinePool, "seed")
	run := "r" + strconv.FormatInt(time.Now().Unix(), 10)
	outageToken(t, ctx, cluster, client, outageLedgerPool, run, outagePhaseBaseline)
	actor := startNative(ctx, cluster, client, outageCommand(outageLedgerPool, run, "actor"))
	finished := false
	t.Cleanup(func() {
		if finished {
			return
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			t.Logf("CORRELATED_OUTAGE_ACTOR cleanup error=%v", outcome.err)
		case <-time.After(30 * time.Second):
			t.Error("the retained actor could not be confirmed stopped")
		}
	})
	waitOps := func(key string, want int, what string) []outageOp {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for {
			select {
			case outcome := <-actor.done:
				finished = true
				t.Fatalf("the retained actor ended while waiting for %s: err=%v exit=%d stdout=%s stderr=%s", what, outcome.err, outcome.result.ExitCode, outcome.result.Stdout, outcome.result.Stderr)
			default:
			}
			ops, err := outageOps(outageLedger(t, ctx, cluster, client, outageLedgerPool, run), run)
			if err != nil {
				t.Fatal("ledger refused:", err)
			}
			if outageTally(ops)[key] >= want {
				return ops
			}
			if time.Now().After(deadline) {
				t.Fatalf("waiting for %d %s: tally %v", want, what, outageTally(ops))
			}
			time.Sleep(2 * time.Second)
		}
	}
	waitOps(outagePhaseBaseline+"/acked", 3, "baseline acknowledgements")

	// Inject. The token precedes the first stop request, so every operation
	// that begins under it overlaps a partial injection and is never credited.
	outageToken(t, ctx, cluster, client, outageLedgerPool, run, outagePhaseApplying)
	if err := plan.Apply(ctx, 5*time.Second); err != nil {
		t.Fatalf("partly applied outage, restore follows in cleanup: %v", err)
	}
	interval, ok := plan.AppliedInterval()
	if !ok || interval <= 0 {
		t.Fatalf("no applied interval: %+v", plan.Receipts)
	}

	// Observe each selected target natively. The token is issued only after
	// all of them were observed.
	observed := map[string]bool{}
	var quorumDuring struct {
		Leader string   `json:"quorum_leader_name"`
		Epoch  int      `json:"election_epoch"`
		Names  []string `json:"quorum_names"`
	}
	var managerDuring ceph.ManagerStatus
	var actingDuring []int
	deadline := time.Now().Add(4 * time.Minute)
	for len(observed) < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("native observations incomplete: %v receipts=%+v", observed, plan.Receipts)
		}
		time.Sleep(time.Second)
		if !observed["mon"] {
			var q struct {
				Leader string   `json:"quorum_leader_name"`
				Epoch  int      `json:"election_epoch"`
				Names  []string `json:"quorum_names"`
			}
			if data, err := cluster.Ceph(ctx, "quorum_status", "--format", "json"); err == nil && json.Unmarshal(data, &q) == nil &&
				len(q.Names) == 2 && !slices.Contains(q.Names, monA.Name) {
				observed["mon"], quorumDuring = true, q
				plan.Observe("mon", monA.Name)
			}
		}
		if !observed["mgr"] {
			if m, err := cluster.ManagerStatus(ctx); err == nil && m.Available && m.ActiveName == standbyName && m.ActiveGID != managerBefore.ActiveGID {
				observed["mgr"], managerDuring = true, m
				plan.Observe("mgr", mgrA.Name)
			}
		}
		if !observed["osd"] {
			if current, err := cluster.OSDStates(ctx); err == nil {
				for _, state := range current {
					if state.ID == osdAID && !state.Up && state.In {
						observed["osd"] = true
						plan.Observe("osd", osdA.Name)
					}
				}
			}
		}
		if observed["osd"] && !observed["placement"] {
			var mapping struct {
				Acting []int `json:"acting"`
			}
			if data, err := cluster.Ceph(ctx, "osd", "map", outageLedgerPool, "op-probe", "--format", "json"); err == nil && json.Unmarshal(data, &mapping) == nil &&
				len(mapping.Acting) == 2 && !slices.Contains(mapping.Acting, osdAID) {
				observed["placement"], actingDuring = true, mapping.Acting
			}
		}
	}
	outageToken(t, ctx, cluster, client, outageLedgerPool, run, outagePhaseOutage)
	ops := waitOps(outagePhaseOutage+"/acked", 3, "acknowledged writes under the fully applied outage")
	t.Logf("CORRELATED_OUTAGE_DURING applied_interval_ms=%d quorum=%v leader=%s election_epoch=%d active_mgr=%s active_mgr_gid=%d acting=%v tally=%v",
		interval.Milliseconds(), quorumDuring.Names, quorumDuring.Leader, quorumDuring.Epoch, managerDuring.ActiveName, managerDuring.ActiveGID, actingDuring, outageTally(ops))

	// Restore the same containers and stores.
	outageToken(t, ctx, cluster, client, outageLedgerPool, run, outagePhaseRestoring)
	if err := plan.Restore(ctx, "primary"); err != nil {
		t.Fatal(err)
	}
	topologyWait(t, ctx, func() bool {
		q, qerr := cluster.QuorumStatus(ctx)
		m, merr := cluster.ManagerStatus(ctx)
		s, serr := cluster.OSDStates(ctx)
		return qerr == nil && merr == nil && serr == nil && len(q.QuorumNames) == 3 && m.Available && len(m.Standbys) == 1 &&
			slices.EqualFunc(states, s, func(a, b ceph.OSDState) bool { return a.ID == b.ID && a.UUID == b.UUID && b.Up && b.In })
	})
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	for _, m := range manifest.Members {
		if m.current() != m.Container {
			t.Fatalf("%s.%s now runs in another container", m.Role, m.Name)
		}
	}
	span = map[string]bool{}
	for _, id := range acting("op-probe") {
		span[hostByOSD[id]] = true
	}
	if len(acting("op-probe")) != 3 || len(span) != 3 {
		t.Fatalf("redundancy not restored across hosts: %v", acting("op-probe"))
	}
	outageToken(t, ctx, cluster, client, outageLedgerPool, run, outagePhaseRecovered)
	waitOps(outagePhaseRecovered+"/acked", 2, "acknowledged writes after recovery")
	outageToken(t, ctx, cluster, client, outageLedgerPool, run, outagePhaseStop)
	select {
	case outcome := <-actor.done:
		finished = true
		if outcome.err != nil || outcome.result.ExitCode != 0 {
			t.Fatalf("the retained actor did not end cleanly: err=%v exit=%d stderr=%s", outcome.err, outcome.result.ExitCode, outcome.result.Stderr)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("the retained actor did not end after the stop token")
	}

	// Readback: a fresh connection, and bytes derived by the controller.
	events := outageLedger(t, ctx, cluster, client, outageLedgerPool, run)
	ops, err = outageOps(events, run)
	if err != nil {
		t.Fatal("final ledger refused:", err)
	}
	if !slices.ContainsFunc(events, func(e outageEvent) bool { return e.Event == "stopped" }) {
		t.Fatal("the actor never reported its end")
	}
	var objects map[string]outageObject
	if err := json.Unmarshal(outageRun(t, ctx, cluster, client, outageLedgerPool, run, "read", outageNames(ops)), &objects); err != nil {
		t.Fatal(err)
	}
	unconfirmed, err := outageVerify(ops, objects)
	if err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "python3", "-c", hostPlacementClientScript, outageBaselinePool, "outage")
	execCommand(t, ctx, client, "python3", "-c", hostPlacementClientScript, outageBaselinePool, "verify")
	tally := outageTally(ops)
	if tally[outagePhaseOutage+"/acked"] < 3 || tally[outagePhaseBaseline+"/acked"] < 3 {
		t.Fatalf("tally %v", tally)
	}
	tallyJSON, _ := json.Marshal(tally)
	t.Logf("CORRELATED_OUTAGE_LEDGER run=%s operations=%d tally=%s unconfirmed_present=%d ops=%s", run, len(ops), tallyJSON, unconfirmed, outageShort(ops))
	t.Logf("CORRELATED_OUTAGE_READBACK acknowledged_exact=%d unconfirmed_present=%d placement_pool_survived=true", func() int {
		n := 0
		for _, op := range ops {
			if op.Outcome() == "acked" {
				n++
			}
		}
		return n
	}(), unconfirmed)
}

func outageJSON(t *testing.T, ctx context.Context, cluster *ceph.Container, target any, args ...string) {
	t.Helper()
	data, err := cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err != nil || json.Unmarshal(data, target) != nil {
		t.Fatalf("native observation %v: %v: %s", args, err, data)
	}
}
