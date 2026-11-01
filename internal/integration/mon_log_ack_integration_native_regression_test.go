//go:build (all || (integration && topology)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_mon_log_ack)))

//ci: timeout=20m job-timeout=30 image-roles=essential

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestMONLogAckThroughPeon requires every OSD's cluster log entries to be
// acknowledged, whether its MON session is with the leader or with a peon
// that forwards them (issue 389). It fails on images whose leader also calls
// no_reply for a forwarded MLog, such as 20.2.4: the leader commits the
// peon-forwarded MLog, but the peon discards the MLogAck, so the OSD's
// LogClient queue is never trimmed. It passes on 19.2.2.
//
// Three MONs and three OSDs start. If the OSDs' sessions do not include one
// with the leader and one with a peon, the MON holding most of them is
// restarted, before any measurement, until they do. With debug_monc 10 on
// the OSDs and debug_mon 10 on the MONs, each OSD writes two cluster log
// entries (scrub_purged_snaps). The entries must reach the cluster log, the
// leader-connected OSD must be acknowledged (otherwise the oracle is
// invalid), and then every OSD must be. The leader, election epoch and
// sessions must not change during the window.
func TestMONLogAckThroughPeon(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(3)))
	t.Logf("MON_LOG_ACK ceph=%s", harness.StaticFacts(cluster).CephVersion)
	mons := clusteraccess.Monitors(t, cluster)
	osds := clusteraccess.OSDs(t, cluster)

	leader, epoch, sessions := arrangeLogAckSessions(t, ctx, cluster, mons)
	roles := map[string]string{}
	for _, o := range osds {
		entity := fmt.Sprintf("osd.%d", o.ID)
		roles[entity] = logAckRole(sessions[entity].Mon, leader)
	}
	t.Logf("MON_LOG_ACK leader=mon.%s election_epoch=%d sessions=%v roles=%v", leader, epoch, sessions, roles)

	// The window starts once every daemon logs at debug level 10: from here
	// on, an OSD's MON session change shows in its own log, and the route of
	// each forwarded MLog in its peon's.
	for _, m := range mons {
		logAckSocket(t, ctx, cluster, "mon."+m.DaemonName, "config", "set", "debug_mon", "10")
	}
	for _, o := range osds {
		logAckSocket(t, ctx, cluster, fmt.Sprintf("osd.%d", o.ID), "config", "set", "debug_monc", "10")
	}
	offsets := map[string]int{}
	since := func(name string, ctr interface {
		Logs(context.Context) (io.ReadCloser, error)
	}) []byte {
		raw := logAckContainerLog(t, ctx, ctr)
		return raw[min(offsets[name], len(raw)):]
	}
	for _, m := range mons {
		offsets["mon."+m.DaemonName] = len(logAckContainerLog(t, ctx, m))
	}
	for _, o := range osds {
		offsets[fmt.Sprintf("osd.%d", o.ID)] = len(logAckContainerLog(t, ctx, o))
	}
	if l, e, s, err := readLogAckLayout(t, ctx, cluster, mons); err != nil || l != leader || e != epoch || fmt.Sprint(s) != fmt.Sprint(sessions) {
		t.Fatalf("the layout changed before the window: leader mon.%s, election epoch %d, sessions %v: %v", l, e, s, err)
	}
	for _, o := range osds {
		logAckSocket(t, ctx, cluster, fmt.Sprintf("osd.%d", o.ID), "scrub_purged_snaps")
	}

	// The entries are committed: the leader has them in the cluster log.
	control := clusteraccess.ControlContainer(t, cluster)
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(time.Second) {
		out := harness.ExecOutput(t, ctx, control, "ceph", "log", "last", "200", "debug", "cluster")
		missing := []string{}
		for entity := range roles {
			if !slices.ContainsFunc(strings.Split(out, "\n"), func(line string) bool {
				return strings.Contains(line, " "+entity+" ("+entity+") ") && strings.Contains(line, "purged_snaps scrub ok")
			}) {
				missing = append(missing, entity)
			}
		}
		if len(missing) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster log has no entries of %v:\n%s", missing, out)
		}
	}

	// Wait up to a minute for every OSD's acknowledgement.
	progress := map[string]logClientProgress{}
	waiting := func() []string {
		var w []string
		for _, o := range osds {
			entity := fmt.Sprintf("osd.%d", o.ID)
			progress[entity] = readLogClientProgress(since(entity, o), entity)
			if p := progress[entity]; p.Sent == 0 || p.Acked < p.Sent {
				w = append(w, entity)
			}
		}
		slices.Sort(w)
		return w
	}
	for deadline := time.Now().Add(time.Minute); len(waiting()) != 0 && time.Now().Before(deadline); time.Sleep(2 * time.Second) {
	}
	unacked := waiting()

	// The window is valid only if no OSD's MON session changed in it, even
	// to return to the same MON, and the leader and election epoch held.
	transitions := map[string][]string{}
	for _, o := range osds {
		entity := fmt.Sprintf("osd.%d", o.ID)
		if found := monClientTransitions(since(entity, o)); len(found) != 0 {
			transitions[entity] = found
		}
	}
	routes := map[string]string{}
	lost := map[string][]string{}
	for _, m := range mons {
		if m.DaemonName == leader {
			continue
		}
		peonLog := since("mon."+m.DaemonName, m)
		lost["mon."+m.DaemonName] = lostRoutedAcks(peonLog)
		for entity, id := range sessions {
			if id.Mon != m.DaemonName {
				continue
			}
			if tid, why := routedLogAck(peonLog, entity, progress[entity].Sent); why != "" {
				routes[entity] = why
			} else {
				routes[entity] = "acknowledged through mon." + m.DaemonName + " tid " + tid
			}
		}
	}
	after, afterEpoch, afterSessions, layoutErr := readLogAckLayout(t, ctx, cluster, mons)
	receipt, _ := json.Marshal(map[string]any{"leader": leader, "roles": roles, "sessions": fmt.Sprint(sessions), "progress": progress,
		"routes": routes, "transitions": transitions, "lost_routed_acks": lost})
	t.Logf("MON_LOG_ACK_RECEIPT %s", receipt)
	if layoutErr != nil || len(transitions) != 0 || after != leader || afterEpoch != epoch || fmt.Sprint(afterSessions) != fmt.Sprint(sessions) {
		t.Fatalf("the window changed, so the comparison is invalid: leader mon.%s -> mon.%s, election epoch %d -> %d, sessions %v -> %v, session transitions %v: %v",
			leader, after, epoch, afterEpoch, sessions, afterSessions, transitions, layoutErr)
	}
	var peonless []string
	for _, entity := range unacked {
		if roles[entity] == "leader" {
			peonless = append(peonless, entity)
		}
	}
	if len(peonless) != 0 {
		t.Fatalf("the leader-connected OSDs %v were not acknowledged, so the oracle is invalid: %v", peonless, progress)
	}
	if len(unacked) != 0 {
		counts := map[string]int{}
		for mon, acks := range lost {
			counts[mon] = len(acks)
		}
		t.Fatalf("the cluster log entries of the peon-connected OSDs %v were committed but never acknowledged (%v); the peons discarded routed MLogAcks %v", unacked, routes, counts)
	}
	for entity, role := range roles {
		if role == "peon" && !strings.HasPrefix(routes[entity], "acknowledged through") {
			t.Fatalf("%s was acknowledged, but not through its peon: %s", entity, routes[entity])
		}
	}
}

// arrangeLogAckSessions waits for a stable three-MON quorum whose OSD
// sessions include one with the leader and one with a peon, restarting the
// MON with the most OSD sessions, at most four times, until they do.
func arrangeLogAckSessions(t *testing.T, ctx context.Context, cluster *ceph.Container, mons []*ceph.MonitorContainer) (string, int, map[string]logAckSessionID) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		leader, epoch, sessions := logAckLayout(t, ctx, cluster, mons)
		count := map[string]int{}
		roles := map[string]bool{}
		for _, id := range sessions {
			count[id.Mon]++
			roles[logAckRole(id.Mon, leader)] = true
		}
		if len(sessions) == 3 && roles["leader"] && roles["peon"] {
			return leader, epoch, sessions
		}
		if attempt == 4 {
			t.Fatalf("no arrangement with OSD sessions on the leader and on a peon after %d restarts: leader mon.%s, sessions %v", attempt, leader, sessions)
		}
		busiest := ""
		for mon, n := range count {
			if busiest == "" || n > count[busiest] {
				busiest = mon
			}
		}
		t.Logf("MON_LOG_ACK_ARRANGE attempt=%d leader=mon.%s sessions=%v restart=mon.%s", attempt, leader, sessions, busiest)
		for _, m := range mons {
			if m.DaemonName != busiest {
				continue
			}
			stop := 30 * time.Second
			if err := m.Stop(ctx, &stop); err != nil {
				t.Fatal(err)
			}
			if err := m.Start(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// readLogAckLayout reads the leader, the election epoch and the OSD sessions
// once. All three MONs must report a quorum of three in one election epoch,
// and every OSD must have exactly one session.
func readLogAckLayout(t *testing.T, ctx context.Context, cluster *ceph.Container, mons []*ceph.MonitorContainer) (string, int, map[string]logAckSessionID, error) {
	t.Helper()
	leader, epoch := "", 0
	listings := map[string][]byte{}
	for _, m := range mons {
		raw, err := harness.AdminSocket(t, ctx, cluster, "mon."+m.DaemonName, "mon_status")
		if err != nil {
			return "", 0, nil, err
		}
		var s logAckMonStatus
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", 0, nil, err
		}
		if len(s.Quorum) != 3 || (epoch != 0 && s.ElectionEpoch != epoch) {
			return "", 0, nil, fmt.Errorf("mon.%s: %s, quorum %v, election epoch %d", s.Name, s.State, s.Quorum, s.ElectionEpoch)
		}
		epoch = s.ElectionEpoch
		if s.State == "leader" {
			leader = s.Name
		}
		if listings[m.DaemonName], err = harness.AdminSocket(t, ctx, cluster, "mon."+m.DaemonName, "sessions"); err != nil {
			return "", 0, nil, err
		}
	}
	sessions, err := osdMonSessions(listings)
	if err == nil && (leader == "" || len(sessions) != 3) {
		err = fmt.Errorf("leader %q, OSD sessions %v", leader, sessions)
	}
	return leader, epoch, sessions, err
}

// logAckLayout waits until readLogAckLayout reads the same layout twice in a
// row, five seconds apart. It is used only before the window.
func logAckLayout(t *testing.T, ctx context.Context, cluster *ceph.Container, mons []*ceph.MonitorContainer) (string, int, map[string]logAckSessionID) {
	t.Helper()
	var last error
	for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline); time.Sleep(time.Second) {
		leader, epoch, sessions, err := readLogAckLayout(t, ctx, cluster, mons)
		if err != nil {
			last = err
			continue
		}
		time.Sleep(5 * time.Second)
		again, againEpoch, againSessions, err := readLogAckLayout(t, ctx, cluster, mons)
		if err == nil && again == leader && againEpoch == epoch && fmt.Sprint(againSessions) == fmt.Sprint(sessions) {
			return leader, epoch, sessions
		}
		last = err
	}
	t.Fatalf("no stable three-MON layout: %v", last)
	return "", 0, nil
}

func logAckSocket(t *testing.T, ctx context.Context, cluster *ceph.Container, daemon string, args ...string) {
	t.Helper()
	if out, err := harness.AdminSocket(t, ctx, cluster, daemon, args...); err != nil {
		t.Fatalf("%s %v: %v: %s", daemon, args, err, out)
	}
}

// logAckContainerLog reads a daemon container's whole log.
func logAckContainerLog(t *testing.T, ctx context.Context, ctr interface {
	Logs(context.Context) (io.ReadCloser, error)
}) []byte {
	t.Helper()
	r, err := ctr.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
