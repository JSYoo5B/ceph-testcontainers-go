//go:build all || integration

package integration_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/monsession"
)

// monSession is one entry of a MON's "sessions" admin socket command.
type monSession = monsession.Session

// managerMonSessions returns the sessions of each MGR, keyed by its entity
// name, as every owned MON reports them through its local admin socket.
func managerMonSessions(t *testing.T, ctx context.Context, cluster *ceph.Container) map[string][]monSession {
	t.Helper()
	sessions := map[string][]monSession{}
	for _, monitor := range clusteraccess.Monitors(t, cluster) {
		out, err := harness.MonitorHandle(t, ctx, cluster, monitor.DaemonName).AdminSocket(ctx, "sessions")
		if err != nil {
			t.Fatalf("sessions of mon.%s: %v", monitor.DaemonName, err)
		}
		if _, err := monsession.Nonces(out); err != nil {
			t.Fatalf("native session address schema: %v", err)
		}
		list, err := monsession.Decode(out)
		if err != nil {
			t.Fatalf("sessions of mon.%s: %v: %.512s", monitor.DaemonName, err, out)
		}
		for _, session := range list {
			if len(session.EntityName) > 4 && session.EntityName[:4] == "mgr." {
				sessions[session.EntityName] = append(sessions[session.EntityName], session)
			}
		}
	}
	return sessions
}

// splitManagerSessions separates the beacon session of a MGR, the one its
// daemon opens as con_type "mgr", from the sessions of its other MonClients.
func splitManagerSessions(sessions []monSession) (beacon []monSession, others []uint64) {
	for _, session := range sessions {
		if session.ConType == "mgr" {
			beacon = append(beacon, session)
		} else {
			others = append(others, session.GlobalID)
		}
	}
	slices.Sort(others)
	return beacon, others
}

// testManagerMonClients shows that a MGR process authenticates more than one
// MonClient. The MON sessions of the active MGR include one of type "mgr",
// whose global ID is the MgrMap GID, and the sessions of the clients its
// modules open, each with a global ID of its own; a standby, which runs no
// modules, has its beacon session. A MON's refusal therefore names the
// registered MGR only when the global ID it names is the MgrMap GID.
func testManagerMonClients(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	cluster, _ := harness.NewServiceClusterWithOptions(t, image, append(slices.Clone(roles), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithManagerCount(2).WithNoInitialOSDs()))...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	var last string
	for deadline := time.Now().Add(2 * time.Minute); ; {
		status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sessions := managerMonSessions(t, ctx, cluster)
		beacon, others := splitManagerSessions(sessions["mgr."+status.ActiveName])
		ok := status.Available && len(status.Standbys) == 1 && len(beacon) == 1 && beacon[0].GlobalID == status.ActiveGID && len(others) > 0 &&
			!slices.Contains(others, status.ActiveGID) && len(slices.Compact(slices.Clone(others))) == len(others)
		var standby []monSession
		if len(status.Standbys) == 1 {
			standby, _ = splitManagerSessions(sessions["mgr."+status.Standbys[0].Name])
			ok = ok && len(standby) == 1 && standby[0].GlobalID == status.Standbys[0].GID
		}
		last = fmt.Sprintf("status %+v, active beacon %+v and other global IDs %v, standby beacon %+v", status, beacon, others, standby)
		if ok {
			t.Logf("MGR_MON_CLIENTS active mgr.%s: beacon global ID %d = MgrMap GID, other MonClients %v; standby mgr.%s: beacon global ID %d = MgrMap GID",
				status.ActiveName, beacon[0].GlobalID, others, status.Standbys[0].Name, standby[0].GlobalID)
			testOwnedManagerAbsence(t, ctx, cluster, status.Standbys[0].Name, status.Standbys[0].GID)
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("MGR MON sessions never matched:", last)
		}
		time.Sleep(2 * time.Second)
	}
}

// A stopped owned standby can be absent from a readable MgrMap. The synthetic
// rejection is only classifier input; this control makes no auth-failure claim.
func testOwnedManagerAbsence(t *testing.T, ctx context.Context, cluster *ceph.Container, name string, gid uint64) {
	manager := harness.ManagerHandle(t, ctx, cluster, name)
	if err := manager.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}

	for deadline := time.Now().Add(2 * time.Minute); ; {
		status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.ActiveName != name && !managerNamePresent(status, name) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stopped standby remained registered")
		}
		time.Sleep(time.Second)
	}
	b := authFirstFailureBundle{Rejection: authTestedRejection{Target: "mgr." + name, TargetGID: gid, Result: -13}, Expected: authExpectedTarget{Manager: name, GID: gid}}
	c := authCollector{sources: withSockets(t, cluster), ctx: ctx, bound: 10 * time.Second, bundle: &b}
	b.Managers = c.observeManagers("before", nil)
	b.ManagersAfter = c.observeManagers("after", nil)
	if v := classifyAuthBundle(b); v.Target != "absent" {
		t.Fatalf("owned stopped manager classification: %+v", v)
	}
	t.Log("owned stopped MGR absent from both complete map reads classified as absent")
	if err := manager.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(time.Minute); ; {
		status, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.ActiveName == name || managerNamePresent(status, name) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted manager did not register")
		}
		time.Sleep(time.Second)
	}
}

func managerNamePresent(status ceph.ManagerStatus, name string) bool {
	for _, standby := range status.Standbys {
		if standby.Name == name {
			return true
		}
	}
	return false
}
