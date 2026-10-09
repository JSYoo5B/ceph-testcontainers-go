//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestManagerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
	defer cancel()
	image, options := integrationImages(t)
	options = append(options, ceph.WithOSDCount(1), ceph.WithManagerCount(1), ceph.WithStartupTimeout(3*time.Minute))
	cluster, err := ceph.Run(ctx, image, options...)
	if cluster != nil {
		testcontainers.CleanupContainer(t, cluster)
	}
	if err != nil {
		t.Fatal(err)
	}
	checkMap := func(active string, standbys ...string) {
		t.Helper()
		status, err := cluster.ManagerStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var actual []string
		for _, standby := range status.Standbys {
			actual = append(actual, standby.Name)
		}
		slices.Sort(actual)
		slices.Sort(standbys)
		if !status.Available || status.ActiveName != active || status.ActiveGID == 0 || !slices.Equal(actual, standbys) {
			t.Fatalf("unexpected native manager map: %+v", status)
		}
		owned := cluster.Managers()
		if len(owned) != 1+len(standbys) {
			t.Fatalf("native manager map and owned daemon count differ: %+v", owned)
		}
	}
	checkMap("a")
	managerLifecycleRBDReady(t, ctx, cluster)
	primary := cluster.ManagerContainer()
	b, err := cluster.AddManager(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if b.GetContainerID() == primary.GetContainerID() {
		t.Fatal("standby reused the active manager's container")
	}
	checkMap("a", "b")
	if err := cluster.RemoveManager(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	checkMap("b")
	if cluster.ManagerContainer() != nil || cluster.Managers()[0] != b {
		t.Fatal("removing primary a retained its legacy pointer or replaced b's container")
	}
	managerLifecycleRBDReady(t, ctx, cluster)
	replacement, err := cluster.AddManager(ctx, "replacement")
	if err != nil {
		t.Fatal(err)
	}
	if replacement.GetContainerID() == b.GetContainerID() || replacement.GetContainerID() == primary.GetContainerID() {
		t.Fatal("replacement manager did not receive a fresh container")
	}
	checkMap("b", "replacement")
	if err := cluster.RemoveManager(ctx, "replacement"); err != nil {
		t.Fatal(err)
	}
	checkMap("b")
	managerLifecycleRBDReady(t, ctx, cluster)
	if err := cluster.RemoveManager(ctx, "b"); err == nil {
		t.Fatal("last manager candidate was removed")
	}
	checkMap("b")
	data, err := cluster.Ceph(ctx, "auth", "ls", "--format", "json")
	var identities struct {
		AuthDump []struct {
			Entity string `json:"entity"`
		} `json:"auth_dump"`
	}
	if err != nil || json.Unmarshal(data, &identities) != nil || identities.AuthDump == nil {
		t.Fatal("could not inspect manager identity ownership after removal")
	}
	var managers []string
	for _, identity := range identities.AuthDump {
		if identity.Entity == "mgr.a" || identity.Entity == "mgr.replacement" {
			t.Fatalf("removed manager identity remains: %s", identity.Entity)
		}
		if identity.Entity == "mgr.b" {
			managers = append(managers, identity.Entity)
		}
	}
	if len(managers) != 1 {
		t.Fatal("active manager identity was lost")
	}
	t.Log("MGR candidates 1 -> 2 -> 1 -> 2 -> 1: active a removed, b promoted, replacement standby removed; native rbd_support task command remained available")
}
