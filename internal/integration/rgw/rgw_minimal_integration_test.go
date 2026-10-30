//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs,rgw

package rgw_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// The smallest S3 fixture that rgw.Run builds: one MON, one MGR, one OSD and
// one gateway, with every pool at size 1. It is the recipe in rgw/README.md.
func TestRGWMinimalS3Recipe(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	image, roles := harness.IntegrationImages(t)
	started := time.Now()
	cluster, err := rgw.Run(ctx, image, append(roles,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(1).WithManagerCount(1).WithOSDCount(1).WithPoolDefaults(1, 1)),
	)...)
	testcontainers.CleanupContainer(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	startup := time.Since(started)

	// Four containers: the MON, which also runs the Ceph CLIs, the MGR, the
	// OSD and the gateway.
	gateways := clusteraccess.RGWGateways(t, cluster)
	if len(clusteraccess.Monitors(t, cluster.Container)) != 1 || len(clusteraccess.Managers(t, cluster.Container)) != 1 || len(clusteraccess.OSDs(t, cluster.Container)) != 1 || len(gateways) != 1 {
		t.Fatalf("Run started %d MONs, %d MGRs, %d OSDs and %d gateways, want one of each",
			len(clusteraccess.Monitors(t, cluster.Container)), len(clusteraccess.Managers(t, cluster.Container)), len(clusteraccess.OSDs(t, cluster.Container)), len(gateways))
	}
	gateway := gateways[0]
	containers := map[string]bool{}
	for _, ctr := range []testcontainers.Container{cluster.Container.Container, clusteraccess.InitialManager(t, cluster.Container), clusteraccess.OSDs(t, cluster.Container)[0], gateway} {
		containers[ctr.GetContainerID()] = true
	}
	if control, err := cluster.ControlContainer(ctx); err != nil || control.GetContainerID() != cluster.Container.Container.GetContainerID() {
		t.Fatalf("the control CLI does not run in the MON container: %v", err)
	}
	if len(containers) != 4 {
		t.Fatalf("the fixture has %d distinct daemon containers, want 4", len(containers))
	}

	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region,
		HTTP: &http.Client{Timeout: 45 * time.Second}}
	payload := make([]byte, 1031)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	client.Request(t, ctx, http.MethodPut, "/tc-minimal", nil, http.StatusOK)
	client.Request(t, ctx, http.MethodPut, "/tc-minimal/binary", payload, http.StatusOK)
	if read := client.Request(t, ctx, http.MethodGet, "/tc-minimal/binary", nil, http.StatusOK); !bytes.Equal(read, payload) {
		t.Fatalf("read back %d bytes that differ from the %d written", len(read), len(payload))
	}
	// After the first bucket and object, the gateway has created its index
	// and data pools too; every pool has one copy.
	pools, err := harness.ObservedPoolStates(ctx, cluster.Container)
	if err != nil {
		t.Fatal(err)
	}
	var index, data bool
	for _, pool := range pools {
		if pool.Size != 1 || pool.MinSize != 1 {
			t.Fatalf("pool %s has size %d and min_size %d, want 1 and 1", pool.Name, pool.Size, pool.MinSize)
		}
		index = index || strings.HasSuffix(pool.Name, ".rgw.buckets.index")
		data = data || strings.HasSuffix(pool.Name, ".rgw.buckets.data")
	}
	if !index || !data {
		t.Fatalf("the gateway's bucket index and data pools are missing: %+v", pools)
	}
	client.Request(t, ctx, http.MethodDelete, "/tc-minimal/binary", nil, http.StatusNoContent)
	client.Request(t, ctx, http.MethodGet, "/tc-minimal/binary", nil, http.StatusNotFound)
	client.Request(t, ctx, http.MethodDelete, "/tc-minimal", nil, http.StatusNoContent)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("rgw.Run started 1 MON, 1 MGR, 1 OSD and 1 gateway in %v; %d pools of size 1; signed S3 bucket create, PUT/GET of %d bytes (sha256 %x), DELETE, GET 404 and bucket DELETE passed",
		startup.Round(time.Millisecond), len(pools), len(payload), sha256.Sum256(payload))
}
