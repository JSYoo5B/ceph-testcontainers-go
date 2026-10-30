//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_rgw_topology))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package rgw_test

import (
	"bytes"
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"net/http"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWTopology(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "bridge", "host") {
		name := mode.Name
		var options []testcontainers.ContainerCustomizer
		if mode.Host {
			options = append(options, ceph.WithHostNetwork())
		}
		options = append(options, rgw.WithGateways(rgw.Config{Name: "gateway-a"}))
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			cluster, _ := harness.NewServiceCluster(t, options...)
			initial := clusteraccess.Gateways(t, cluster)
			if len(initial) != 1 || initial[0].GatewayName != "gateway-a" {
				t.Fatal("Run did not expose its initially configured gateway")
			}
			first := initial[0]
			second, err := rgw.Start(ctx, cluster, rgw.Config{Name: "gateway-b", SkipUserCreation: true})
			if err != nil {
				t.Fatal(err)
			}
			gateways := clusteraccess.Gateways(t, cluster)
			if len(gateways) != 2 || gateways[0] != first || gateways[1] != second || first.GatewayName != "gateway-a" || second.GatewayName != "gateway-b" || first.GetContainerID() == second.GetContainerID() || len(cluster.ServiceContainers()) != 2 {
				t.Fatal("fixture did not own two separately named gateways")
			}
			if second.AccessKey != "" || second.SecretKey != "" {
				t.Fatal("second gateway created another S3 user despite SkipUserCreation")
			}
			firstEndpoint, err := first.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			secondEndpoint, err := second.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			firstPeer, err := first.DaemonEndpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			secondPeer, err := second.DaemonEndpoint(ctx)
			if err != nil || firstEndpoint == secondEndpoint || firstPeer == secondPeer {
				t.Fatalf("separate gateways share a listener: error=%v", err)
			}
			// Both gateways serve one zone and the same ordinary S3 user. This
			// proves shared native RGW state, without application-side replication.
			a := harness.S3HTTPClient{Endpoint: firstEndpoint, AccessKey: first.AccessKey, SecretKey: first.SecretKey, Region: first.Region, HTTP: &http.Client{Timeout: 20 * time.Second}}
			b := a
			b.Endpoint = secondEndpoint
			const bucket = "/tc-rgw-gateways"
			payload := bytes.Repeat([]byte("same-zone independent gateway data\n"), 2048)
			a.Request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
			a.Request(t, ctx, http.MethodPut, bucket+"/from-a", payload, http.StatusOK)
			check := func(client harness.S3HTTPClient, key string, expected []byte) {
				t.Helper()
				if got := client.Request(t, ctx, http.MethodGet, bucket+"/"+key, nil, http.StatusOK); !bytes.Equal(got, expected) {
					t.Fatalf("gateway returned changed payload for %s", key)
				}
			}
			check(b, "from-a", payload)
			reverse := bytes.Repeat([]byte("written through gateway b\n"), 3072)
			b.Request(t, ctx, http.MethodPut, bucket+"/from-b", reverse, http.StatusOK)
			check(a, "from-b", reverse)
			stop := 3 * time.Second
			if err := first.Stop(ctx, &stop); err != nil {
				t.Fatal(err)
			}
			check(b, "from-a", payload)
			outage := []byte("gateway-b accepted a write while gateway-a was stopped\n")
			b.Request(t, ctx, http.MethodPut, bucket+"/during-outage", outage, http.StatusOK)
			check(b, "during-outage", outage)
			if err := first.Start(ctx); err != nil {
				t.Fatal(err)
			}
			a.Endpoint, err = first.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			check(a, "from-a", payload)
			check(a, "from-b", reverse)
			check(a, "during-outage", outage)
			oldID := second.GetContainerID()
			if err := second.Remove(ctx); err != nil {
				t.Fatal(err)
			}
			if len(clusteraccess.Gateways(t, cluster)) != 1 || len(cluster.ServiceContainers()) != 1 {
				t.Fatal("removed gateway is still owned")
			}
			check(a, "from-b", reverse)
			second, err = rgw.Start(ctx, cluster, rgw.Config{Name: "gateway-b", SkipUserCreation: true})
			if err != nil || second.GetContainerID() == oldID || len(clusteraccess.Gateways(t, cluster)) != 2 {
				t.Fatalf("replacement gateway was not created: %v", err)
			}
			b.Endpoint, err = second.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			check(b, "from-a", payload)
			check(b, "from-b", reverse)
			check(b, "during-outage", outage)
			for _, key := range []string{"from-a", "from-b", "during-outage"} {
				b.Request(t, ctx, http.MethodDelete, bucket+"/"+key, nil, http.StatusNoContent)
			}
			b.Request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
			t.Logf("one zone: initial gateway-a, dynamic gateway-b, 2 -> 1 -> 2 replacement, stop/restart and retained communication passed; initial daemon endpoints %s %s", firstPeer, secondPeer)
		})
	}
}
