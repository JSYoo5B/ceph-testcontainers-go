//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWTopology(t *testing.T) {
	parallelWhenEnabled(t)
	for _, host := range []bool{false, true} {
		name := "bridge"
		var options []testcontainers.ContainerCustomizer
		if host {
			name = "host"
			options = append(options, ceph.WithHostNetwork())
		}
		options = append(options, rgw.WithGateways(rgw.Config{Name: "gateway-a"}))
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			cluster, _ := newServiceCluster(t, options...)
			initial := rgw.Gateways(cluster)
			if len(initial) != 1 || initial[0].GatewayName != "gateway-a" {
				t.Fatal("Run did not expose its initially configured gateway")
			}
			first := initial[0]
			second, err := rgw.Start(ctx, cluster, rgw.Config{Name: "gateway-b", SkipUserCreation: true})
			if err != nil {
				t.Fatal(err)
			}
			gateways := rgw.Gateways(cluster)
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
			a := s3HTTPClient{endpoint: firstEndpoint, accessKey: first.AccessKey, secretKey: first.SecretKey, region: first.Region, http: &http.Client{Timeout: 20 * time.Second}}
			b := a
			b.endpoint = secondEndpoint
			const bucket = "/tc-rgw-gateways"
			payload := bytes.Repeat([]byte("same-zone independent gateway data\n"), 2048)
			a.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
			a.request(t, ctx, http.MethodPut, bucket+"/from-a", payload, http.StatusOK)
			check := func(client s3HTTPClient, key string, expected []byte) {
				t.Helper()
				if got := client.request(t, ctx, http.MethodGet, bucket+"/"+key, nil, http.StatusOK); !bytes.Equal(got, expected) {
					t.Fatalf("gateway returned changed payload for %s", key)
				}
			}
			check(b, "from-a", payload)
			reverse := bytes.Repeat([]byte("written through gateway b\n"), 3072)
			b.request(t, ctx, http.MethodPut, bucket+"/from-b", reverse, http.StatusOK)
			check(a, "from-b", reverse)
			stop := 3 * time.Second
			if err := first.Stop(ctx, &stop); err != nil {
				t.Fatal(err)
			}
			check(b, "from-a", payload)
			outage := []byte("gateway-b accepted a write while gateway-a was stopped\n")
			b.request(t, ctx, http.MethodPut, bucket+"/during-outage", outage, http.StatusOK)
			check(b, "during-outage", outage)
			if err := first.Start(ctx); err != nil {
				t.Fatal(err)
			}
			a.endpoint, err = first.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			check(a, "from-a", payload)
			check(a, "from-b", reverse)
			check(a, "during-outage", outage)
			oldID := second.GetContainerID()
			if err := rgw.Remove(ctx, cluster, "gateway-b"); err != nil {
				t.Fatal(err)
			}
			if len(rgw.Gateways(cluster)) != 1 || len(cluster.ServiceContainers()) != 1 {
				t.Fatal("removed gateway is still owned")
			}
			check(a, "from-b", reverse)
			second, err = rgw.Start(ctx, cluster, rgw.Config{Name: "gateway-b", SkipUserCreation: true})
			if err != nil || second.GetContainerID() == oldID || len(rgw.Gateways(cluster)) != 2 {
				t.Fatalf("replacement gateway was not created: %v", err)
			}
			b.endpoint, err = second.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			check(b, "from-a", payload)
			check(b, "from-b", reverse)
			check(b, "during-outage", outage)
			for _, key := range []string{"from-a", "from-b", "during-outage"} {
				b.request(t, ctx, http.MethodDelete, bucket+"/"+key, nil, http.StatusNoContent)
			}
			b.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
			t.Logf("one zone: initial gateway-a, dynamic gateway-b, 2 -> 1 -> 2 replacement, stop/restart and retained communication passed; initial daemon endpoints %s %s", firstPeer, secondPeer)
		})
	}
}
