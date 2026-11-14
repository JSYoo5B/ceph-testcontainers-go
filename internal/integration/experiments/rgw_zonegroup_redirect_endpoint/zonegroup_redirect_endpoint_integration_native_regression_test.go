//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_zonegroup_redirect_endpoint)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rgw

package rgw_zonegroup_redirect_endpoint_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

// image is the Ceph image every cluster and gateway of the case runs:
// Tentacle 20.2.4, which reproduces the defect. A run selects another image
// with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_zonegroup_redirect_endpoint_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.5, Umbrella 21.2.0) does not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"

const (
	// The two zonegroups, each with one zone on its own cluster. The first
	// is the master zonegroup of the realm.
	firstGroup, firstZone   = "us", "a"
	secondGroup, secondZone = "eu", "b"
	redirectKey             = "object"
	absentBucket            = "tc-redirect-absent"
)

// redirectCase is the realm with one S3 client per zonegroup. The clients
// do not follow redirects.
type redirectCase struct {
	t       *testing.T
	ctx     context.Context
	clients map[string]harness.S3HTTPClient
}

// do sends one signed S3 request to the gateway of a zonegroup and returns
// the answer without stopping the test, so that it can be used in a probe.
func (c *redirectCase) do(ctx context.Context, group, method, uri string, payload []byte) (*http.Response, []byte, error) {
	client := c.clients[group]
	request, err := http.NewRequestWithContext(ctx, method, client.Endpoint+uri, bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	harness.S3FeatureSign(request, client, payload, time.Now().UTC(), "s3")
	response, err := client.HTTP.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return response, body, err
}

// must is do for a step that the case cannot continue without.
func (c *redirectCase) must(group, method, uri string, payload []byte, want int) []byte {
	c.t.Helper()
	response, body, err := c.do(c.ctx, group, method, uri, payload)
	if err != nil || response.StatusCode != want {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		c.t.Fatalf("condition not established: zonegroup %s %s %s: status=%d code=%q error=%v, want %d", group, method, uri, status, harness.RGWBackendErrorCode(body), err, want)
	}
	return body
}

// TestMultiClusterRGWZonegroupRedirectEndpoint reports whether a gateway
// that receives a request for a bucket of another zonegroup redirects the
// client back to its own zonegroup's endpoint.
//
// Given a realm of two zonegroups on two clusters whose zones hold one
// committed period in which the zonegroups advertise distinct endpoints, a
// bucket with one object in each zonegroup that its own gateway serves, both
// gateways answering GET ?location of the other zonegroup's bucket with that
// zonegroup's name, and a bucket that does not exist answered with 404 and
// no Location, when each gateway is sent a GET of the object and a listing
// of the other zonegroup's bucket without following redirects, then the
// defect is REPRODUCED if every 301 names the receiving
// zonegroup's endpoint in its Location, and NOT_REPRODUCED if every one
// names the endpoint of the zonegroup that holds the bucket. Tentacle 20.2.4
// builds the Location from the zonegroup of the gateway that received the
// request, so it reproduces, and so does Squid 19.2.5; Tentacle 20.2.5 and
// Umbrella 21.2.0 do not.
//
// A failure of the given or when steps fails the test, and so does an
// answer that is no such redirect or redirects that disagree; the result
// does not.
func TestMultiClusterRGWZonegroupRedirectEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	t.Logf("RGW_REDIRECT image=%s", image)
	first, _ := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	second, _ := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_REDIRECT ceph=%s,%s", harness.StaticFacts(first).CephVersion, harness.StaticFacts(second).CephVersion)
	fixture, err := multisite.Run(ctx, image, multisite.WithTopology(multisite.TopologyConfig{
		MasterZonegroup: firstGroup,
		Zonegroups: []multisite.ZonegroupConfig{
			{Name: firstGroup, Zones: []multisite.ZoneConfig{{Name: firstZone, Cluster: first}}},
			{Name: secondGroup, Zones: []multisite.ZoneConfig{{Name: secondZone, Cluster: second}}},
		},
	}))
	if fixture != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			if err := fixture.Terminate(cleanup); err != nil {
				t.Errorf("terminate the realm: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatalf("unavailable prerequisite: the realm did not start: %v", err)
	}
	c := &redirectCase{t: t, ctx: ctx, clients: map[string]harness.S3HTTPClient{}}
	zones := fixture.Zones()
	if len(zones) != 2 || zones[0].Name != firstZone || zones[0].Zonegroup != firstGroup || zones[1].Name != secondZone || zones[1].Zonegroup != secondGroup {
		t.Fatalf("unavailable prerequisite: zones %+v", zones)
	}

	// Given: one committed period in every zone, in which the two
	// zonegroups advertise distinct endpoints.
	var periods []periodView
	for _, zone := range zones {
		endpoint, err := zone.Gateway.S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c.clients[zone.Zonegroup] = harness.S3HTTPClient{Endpoint: endpoint, AccessKey: zone.Gateway.AccessKey, SecretKey: zone.Gateway.SecretKey, Region: zone.Gateway.Region, HTTP: &http.Client{
			Timeout:       20 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}}
		t.Logf("RGW_REDIRECT zone=%s zonegroup=%s id=%s gateway=%s", zone.Name, zone.Zonegroup, zone.ID, strings.TrimSpace(harness.ExecOutput(t, ctx, zone.Gateway, "radosgw", "--version")))
		report, err := fixture.ZoneAdmin(ctx, zone.Name, "period", "get")
		if err != nil {
			t.Fatalf("zone %s: period get: %v", zone.Name, err)
		}
		period, err := parsePeriod(report)
		if err != nil {
			t.Fatalf("zone %s: %v", zone.Name, err)
		}
		t.Logf("RGW_REDIRECT period zone=%s id=%s epoch=%d zonegroups=%+v", zone.Name, period.ID, period.Epoch, period.Zonegroups)
		periods = append(periods, period)
	}
	us, eu, err := advertisedSites(periods, firstGroup, secondGroup)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_REDIRECT advertised %s=%s %s=%s", us.Name, us.Endpoint, eu.Name, eu.Endpoint)

	// The user is created in the master zonegroup and reaches the other one
	// through metadata sync: both gateways must accept its key.
	for _, group := range []site{us, eu} {
		seen, err := harness.Observe(ctx, 5*time.Minute, 20*time.Second, time.Second, func(probe context.Context) (string, bool, error) {
			response, body, err := c.do(probe, group.Name, http.MethodGet, "/", nil)
			if err != nil {
				return err.Error(), false, nil
			}
			return harness.RGWBackendErrorCode(body), response.StatusCode == http.StatusOK, nil
		})
		t.Logf("RGW_REDIRECT user zonegroup=%s reads=%d after=%s", group.Name, seen.Attempts, seen.Elapsed.Round(time.Millisecond))
		if err != nil {
			t.Fatalf("condition not established: the zonegroup %s does not accept the user's key (last answer %q): %v", group.Name, seen.Value, err)
		}
	}

	// A bucket with one object in each zonegroup, created through and served
	// by its own gateway.
	payload := []byte("stored in the zonegroup that holds the bucket\n")
	type owned struct {
		bucket          string
		owner, receiver site
	}
	buckets := []owned{{"tc-redirect-us", us, eu}, {"tc-redirect-eu", eu, us}}
	for _, b := range buckets {
		c.must(b.owner.Name, http.MethodPut, "/"+b.bucket, []byte(`<CreateBucketConfiguration><LocationConstraint>`+b.owner.APIName+`</LocationConstraint></CreateBucketConfiguration>`), http.StatusOK)
		c.must(b.owner.Name, http.MethodPut, "/"+b.bucket+"/"+redirectKey, payload, http.StatusOK)
		if got := c.must(b.owner.Name, http.MethodGet, "/"+b.bucket+"/"+redirectKey, nil, http.StatusOK); !bytes.Equal(got, payload) {
			t.Fatalf("condition not established: the zonegroup %s does not serve the object of its bucket %s", b.owner.Name, b.bucket)
		}
	}
	// The other gateway holds the bucket's metadata and names the owning
	// zonegroup as its location. GET ?location is not redirected.
	for _, b := range buckets {
		for _, group := range []site{b.owner, b.receiver} {
			seen, err := harness.Observe(ctx, 5*time.Minute, 20*time.Second, time.Second, func(probe context.Context) (string, bool, error) {
				response, body, err := c.do(probe, group.Name, http.MethodGet, "/"+b.bucket+"?location", nil)
				if err != nil || response.StatusCode != http.StatusOK {
					return harness.RGWBackendErrorCode(body), false, nil
				}
				location, err := parseLocationConstraint(body)
				return location, err == nil && location == b.owner.APIName, nil
			})
			t.Logf("RGW_REDIRECT location bucket=%s asked=%s answer=%q reads=%d after=%s", b.bucket, group.Name, seen.Value, seen.Attempts, seen.Elapsed.Round(time.Millisecond))
			if err != nil {
				t.Fatalf("condition not established: the zonegroup %s does not name %s as the location of %s (last answer %q): %v", group.Name, b.owner.APIName, b.bucket, seen.Value, err)
			}
		}
	}
	// Control: a bucket that does not exist is not redirected.
	for _, group := range []site{us, eu} {
		response, body, err := c.do(ctx, group.Name, http.MethodGet, "/"+absentBucket+"/"+redirectKey, nil)
		if err != nil || response.StatusCode != http.StatusNotFound || harness.RGWBackendErrorCode(body) != "NoSuchBucket" || len(response.Header.Values("Location")) != 0 {
			t.Fatalf("control not established: the zonegroup %s does not answer 404 NoSuchBucket without a Location for a bucket that does not exist: %v %v", group.Name, response, err)
		}
	}

	// When: each gateway is asked for the other zonegroup's bucket.
	var observations []redirectObservation
	for _, b := range buckets {
		for _, uri := range []string{"/" + b.bucket + "/" + redirectKey, "/" + b.bucket + "?list-type=2&prefix=obj"} {
			response, body, err := c.do(ctx, b.receiver.Name, http.MethodGet, uri, nil)
			if err != nil {
				t.Fatalf("zonegroup %s GET %s: %v", b.receiver.Name, uri, err)
			}
			answer := redirectAnswer{Status: response.StatusCode, Locations: response.Header.Values("Location")}
			t.Logf("RGW_REDIRECT answer receiver=%s owner=%s request=%q status=%d location=%q body=%q", b.receiver.Name, b.owner.Name, "GET "+uri, answer.Status, answer.Locations, body[:min(len(body), 512)])
			target, err := classifyRedirect(answer, b.owner, b.receiver, uri)
			if err != nil {
				t.Fatal(err)
			}
			observations = append(observations, redirectObservation{Request: "GET " + uri, Receiver: b.receiver.Name, Owner: b.owner.Name, Location: answer.Locations[0], Target: target})
		}
	}

	// Then: the redirects are judged together.
	symptoms, err := judgeRedirects(observations)
	if err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}
	verdict := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		verdict = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_zonegroup_redirect_endpoint result=%s image=%s symptoms=%q", verdict, image, symptoms)
}
