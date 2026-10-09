//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestRGWS3(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t)
	rgw, err := cluster.StartRGW(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := rgw.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	user, err := rgw.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-rgw-storage-check"})
	if err != nil {
		t.Fatal(err)
	}
	access, secret, err := user.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := s3HTTPClient{
		endpoint: endpoint, accessKey: rgw.AccessKey, secretKey: rgw.SecretKey, region: rgw.Region,
		http: &http.Client{Timeout: 45 * time.Second},
	}
	owned := bootstrap
	owned.accessKey, owned.secretKey = access, secret
	// Keep the original bootstrap credential scenario alongside the owned user's
	// storage observations. Both must pass the complete S3 data/auth sequence.
	fixtures := []struct {
		name, bucket string
		client       s3HTTPClient
		user         *ceph.RGWUser
	}{
		{name: "bootstrap", bucket: "/tc-rgw-poc", client: bootstrap},
		{name: "owned", bucket: "/tc-rgw-storage-check", client: owned, user: user},
	}
	payload := bytes.Repeat([]byte("RGW signed S3 roundtrip\n"), 4096)
	keys := []string{"payload-0", "payload-1", "nested/payload-2", "nested/payload-3"}
	verify := func(client s3HTTPClient, bucket string) {
		t.Helper()
		for _, key := range keys {
			actual := client.request(t, ctx, http.MethodGet, bucket+"/"+key, nil, http.StatusOK)
			if !bytes.Equal(actual, payload) {
				t.Fatalf("S3 object %s/%s payload changed: got %d bytes, want %d", bucket, key, len(actual), len(payload))
			}
		}
		listed := client.listKeys(t, ctx, bucket)
		if !slices.Equal(listed, keys) {
			t.Fatalf("S3 list %s: got %v, want %v", bucket, listed, keys)
		}
	}
	slices.Sort(keys)
	for _, fixture := range fixtures {
		client, bucket := fixture.client, fixture.bucket
		client.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
		var buckets struct {
			Buckets []struct {
				Name string `xml:"Name"`
			} `xml:"Buckets>Bucket"`
		}
		if err := xml.Unmarshal(client.request(t, ctx, http.MethodGet, "/", nil, http.StatusOK), &buckets); err != nil {
			t.Fatal(err)
		}
		if len(buckets.Buckets) != 1 || buckets.Buckets[0].Name != strings.TrimPrefix(bucket, "/") {
			t.Fatalf("unexpected %s S3 bucket listing: %+v", fixture.name, buckets)
		}
		if fixture.user != nil {
			rgwFixtureUserUsage(t, ctx, rgw, fixture.user, "zero-before-writes", 0, 0)
		}
		for _, key := range keys {
			client.request(t, ctx, http.MethodPut, bucket+"/"+key, payload, http.StatusOK)
		}
		verify(client, bucket)
		if fixture.user != nil {
			rgwFixtureUserUsage(t, ctx, rgw, fixture.user, "four-objects", uint64(len(keys)), uint64(len(keys)*len(payload)))
		}

		// Private buckets must reject both unsigned and incorrectly signed requests.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+bucket+"/"+keys[0], nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.http.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("unsigned private %s S3 read returned %d, want 403", fixture.name, response.StatusCode)
		}
		wrongKey := client
		wrongKey.secretKey += "wrong"
		wrongKey.request(t, ctx, http.MethodGet, bucket+"/"+keys[0], nil, http.StatusForbidden)
		t.Logf("host HTTP S3 client %s: created bucket, signed PUT/GET/ListObjectsV2 for %d objects; invalid/absent credentials rejected", fixture.name, len(keys))
	}

	// One shared OSD transition exercises retained data for both credential paths.
	advanceServiceTopology(t, ctx, cluster)
	for _, fixture := range fixtures {
		client, bucket := fixture.client, fixture.bucket
		verify(client, bucket)
		client.request(t, ctx, http.MethodPut, bucket+"/after-topology", payload, http.StatusOK)
		if actual := client.request(t, ctx, http.MethodGet, bucket+"/after-topology", nil, http.StatusOK); !bytes.Equal(actual, payload) {
			t.Fatalf("%s S3 write/read after topology change changed payload", fixture.name)
		}
		t.Logf("%s S3 payloads survived OSD add/remove; new writes/read after replacement succeeded", fixture.name)
		if fixture.user != nil {
			rgwFixtureUserUsage(t, ctx, rgw, fixture.user, "after-topology-five-objects", uint64(len(keys)+1), uint64((len(keys)+1)*len(payload)))
		}

		for _, key := range append(keys, "after-topology") {
			client.request(t, ctx, http.MethodDelete, bucket+"/"+key, nil, http.StatusNoContent)
		}
		client.request(t, ctx, http.MethodGet, bucket+"/payload-0", nil, http.StatusNotFound)
		if listed := client.listKeys(t, ctx, bucket); len(listed) != 0 {
			t.Fatalf("%s S3 delete left objects: %v", fixture.name, listed)
		}
		if fixture.user != nil {
			rgwFixtureUserUsage(t, ctx, rgw, fixture.user, "zero-after-object-delete", 0, 0)
		}
		client.request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
		t.Logf("%s S3 object deletion, missing-object 404, empty listing and bucket deletion succeeded", fixture.name)
	}
}
