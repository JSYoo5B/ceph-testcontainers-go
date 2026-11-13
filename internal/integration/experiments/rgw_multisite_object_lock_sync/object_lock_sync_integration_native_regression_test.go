//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_object_lock_sync)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rgw

package rgw_multisite_object_lock_sync_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

const (
	lockBucket  = "tc-lock"
	plainBucket = "tc-plain"
	// The marker buckets of the two metadata checkpoints.
	lockFirstMarker  = "tc-lock-marker-1"
	lockSecondMarker = "tc-lock-marker-2"
	lockCanaryKey    = "canary"
	// How long the destination zone is read for a configuration after the
	// metadata checkpoint. Every measured build that propagates it does so
	// before the checkpoint returns.
	lockArrivalBound = 2 * time.Minute
)

// image is the Ceph image every cluster and gateway of the case runs: Tentacle
// 20.2.4, which reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_multisite_object_lock_sync_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.5, Umbrella 21.2.0) does not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

type lockCase struct {
	t       *testing.T
	ctx     context.Context
	fixture *multisite.Fixture
	clients map[string]harness.S3HTTPClient
	// made lists the buckets whose creation zone a answered with 200, in
	// order; the cleanup deletes exactly these.
	made []string
}

// createBucket creates a bucket in zone a, the metadata master, and records
// it for the cleanup.
func (c *lockCase) createBucket(name string, headers http.Header) {
	c.t.Helper()
	c.must("a", http.MethodPut, "/"+name, nil, headers, http.StatusOK)
	c.made = append(c.made, name)
}

// do sends one signed S3 request to a zone and returns the answer without
// stopping the test, so that it can be used in a probe.
func (c *lockCase) do(ctx context.Context, zone, method, path string, payload []byte, headers http.Header) (int, []byte, error) {
	client := c.clients[zone]
	request, err := http.NewRequestWithContext(ctx, method, client.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	for key, values := range headers {
		request.Header[key] = values
	}
	harness.S3FeatureSign(request, client, payload, time.Now().UTC(), "s3")
	response, err := client.HTTP.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

// must is do for a step that the case cannot continue without.
func (c *lockCase) must(zone, method, path string, payload []byte, headers http.Header, want int) []byte {
	c.t.Helper()
	status, body, err := c.do(c.ctx, zone, method, path, payload, headers)
	if err != nil || status != want {
		c.t.Fatalf("zone %s %s %s: status=%d code=%q error=%v, want %d", zone, method, path, status, harness.RGWBackendErrorCode(body), err, want)
	}
	return body
}

// read is one GET ?object-lock of a bucket in a zone.
func (c *lockCase) read(ctx context.Context, zone, bucket string) lockRead {
	read := lockRead{Zone: zone, Bucket: bucket}
	status, body, err := c.do(ctx, zone, http.MethodGet, "/"+bucket+"?object-lock", nil, nil)
	if err != nil {
		read.Err = err.Error()
		return read
	}
	read.Status, read.Code, read.Raw = status, harness.RGWBackendErrorCode(body), rawBody(body)
	if status == http.StatusOK {
		if read.Config, err = parseLockConfig(body); err != nil {
			read.Unparsed = err.Error()
		}
	}
	return read
}

// stored reports whether the bucket instance metadata that a zone's
// radosgw-admin prints holds an object lock configuration. This is the
// record that metadata sync transfers between zones.
func (c *lockCase) stored(zone, bucket string) (id string, hasLock bool) {
	c.t.Helper()
	var stats struct {
		ID string `json:"id"`
	}
	out, err := c.fixture.ZoneAdmin(c.ctx, zone, "bucket", "stats", "--bucket", bucket)
	if err != nil || json.Unmarshal(out, &stats) != nil || stats.ID == "" {
		c.t.Fatalf("zone %s: bucket stats %s: %v", zone, bucket, err)
	}
	var instance struct {
		Data struct {
			Info map[string]json.RawMessage `json:"bucket_info"`
		} `json:"data"`
	}
	out, err = c.fixture.ZoneAdmin(c.ctx, zone, "metadata", "get", "bucket.instance:"+bucket+":"+stats.ID)
	if err != nil || json.Unmarshal(out, &instance) != nil || len(instance.Data.Info) == 0 {
		c.t.Fatalf("zone %s: metadata get of the instance of %s: %v", zone, bucket, err)
	}
	_, hasLock = instance.Data.Info["obj_lock"]
	return stats.ID, hasLock
}

// checkpoint waits until zone b reports its metadata caught up with zone
// a's, and then until a marker bucket created in zone a after the change
// under test exists in zone b: metadata written later than the change has
// arrived.
func (c *lockCase) checkpoint(step, marker string) {
	c.t.Helper()
	c.createBucket(marker, nil)
	status, err := c.fixture.WaitSyncReady(c.ctx, "b", "a")
	if err != nil {
		c.t.Fatalf("another fault: %s: zone b's sync did not become ready: %v", step, err)
	}
	seen, err := harness.Observe(c.ctx, 3*time.Minute, 20*time.Second, time.Second, func(probe context.Context) (int, bool, error) {
		code, _, err := c.do(probe, "b", http.MethodHead, "/"+marker, nil, nil)
		if err != nil {
			return 0, false, nil
		}
		return code, code == http.StatusOK, nil
	})
	c.t.Logf("RGW_LOCK checkpoint step=%s metadata-caught-up=%t marker=%s marker-in-b=%d after=%s", step, status.Metadata.CaughtUp, marker, seen.Value, seen.Elapsed.Round(time.Millisecond))
	if err != nil {
		c.t.Fatalf("another fault: %s: the marker bucket %s, created in zone a after the configuration, did not reach zone b (last status %d): %v", step, marker, seen.Value, err)
	}
}

// propagate applies the configuration to the locked bucket in zone a,
// requires zone a to read it back, passes the metadata checkpoint and then
// reads zone b until it answers the same configuration or the bound ends.
// It returns the verdict for zone b.
func (c *lockCase) propagate(step, marker string, want lockConfig) string {
	c.t.Helper()
	body := want.document()
	sum := md5.Sum(body)
	c.must("a", http.MethodPut, "/"+lockBucket+"?object-lock", body, http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(sum[:])}}, http.StatusOK)
	source := c.read(c.ctx, "a", lockBucket)
	c.t.Logf("RGW_LOCK read step=%s %s", step, source)
	c.t.Logf("RGW_LOCK raw step=%s zone=a bucket=%s body=%s", step, lockBucket, source.Raw)
	if verdict := judgeSource(source, want); verdict != "" {
		c.t.Fatal(verdict)
	}
	c.checkpoint(step, marker)
	seen, _ := harness.Observe(c.ctx, lockArrivalBound, 20*time.Second, 2*time.Second, func(probe context.Context) (lockRead, bool, error) {
		read := c.read(probe, "b", lockBucket)
		return read, read.has(want), nil
	})
	c.t.Logf("RGW_LOCK read step=%s %s reads=%d after=%s", step, seen.Value, seen.Attempts, seen.Elapsed.Round(time.Millisecond))
	// The body of the last read only, not of every poll.
	c.t.Logf("RGW_LOCK raw step=%s zone=b bucket=%s body=%s", step, lockBucket, seen.Value.Raw)
	for _, zone := range []string{"a", "b"} {
		id, hasLock := c.stored(zone, lockBucket)
		c.t.Logf("RGW_LOCK stored step=%s zone=%s bucket=%s instance=%s metadata-has-obj_lock=%t", step, zone, lockBucket, id, hasLock)
	}
	// The control bucket and the source are read again after the wait, so
	// that the verdict compares answers of the same moment.
	for _, zone := range []string{"a", "b"} {
		plain := c.read(c.ctx, zone, plainBucket)
		c.t.Logf("RGW_LOCK read step=%s %s", step, plain)
		if verdict := judgeUnlocked(plain); verdict != "" {
			c.t.Fatal(verdict)
		}
	}
	if verdict := judgeSource(c.read(c.ctx, "a", lockBucket), want); verdict != "" {
		c.t.Fatal(verdict)
	}
	return judgeDestination(seen.Value, want)
}

// cleanup deletes the canary and the buckets in zone a, where they were
// created. The locked bucket never holds an object, so nothing retained is
// left behind.
func (c *lockCase) cleanup() {
	buckets := c.made
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := false
	remove := func(path string, allowed ...int) {
		status, body, err := c.do(ctx, "a", http.MethodDelete, path, nil, nil)
		for _, code := range allowed {
			if err == nil && status == code {
				return
			}
		}
		c.t.Errorf("RGW_LOCK cleanup failed: delete %s: status=%d code=%q error=%v", path, status, harness.RGWBackendErrorCode(body), err)
		failed = true
	}
	for _, bucket := range buckets {
		if bucket == plainBucket {
			// Deleting an object that was never written also answers 204.
			remove("/"+plainBucket+"/"+lockCanaryKey, http.StatusNoContent)
		}
		remove("/"+bucket, http.StatusNoContent)
	}
	c.t.Logf("RGW_LOCK cleanup buckets=%d failed=%t", len(buckets), failed)
}

// TestMultiClusterRGWBucketObjectLockSync reports whether a bucket's object
// lock configuration, applied in the metadata master zone, fails to reach the
// other zone of the realm through metadata sync.
//
// Given a two-zone realm whose data path works (an object of a bucket created
// without object lock, the control, reaches zone b with its bytes) and a
// bucket created with object lock that zone a reads back with versioning
// enabled, when a default retention rule is applied in zone a, zone a reads
// it back and a metadata checkpoint created after it reaches zone b, then the
// defect is REPRODUCED if zone b answers no configuration or another one
// (judgeDestination's "observed defect"), and NOT_REPRODUCED if zone b
// answers the same rule, and later the rule that replaces it. An answer that
// is not about the configuration is another fault and fails the test.
// Tentacle 20.2.4 leaves the rule out of the bucket instance record that
// metadata sync transfers, so it reproduces; Tentacle 20.2.5 and Umbrella
// 21.2.0 do not.
//
// A failure of the given or when steps fails the test; the result does not.
func TestMultiClusterRGWBucketObjectLockSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	t.Logf("RGW_LOCK image=%s", image)
	source, _ := harness.NewServiceClusterWithOptions(t, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	destination, _ := harness.NewServiceClusterWithOptions(t, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_LOCK ceph=%s,%s", harness.StaticFacts(source).CephVersion, harness.StaticFacts(destination).CephVersion)
	fixture, err := multisite.Run(ctx, image, multisite.WithTopology(multisite.TopologyConfig{
		Zonegroup: "us", MetadataMaster: "a",
		Zones: []multisite.ZoneConfig{{Name: "a", Cluster: source}, {Name: "b", Cluster: destination}},
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
	c := &lockCase{t: t, ctx: ctx, fixture: fixture, clients: map[string]harness.S3HTTPClient{}}
	zones := fixture.Zones()
	if len(zones) != 2 || zones[0].Name != "a" || zones[1].Name != "b" {
		t.Fatalf("zones %+v", zones)
	}
	for _, zone := range zones {
		endpoint, err := zone.Gateway.S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c.clients[zone.Name] = harness.S3HTTPClient{Endpoint: endpoint, AccessKey: zone.Gateway.AccessKey, SecretKey: zone.Gateway.SecretKey, Region: zone.Gateway.Region, HTTP: &http.Client{Timeout: 20 * time.Second}}
		t.Logf("RGW_LOCK zone=%s id=%s gateway=%s", zone.Name, zone.ID, strings.TrimSpace(harness.ExecOutput(t, ctx, zone.Gateway, "radosgw", "--version")))
	}

	// Every bucket is created in zone a, the metadata master. The cleanup
	// is registered before the first one and deletes the buckets whose
	// creation succeeded, the checkpoints' markers included.
	t.Cleanup(c.cleanup)
	c.createBucket(lockBucket, http.Header{"X-Amz-Bucket-Object-Lock-Enabled": {"true"}})
	c.createBucket(plainBucket, nil)

	// Condition: both buckets exist in zone b, and an object of the control
	// bucket reaches it with its bytes.
	if _, err := fixture.WaitSyncReady(ctx, "b", "a"); err != nil {
		t.Fatalf("condition not established: zone b's sync did not become ready: %v", err)
	}
	canary := []byte("written to zone a before any object lock configuration\n")
	for _, bucket := range []string{lockBucket, plainBucket} {
		seen, err := harness.Observe(ctx, 3*time.Minute, 20*time.Second, time.Second, func(probe context.Context) (int, bool, error) {
			code, _, err := c.do(probe, "b", http.MethodHead, "/"+bucket, nil, nil)
			return code, err == nil && code == http.StatusOK, nil
		})
		if err != nil {
			t.Fatalf("condition not established: bucket %s did not reach zone b (last status %d): %v", bucket, seen.Value, err)
		}
	}
	c.must("a", http.MethodPut, "/"+plainBucket+"/"+lockCanaryKey, canary, nil, http.StatusOK)
	arrived, err := harness.Observe(ctx, 6*time.Minute, 20*time.Second, 2*time.Second, func(probe context.Context) (int, bool, error) {
		code, body, err := c.do(probe, "b", http.MethodGet, "/"+plainBucket+"/"+lockCanaryKey, nil, nil)
		return code, err == nil && code == http.StatusOK && bytes.Equal(body, canary), nil
	})
	t.Logf("RGW_LOCK canary bucket=%s status-in-b=%d after=%s", plainBucket, arrived.Value, arrived.Elapsed.Round(time.Millisecond))
	if err != nil {
		t.Fatalf("condition not established: the canary object did not reach zone b with its bytes (last status %d): %v", arrived.Value, err)
	}
	// Before any configuration is applied, the bucket created with object
	// lock answers "enabled, no rule" in the zone it was created in.
	before := c.read(ctx, "a", lockBucket)
	t.Logf("RGW_LOCK read step=created %s", before)
	if !before.has(lockConfig{Enabled: true}) {
		t.Fatalf("condition not established: the bucket created with object lock does not answer an enabled configuration without a rule in zone a: %s", before)
	}
	t.Logf("RGW_LOCK raw step=created zone=a bucket=%s body=%s", lockBucket, before.Raw)
	created := c.read(ctx, "b", lockBucket)
	t.Logf("RGW_LOCK read step=created %s", created)
	t.Logf("RGW_LOCK raw step=created zone=b bucket=%s body=%s", lockBucket, created.Raw)
	// Object lock requires versioning. The source must read it back as
	// enabled before any rule is applied; zone b's answer is recorded.
	for _, zone := range []string{"a", "b"} {
		status, body, err := c.do(ctx, zone, http.MethodGet, "/"+lockBucket+"?versioning", nil, nil)
		t.Logf("RGW_LOCK versioning zone=%s bucket=%s status=%d error=%v body=%s", zone, lockBucket, status, err, rawBody(body))
		if verdict := judgeVersioning(status, body, err); zone == "a" && verdict != "" {
			t.Fatal(verdict)
		}
	}

	first := lockConfig{Enabled: true, Mode: "GOVERNANCE", Days: 1}
	second := lockConfig{Enabled: true, Mode: "COMPLIANCE", Years: 1}
	// Each symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	observe := func(verdict string) bool {
		switch {
		case verdict == "":
			return true
		case strings.HasPrefix(verdict, "observed defect:"):
			symptoms = append(symptoms, verdict)
		default:
			t.Fatal(verdict)
		}
		return false
	}
	if observe(c.propagate("initial", lockFirstMarker, first)) {
		// An initial configuration that arrives does not show that a change
		// of it does: the second one differs in mode and in the unit of its
		// period.
		observe(c.propagate("update", lockSecondMarker, second))
	} else {
		t.Logf("RGW_LOCK step=update not run: the first configuration did not reach zone b, so an update has nothing to replace there")
	}
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_multisite_object_lock_sync result=%s image=%s symptoms=%q", result, image, symptoms)
}
