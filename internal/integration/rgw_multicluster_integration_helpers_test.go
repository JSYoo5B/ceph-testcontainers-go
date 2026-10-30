//go:build all || (integration && multicluster)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func testMultiClusterRGWMultisite(t *testing.T, options ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	source, destination, _, _ := newMultiClusterPair(t, options...)
	essentialImage, _ := integrationImages(t)
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = essentialImage
	}
	// The RGW role needs no Python or Ceph CLI. Setup must select the
	// source and destination essential images without an explicit override.
	multisite, err := multisite.Run(ctx, rgwImage, multisite.WithConfig(multisite.Config{
		Source: source, Destination: destination,
	}))
	if multisite != nil {
		t.Cleanup(func() {
			if t.Failed() {
				logMultisiteReplicationState(t, multisite)
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := multisite.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate RGW multisite: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	sourceRGW := multisite.Source
	sourceEndpoint, err := sourceRGW.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationEndpoint, err := multisite.Destination.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("independent clusters share realm=%s with distinct primary=%s and secondary=%s zones", multisite.RealmID, multisite.SourceZoneID, multisite.DestinationZoneID)
	sourcePeer, sourcePeerErr := multisite.Source.DaemonEndpoint(ctx)
	destinationPeer, destinationPeerErr := multisite.Destination.DaemonEndpoint(ctx)
	if sourcePeerErr != nil || destinationPeerErr != nil {
		t.Fatalf("gateway daemon endpoints unavailable: source=%v destination=%v", sourcePeerErr, destinationPeerErr)
	}
	t.Logf("S3 endpoints source=%s destination=%s; daemon endpoints source=%s destination=%s; committed period master and zone endpoints verified", sourceEndpoint, destinationEndpoint, sourcePeer, destinationPeer)
	sourceS3 := s3HTTPClient{endpoint: sourceEndpoint, accessKey: sourceRGW.AccessKey, secretKey: sourceRGW.SecretKey, region: sourceRGW.Region, http: &http.Client{Timeout: 20 * time.Second}}
	destinationS3 := sourceS3
	destinationS3.endpoint = destinationEndpoint
	const bucket = multisiteDiagnosticBucket
	createMultisiteBucket(t, ctx, multisite, sourceS3, bucket, nil)
	payload := bytes.Repeat([]byte("native RGW cross-cluster replication\n"), 1024)
	keys := []string{"keep", "nested/payload", "remove"}
	for _, key := range keys {
		sourceS3.request(t, ctx, http.MethodPut, bucket+"/"+key, payload, http.StatusOK)
	}
	for _, key := range keys {
		waitMultisiteObject(t, ctx, destinationS3, bucket+"/"+key, http.StatusOK, payload)
	}
	slices.Sort(keys)
	if actual := destinationS3.listKeys(t, ctx, bucket); !slices.Equal(actual, keys) {
		t.Fatalf("secondary bucket listing: got %v, want %v", actual, keys)
	}
	t.Log("primary -> secondary: ordinary S3 user, bucket, listing and three object payloads replicated through native RGW multisite")

	replacement := bytes.Repeat([]byte("overwritten RGW replication payload\n"), 2048)
	sourceS3.request(t, ctx, http.MethodPut, bucket+"/keep", replacement, http.StatusOK)
	waitMultisiteObject(t, ctx, destinationS3, bucket+"/keep", http.StatusOK, replacement)
	sourceS3.request(t, ctx, http.MethodDelete, bucket+"/remove", nil, http.StatusNoContent)
	waitMultisiteObject(t, ctx, destinationS3, bucket+"/remove", http.StatusNotFound, nil)
	t.Log("primary object overwrite and deletion replicated to secondary; multisite also propagates deletion")

	// Both zones accept object writes by default. Bucket/user metadata still
	// has one master; this is not metadata-master promotion or split-brain DR.
	reversePayload := []byte("created on secondary, replicated to primary\n")
	destinationS3.request(t, ctx, http.MethodPut, bucket+"/reverse", reversePayload, http.StatusOK)
	waitMultisiteObject(t, ctx, sourceS3, bucket+"/reverse", http.StatusOK, reversePayload)
	destinationS3.request(t, ctx, http.MethodDelete, bucket+"/reverse", nil, http.StatusNoContent)
	waitMultisiteObject(t, ctx, sourceS3, bucket+"/reverse", http.StatusNotFound, nil)
	t.Log("secondary -> primary: object creation and deletion replicated in active-active mode")
	for _, zone := range multisite.Zones() {
		if _, err := multisite.ZoneAdmin(ctx, zone.Name, "sync", "status"); err != nil {
			t.Fatal(err)
		}
	}

	stopTimeout := 5 * time.Second
	if err := sourceRGW.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	if actual := destinationS3.request(t, ctx, http.MethodGet, bucket+"/keep", nil, http.StatusOK); !bytes.Equal(actual, replacement) {
		t.Fatal("secondary read during primary gateway outage changed payload")
	}
	outagePayload := []byte("secondary object write while primary gateway is stopped\n")
	destinationS3.request(t, ctx, http.MethodPut, bucket+"/during-primary-outage", outagePayload, http.StatusOK)
	if actual := destinationS3.request(t, ctx, http.MethodGet, bucket+"/during-primary-outage", nil, http.StatusOK); !bytes.Equal(actual, outagePayload) {
		t.Fatal("secondary write/read during primary gateway outage changed payload")
	}
	t.Log("with primary gateway stopped: secondary served a replicated object and accepted a new object in the existing bucket")
	if err := sourceRGW.Start(ctx); err != nil {
		t.Fatal(err)
	}
	sourceS3.endpoint, err = sourceRGW.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitMultisiteObject(t, ctx, sourceS3, bucket+"/during-primary-outage", http.StatusOK, outagePayload)
	t.Log("primary gateway restart caught up the object written during its outage")
	if err := sourceRGW.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	stopMultiClusterSource(t, ctx, source)
	if actual := destinationS3.request(t, ctx, http.MethodGet, bucket+"/keep", nil, http.StatusOK); !bytes.Equal(actual, replacement) {
		t.Fatal("secondary read with the primary gateway, MON and OSDs stopped changed payload")
	}
	t.Log("secondary served its replicated payload with the primary gateway, MON and OSDs stopped")
}

func waitMultisiteObject(t *testing.T, ctx context.Context, client s3HTTPClient, path string, wantStatus int, payload []byte) {
	t.Helper()
	// The first object of a freshly synced bucket regularly takes about two
	// minutes on CI runners; keep enough headroom for one extra sync round.
	pollCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	started := time.Now()
	var lastErr error
	for {
		request, err := http.NewRequestWithContext(pollCtx, http.MethodGet, client.endpoint+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		client.sign(request, nil, time.Now().UTC())
		response, err := client.http.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == wantStatus && (wantStatus != http.StatusOK || bytes.Equal(body, payload)) {
				t.Logf("multisite %s returned %d with expected payload after %s", path, wantStatus, time.Since(started).Round(time.Millisecond))
				return
			}
			var failure struct {
				Code, Message string
			}
			if response.StatusCode >= http.StatusBadRequest {
				_ = xml.Unmarshal(body, &failure)
			}
			lastErr = fmt.Errorf("HTTP %d, code=%q message=%q, payload bytes=%d, expected bytes=%d, read error=%v", response.StatusCode, failure.Code, failure.Message, len(body), len(payload), readErr)
		} else {
			lastErr = err
		}
		select {
		case <-pollCtx.Done():
			t.Fatalf("wait for RGW native replication of %s: %v; last response: %v", path, pollCtx.Err(), lastErr)
		case <-time.After(time.Second):
		}
	}
}

const multisiteDiagnosticBucket = "/tc-multicluster-bucket"

// createMultisiteBucket creates bucket through client and waits until every
// zone has synced the bucket's metadata. RGW can lose the objects written to a
// new bucket before that, as seen with Tentacle 20.2.4: the data sync of
// another zone then stores the bucket's metadata itself, and a second data
// sync shard may sync the bucket while its index does not exist yet; RGW takes
// the failed index updates for deleted objects and reports the zone caught up.
func createMultisiteBucket(t *testing.T, ctx context.Context, multisite *multisite.Fixture, client s3HTTPClient, bucket string, body []byte) {
	t.Helper()
	client.request(t, ctx, http.MethodPut, bucket, body, http.StatusOK)
	for _, zone := range multisite.Zones() {
		if _, err := multisite.WaitSyncReady(ctx, zone.Name); err != nil {
			t.Fatalf("wait for the metadata of %s in zone %s: %v", bucket, zone.Name, err)
		}
	}
}

// logMultisiteReplicationState records, after a failed replication wait, what
// each zone holds and how far its sync went. RGW passes a bucket log entry
// without a sync error when fetching the object returns ENOENT or EPERM, and a
// data log entry when the local bucket does not exist yet, so a zone can
// report caught up while an object is missing. The bucket and data logs, the
// sync markers, the zone endpoints and the gateways' sync warnings show which
// of these happened.
func logMultisiteReplicationState(t *testing.T, multisite *multisite.Fixture) {
	t.Helper()
	bucket := strings.TrimPrefix(multisiteDiagnosticBucket, "/")
	zones := multisite.Zones()
	for _, zone := range zones {
		inspect, stop := context.WithTimeout(context.Background(), 35*time.Second)
		status, statusErr := multisite.SyncStatus(inspect, zone.Name)
		stop()
		t.Logf("multisite sync diagnostic zone=%s status=%+v error=%v", zone.Name, status, statusErr)
		commands := [][]string{
			{"sync", "error", "list"},
			{"bucket", "sync", "status", "--bucket", bucket},
			{"bucket", "stats", "--bucket", bucket},
			{"bucket", "list", "--bucket", bucket},
			{"bilog", "list", "--bucket", bucket, "--max-entries", "20"},
			{"datalog", "status"},
			{"zonegroup", "get"},
		}
		for _, source := range zones {
			if source.Name != zone.Name {
				commands = append(commands,
					[]string{"bucket", "sync", "markers", "--bucket", bucket, "--source-zone", source.Name},
					[]string{"data", "sync", "status", "--source-zone", source.Name})
			}
		}
		for _, args := range commands {
			inspect, stop := context.WithTimeout(context.Background(), 35*time.Second)
			out, err := multisite.ZoneAdmin(inspect, zone.Name, args...)
			stop()
			text := string(out)
			if args[0] == "sync" {
				text = compactDiagnostic(out)
			}
			t.Logf("multisite native diagnostic zone=%s %v: %s error=%v", zone.Name, args, text, err)
		}
	}
	for _, gateway := range []*rgw.Gateway{multisite.Source, multisite.Destination} {
		if gateway == nil {
			continue
		}
		logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		logRGWSyncProblems(t, logCtx, gateway.Container)
		multiClusterLogContainer(t, logCtx, gateway.Container)
		cancel()
	}
}

// rgwSyncProblem matches the RGW log lines of level 0 or -1 that report a
// skipped or failed step, such as "bucket shard callback failed", which RGW
// logs whether or not it records a sync error.
var rgwSyncProblem = regexp.MustCompile(`^\S+ \S+ +(?:-1|0) .*(?i:skipping|error|warning|failed)`)

// logRGWSyncProblems logs the first and last 100 sync problem lines of a
// gateway's whole log, which the tail of the log no longer holds.
func logRGWSyncProblems(t *testing.T, ctx context.Context, ctr testcontainers.Container) {
	t.Helper()
	logs, err := ctr.Logs(ctx)
	if err != nil {
		t.Logf("container %s logs: %v", ctr.GetContainerID(), err)
		return
	}
	data, _ := io.ReadAll(logs)
	_ = logs.Close()
	var problems []string
	for _, line := range strings.Split(string(data), "\n") {
		if rgwSyncProblem.MatchString(line) {
			problems = append(problems, line)
		}
	}
	total := len(problems)
	if total > 200 {
		problems = append(problems[:100], problems[total-100:]...)
	}
	t.Logf("container %s: %d log lines report sync problems:\n%s", ctr.GetContainerID(), total, strings.Join(problems, "\n"))
}

// compactDiagnostic drops empty sync error shards so a failure log keeps the
// entries that explain a stalled bucket.
func compactDiagnostic(out []byte) string {
	var shards []struct {
		ShardID int               `json:"shard_id"`
		Entries []json.RawMessage `json:"entries"`
	}
	if json.Unmarshal(out, &shards) != nil {
		return string(out)
	}
	shards = slices.DeleteFunc(shards, func(shard struct {
		ShardID int               `json:"shard_id"`
		Entries []json.RawMessage `json:"entries"`
	}) bool {
		return len(shard.Entries) == 0
	})
	compact, err := json.Marshal(shards)
	if err != nil {
		return string(out)
	}
	return string(compact)
}
