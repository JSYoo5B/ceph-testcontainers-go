//go:build integration && multicluster

package ceph_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This exercises RGW's native HTTP multisite replication. It never copies an
// object through the host or shares an OSD between the independent clusters.
func TestMultiClusterRGWMultisite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)

	// The gateways share an HTTP-only bridge. Each also keeps an alias on its
	// own Ceph network, which the fixture's dual-network admin clients can use.
	// Neither gateway joins the other cluster's MON/OSD network.
	bridge, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := bridge.Remove(cleanupCtx); err != nil {
			t.Errorf("remove RGW federation network: %v", err)
		}
	})
	const realm, zonegroup, sourceZone, destinationZone = "tc-federation", "us-east-1", "tc-primary", "tc-secondary"
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	sourceAlias, destinationAlias := "rgw-primary-"+suffix, "rgw-secondary-"+suffix
	sourceURL, destinationURL := "http://"+sourceAlias+":7480", "http://"+destinationAlias+":7480"
	systemAccess := strings.ReplaceAll(uuid.NewString(), "-", "")
	systemSecret := uuid.NewString() + uuid.NewString()

	primaryAdmin := func(args ...string) []byte {
		return rgwMultisiteAdmin(t, ctx, sourceClient, realm, zonegroup, sourceZone, args...)
	}
	secondaryAdmin := func(args ...string) []byte {
		return rgwMultisiteAdmin(t, ctx, destinationClient, realm, zonegroup, destinationZone, args...)
	}
	primaryAdmin("realm", "create", "--default")
	primaryAdmin("zonegroup", "create", "--master", "--default", "--endpoints", sourceURL)
	primaryAdmin("zone", "create", "--master", "--default", "--endpoints", sourceURL)
	primaryAdmin("user", "create", "--uid", "tc-sync", "--display-name", "Testcontainers multisite sync", "--system",
		"--access-key", systemAccess, "--secret-key", systemSecret)
	primaryAdmin("zone", "modify", "--access-key", systemAccess, "--secret", systemSecret)
	primaryAdmin("period", "update", "--commit")
	sourceRGW, sourceEndpoint := startMultisiteRGW(t, ctx, source, bridge, sourceAlias, realm, zonegroup, sourceZone)

	// realm pull also imports the master's current period. The secondary's
	// period commit registers its new zone with the metadata master over HTTP.
	secondaryAdmin("realm", "pull", "--url", sourceURL, "--access-key", systemAccess, "--secret", systemSecret)
	secondaryAdmin("realm", "default")
	secondaryAdmin("zone", "create", "--endpoints", destinationURL, "--access-key", systemAccess, "--secret", systemSecret)
	secondaryAdmin("period", "update", "--commit")
	_, destinationEndpoint := startMultisiteRGW(t, ctx, destination, bridge, destinationAlias, realm, zonegroup, destinationZone)

	idFromJSON := func(data []byte) string {
		t.Helper()
		var document struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(data, &document); err != nil || document.ID == "" {
			t.Fatalf("decode RGW realm/zone ID: %v", err)
		}
		return document.ID
	}
	sourceRealm := idFromJSON(primaryAdmin("realm", "get"))
	destinationRealm := idFromJSON(secondaryAdmin("realm", "get"))
	sourceZoneID := idFromJSON(primaryAdmin("zone", "get"))
	destinationZoneID := idFromJSON(secondaryAdmin("zone", "get"))
	if sourceRealm != destinationRealm || sourceZoneID == destinationZoneID {
		t.Fatalf("multisite identity: realm %s/%s zone %s/%s", sourceRealm, destinationRealm, sourceZoneID, destinationZoneID)
	}
	t.Logf("independent clusters share realm=%s with distinct primary=%s and secondary=%s zones", sourceRealm, sourceZoneID, destinationZoneID)

	// Create an ordinary user after both gateways start, so successful signed
	// reads on the secondary prove metadata replication as well as object I/O.
	access := strings.ReplaceAll(uuid.NewString(), "-", "")
	secret := uuid.NewString() + uuid.NewString()
	primaryAdmin("user", "create", "--uid", "tc-federated-user", "--display-name", "Testcontainers federated S3 user",
		"--access-key", access, "--secret-key", secret)
	sourceS3 := s3HTTPClient{endpoint: sourceEndpoint, accessKey: access, secretKey: secret, region: zonegroup, http: &http.Client{Timeout: 20 * time.Second}}
	destinationS3 := sourceS3
	destinationS3.endpoint = destinationEndpoint
	const bucket = "/tc-federated-bucket"
	sourceS3.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
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
	primaryAdmin("sync", "status")
	secondaryAdmin("sync", "status")

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
	sourceS3.endpoint, err = sourceRGW.PortEndpoint(ctx, "7480/tcp", "http")
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

func startMultisiteRGW(t *testing.T, ctx context.Context, cluster *ceph.Container, bridge *testcontainers.DockerNetwork, alias, realm, zonegroup, zone string) (testcontainers.Container, string) {
	t.Helper()
	image := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if image == "" {
		image = os.Getenv("CEPH_TEST_IMAGE")
	}
	if image == "" {
		image = ceph.DefaultImage
	}
	ctr, err := testcontainers.Run(ctx, image, cluster.WithClient(),
		network.WithNetwork([]string{alias}, bridge),
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			req.NetworkAliases[cluster.NetworkName()] = []string{alias}
			return nil
		}),
		testcontainers.WithEntrypoint("/bin/sh", "-c", `mkdir -p /var/run/ceph; exec radosgw -f -n client.admin --keyring /etc/ceph/ceph.client.admin.keyring --rgw-realm "$1" --rgw-zonegroup "$2" --rgw-zone "$3" --rgw-frontends 'beast port=7480' --rgw-thread-pool-size 4 --rgw-sync-obj-etag-verify true --osd-pool-default-pg-num 1 --osd-pool-default-pgp-num 0`, "rgw-multisite"),
		testcontainers.WithCmd(realm, zonegroup, zone),
		testcontainers.WithExposedPorts("7480/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/").WithPort("7480/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK || status == http.StatusForbidden }).
			WithStartupTimeout(3*time.Minute)),
	)
	if ctr != nil {
		cleanupMultiClusterContainer(t, ctr)
	}
	if err != nil {
		t.Fatalf("start %s RGW multisite gateway: %v", zone, err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, "7480/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}
	return ctr, endpoint
}

func rgwMultisiteAdmin(t *testing.T, ctx context.Context, client testcontainers.Container, realm, zonegroup, zone string, args ...string) []byte {
	t.Helper()
	command := []string{"radosgw-admin", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--rgw-realm", realm, "--rgw-zonegroup", zonegroup, "--rgw-zone", zone, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0"}
	code, reader, err := client.Exec(ctx, append(command, args...), tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("radosgw-admin %s %s: %v", args[0], args[1], err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("radosgw-admin %s %s in %s exited %d: %s", args[0], args[1], zone, code, output)
	}
	return output
}

func waitMultisiteObject(t *testing.T, ctx context.Context, client s3HTTPClient, path string, wantStatus int, payload []byte) {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
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
			response.Body.Close()
			if readErr == nil && response.StatusCode == wantStatus && (wantStatus != http.StatusOK || bytes.Equal(body, payload)) {
				t.Logf("multisite %s returned %d with expected payload after %s", path, wantStatus, time.Since(started).Round(time.Millisecond))
				return
			}
			lastErr = fmt.Errorf("HTTP %d, payload bytes=%d, expected bytes=%d, read error=%v", response.StatusCode, len(body), len(payload), readErr)
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
