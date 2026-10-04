//go:build integration && multicluster

package integration_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	"os"
)

// This exercises RGW's native HTTP multisite replication. It never copies an
// object through the host or shares an OSD between the independent clusters.
func TestMultiClusterRGWMultisite(t *testing.T) {
	testMultiClusterRGWMultisite(t)
}

func testMultiClusterRGWMultisite(t *testing.T, options ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	source, destination, _, _ := newMultiClusterPair(t, options...)
	controlImage, _ := integrationImages(t)
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = controlImage
	}
	// The RGW role needs no Python or Ceph CLI. Setup must select the
	// source and destination control images without an explicit override.
	multisite, err := multicluster.RunRGWMultisite(ctx, rgwImage, multicluster.RGWMultisiteConfig{
		Source: source, Destination: destination,
	})
	if multisite != nil {
		t.Cleanup(func() {
			if t.Failed() {
				for _, gateway := range []*ceph.RGWContainer{multisite.Source, multisite.Destination} {
					if gateway == nil {
						continue
					}
					logCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					multiClusterLogContainer(t, logCtx, gateway.Container)
					cancel()
				}
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
	const bucket = "/tc-multicluster-bucket"
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
	if _, err := multisite.SourceAdmin(ctx, "sync", "status"); err != nil {
		t.Fatal(err)
	}
	if _, err := multisite.DestinationAdmin(ctx, "sync", "status"); err != nil {
		t.Fatal(err)
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
