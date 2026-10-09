//go:build all || (integration && multicluster && features)

package integration_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"os"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

func waitRGWTranslationPolicyReady(t *testing.T, ctx context.Context, link *multicluster.RGWMultisite, group *multicluster.RGWSyncGroup) {
	t.Helper()
	status, err := link.WaitBucketSyncPolicyReady(ctx, group, "destination")
	if err != nil || !status.Imported || !status.PeriodImported || !status.PolicyImported || !status.BucketsImported || status.Bucket.ID == "" || len(status.Buckets) == 0 {
		t.Fatalf("wait owned bucket policy import: group=%s period=%s zone=%s/%s bucket=%s/%s:%s period_imported=%t policy_imported=%t buckets_imported=%t error=%s", group.ID(), status.PeriodID, status.Zone, status.ZoneID, status.Bucket.Tenant, status.Bucket.Name, status.Bucket.ID, status.PeriodImported, status.PolicyImported, status.BucketsImported, rgwSyncDiagnosticError(err))
	}
	t.Logf("owned bucket policy imported before writes: group=%s period=%s zone=%s/%s scope=%s/%s:%s referenced_buckets=%+v", group.ID(), status.PeriodID, status.Zone, status.ZoneID, status.Bucket.Tenant, status.Bucket.Name, status.Bucket.ID, status.Buckets)
}

// Child cleanups run before the next sequential t.Run, including after Fatal.
// The public removal guard verifies the owned bucket instance and unchanged
// group, and successful explicit removals make this cleanup idempotent.
func cleanupRGWTranslationSyncGroup(t *testing.T, link *multicluster.RGWMultisite, group *multicluster.RGWSyncGroup, pipeIDs ...string) {
	t.Helper()
	pipeIDs = append([]string(nil), pipeIDs...)
	t.Cleanup(func() {
		if t.Failed() {
			inspect, stop := context.WithTimeout(context.Background(), time.Minute)
			for _, pipeID := range pipeIDs {
				attempt, cancel := context.WithTimeout(inspect, 20*time.Second)
				state, err := link.BucketSyncStatus(attempt, group, pipeID, "source", "destination")
				cancel()
				t.Logf("translation bucket diagnostic group=%s pipe=%s realm=%s period=%s zonegroup=%s realm_epoch=%d source_zone=%s/%s source_bucket=%s/%s:%s destination_zone=%s/%s destination_bucket=%s/%s:%s state=%s shards=%d behind_shards=%d caught_up=%t status_error=%s", group.ID(), pipeID, state.RealmID, state.PeriodID, state.ZonegroupID, state.RealmEpoch, state.SourceZone, state.SourceZoneID, state.SourceBucket.Tenant, state.SourceBucket.Name, state.SourceBucket.ID, state.Zone, state.ZoneID, state.DestinationBucket.Tenant, state.DestinationBucket.Name, state.DestinationBucket.ID, state.State, state.Shards, state.BehindShards, state.CaughtUp, rgwSyncDiagnosticError(err))
			}
			stop()
		}
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		if err := link.RemoveSyncGroup(cleanup, group); err != nil {
			t.Errorf("cleanup isolated translation sync group %s: %v", group.ID(), err)
			return
		}
		t.Logf("removed isolated owned translation sync group %s before later subtests", group.ID())
	})
}

func assertRGWSyncClass(t *testing.T, ctx context.Context, client s3HTTPClient, bucket, key, class string) {
	t.Helper()
	var listing struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(client.request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &listing); err != nil {
		t.Fatal(err)
	}
	for _, entry := range listing.Contents {
		if entry.Key == key {
			if entry.StorageClass != class {
				t.Fatalf("replica %s class=%s want=%s", key, entry.StorageClass, class)
			}
			return
		}
	}
	t.Fatalf("replica listing omitted %s", key)
}

type rgwSyncTranslationFixture struct {
	ctx                                  context.Context
	source, destination                  *ceph.Container
	link                                 *multicluster.RGWMultisite
	primaryPlacement, secondaryPlacement *ceph.RGWPlacement
	ownerA, ownerB                       *ceph.RGWUser
	client                               func(t *testing.T, user *ceph.RGWUser, endpoint string) s3HTTPClient
	sourceEndpoint, destEndpoint         string
	a, b, destA, destB                   s3HTTPClient
	body                                 []byte
}

func newRGWSyncTranslationFixture(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) *rgwSyncTranslationFixture {
	// Independent scenarios share realm/storage bootstrap. Reserve bounded
	// fixture time for strict four-minute observations and child cleanup after
	// a failing scenario; per-object/checkpoint and exclusion windows stay fixed.
	source, destination, _, _ := newMultiClusterPair(t, append(opts, ceph.WithOSDCount(1))...)
	image, _ := integrationImages(t)
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = image
	}
	link, err := multicluster.RunRGWMultisite(ctx, rgwImage, multicluster.RGWMultisiteConfig{Source: source, Destination: destination, ControlImage: image, Realm: "tc-sync-translation", SourceZone: "source", DestinationZone: "destination"})
	if link != nil {
		t.Cleanup(func() {
			if t.Failed() {
				for _, name := range []string{"source", "destination"} {
					inspect, stop := context.WithTimeout(context.Background(), 35*time.Second)
					state, statusErr := link.SyncStatus(inspect, name)
					stop()
					t.Logf("sync translation diagnostic zone=%s status=%+v error=%v", name, state, statusErr)
				}
			}
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			if err := link.Terminate(cleanup); err != nil {
				t.Errorf("terminate translation fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		for _, pool := range []string{"tc-sync-index", "tc-sync-extra", "tc-sync-standard", "tc-sync-ia"} {
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Application: "rgw"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inline := false
	placementConfig := ceph.RGWPlacementConfig{Name: "tc-sync-tiered", IndexPool: "tc-sync-index", DataExtraPool: "tc-sync-extra", InlineData: &inline, StorageClasses: []ceph.RGWStorageClassConfig{{Name: "STANDARD", DataPool: "tc-sync-standard"}, {Name: "STANDARD_IA", DataPool: "tc-sync-ia"}}}
	primaryPlacement, err := link.Source.CreatePlacement(ctx, placementConfig)
	if err != nil {
		t.Fatal(err)
	}
	secondaryPlacement, err := link.Destination.CreatePlacement(ctx, placementConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Source.ApplyPlacement(ctx, primaryPlacement); err != nil {
		t.Fatal(err)
	}
	if err := link.PullDestinationPeriod(ctx); err != nil {
		t.Fatal(err)
	}
	if err := link.Destination.ReloadPlacement(ctx, secondaryPlacement); err != nil {
		t.Fatal(err)
	}
	global, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{}, multicluster.RGWSyncGroupConfig{ID: "permission", Status: multicluster.RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncFlow(ctx, global, multicluster.RGWSyncFlowConfig{SourceZone: "source", DestinationZone: "destination"}); err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncPipe(ctx, global, multicluster.RGWSyncPipeConfig{ID: "permit", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := link.ApplySyncGroup(ctx, global); err != nil {
		t.Fatal(err)
	}
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	ownerA, err := link.Source.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-sync-owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	ownerB, err := link.Source.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-sync-owner-b"})
	if err != nil {
		t.Fatal(err)
	}
	sourceEndpoint, err := link.Source.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destEndpoint, err := link.Destination.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := func(t *testing.T, user *ceph.RGWUser, endpoint string) s3HTTPClient {
		t.Helper()
		access, secret, err := user.Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: link.Source.Region, http: &http.Client{Timeout: 15 * time.Second}}
	}
	a, b := client(t, ownerA, sourceEndpoint), client(t, ownerB, sourceEndpoint)
	destA, destB := client(t, ownerA, destEndpoint), client(t, ownerB, destEndpoint)
	body := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + primaryPlacement.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	return &rgwSyncTranslationFixture{ctx: ctx, source: source, destination: destination, link: link, primaryPlacement: primaryPlacement, secondaryPlacement: secondaryPlacement, ownerA: ownerA, ownerB: ownerB, client: client, sourceEndpoint: sourceEndpoint, destEndpoint: destEndpoint, a: a, b: b, destA: destA, destB: destB, body: body}
}

func testRGWSyncTranslationFiltering(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	f := newRGWSyncTranslationFixture(t, ctx, opts...)
	runRGWSyncTranslationCases(t, f)
}
