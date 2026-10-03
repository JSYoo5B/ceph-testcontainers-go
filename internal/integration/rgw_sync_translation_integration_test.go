//go:build integration && multicluster && features

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

func TestMultiClusterRGWSyncTranslationFiltering(t *testing.T) {
	testRGWSyncTranslationFiltering(t)
}
func TestHostNetworkMultiClusterRGWSyncTranslationFiltering(t *testing.T) {
	testRGWSyncTranslationFiltering(t, ceph.WithHostNetwork())
}

func testRGWSyncTranslationFiltering(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
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
	client := func(user *ceph.RGWUser, endpoint string) s3HTTPClient {
		access, secret, err := user.Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: link.Source.Region, http: &http.Client{Timeout: 15 * time.Second}}
	}
	a, b := client(ownerA, sourceEndpoint), client(ownerB, sourceEndpoint)
	destA, destB := client(ownerA, destEndpoint), client(ownerB, destEndpoint)
	body := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + primaryPlacement.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	const input, output, other = "/tc-sync-input", "/tc-sync-output", "/tc-sync-other"
	for _, bucket := range []string{input, other} {
		a.request(t, ctx, http.MethodPut, bucket, body, http.StatusOK)
	}
	b.request(t, ctx, http.MethodPut, output, body, http.StatusOK)
	selected, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{Bucket: strings.TrimPrefix(input, "/")}, multicluster.RGWSyncGroupConfig{ID: "translation", Status: multicluster.RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	base := multicluster.RGWSyncPipeConfig{SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(input, "/")}, DestinationBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(output, "/")}, Prefix: "published/", DestinationOwner: ownerB, DestinationPlacements: map[string]*ceph.RGWPlacement{"destination": secondaryPlacement}}
	low := base
	low.ID = "low-standard"
	low.Priority = 1
	low.Tags = []multicluster.RGWSyncObjectTag{{Key: "color", Value: "blue"}}
	low.DestinationStorageClass = "STANDARD"
	if err := link.CreateSyncPipe(ctx, selected, low); err != nil {
		t.Fatal(err)
	}
	high := base
	high.ID = "high-ia"
	high.Priority = 7
	high.Tags = []multicluster.RGWSyncObjectTag{{Key: "color", Value: "red"}, {Key: "color", Value: "blue"}}
	high.DestinationStorageClass = "STANDARD_IA"
	if err := link.CreateSyncPipe(ctx, selected, high); err != nil {
		t.Fatal(err)
	}
	putTagged := func(c s3HTTPClient, path, color string, payload []byte) {
		var headers http.Header
		if color != "" {
			headers = http.Header{"X-Amz-Tagging": []string{"color=" + color}}
		}
		s3FeatureRequest(t, ctx, c, http.MethodPut, path, payload, headers, http.StatusOK)
	}
	payload := bytes.Repeat([]byte("translated cross-bucket OR tags and priority\n"), 2048)
	for _, object := range []struct{ path, color string }{{input + "/published/blue", "blue"}, {input + "/published/red", "red"}, {input + "/published/green", "green"}, {input + "/published/untagged", ""}, {input + "/private/blue", "blue"}, {other + "/published/from-other-bucket", "blue"}} {
		putTagged(a, object.path, object.color, payload)
	}
	if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
		t.Fatal(err)
	}
	waitOwnedSyncObject(t, ctx, destB, output+"/published/blue", http.StatusOK, payload)
	waitOwnedSyncObject(t, ctx, destB, output+"/published/red", http.StatusOK, payload)
	waitOwnedBucketCheckpoint(t, ctx, link, selected, high.ID)
	assertRGWSyncClass(t, ctx, destB, output, "published/blue", "STANDARD_IA")
	assertRGWSyncClass(t, ctx, destB, output, "published/red", "STANDARD_IA")
	rgwPlacementPoolPayload(t, ctx, destination, "tc-sync-ia", "translated priority STANDARD_IA", payload)
	// Object ownership changed to the destination bucket's ordinary owner.
	destA.request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusForbidden)
	putTagged(destB, output+"/published/reverse", "blue", payload)
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destB, output + "/published/green"}, rgwAbsentObject{destB, output + "/published/untagged"}, rgwAbsentObject{destB, output + "/private/blue"}, rgwAbsentObject{destB, output + "/published/from-other-bucket"}, rgwAbsentObject{b, output + "/published/reverse"})
	// Remove only the high-priority pipe. The matching lower-priority pipe
	// now selects STANDARD for a new write and preserves previous IA objects.
	if err := link.RemoveSyncPipe(ctx, selected, high.ID); err != nil {
		t.Fatal(err)
	}
	fallback := bytes.Repeat([]byte("new write after priority pipe removal\n"), 2048)
	putTagged(a, input+"/published/fallback", "blue", fallback)
	waitOwnedSyncObject(t, ctx, destB, output+"/published/fallback", http.StatusOK, fallback)
	waitOwnedBucketCheckpoint(t, ctx, link, selected, low.ID)
	assertRGWSyncClass(t, ctx, destB, output, "published/fallback", "STANDARD")
	rgwPlacementPoolPayload(t, ctx, destination, "tc-sync-standard", "translated fallback STANDARD", fallback)
	if got := destB.request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("priority removal changed previous replica")
	}
	t.Log("captured cross-bucket instances; prefix AND (blue OR red), owner translation, priority winner and fallback classes verified with S3 bytes and exact native data pools")
	// User mode executes as B: B can write its destination, but initially has
	// no read permission on A's source. S3 policy belongs to the client recipe.
	const modeInput, modeOutput = "/tc-sync-mode-input", "/tc-sync-mode-output"
	a.request(t, ctx, http.MethodPut, modeInput, body, http.StatusOK)
	b.request(t, ctx, http.MethodPut, modeOutput, body, http.StatusOK)
	modeGroup, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{Bucket: strings.TrimPrefix(modeInput, "/")}, multicluster.RGWSyncGroupConfig{ID: "user-mode", Status: multicluster.RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	modePipe := multicluster.RGWSyncPipeConfig{ID: "authorized-principal", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(modeInput, "/")}, DestinationBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(modeOutput, "/")}, Prefix: "auth/", User: ownerB, DestinationOwner: ownerB}
	if err := link.CreateSyncPipe(ctx, modeGroup, modePipe); err != nil {
		t.Fatal(err)
	}
	modePayload := bytes.Repeat([]byte("user-mode read authorization\n"), 2048)
	a.request(t, ctx, http.MethodPut, modeInput+"/auth/before-grant", modePayload, http.StatusOK)
	b.request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant", nil, http.StatusForbidden)
	if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
		t.Fatal(err)
	}
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destB, modeOutput + "/auth/before-grant"})
	policy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + ownerB.ID()}, "Action": "s3:GetObject", "Resource": "arn:aws:s3:::" + strings.TrimPrefix(modeInput, "/") + "/*"}}})
	s3FeatureRequest(t, ctx, a, http.MethodPut, modeInput+"?policy", policy, nil, http.StatusNoContent)
	if got := b.request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant", nil, http.StatusOK); !bytes.Equal(got, modePayload) {
		t.Fatal("granted principal read wrong bytes")
	}
	a.request(t, ctx, http.MethodPut, modeInput+"/auth/after-grant", modePayload, http.StatusOK)
	waitOwnedSyncObject(t, ctx, destB, modeOutput+"/auth/after-grant", http.StatusOK, modePayload)
	waitOwnedBucketCheckpoint(t, ctx, link, modeGroup, modePipe.ID)
	if err := link.RemoveSyncGroup(ctx, modeGroup); err != nil {
		t.Fatal(err)
	}
	if err := link.RemoveSyncGroup(ctx, selected); err != nil {
		t.Fatal(err)
	}
	t.Log("ordinary confirmed user mode refused a source read before client policy grant and replicated exact future bytes after grant; owned policy removal retained destination data")
	// Reuse this realm to exercise canonical tenant UIDs and bucket selectors.
	// Identical local UID/bucket names must not broaden the selected namespace.
	var tenantUsers [2]*ceph.RGWUser
	var tenantSources, tenantDestinations [2]s3HTTPClient
	for i, tenant := range []string{"tenant_sync_alpha", "tenant_sync_beta"} {
		tenantUsers[i], err = link.Source.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-sync-tenant-user", Tenant: tenant})
		if err != nil {
			t.Fatal(err)
		}
		info, err := link.Source.UserInfo(ctx, tenantUsers[i])
		if err != nil || info.ID != tenant+"$tc-sync-tenant-user" || info.Tenant != tenant {
			t.Fatal("tenant sync principal did not retain its exact canonical native UID")
		}
		tenantSources[i], tenantDestinations[i] = client(tenantUsers[i], sourceEndpoint), client(tenantUsers[i], destEndpoint)
		for _, bucket := range []string{"/tc-sync-tenant-input", "/tc-sync-tenant-output"} {
			tenantSources[i].request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
		}
	}
	tenantGroup, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{Bucket: "tc-sync-tenant-input", Tenant: "tenant_sync_alpha"}, multicluster.RGWSyncGroupConfig{ID: "tenant-selected", Status: multicluster.RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	tenantPipe := multicluster.RGWSyncPipeConfig{ID: "tenant-system", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multicluster.RGWSyncBucketSelector{Name: "tc-sync-tenant-input", Tenant: "tenant_sync_alpha"}, DestinationBucket: &multicluster.RGWSyncBucketSelector{Name: "tc-sync-tenant-output", Tenant: "tenant_sync_alpha"}, Prefix: "system/", DestinationOwner: tenantUsers[0]}
	if err := link.CreateSyncPipe(ctx, tenantGroup, tenantPipe); err != nil {
		t.Fatal(err)
	}
	tenantUserPipe := tenantPipe
	tenantUserPipe.ID, tenantUserPipe.Prefix, tenantUserPipe.User = "tenant-user", "user/", tenantUsers[0]
	if err := link.CreateSyncPipe(ctx, tenantGroup, tenantUserPipe); err != nil {
		t.Fatal(err)
	}
	crossTenant := tenantPipe
	crossTenant.ID, crossTenant.DestinationOwner = "cross-tenant-unproven", tenantUsers[1]
	if err := link.CreateSyncPipe(ctx, tenantGroup, crossTenant); err == nil {
		t.Fatal("unproven cross-tenant principal translation was accepted")
	}
	tenantPayload := bytes.Repeat([]byte("same-tenant canonical user and exact bucket instance\n"), 1024)
	for _, prefix := range []string{"system", "user"} {
		tenantSources[0].request(t, ctx, http.MethodPut, "/tc-sync-tenant-input/"+prefix+"/selected", tenantPayload, http.StatusOK)
		waitOwnedSyncObject(t, ctx, tenantDestinations[0], "/tc-sync-tenant-output/"+prefix+"/selected", http.StatusOK, tenantPayload)
		waitOwnedBucketCheckpoint(t, ctx, link, tenantGroup, "tenant-"+prefix)
	}
	tenantSources[1].request(t, ctx, http.MethodPut, "/tc-sync-tenant-input/system/other-tenant", []byte("unselected beta namespace"), http.StatusOK)
	if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioResponse(t, ctx, tenantDestinations[1], http.MethodHead, "/tc-sync-tenant-output", http.StatusOK)
	denied := rgwTenantRequest(t, ctx, tenantSources[1], http.MethodGet, "/tenant_sync_alpha:tc-sync-tenant-input/system/selected", nil, http.StatusForbidden)
	requireRGWUserPlacementDenied(t, denied)
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{tenantDestinations[0], "/tc-sync-tenant-output/system/other-tenant"}, rgwAbsentObject{tenantDestinations[1], "/tc-sync-tenant-output/system/selected"}, rgwAbsentObject{tenantDestinations[1], "/tc-sync-tenant-output/system/other-tenant"})
	if err := link.RemoveSyncGroup(ctx, tenantGroup); err != nil {
		t.Fatal(err)
	}
	if got := tenantDestinations[0].request(t, ctx, http.MethodGet, "/tc-sync-tenant-output/user/selected", nil, http.StatusOK); !bytes.Equal(got, tenantPayload) {
		t.Fatal("tenant policy cleanup changed previously replicated bytes")
	}
	t.Log("same canonical local UID/bucket names in two tenants remained isolated; selected tenant system/owned ordinary user modes copied exact bytes and native bucket checkpoints; cross-tenant principal translation refused and old replicas retained")
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
