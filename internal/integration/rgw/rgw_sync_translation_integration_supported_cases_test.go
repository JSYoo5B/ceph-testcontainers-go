//go:build all || (integration && multicluster && features)

package rgw_test

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement"
)

func rgwSyncTagOwnerClass(t *testing.T, f *harness.RGWSyncTranslationFixture) {
	ctx := f.Ctx
	destination := f.Destination
	link := f.Link
	secondaryPlacement := f.SecondaryPlacement
	ownerB := f.OwnerB
	a := f.A
	b := f.B
	destA := f.DestA
	destB := f.DestB
	body := f.Body
	t.Run("tag_owner_class", func(t *testing.T) {
		// A single pipe proves tag/owner/class provisioning independently of
		// the overlapping native priority selection tested above.
		const input, output, other = "/tc-sync-tags-input", "/tc-sync-tags-output", "/tc-sync-tags-other"
		for _, bucket := range []string{input, other} {
			a.Request(t, ctx, http.MethodPut, bucket, body, http.StatusOK)
		}
		b.Request(t, ctx, http.MethodPut, output, body, http.StatusOK)
		selected, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{Bucket: strings.TrimPrefix(input, "/")}, multisite.SyncGroupConfig{ID: "tags-owner-class", Status: multisite.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		harness.CleanupRGWTranslationSyncGroup(t, link, selected, "tag-ia")
		pipe := multisite.SyncPipeConfig{ID: "tag-ia", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multisite.SyncBucketSelector{Name: strings.TrimPrefix(input, "/")}, DestinationBucket: &multisite.SyncBucketSelector{Name: strings.TrimPrefix(output, "/")}, Prefix: "published/", Tags: []multisite.SyncObjectTag{{Key: "color", Value: "blue"}, {Key: "color", Value: "red"}}, DestinationOwner: ownerB, DestinationStorageClass: "STANDARD_IA", DestinationPlacements: map[string]*placement.Placement{"destination": secondaryPlacement}}
		if err := selected.CreatePipe(ctx, pipe); err != nil {
			t.Fatal(err)
		}
		harness.WaitRGWTranslationPolicyReady(t, ctx, link, selected)
		putTagged := func(c harness.S3HTTPClient, path, color string, payload []byte) {
			var headers http.Header
			if color != "" {
				headers = http.Header{"X-Amz-Tagging": []string{"color=" + color}}
			}
			harness.S3FeatureRequest(t, ctx, c, http.MethodPut, path, payload, headers, http.StatusOK)
		}
		payload := bytes.Repeat([]byte("single-pipe tag and ordinary owner storage-class translation\n"), 2048)
		for _, object := range []struct{ path, color string }{{input + "/published/blue", "blue"}, {input + "/published/red", "red"}, {input + "/published/green", "green"}, {input + "/published/untagged", ""}, {input + "/private/blue", "blue"}, {other + "/published/from-other-bucket", "blue"}} {
			putTagged(a, object.path, object.color, payload)
		}
		if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"published/blue", "published/red"} {
			harness.WaitOwnedSyncObject(t, ctx, destB, output+"/"+key, http.StatusOK, payload)
			harness.AssertRGWSyncClass(t, ctx, destB, output, key, "STANDARD_IA")
		}
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, selected, pipe.ID)
		harness.RGWPlacementPoolPayload(t, ctx, destination, "tc-sync-ia", "single-pipe translated STANDARD_IA", payload)
		// The old source owner must lose object access; a missing principal or
		// signing error must not count as the owner-translation denial.
		denied := destA.Request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusForbidden)
		harness.RequireRGWUserPlacementDenied(t, denied)
		putTagged(destB, output+"/published/reverse", "blue", payload)
		harness.RequireRGWObjectsAbsent(t, ctx, 35*time.Second, harness.RGWAbsentObject{Client: destB, Path: output + "/published/green"}, harness.RGWAbsentObject{Client: destB, Path: output + "/published/untagged"}, harness.RGWAbsentObject{Client: destB, Path: output + "/private/blue"}, harness.RGWAbsentObject{Client: destB, Path: output + "/published/from-other-bucket"}, harness.RGWAbsentObject{Client: b, Path: output + "/published/reverse"})
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, selected, pipe.ID)
		if err := selected.Remove(ctx); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"published/blue", "published/red"} {
			if got := destB.Request(t, ctx, http.MethodGet, output+"/"+key, nil, http.StatusOK); !bytes.Equal(got, payload) {
				t.Fatal("single-pipe group removal changed previously replicated bytes")
			}
			harness.AssertRGWSyncClass(t, ctx, destB, output, key, "STANDARD_IA")
		}
		t.Log("single captured pipe copied prefix AND (blue OR red) to the ordinary destination owner and STANDARD_IA native pool; green/untagged/private/other-bucket/reverse paths stayed NoSuchKey, exact checkpoints completed, and owned group removal retained both replicas")
	})
}

func rgwSyncTenantSystemUserIsolation(t *testing.T, f *harness.RGWSyncTranslationFixture) {
	ctx := f.Ctx
	link := f.Link
	client := f.Client
	sourceEndpoint := f.SourceEndpoint
	destEndpoint := f.DestEndpoint
	t.Run("tenant_system_user_isolation", func(t *testing.T) {
		// Reuse this realm to exercise canonical tenant UIDs and bucket selectors.
		// Identical local UID/bucket names must not broaden the selected namespace.
		var tenantUsers [2]*rgw.User
		var tenantSources, tenantDestinations [2]harness.S3HTTPClient
		var err error
		for i, tenant := range []string{"tenant_sync_alpha", "tenant_sync_beta"} {
			tenantUsers[i], err = link.Source.CreateUser(ctx, rgw.UserConfig{ID: "tc-sync-tenant-user", Tenant: tenant})
			if err != nil {
				t.Fatal(err)
			}
			info, err := (tenantUsers[i]).Info(ctx)
			if err != nil || info.ID != tenant+"$tc-sync-tenant-user" || info.Tenant != tenant {
				t.Fatal("tenant sync principal did not retain its exact canonical native UID")
			}
			tenantSources[i], tenantDestinations[i] = client(t, tenantUsers[i], sourceEndpoint), client(t, tenantUsers[i], destEndpoint)
			for _, bucket := range []string{"/tc-sync-tenant-input", "/tc-sync-tenant-output"} {
				tenantSources[i].Request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
			}
		}
		tenantGroup, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{Bucket: "tc-sync-tenant-input", Tenant: "tenant_sync_alpha"}, multisite.SyncGroupConfig{ID: "tenant-selected", Status: multisite.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		harness.CleanupRGWTranslationSyncGroup(t, link, tenantGroup, "tenant-system", "tenant-user")
		tenantPipe := multisite.SyncPipeConfig{ID: "tenant-system", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multisite.SyncBucketSelector{Name: "tc-sync-tenant-input", Tenant: "tenant_sync_alpha"}, DestinationBucket: &multisite.SyncBucketSelector{Name: "tc-sync-tenant-output", Tenant: "tenant_sync_alpha"}, Prefix: "system/", DestinationOwner: tenantUsers[0]}
		if err := tenantGroup.CreatePipe(ctx, tenantPipe); err != nil {
			t.Fatal(err)
		}
		tenantUserPipe := tenantPipe
		tenantUserPipe.ID, tenantUserPipe.Prefix, tenantUserPipe.User = "tenant-user", "user/", tenantUsers[0]
		if err := tenantGroup.CreatePipe(ctx, tenantUserPipe); err != nil {
			t.Fatal(err)
		}
		crossTenant := tenantPipe
		crossTenant.ID, crossTenant.DestinationOwner = "cross-tenant-unproven", tenantUsers[1]
		if err := tenantGroup.CreatePipe(ctx, crossTenant); err == nil {
			t.Fatal("unproven cross-tenant principal translation was accepted")
		}
		harness.WaitRGWTranslationPolicyReady(t, ctx, link, tenantGroup)
		tenantPayload := bytes.Repeat([]byte("same-tenant canonical user and exact bucket instance\n"), 1024)
		for _, prefix := range []string{"system", "user"} {
			tenantSources[0].Request(t, ctx, http.MethodPut, "/tc-sync-tenant-input/"+prefix+"/selected", tenantPayload, http.StatusOK)
			harness.WaitOwnedSyncObject(t, ctx, tenantDestinations[0], "/tc-sync-tenant-output/"+prefix+"/selected", http.StatusOK, tenantPayload)
			harness.WaitOwnedBucketCheckpoint(t, ctx, link, tenantGroup, "tenant-"+prefix)
		}
		tenantSources[1].Request(t, ctx, http.MethodPut, "/tc-sync-tenant-input/system/other-tenant", []byte("unselected beta namespace"), http.StatusOK)
		if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
			t.Fatal(err)
		}
		waitRGWScenarioResponse(t, ctx, tenantDestinations[1], http.MethodHead, "/tc-sync-tenant-output", http.StatusOK)
		denied := rgwTenantRequest(t, ctx, tenantSources[1], http.MethodGet, "/tenant_sync_alpha:tc-sync-tenant-input/system/selected", nil, http.StatusForbidden)
		harness.RequireRGWUserPlacementDenied(t, denied)
		harness.RequireRGWObjectsAbsent(t, ctx, 35*time.Second, harness.RGWAbsentObject{Client: tenantDestinations[0], Path: "/tc-sync-tenant-output/system/other-tenant"}, harness.RGWAbsentObject{Client: tenantDestinations[1], Path: "/tc-sync-tenant-output/system/selected"}, harness.RGWAbsentObject{Client: tenantDestinations[1], Path: "/tc-sync-tenant-output/system/other-tenant"})
		if err := tenantGroup.Remove(ctx); err != nil {
			t.Fatal(err)
		}
		if got := tenantDestinations[0].Request(t, ctx, http.MethodGet, "/tc-sync-tenant-output/user/selected", nil, http.StatusOK); !bytes.Equal(got, tenantPayload) {
			t.Fatal("tenant policy cleanup changed previously replicated bytes")
		}
		t.Log("same canonical local UID/bucket names in two tenants remained isolated; selected tenant system/owned ordinary user modes copied exact bytes and native bucket checkpoints; cross-tenant principal translation refused and old replicas retained")
	})
}
