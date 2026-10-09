//go:build all || (integration && multicluster && features)

package integration_test

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
)

func (f *rgwSyncTranslationFixture) tag_owner_class(t *testing.T) {
	ctx := f.ctx
	destination := f.destination
	link := f.link
	secondaryPlacement := f.secondaryPlacement
	ownerB := f.ownerB
	a := f.a
	b := f.b
	destA := f.destA
	destB := f.destB
	body := f.body
	t.Run("tag_owner_class", func(t *testing.T) {
		// A single pipe proves tag/owner/class provisioning independently of
		// the overlapping native priority selection tested above.
		const input, output, other = "/tc-sync-tags-input", "/tc-sync-tags-output", "/tc-sync-tags-other"
		for _, bucket := range []string{input, other} {
			a.request(t, ctx, http.MethodPut, bucket, body, http.StatusOK)
		}
		b.request(t, ctx, http.MethodPut, output, body, http.StatusOK)
		selected, err := link.CreateSyncGroup(ctx, rgw.SyncPolicyScope{Bucket: strings.TrimPrefix(input, "/")}, rgw.SyncGroupConfig{ID: "tags-owner-class", Status: rgw.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		cleanupRGWTranslationSyncGroup(t, link, selected, "tag-ia")
		pipe := rgw.SyncPipeConfig{ID: "tag-ia", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &rgw.SyncBucketSelector{Name: strings.TrimPrefix(input, "/")}, DestinationBucket: &rgw.SyncBucketSelector{Name: strings.TrimPrefix(output, "/")}, Prefix: "published/", Tags: []rgw.SyncObjectTag{{Key: "color", Value: "blue"}, {Key: "color", Value: "red"}}, DestinationOwner: ownerB, DestinationStorageClass: "STANDARD_IA", DestinationPlacements: map[string]*rgw.Placement{"destination": secondaryPlacement}}
		if err := link.CreateSyncPipe(ctx, selected, pipe); err != nil {
			t.Fatal(err)
		}
		waitRGWTranslationPolicyReady(t, ctx, link, selected)
		putTagged := func(c s3HTTPClient, path, color string, payload []byte) {
			var headers http.Header
			if color != "" {
				headers = http.Header{"X-Amz-Tagging": []string{"color=" + color}}
			}
			s3FeatureRequest(t, ctx, c, http.MethodPut, path, payload, headers, http.StatusOK)
		}
		payload := bytes.Repeat([]byte("single-pipe tag and ordinary owner storage-class translation\n"), 2048)
		for _, object := range []struct{ path, color string }{{input + "/published/blue", "blue"}, {input + "/published/red", "red"}, {input + "/published/green", "green"}, {input + "/published/untagged", ""}, {input + "/private/blue", "blue"}, {other + "/published/from-other-bucket", "blue"}} {
			putTagged(a, object.path, object.color, payload)
		}
		if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"published/blue", "published/red"} {
			waitOwnedSyncObject(t, ctx, destB, output+"/"+key, http.StatusOK, payload)
			assertRGWSyncClass(t, ctx, destB, output, key, "STANDARD_IA")
		}
		waitOwnedBucketCheckpoint(t, ctx, link, selected, pipe.ID)
		rgwPlacementPoolPayload(t, ctx, destination, "tc-sync-ia", "single-pipe translated STANDARD_IA", payload)
		// The old source owner must lose object access; a missing principal or
		// signing error must not count as the owner-translation denial.
		denied := destA.request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusForbidden)
		requireRGWUserPlacementDenied(t, denied)
		putTagged(destB, output+"/published/reverse", "blue", payload)
		requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destB, output + "/published/green"}, rgwAbsentObject{destB, output + "/published/untagged"}, rgwAbsentObject{destB, output + "/private/blue"}, rgwAbsentObject{destB, output + "/published/from-other-bucket"}, rgwAbsentObject{b, output + "/published/reverse"})
		waitOwnedBucketCheckpoint(t, ctx, link, selected, pipe.ID)
		if err := link.RemoveSyncGroup(ctx, selected); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"published/blue", "published/red"} {
			if got := destB.request(t, ctx, http.MethodGet, output+"/"+key, nil, http.StatusOK); !bytes.Equal(got, payload) {
				t.Fatal("single-pipe group removal changed previously replicated bytes")
			}
			assertRGWSyncClass(t, ctx, destB, output, key, "STANDARD_IA")
		}
		t.Log("single captured pipe copied prefix AND (blue OR red) to the ordinary destination owner and STANDARD_IA native pool; green/untagged/private/other-bucket/reverse paths stayed NoSuchKey, exact checkpoints completed, and owned group removal retained both replicas")
	})
}

func (f *rgwSyncTranslationFixture) tenant_system_user_isolation(t *testing.T) {
	ctx := f.ctx
	link := f.link
	client := f.client
	sourceEndpoint := f.sourceEndpoint
	destEndpoint := f.destEndpoint
	t.Run("tenant_system_user_isolation", func(t *testing.T) {
		// Reuse this realm to exercise canonical tenant UIDs and bucket selectors.
		// Identical local UID/bucket names must not broaden the selected namespace.
		var tenantUsers [2]*rgw.User
		var tenantSources, tenantDestinations [2]s3HTTPClient
		var err error
		for i, tenant := range []string{"tenant_sync_alpha", "tenant_sync_beta"} {
			tenantUsers[i], err = link.Source.CreateUser(ctx, rgw.UserConfig{ID: "tc-sync-tenant-user", Tenant: tenant})
			if err != nil {
				t.Fatal(err)
			}
			info, err := link.Source.UserInfo(ctx, tenantUsers[i])
			if err != nil || info.ID != tenant+"$tc-sync-tenant-user" || info.Tenant != tenant {
				t.Fatal("tenant sync principal did not retain its exact canonical native UID")
			}
			tenantSources[i], tenantDestinations[i] = client(t, tenantUsers[i], sourceEndpoint), client(t, tenantUsers[i], destEndpoint)
			for _, bucket := range []string{"/tc-sync-tenant-input", "/tc-sync-tenant-output"} {
				tenantSources[i].request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
			}
		}
		tenantGroup, err := link.CreateSyncGroup(ctx, rgw.SyncPolicyScope{Bucket: "tc-sync-tenant-input", Tenant: "tenant_sync_alpha"}, rgw.SyncGroupConfig{ID: "tenant-selected", Status: rgw.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		cleanupRGWTranslationSyncGroup(t, link, tenantGroup, "tenant-system", "tenant-user")
		tenantPipe := rgw.SyncPipeConfig{ID: "tenant-system", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &rgw.SyncBucketSelector{Name: "tc-sync-tenant-input", Tenant: "tenant_sync_alpha"}, DestinationBucket: &rgw.SyncBucketSelector{Name: "tc-sync-tenant-output", Tenant: "tenant_sync_alpha"}, Prefix: "system/", DestinationOwner: tenantUsers[0]}
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
		waitRGWTranslationPolicyReady(t, ctx, link, tenantGroup)
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
	})
}
