//go:build (all || (integration && multicluster && features)) && native_regression

package rgw_sync_policy_translation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement"
	"github.com/testcontainers/testcontainers-go"
)

// testRGWSyncTranslationFiltering runs the two strict cases on one shared
// realm, in the order the required supported cases use, each with its own
// buckets and sync group.
func testRGWSyncTranslationFiltering(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	f := harness.NewRGWSyncTranslationFixture(t, ctx, opts...)
	rgwSyncPriorityTagsOwnerClass(t, f)
	rgwSyncOrdinaryUserDenialGrant(t, f)
}

func rgwSyncPriorityTagsOwnerClass(t *testing.T, f *harness.RGWSyncTranslationFixture) {
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
	t.Run("priority_tags_owner_class", func(t *testing.T) {
		const input, output, other = "/tc-sync-input", "/tc-sync-output", "/tc-sync-other"
		for _, bucket := range []string{input, other} {
			a.Request(t, ctx, http.MethodPut, bucket, body, http.StatusOK)
		}
		b.Request(t, ctx, http.MethodPut, output, body, http.StatusOK)
		selected, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{Bucket: strings.TrimPrefix(input, "/")}, multisite.SyncGroupConfig{ID: "translation", Status: multisite.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		harness.CleanupRGWTranslationSyncGroup(t, link, selected, "low-standard", "high-ia")
		base := multisite.SyncPipeConfig{SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multisite.SyncBucketSelector{Name: strings.TrimPrefix(input, "/")}, DestinationBucket: &multisite.SyncBucketSelector{Name: strings.TrimPrefix(output, "/")}, Prefix: "published/", DestinationOwner: ownerB, DestinationPlacements: map[string]*placement.Placement{"destination": secondaryPlacement}}
		low := base
		low.ID = "low-standard"
		low.Priority = 1
		low.Tags = []multisite.SyncObjectTag{{Key: "color", Value: "blue"}}
		low.DestinationStorageClass = "STANDARD"
		if err := selected.CreatePipe(ctx, low); err != nil {
			t.Fatal(err)
		}
		high := base
		high.ID = "high-ia"
		high.Priority = 7
		high.Tags = []multisite.SyncObjectTag{{Key: "color", Value: "red"}, {Key: "color", Value: "blue"}}
		high.DestinationStorageClass = "STANDARD_IA"
		if err := selected.CreatePipe(ctx, high); err != nil {
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
		payload := bytes.Repeat([]byte("translated cross-bucket OR tags and priority\n"), 2048)
		for _, object := range []struct{ path, color string }{{input + "/published/blue", "blue"}, {input + "/published/red", "red"}, {input + "/published/green", "green"}, {input + "/published/untagged", ""}, {input + "/private/blue", "blue"}, {other + "/published/from-other-bucket", "blue"}} {
			putTagged(a, object.path, object.color, payload)
		}
		if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
			t.Fatal(err)
		}
		harness.WaitOwnedSyncObject(t, ctx, destB, output+"/published/blue", http.StatusOK, payload)
		harness.WaitOwnedSyncObject(t, ctx, destB, output+"/published/red", http.StatusOK, payload)
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, selected, high.ID)
		harness.AssertRGWSyncClass(t, ctx, destB, output, "published/blue", "STANDARD_IA")
		harness.AssertRGWSyncClass(t, ctx, destB, output, "published/red", "STANDARD_IA")
		harness.RGWPlacementPoolPayload(t, ctx, destination, "tc-sync-ia", "translated priority STANDARD_IA", payload)
		// Object ownership changed to the destination bucket's ordinary owner.
		destA.Request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusForbidden)
		putTagged(destB, output+"/published/reverse", "blue", payload)
		harness.RequireRGWObjectsAbsent(t, ctx, 35*time.Second, harness.RGWAbsentObject{Client: destB, Path: output + "/published/green"}, harness.RGWAbsentObject{Client: destB, Path: output + "/published/untagged"}, harness.RGWAbsentObject{Client: destB, Path: output + "/private/blue"}, harness.RGWAbsentObject{Client: destB, Path: output + "/published/from-other-bucket"}, harness.RGWAbsentObject{Client: b, Path: output + "/published/reverse"})
		// Remove only the high-priority pipe. The matching lower-priority pipe
		// now selects STANDARD for a new write and preserves previous IA objects.
		if err := selected.RemovePipe(ctx, high.ID); err != nil {
			t.Fatal(err)
		}
		harness.WaitRGWTranslationPolicyReady(t, ctx, link, selected)
		fallback := bytes.Repeat([]byte("new write after priority pipe removal\n"), 2048)
		putTagged(a, input+"/published/fallback", "blue", fallback)
		harness.WaitOwnedSyncObject(t, ctx, destB, output+"/published/fallback", http.StatusOK, fallback)
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, selected, low.ID)
		harness.AssertRGWSyncClass(t, ctx, destB, output, "published/fallback", "STANDARD")
		harness.RGWPlacementPoolPayload(t, ctx, destination, "tc-sync-standard", "translated fallback STANDARD", fallback)
		if got := destB.Request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusOK); !bytes.Equal(got, payload) {
			t.Fatal("priority removal changed previous replica")
		}
		if err := selected.Remove(ctx); err != nil {
			t.Fatal(err)
		}
		t.Log("captured cross-bucket instances; prefix AND (blue OR red), owner translation, priority winner and fallback classes verified with S3 bytes and exact native data pools")
	})
}

func rgwSyncOrdinaryUserDenialGrant(t *testing.T, f *harness.RGWSyncTranslationFixture) {
	ctx := f.Ctx
	link := f.Link
	ownerB := f.OwnerB
	a := f.A
	b := f.B
	destB := f.DestB
	body := f.Body
	t.Run("ordinary_user_denial_grant", func(t *testing.T) {
		// User mode executes as B: B can write its destination, but initially has
		// no read permission on A's source. S3 policy belongs to the client recipe.
		const modeInput, modeOutput = "/tc-sync-mode-input", "/tc-sync-mode-output"
		a.Request(t, ctx, http.MethodPut, modeInput, body, http.StatusOK)
		b.Request(t, ctx, http.MethodPut, modeOutput, body, http.StatusOK)
		modeGroup, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{Bucket: strings.TrimPrefix(modeInput, "/")}, multisite.SyncGroupConfig{ID: "user-mode", Status: multisite.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		harness.CleanupRGWTranslationSyncGroup(t, link, modeGroup, "authorized-principal")
		modePipe := multisite.SyncPipeConfig{ID: "authorized-principal", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multisite.SyncBucketSelector{Name: strings.TrimPrefix(modeInput, "/")}, DestinationBucket: &multisite.SyncBucketSelector{Name: strings.TrimPrefix(modeOutput, "/")}, Prefix: "auth/", User: ownerB, DestinationOwner: ownerB}
		if err := modeGroup.CreatePipe(ctx, modePipe); err != nil {
			t.Fatal(err)
		}
		// Permit the native replication GET's mandatory ACL check while still
		// denying the actual source payload read. This separates GetObject
		// authorization from an earlier GetObjectAcl failure.
		aclOnlyPolicy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + ownerB.ID()}, "Action": "s3:GetObjectAcl", "Resource": "arn:aws:s3:::" + strings.TrimPrefix(modeInput, "/") + "/*"}}})
		harness.S3FeatureRequest(t, ctx, a, http.MethodPut, modeInput+"?policy", aclOnlyPolicy, nil, http.StatusNoContent)
		harness.WaitRGWTranslationPolicyReady(t, ctx, link, modeGroup)
		modePayload := bytes.Repeat([]byte("user-mode read authorization\n"), 2048)
		a.Request(t, ctx, http.MethodPut, modeInput+"/auth/before-grant", modePayload, http.StatusOK)
		b.Request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant?acl", nil, http.StatusOK)
		denied := b.Request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant", nil, http.StatusForbidden)
		harness.RequireRGWUserPlacementDenied(t, denied)
		if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
			t.Fatal(err)
		}
		harness.RequireRGWObjectsAbsent(t, ctx, 35*time.Second, harness.RGWAbsentObject{Client: destB, Path: modeOutput + "/auth/before-grant"})
		// Require native progress while the source still denies this principal.
		// A worker that merely paused must not satisfy the exclusion proof.
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, modeGroup, modePipe.ID)
		harness.RequireRGWObjectsAbsent(t, ctx, time.Second, harness.RGWAbsentObject{Client: destB, Path: modeOutput + "/auth/before-grant"})
		// Native impersonated replication GET first requires GetObjectAcl,
		// then GetObject for this unversioned source, unlike an ordinary GET.
		policy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + ownerB.ID()}, "Action": []string{"s3:GetObject", "s3:GetObjectAcl"}, "Resource": "arn:aws:s3:::" + strings.TrimPrefix(modeInput, "/") + "/*"}}})
		harness.S3FeatureRequest(t, ctx, a, http.MethodPut, modeInput+"?policy", policy, nil, http.StatusNoContent)
		if got := b.Request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant", nil, http.StatusOK); !bytes.Equal(got, modePayload) {
			t.Fatal("granted principal read wrong bytes")
		}
		a.Request(t, ctx, http.MethodPut, modeInput+"/auth/after-grant", modePayload, http.StatusOK)
		harness.WaitOwnedSyncObject(t, ctx, destB, modeOutput+"/auth/after-grant", http.StatusOK, modePayload)
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, modeGroup, modePipe.ID)
		if err := modeGroup.Remove(ctx); err != nil {
			t.Fatal(err)
		}
		if got := destB.Request(t, ctx, http.MethodGet, modeOutput+"/auth/after-grant", nil, http.StatusOK); !bytes.Equal(got, modePayload) {
			t.Fatal("user-mode policy removal changed previously replicated bytes")
		}
		t.Log("ordinary confirmed user mode refused a source read before client policy grant and replicated exact future bytes after grant; owned policy removal retained destination data")
	})
}
