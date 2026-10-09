//go:build ((all || (integration && multicluster && features)) && native_regression) || (integration && multicluster && features && !all && !ci)

package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
)

func (f *rgwSyncTranslationFixture) priority_tags_owner_class(t *testing.T) {
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
	t.Run("priority_tags_owner_class", func(t *testing.T) {
		const input, output, other = "/tc-sync-input", "/tc-sync-output", "/tc-sync-other"
		for _, bucket := range []string{input, other} {
			a.request(t, ctx, http.MethodPut, bucket, body, http.StatusOK)
		}
		b.request(t, ctx, http.MethodPut, output, body, http.StatusOK)
		selected, err := link.CreateSyncGroup(ctx, rgw.SyncPolicyScope{Bucket: strings.TrimPrefix(input, "/")}, rgw.SyncGroupConfig{ID: "translation", Status: rgw.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		cleanupRGWTranslationSyncGroup(t, link, selected, "low-standard", "high-ia")
		base := rgw.SyncPipeConfig{SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &rgw.SyncBucketSelector{Name: strings.TrimPrefix(input, "/")}, DestinationBucket: &rgw.SyncBucketSelector{Name: strings.TrimPrefix(output, "/")}, Prefix: "published/", DestinationOwner: ownerB, DestinationPlacements: map[string]*rgw.Placement{"destination": secondaryPlacement}}
		low := base
		low.ID = "low-standard"
		low.Priority = 1
		low.Tags = []rgw.SyncObjectTag{{Key: "color", Value: "blue"}}
		low.DestinationStorageClass = "STANDARD"
		if err := link.CreateSyncPipe(ctx, selected, low); err != nil {
			t.Fatal(err)
		}
		high := base
		high.ID = "high-ia"
		high.Priority = 7
		high.Tags = []rgw.SyncObjectTag{{Key: "color", Value: "red"}, {Key: "color", Value: "blue"}}
		high.DestinationStorageClass = "STANDARD_IA"
		if err := link.CreateSyncPipe(ctx, selected, high); err != nil {
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
		waitRGWTranslationPolicyReady(t, ctx, link, selected)
		fallback := bytes.Repeat([]byte("new write after priority pipe removal\n"), 2048)
		putTagged(a, input+"/published/fallback", "blue", fallback)
		waitOwnedSyncObject(t, ctx, destB, output+"/published/fallback", http.StatusOK, fallback)
		waitOwnedBucketCheckpoint(t, ctx, link, selected, low.ID)
		assertRGWSyncClass(t, ctx, destB, output, "published/fallback", "STANDARD")
		rgwPlacementPoolPayload(t, ctx, destination, "tc-sync-standard", "translated fallback STANDARD", fallback)
		if got := destB.request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusOK); !bytes.Equal(got, payload) {
			t.Fatal("priority removal changed previous replica")
		}
		if err := link.RemoveSyncGroup(ctx, selected); err != nil {
			t.Fatal(err)
		}
		t.Log("captured cross-bucket instances; prefix AND (blue OR red), owner translation, priority winner and fallback classes verified with S3 bytes and exact native data pools")
	})
}

func (f *rgwSyncTranslationFixture) ordinary_user_denial_grant(t *testing.T) {
	ctx := f.ctx
	link := f.link
	ownerB := f.ownerB
	a := f.a
	b := f.b
	destB := f.destB
	body := f.body
	t.Run("ordinary_user_denial_grant", func(t *testing.T) {
		// User mode executes as B: B can write its destination, but initially has
		// no read permission on A's source. S3 policy belongs to the client recipe.
		const modeInput, modeOutput = "/tc-sync-mode-input", "/tc-sync-mode-output"
		a.request(t, ctx, http.MethodPut, modeInput, body, http.StatusOK)
		b.request(t, ctx, http.MethodPut, modeOutput, body, http.StatusOK)
		modeGroup, err := link.CreateSyncGroup(ctx, rgw.SyncPolicyScope{Bucket: strings.TrimPrefix(modeInput, "/")}, rgw.SyncGroupConfig{ID: "user-mode", Status: rgw.SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		cleanupRGWTranslationSyncGroup(t, link, modeGroup, "authorized-principal")
		modePipe := rgw.SyncPipeConfig{ID: "authorized-principal", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &rgw.SyncBucketSelector{Name: strings.TrimPrefix(modeInput, "/")}, DestinationBucket: &rgw.SyncBucketSelector{Name: strings.TrimPrefix(modeOutput, "/")}, Prefix: "auth/", User: ownerB, DestinationOwner: ownerB}
		if err := link.CreateSyncPipe(ctx, modeGroup, modePipe); err != nil {
			t.Fatal(err)
		}
		// Permit the native replication GET's mandatory ACL check while still
		// denying the actual source payload read. This separates GetObject
		// authorization from an earlier GetObjectAcl failure.
		aclOnlyPolicy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + ownerB.ID()}, "Action": "s3:GetObjectAcl", "Resource": "arn:aws:s3:::" + strings.TrimPrefix(modeInput, "/") + "/*"}}})
		s3FeatureRequest(t, ctx, a, http.MethodPut, modeInput+"?policy", aclOnlyPolicy, nil, http.StatusNoContent)
		waitRGWTranslationPolicyReady(t, ctx, link, modeGroup)
		modePayload := bytes.Repeat([]byte("user-mode read authorization\n"), 2048)
		a.request(t, ctx, http.MethodPut, modeInput+"/auth/before-grant", modePayload, http.StatusOK)
		b.request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant?acl", nil, http.StatusOK)
		denied := b.request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant", nil, http.StatusForbidden)
		requireRGWUserPlacementDenied(t, denied)
		if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
			t.Fatal(err)
		}
		requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destB, modeOutput + "/auth/before-grant"})
		// Require native progress while the source still denies this principal.
		// A worker that merely paused must not satisfy the exclusion proof.
		waitOwnedBucketCheckpoint(t, ctx, link, modeGroup, modePipe.ID)
		requireRGWObjectsAbsent(t, ctx, time.Second, rgwAbsentObject{destB, modeOutput + "/auth/before-grant"})
		// Native impersonated replication GET first requires GetObjectAcl,
		// then GetObject for this unversioned source, unlike an ordinary GET.
		policy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + ownerB.ID()}, "Action": []string{"s3:GetObject", "s3:GetObjectAcl"}, "Resource": "arn:aws:s3:::" + strings.TrimPrefix(modeInput, "/") + "/*"}}})
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
		if got := destB.request(t, ctx, http.MethodGet, modeOutput+"/auth/after-grant", nil, http.StatusOK); !bytes.Equal(got, modePayload) {
			t.Fatal("user-mode policy removal changed previously replicated bytes")
		}
		t.Log("ordinary confirmed user mode refused a source read before client policy grant and replicated exact future bytes after grant; owned policy removal retained destination data")
	})
}
