//go:build (all || (integration && multicluster && features)) && native_regression

package rgw_sync_policy_translation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement"
	"github.com/testcontainers/testcontainers-go"
)

// image is the Ceph image every cluster and gateway of the case runs: Tentacle
// 20.2.4, which reproduces both defects. A run selects another image, such as
// a patched gateway build, with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_sync_policy_translation_test.image=<image>"
//
// No release is known to fix either.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

// testRGWSyncTranslationFiltering runs the two strict cases on one shared
// realm, in the order the required supported cases use, each with its own
// buckets and sync group. network names the variant in the result lines.
func testRGWSyncTranslationFiltering(t *testing.T, network string, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	f := newTranslationFixture(t, ctx, opts...)
	rgwSyncPriorityTagsOwnerClass(t, network, f)
	rgwSyncOrdinaryUserDenialGrant(t, network, f)
}

// newTranslationFixture is harness.NewRGWSyncTranslationFixture with both
// clusters and both gateways on image: the two-zone realm, the tiered
// placement in both zones, the permission policy and the two source owners.
func newTranslationFixture(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) *harness.RGWSyncTranslationFixture {
	t.Helper()
	t.Logf("image: %s", image)
	clusterOpts := append(append([]testcontainers.ContainerCustomizer(nil), opts...), harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	source, _ := harness.NewServiceClusterWithOptions(t, image, clusterOpts...)
	destination, _ := harness.NewServiceClusterWithOptions(t, image, clusterOpts...)
	t.Logf("ceph: source %s, destination %s", harness.StaticFacts(source).CephVersion, harness.StaticFacts(destination).CephVersion)
	link, err := multisite.Run(ctx, image, multisite.WithConfig(multisite.Config{Source: source, Destination: destination, Realm: "tc-sync-translation", SourceZone: "source", DestinationZone: "destination"}))
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
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inline := false
	placementConfig := placement.Config{Name: "tc-sync-tiered", IndexPool: "tc-sync-index", DataExtraPool: "tc-sync-extra", InlineData: &inline, StorageClasses: []placement.StorageClass{{Name: "STANDARD", DataPool: "tc-sync-standard"}, {Name: "STANDARD_IA", DataPool: "tc-sync-ia"}}}
	primaryPlacement, err := placement.Create(ctx, link.Source, placementConfig)
	if err != nil {
		t.Fatal(err)
	}
	secondaryPlacement, err := placement.Create(ctx, link.Destination, placementConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := primaryPlacement.Apply(ctx, link.Source); err != nil {
		t.Fatal(err)
	}
	if err := link.PullDestinationPeriod(ctx); err != nil {
		t.Fatal(err)
	}
	if err := secondaryPlacement.Reload(ctx, link.Destination); err != nil {
		t.Fatal(err)
	}
	global, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{}, multisite.SyncGroupConfig{ID: "permission", Status: multisite.SyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := global.CreateFlow(ctx, multisite.SyncFlowConfig{SourceZone: "source", DestinationZone: "destination"}); err != nil {
		t.Fatal(err)
	}
	if err := global.CreatePipe(ctx, multisite.SyncPipeConfig{ID: "permit", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := global.Apply(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	ownerA, err := link.Source.CreateUser(ctx, rgw.UserConfig{ID: "tc-sync-owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	ownerB, err := link.Source.CreateUser(ctx, rgw.UserConfig{ID: "tc-sync-owner-b"})
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
	client := func(t *testing.T, user *rgw.User, endpoint string) harness.S3HTTPClient {
		t.Helper()
		access, secret, err := user.CreationKey().Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: link.Source.Region, HTTP: &http.Client{Timeout: 15 * time.Second}}
	}
	a, b := client(t, ownerA, sourceEndpoint), client(t, ownerB, sourceEndpoint)
	destA, destB := client(t, ownerA, destEndpoint), client(t, ownerB, destEndpoint)
	body := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + primaryPlacement.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	return &harness.RGWSyncTranslationFixture{Ctx: ctx, Source: source, Destination: destination, Link: link, PrimaryPlacement: primaryPlacement, SecondaryPlacement: secondaryPlacement, OwnerA: ownerA, OwnerB: ownerB, Client: client, SourceEndpoint: sourceEndpoint, DestEndpoint: destEndpoint, A: a, B: b, DestA: destA, DestB: destB, Body: body}
}

// replicated waits up to four minutes for path to read back as payload. It
// returns the last answer when it does not, without failing the test.
func replicated(ctx context.Context, client harness.S3HTTPClient, path string, payload []byte) (string, bool) {
	wait, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	for {
		attempt, stop := context.WithTimeout(wait, 15*time.Second)
		status, body, err := harness.RGWScenarioRequest(attempt, client, http.MethodGet, path)
		stop()
		if err == nil && status == http.StatusOK && bytes.Equal(body, payload) {
			return "", true
		}
		last := fmt.Sprintf("status=%d code=%s bytes=%d error=%v", status, harness.RGWBackendErrorCode(body), len(body), err)
		select {
		case <-wait.Done():
			return last, false
		case <-time.After(time.Second):
		}
	}
}

// replicaClass is the storage class the bucket listing gives key, or "" when
// the listing omits it.
func replicaClass(t *testing.T, ctx context.Context, client harness.S3HTTPClient, bucket, key string) string {
	t.Helper()
	var listing struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(client.Request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &listing); err != nil {
		t.Fatal(err)
	}
	for _, entry := range listing.Contents {
		if entry.Key == key {
			return entry.StorageClass
		}
	}
	return ""
}

// present reads each object every second for duration and returns those that
// a read found, as harness.RequireRGWObjectsAbsent does without failing the
// test for them. An answer other than the object or NoSuchKey fails it.
func present(t *testing.T, ctx context.Context, duration time.Duration, objects ...harness.RGWAbsentObject) []string {
	t.Helper()
	found := map[string]bool{}
	var out []string
	for deadline := time.Now().Add(duration); time.Now().Before(deadline); {
		for _, object := range objects {
			attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
			status, body, err := harness.RGWScenarioRequest(attempt, object.Client, http.MethodGet, object.Path)
			cancel()
			switch code := harness.RGWBackendErrorCode(body); {
			case err == nil && status == http.StatusOK:
				if !found[object.Path] {
					found[object.Path] = true
					out = append(out, object.Path)
				}
			case err == nil && status == http.StatusNotFound && code == "NoSuchKey":
			default:
				t.Fatalf("excluded RGW object %s answered neither the object nor NoSuchKey: status=%d error=%v code=%s", object.Path, status, err, code)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	return out
}

// report prints the result line of one subtest.
func report(t *testing.T, unit string, symptoms []string) {
	t.Helper()
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_sync_policy_translation/%s result=%s image=%s symptoms=%q", unit, result, image, symptoms)
}

// rgwSyncPriorityTagsOwnerClass reports whether two overlapping bucket
// pipes resolve by priority.
//
// Given a pipe of priority 1 (blue tags, STANDARD) and one of priority 7 (red
// or blue tags, STANDARD_IA) on the same prefix with owner translation, whose
// policy the destination imported, when objects of every combination of
// prefix, tag and bucket are written and the blue object, which both pipes
// select, replicates with owner translation, then the defect is REPRODUCED
// if the red object, which only the higher-priority pipe selects, does not
// replicate, a replica is not of the higher-priority pipe's class, an object
// that no pipe selects replicates, or, after the higher-priority pipe is
// removed, a new write does not replicate with the lower-priority pipe's
// class or an earlier replica changes. It is NOT_REPRODUCED otherwise.
func rgwSyncPriorityTagsOwnerClass(t *testing.T, network string, f *harness.RGWSyncTranslationFixture) {
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
		payload := func(path string) []byte { return bytes.Repeat([]byte("translated "+path+"\n"), 2048) }
		for _, object := range []struct{ path, color string }{{input + "/published/blue", "blue"}, {input + "/published/red", "red"}, {input + "/published/green", "green"}, {input + "/published/untagged", ""}, {input + "/private/blue", "blue"}, {other + "/published/from-other-bucket", "blue"}} {
			putTagged(a, object.path, object.color, payload(object.path))
		}
		if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
			t.Fatal(err)
		}
		// Control: the object both pipes select replicates.
		harness.WaitOwnedSyncObject(t, ctx, destB, output+"/published/blue", http.StatusOK, payload(input+"/published/blue"))

		// Each symptom observed by the then step; none means NOT_REPRODUCED.
		var symptoms []string
		// The replicas the higher-priority pipe must have written, by color.
		arrived := map[string]bool{"blue": true}
		if last, ok := replicated(ctx, destB, output+"/published/red", payload(input+"/published/red")); ok {
			arrived["red"] = true
		} else {
			symptoms = append(symptoms, "the red object, which only the higher-priority pipe selects, did not replicate: "+last)
		}
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, selected, high.ID)
		for _, color := range []string{"blue", "red"} {
			if !arrived[color] {
				continue
			}
			if class := replicaClass(t, ctx, destB, output, "published/"+color); class != "STANDARD_IA" {
				symptoms = append(symptoms, fmt.Sprintf("the %s replica has class %q, not the higher-priority pipe's STANDARD_IA", color, class))
				continue
			}
			harness.RGWPlacementPoolPayload(t, ctx, destination, "tc-sync-ia", "translated "+color+" STANDARD_IA", payload(input+"/published/"+color))
		}
		// Control: object ownership changed to the destination bucket's
		// ordinary owner.
		destA.Request(t, ctx, http.MethodGet, output+"/published/blue", nil, http.StatusForbidden)
		putTagged(destB, output+"/published/reverse", "blue", payload(output+"/published/reverse"))
		if found := present(t, ctx, 35*time.Second, harness.RGWAbsentObject{Client: destB, Path: output + "/published/green"}, harness.RGWAbsentObject{Client: destB, Path: output + "/published/untagged"}, harness.RGWAbsentObject{Client: destB, Path: output + "/private/blue"}, harness.RGWAbsentObject{Client: destB, Path: output + "/published/from-other-bucket"}, harness.RGWAbsentObject{Client: b, Path: output + "/published/reverse"}); len(found) != 0 {
			symptoms = append(symptoms, fmt.Sprintf("objects that no pipe selects replicated: %v", found))
		}
		// Remove only the high-priority pipe. The matching lower-priority pipe
		// now selects STANDARD for a new write and preserves previous IA objects.
		if err := selected.RemovePipe(ctx, high.ID); err != nil {
			t.Fatal(err)
		}
		harness.WaitRGWTranslationPolicyReady(t, ctx, link, selected)
		fallback := bytes.Repeat([]byte("new write after priority pipe removal\n"), 2048)
		putTagged(a, input+"/published/fallback", "blue", fallback)
		if last, ok := replicated(ctx, destB, output+"/published/fallback", fallback); !ok {
			symptoms = append(symptoms, "a new write after the higher-priority pipe was removed did not replicate through the lower-priority pipe: "+last)
		} else {
			harness.WaitOwnedBucketCheckpoint(t, ctx, link, selected, low.ID)
			if class := replicaClass(t, ctx, destB, output, "published/fallback"); class != "STANDARD" {
				symptoms = append(symptoms, fmt.Sprintf("the replica written after the higher-priority pipe was removed has class %q, not the lower-priority pipe's STANDARD", class))
			} else {
				harness.RGWPlacementPoolPayload(t, ctx, destination, "tc-sync-standard", "translated fallback STANDARD", fallback)
			}
		}
		for _, color := range []string{"blue", "red"} {
			if !arrived[color] {
				continue
			}
			path := "/published/" + color
			if got := destB.Request(t, ctx, http.MethodGet, output+path, nil, http.StatusOK); !bytes.Equal(got, payload(input+path)) {
				symptoms = append(symptoms, fmt.Sprintf("priority removal changed the %s replica's bytes", color))
			}
			if class := replicaClass(t, ctx, destB, output, "published/"+color); class != "STANDARD_IA" {
				symptoms = append(symptoms, fmt.Sprintf("after priority removal the %s replica has class %q, not STANDARD_IA", color, class))
			}
		}
		if err := selected.Remove(ctx); err != nil {
			t.Fatal(err)
		}
		if t.Failed() {
			return
		}
		report(t, network+"/priority_tags_owner_class", symptoms)
	})
}

// rgwSyncOrdinaryUserDenialGrant reports whether a user-mode pipe replicates
// an object that the source denies its user.
//
// Given a user-mode pipe that runs as ordinary user B, which may read the
// source object's ACL but whose direct read of the object is denied, and
// whose policy the destination imported, when an object is written to the
// source and the destination's sync becomes ready, then the defect is
// REPRODUCED if the object replicates before the grant, over a window or
// after the pipe's bucket checkpoint, and NOT_REPRODUCED if it does not.
// After the source grants s3:GetObject, a later write must replicate exactly
// (a control).
func rgwSyncOrdinaryUserDenialGrant(t *testing.T, network string, f *harness.RGWSyncTranslationFixture) {
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

		// Each symptom observed by the then step; none means NOT_REPRODUCED.
		var symptoms []string
		denial := harness.RGWAbsentObject{Client: destB, Path: modeOutput + "/auth/before-grant"}
		if found := present(t, ctx, 35*time.Second, denial); len(found) != 0 {
			symptoms = append(symptoms, "the object the source denies the pipe's user replicated before the grant")
		}
		// Require native progress while the source still denies this principal.
		// A worker that merely paused must not satisfy the exclusion proof.
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, modeGroup, modePipe.ID)
		if found := present(t, ctx, time.Second, denial); len(found) != 0 && len(symptoms) == 0 {
			symptoms = append(symptoms, "the object the source denies the pipe's user replicated by the bucket checkpoint before the grant")
		}
		// Native impersonated replication GET first requires GetObjectAcl,
		// then GetObject for this unversioned source, unlike an ordinary GET.
		policy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Principal": map[string]any{"AWS": "arn:aws:iam:::user/" + ownerB.ID()}, "Action": []string{"s3:GetObject", "s3:GetObjectAcl"}, "Resource": "arn:aws:s3:::" + strings.TrimPrefix(modeInput, "/") + "/*"}}})
		harness.S3FeatureRequest(t, ctx, a, http.MethodPut, modeInput+"?policy", policy, nil, http.StatusNoContent)
		if got := b.Request(t, ctx, http.MethodGet, modeInput+"/auth/before-grant", nil, http.StatusOK); !bytes.Equal(got, modePayload) {
			t.Fatal("granted principal read wrong bytes")
		}
		// Control: a write after the grant replicates exactly.
		a.Request(t, ctx, http.MethodPut, modeInput+"/auth/after-grant", modePayload, http.StatusOK)
		harness.WaitOwnedSyncObject(t, ctx, destB, modeOutput+"/auth/after-grant", http.StatusOK, modePayload)
		harness.WaitOwnedBucketCheckpoint(t, ctx, link, modeGroup, modePipe.ID)
		if err := modeGroup.Remove(ctx); err != nil {
			t.Fatal(err)
		}
		if got := destB.Request(t, ctx, http.MethodGet, modeOutput+"/auth/after-grant", nil, http.StatusOK); !bytes.Equal(got, modePayload) {
			t.Fatal("user-mode policy removal changed previously replicated bytes")
		}
		if t.Failed() {
			return
		}
		report(t, network+"/ordinary_user_denial_grant", symptoms)
	})
}
