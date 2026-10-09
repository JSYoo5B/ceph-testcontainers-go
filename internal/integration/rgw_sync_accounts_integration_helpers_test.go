//go:build all || (integration && multicluster && features)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

func testRGWAccountRootSync(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, _, _ := newMultiClusterPair(t, append(opts, ceph.WithOSDCount(1))...)
	image, _ := integrationImages(t)
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = image
	}
	link, err := multicluster.RunRGWMultisite(ctx, rgwImage, multicluster.RGWMultisiteConfig{Source: source, Destination: destination, ControlImage: image, Realm: "tc-sync-accounts", SourceZone: "source", DestinationZone: "destination"})
	if link != nil {
		t.Cleanup(func() {
			if t.Failed() {
				inspect, stop := context.WithTimeout(context.Background(), 35*time.Second)
				state, inspectErr := link.SyncStatus(inspect, "destination")
				stop()
				t.Logf("account sync diagnostic status=%+v error=%v", state, inspectErr)
			}
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			if err := link.Terminate(cleanup); err != nil {
				t.Errorf("terminate account sync fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	permission, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{}, multicluster.RGWSyncGroupConfig{ID: "account-permission", Status: multicluster.RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncFlow(ctx, permission, multicluster.RGWSyncFlowConfig{SourceZone: "source", DestinationZone: "destination"}); err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncPipe(ctx, permission, multicluster.RGWSyncPipeConfig{ID: "permit", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := link.ApplySyncGroup(ctx, permission); err != nil {
		t.Fatal(err)
	}
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	sourceEndpoint, err := link.Source.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationEndpoint, err := link.Destination.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	account, err := link.Source.CreateAccount(ctx, ceph.RGWAccountConfig{Name: "tc-sync-account"})
	if err != nil {
		t.Fatal(err)
	}
	root, err := link.Source.CreateAccountRootUser(ctx, account, ceph.RGWUserConfig{ID: "tc-sync-account-root", DisplayName: "tc-sync-account-root"})
	if err != nil {
		t.Fatal(err)
	}
	info, err := link.Source.UserInfo(ctx, root)
	if err != nil || info.AccountID != account.ID() || info.Type != "root" || info.Admin || info.System {
		t.Fatal("native root identity was not confirmed against its creation-owned account")
	}
	primary := rgwSyncUserClient(t, root, sourceEndpoint, link.Source.Region)
	secondary := rgwSyncUserClient(t, root, destinationEndpoint, link.Source.Region)
	const input, output = "/tc-sync-account-input", "/tc-sync-account-output"
	for _, bucket := range []string{input, output} {
		primary.request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
	}
	selected, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{Bucket: strings.TrimPrefix(input, "/")}, multicluster.RGWSyncGroupConfig{ID: "account-root", Status: multicluster.RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	pipe := multicluster.RGWSyncPipeConfig{ID: "same-account-user", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(input, "/")}, DestinationBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(output, "/")}, Prefix: "replica/", User: root}
	if err := link.CreateSyncPipe(ctx, selected, pipe); err != nil {
		t.Fatal(err)
	}
	waitRGWTranslationPolicyReady(t, ctx, link, selected)
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("creation-owned account root native user-mode replication\n"), 1024)
	primary.request(t, ctx, http.MethodPut, input+"/replica/initial", payload, http.StatusOK)
	waitOwnedSyncObject(t, ctx, secondary, output+"/replica/initial", http.StatusOK, payload)
	waitOwnedBucketCheckpoint(t, ctx, link, selected, pipe.ID)
	outputIdentity := captureRGWSyncAccountBucket(t, ctx, link.Source, output, account.ID(), "")
	captureRGWSyncAccountBucket(t, ctx, link.Destination, output, account.ID(), outputIdentity)
	waitRGWSyncBucketPolicy(t, ctx, primary, output, nil)
	waitRGWSyncBucketPolicy(t, ctx, secondary, output, nil)

	// Deny the native replication action while retaining normal client access.
	// The destination's replicated bucket policy is observed before the write.
	deny, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Deny", "Principal": "*", "Action": "s3:ReplicateObject", "Resource": "*"}}})
	captureRGWSyncAccountBucket(t, ctx, link.Source, output, account.ID(), outputIdentity)
	s3FeatureRequest(t, ctx, primary, http.MethodPut, output+"?policy", deny, nil, http.StatusNoContent)
	waitRGWSyncBucketPolicy(t, ctx, secondary, output, deny)
	primary.request(t, ctx, http.MethodPut, input+"/replica/denied", payload, http.StatusOK)
	if got := primary.request(t, ctx, http.MethodGet, input+"/replica/denied", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("replication-only denial changed source S3 read/write")
	}
	secondary.request(t, ctx, http.MethodPut, output+"/client-control", payload, http.StatusOK)
	if got := secondary.request(t, ctx, http.MethodGet, output+"/client-control", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("replication-only denial changed destination S3 read/write")
	}
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{secondary, output + "/replica/denied"})
	// Native user-mode permission denial skips the entry and advances its
	// checkpoint. Require that progress while Deny still applies so a stalled
	// replication worker cannot satisfy the negative test merely by doing nothing.
	waitOwnedBucketCheckpoint(t, ctx, link, selected, pipe.ID)
	requireRGWObjectsAbsent(t, ctx, time.Second, rgwAbsentObject{secondary, output + "/replica/denied"})
	captureRGWSyncAccountBucket(t, ctx, link.Source, output, account.ID(), outputIdentity)
	captureRGWSyncAccountBucket(t, ctx, link.Destination, output, account.ID(), outputIdentity)
	s3FeatureRequest(t, ctx, primary, http.MethodDelete, output+"?policy", nil, nil, http.StatusNoContent)
	waitRGWSyncBucketPolicy(t, ctx, secondary, output, nil)
	primary.request(t, ctx, http.MethodPut, input+"/replica/after-policy-clear", payload, http.StatusOK)
	waitOwnedSyncObject(t, ctx, secondary, output+"/replica/after-policy-clear", http.StatusOK, payload)
	waitOwnedBucketCheckpoint(t, ctx, link, selected, pipe.ID)
	if err := link.RemoveSyncGroup(ctx, selected); err != nil {
		t.Fatal(err)
	}
	if got := secondary.request(t, ctx, http.MethodGet, output+"/replica/initial", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("account-root policy removal changed previous replica bytes")
	}
	t.Log("confirmed account-root canonical UID copied same-account buckets; native s3:ReplicateObject Deny blocked exact future key while normal S3 remained healthy, policy removal restored new bytes/checkpoint")
	testRGWCrossTenantSystemSync(t, ctx, link, sourceEndpoint, destinationEndpoint)
	if err := link.RemoveSyncGroup(ctx, permission); err != nil {
		t.Fatal(err)
	}
	if err := link.ApplySyncGroup(ctx, permission); err != nil {
		t.Fatal(err)
	}
}

func rgwSyncUserClient(t *testing.T, user *ceph.RGWUser, endpoint, region string) s3HTTPClient {
	t.Helper()
	access, secret, err := user.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	return s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: region, http: &http.Client{Timeout: 15 * time.Second}}
}

func waitRGWSyncBucketPolicy(t *testing.T, ctx context.Context, client s3HTTPClient, bucket string, expected []byte) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var want any
	if expected != nil {
		if err := json.Unmarshal(expected, &want); err != nil {
			t.Fatal(err)
		}
	}
	lastStatus := 0
	for {
		if err := wait.Err(); err != nil {
			t.Fatalf("wait replicated bucket policy: %v; status=%d", err, lastStatus)
		}
		request, err := http.NewRequestWithContext(wait, http.MethodGet, client.endpoint+bucket+"?policy", nil)
		if err != nil {
			t.Fatal(err)
		}
		client.sign(request, nil, time.Now())
		response, err := client.http.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
			response.Body.Close()
			lastStatus = response.StatusCode
			if err := wait.Err(); err != nil {
				t.Fatalf("wait replicated bucket policy: %v; status=%d", err, lastStatus)
			}
			var missing struct {
				Code string `xml:"Code"`
			}
			if expected == nil && response.StatusCode == http.StatusNotFound && readErr == nil && xml.Unmarshal(body, &missing) == nil && missing.Code == "NoSuchBucketPolicy" {
				return
			}
			var got any
			if expected != nil && response.StatusCode == http.StatusOK && readErr == nil && json.Unmarshal(body, &got) == nil && reflect.DeepEqual(got, want) {
				return
			}
		}
		select {
		case <-wait.Done():
			t.Fatalf("wait replicated bucket policy: %v; status=%d", wait.Err(), lastStatus)
		case <-time.After(time.Second):
		}
	}
}

func captureRGWSyncAccountBucket(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, path, owner, expectedID string) string {
	t.Helper()
	name := strings.TrimPrefix(path, "/")
	data, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", name)
	if err != nil {
		t.Fatal(err)
	}
	var native struct {
		Name  string `json:"bucket"`
		ID    string `json:"id"`
		Owner string `json:"owner"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Name != name || native.ID == "" || native.Owner != owner || (expectedID != "" && native.ID != expectedID) {
		t.Fatalf("account sync bucket name/instance/owner changed: name=%s id=%s owner=%s decode_error=%v", native.Name, native.ID, native.Owner, err)
	}
	return native.ID
}

func testRGWCrossTenantSystemSync(t *testing.T, ctx context.Context, link *multicluster.RGWMultisite, sourceEndpoint, destinationEndpoint string) {
	t.Helper()
	alpha, err := link.Source.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-system-cross-tenant", Tenant: "system_alpha"})
	if err != nil {
		t.Fatal(err)
	}
	beta, err := link.Source.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-system-cross-tenant", Tenant: "system_beta"})
	if err != nil {
		t.Fatal(err)
	}
	primaryAlpha := rgwSyncUserClient(t, alpha, sourceEndpoint, link.Source.Region)
	primaryBeta := rgwSyncUserClient(t, beta, sourceEndpoint, link.Source.Region)
	secondaryAlpha := rgwSyncUserClient(t, alpha, destinationEndpoint, link.Source.Region)
	secondaryBeta := rgwSyncUserClient(t, beta, destinationEndpoint, link.Source.Region)
	const input, output = "/tc-system-tenant-input", "/tc-system-tenant-output"
	primaryAlpha.request(t, ctx, http.MethodPut, input, nil, http.StatusOK)
	primaryBeta.request(t, ctx, http.MethodPut, output, nil, http.StatusOK)
	group, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{Bucket: strings.TrimPrefix(input, "/"), Tenant: "system_alpha"}, multicluster.RGWSyncGroupConfig{ID: "system-cross-tenant", Status: multicluster.RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	pipe := multicluster.RGWSyncPipeConfig{ID: "exact-tenant-instances", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(input, "/"), Tenant: "system_alpha"}, DestinationBucket: &multicluster.RGWSyncBucketSelector{Name: strings.TrimPrefix(output, "/"), Tenant: "system_beta"}, Prefix: "system/"}
	if err := link.CreateSyncPipe(ctx, group, pipe); err != nil {
		t.Fatal(err)
	}
	// Register after the owned pipe exists: Cleanup runs before the multisite
	// fixture's earlier Terminate, using a fresh bounded context after failure.
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		inspect, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		attempt, cancel := context.WithTimeout(inspect, 20*time.Second)
		state, err := link.BucketSyncStatus(attempt, group, pipe.ID, "source", "destination")
		cancel()
		t.Logf("cross-tenant bucket diagnostic realm=%s period=%s zonegroup=%s realm_epoch=%d source_zone=%s/%s source_bucket=%s/%s:%s destination_zone=%s/%s destination_bucket=%s/%s:%s state=%s shards=%d behind_shards=%d caught_up=%t status_error=%s", state.RealmID, state.PeriodID, state.ZonegroupID, state.RealmEpoch, state.SourceZone, state.SourceZoneID, state.SourceBucket.Tenant, state.SourceBucket.Name, state.SourceBucket.ID, state.Zone, state.ZoneID, state.DestinationBucket.Tenant, state.DestinationBucket.Name, state.DestinationBucket.ID, state.State, state.Shards, state.BehindShards, state.CaughtUp, rgwSyncDiagnosticError(err))
		rgwCrossTenantObjectStatDiagnostic(t, inspect, link.Destination, state.DestinationBucket)
		const path = "/system_beta:tc-system-tenant-output/system/selected"
		rgwCrossTenantS3Diagnostic(t, inspect, "alpha", secondaryAlpha, path)
		rgwCrossTenantS3Diagnostic(t, inspect, "beta", secondaryBeta, path)
	})
	waitRGWTranslationPolicyReady(t, ctx, link, group)
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("system mode exact cross-tenant bucket selector\n"), 1024)
	primaryAlpha.request(t, ctx, http.MethodPut, input+"/system/selected", payload, http.StatusOK)
	// System mode preserves the source object's ACL when no owner translation
	// is requested. Address the destination tenant explicitly as that owner.
	waitOwnedSyncObject(t, ctx, secondaryAlpha, "/system_beta:tc-system-tenant-output/system/selected", http.StatusOK, payload)
	waitOwnedBucketCheckpoint(t, ctx, link, group, pipe.ID)
	secondaryBeta.request(t, ctx, http.MethodHead, output, nil, http.StatusOK)
	primaryAlpha.request(t, ctx, http.MethodPut, input+"/private/excluded", payload, http.StatusOK)
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{secondaryBeta, output + "/private/excluded"})
	if err := link.RemoveSyncGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	if got := secondaryAlpha.request(t, ctx, http.MethodGet, "/system_beta:tc-system-tenant-output/system/selected", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("cross-tenant system policy removal changed previous replica")
	}
	t.Log("system mode with no principal/owner translation copied exact bytes between captured buckets in different tenants and retained source object access; another prefix remained absent")
}

// Diagnostic failures never replace the test's original failure. Native output,
// credentials, sync markers and S3 error bodies are deliberately not logged.
func rgwSyncDiagnosticError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "read_failed_redacted"
	}
}

func rgwCrossTenantObjectStatDiagnostic(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, identity multicluster.RGWSyncBucketIdentity) {
	t.Helper()
	attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	args := []string{"object", "stat", "--bucket", "system_beta/tc-system-tenant-output", "--object", "system/selected"}
	scope := "name_only"
	if identity.Tenant == "system_beta" && identity.Name == "tc-system-tenant-output" && identity.ID != "" {
		args = append(args, "--bucket-id", identity.ID)
		scope = "captured_instance"
	}
	data, err := gateway.Admin(attempt, args...)
	if err == nil {
		err = attempt.Err()
	}
	if err != nil {
		t.Logf("cross-tenant native object stat scope=%s command_success=false object_exists=unconfirmed status_error=%s", scope, rgwSyncDiagnosticError(err))
		return
	}
	// Native v20.2.4 opens its first JSON section as object_metadata. The
	// JSONFormatter suppresses the first section's name, so name/size are root
	// fields (radosgw-admin.cc 9028–9030; Formatter.cc 193–229).
	var native struct {
		Name string  `json:"name"`
		Size *uint64 `json:"size"`
	}
	decodeErr := json.Unmarshal(data, &native)
	if decodeErr != nil || native.Name != "system/selected" || native.Size == nil {
		t.Logf("cross-tenant native object stat scope=%s command_success=true object_exists=unconfirmed status_error=invalid_safe_fields", scope)
		return
	}
	t.Logf("cross-tenant native object stat scope=%s incarnation_confirmed=%t command_success=true object_exists=true size=%d status_error=none", scope, scope == "captured_instance", *native.Size)
}

func rgwCrossTenantS3Diagnostic(t *testing.T, ctx context.Context, principal string, client s3HTTPClient, path string) {
	t.Helper()
	attempt, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(attempt, http.MethodGet, client.endpoint+path, nil)
	if err != nil {
		t.Logf("cross-tenant S3 diagnostic principal=%s status=0 code=unconfirmed status_error=%s", principal, rgwSyncDiagnosticError(err))
		return
	}
	client.sign(request, nil, time.Now().UTC())
	response, err := client.http.Do(request)
	if err != nil {
		t.Logf("cross-tenant S3 diagnostic principal=%s status=0 code=unconfirmed status_error=%s", principal, rgwSyncDiagnosticError(err))
		return
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err := attempt.Err(); err != nil {
		readErr = err
	}
	code := "none"
	if response.StatusCode != http.StatusOK {
		var native struct {
			Code string `xml:"Code"`
		}
		code = "unrecognized_redacted"
		if readErr == nil && xml.Unmarshal(body, &native) == nil {
			// Only known protocol codes are printable; an unexpected upstream
			// response cannot inject body text into diagnostic logs.
			switch native.Code {
			case "NoSuchKey", "NoSuchBucket", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "RequestTimeTooSkewed", "InternalError", "ServiceUnavailable":
				code = native.Code
			}
		}
	}
	t.Logf("cross-tenant S3 diagnostic principal=%s status=%d code=%s response_bytes=%d status_error=%s", principal, response.StatusCode, code, len(body), rgwSyncDiagnosticError(readErr))
}
