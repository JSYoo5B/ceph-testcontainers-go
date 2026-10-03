//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWTenantsAndAccounts(t *testing.T) { testRGWTenantsAndAccounts(t) }
func TestHostNetworkRGWTenantsAndAccounts(t *testing.T) {
	testRGWTenantsAndAccounts(t, ceph.WithHostNetwork())
}

func testRGWTenantsAndAccounts(t *testing.T, options ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, _ := newServiceCluster(t, options...)
	gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{Name: "identities", SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientFor := func(user *ceph.RGWUser) s3HTTPClient {
		t.Helper()
		access, secret, err := user.Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 20 * time.Second}}
	}
	users := make([]*ceph.RGWUser, 3)
	clients := make([]s3HTTPClient, 3)
	const localID, sharedBucket = "tc-same-user", "/tc-same-bucket"
	payloads := [][]byte{[]byte("legacy namespace payload"), []byte("tenant alpha payload"), []byte("tenant beta payload")}
	for i, tenant := range []string{"", "tenant_alpha", "tenant_beta"} {
		users[i], err = gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: localID, Tenant: tenant, AdminCaps: "users=read"})
		if err != nil {
			t.Fatal(err)
		}
		info, err := gateway.UserInfo(ctx, users[i])
		wantID := localID
		if tenant != "" {
			wantID = tenant + "$" + localID
		}
		if err != nil || users[i].ID() != wantID || info.ID != wantID || info.LocalID != localID || info.Tenant != tenant || info.AccountID != "" || info.Admin || info.System {
			t.Fatalf("native tenant identity was not exact: error=%v handleID=%q canonicalID=%q localID=%q tenant=%q accountID=%q admin=%t system=%t; expected canonicalID=%q localID=%q tenant=%q", err, users[i].ID(), info.ID, info.LocalID, info.Tenant, info.AccountID, info.Admin, info.System, wantID, localID, tenant)
		}
		clients[i] = clientFor(users[i])
		clients[i].request(t, ctx, http.MethodPut, sharedBucket, nil, http.StatusOK)
		clients[i].request(t, ctx, http.MethodPut, sharedBucket+"/object", payloads[i], http.StatusOK)
	}
	for i, client := range clients {
		if actual := client.request(t, ctx, http.MethodGet, sharedBucket+"/object", nil, http.StatusOK); !bytes.Equal(actual, payloads[i]) {
			t.Fatal("same UID/bucket names mixed tenant object bytes")
		}
		var listing struct {
			Buckets []struct {
				Name string `xml:"Name"`
			} `xml:"Buckets>Bucket"`
		}
		if xml.Unmarshal(client.request(t, ctx, http.MethodGet, "/", nil, http.StatusOK), &listing) != nil || len(listing.Buckets) != 1 || listing.Buckets[0].Name != strings.TrimPrefix(sharedBucket, "/") {
			t.Fatal("tenant list buckets exposed another namespace")
		}
	}
	// Explicit tenant paths must be signed with their escaped colon and denied
	// for another tenant's ordinary identity, rather than fail authentication.
	denied := rgwTenantRequest(t, ctx, clients[2], http.MethodGet, "/tenant_alpha:"+strings.TrimPrefix(sharedBucket, "/")+"/object", nil, http.StatusForbidden)
	requireRGWUserPlacementDenied(t, denied)
	if _, err := gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: localID, Tenant: "tenant_alpha"}); err == nil {
		t.Fatal("existing tenant UID could be overwritten")
	}
	quota := ceph.RGWQuota{Enabled: true, MaxSizeBytes: 1 << 20, MaxObjects: 8}
	if err := gateway.SetUserQuota(ctx, users[1], quota); err != nil {
		t.Fatal(err)
	}
	if err := gateway.SetBucketQuota(ctx, users[1], quota); err != nil {
		t.Fatal(err)
	}
	alpha, err := gateway.UserInfo(ctx, users[1])
	beta, betaErr := gateway.UserInfo(ctx, users[2])
	if err != nil || betaErr != nil || alpha.UserQuota != quota || alpha.BucketQuota != quota || beta.UserQuota == quota || beta.BucketQuota == quota {
		t.Fatal("tenant quota operation changed the wrong native user")
	}
	unlimited := ceph.RGWQuota{MaxSizeBytes: -1, MaxObjects: -1}
	for _, user := range []*ceph.RGWUser{users[0], users[1]} {
		if err := gateway.SetUserQuota(ctx, user, unlimited); err != nil {
			t.Fatal(err)
		}
		if err := gateway.SetBucketQuota(ctx, user, unlimited); err != nil {
			t.Fatal(err)
		}
		info, err := gateway.UserInfo(ctx, user)
		if err != nil || info.UserQuota != unlimited || info.BucketQuota != unlimited {
			t.Fatalf("native legacy/tenant unlimited quota failed: error=%v user_quota=%+v bucket_quota=%+v", err, info.UserQuota, info.BucketQuota)
		}
	}
	if err := gateway.SuspendUser(ctx, users[1], true); err != nil {
		t.Fatal(err)
	}
	rgwFixtureWaitStatus(t, ctx, clients[1], sharedBucket+"/object", http.StatusForbidden)
	if actual := clients[2].request(t, ctx, http.MethodGet, sharedBucket+"/object", nil, http.StatusOK); !bytes.Equal(actual, payloads[2]) {
		t.Fatal("suspending one tenant changed another tenant")
	}
	if err := gateway.SuspendUser(ctx, users[1], false); err != nil {
		t.Fatal(err)
	}
	if actual := rgwFixtureWaitStatus(t, ctx, clients[1], sharedBucket+"/object", http.StatusOK); !bytes.Equal(actual, payloads[1]) {
		t.Fatal("tenant suspension lost data")
	}
	if err := gateway.RemoveUser(ctx, users[1]); err == nil {
		t.Fatal("populated tenant user removed without purge")
	}
	for i, user := range users {
		clients[i].request(t, ctx, http.MethodDelete, sharedBucket+"/object", nil, http.StatusNoContent)
		clients[i].request(t, ctx, http.MethodDelete, sharedBucket, nil, http.StatusNoContent)
		if err := gateway.RemoveUser(ctx, user); err != nil {
			t.Fatal(err)
		}
		if err := gateway.RemoveUser(ctx, user); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("legacy plus two tenants reused identical local UID/bucket/object names with distinct native full IDs and bytes; explicit cross-tenant access denied; quota/suspend/removal affected only the selected identity")

	account, err := gateway.CreateAccount(ctx, ceph.RGWAccountConfig{Name: "Fixture Account", Tenant: "account_tenant"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.CreateAccount(ctx, ceph.RGWAccountConfig{ID: account.ID(), Name: "overwrite"}); err == nil {
		t.Fatal("fresh account API adopted existing ID")
	}
	roots := make([]*ceph.RGWUser, 2)
	rootClients := make([]s3HTTPClient, 2)
	for i := range roots {
		roots[i], err = gateway.CreateAccountRootUser(ctx, account, ceph.RGWUserConfig{ID: fmt.Sprintf("tc-account-root-%d", i), DisplayName: fmt.Sprintf("FixtureRoot%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		info, err := gateway.UserInfo(ctx, roots[i])
		if err != nil || info.AccountID != account.ID() || info.Type != "root" || info.Tenant != "account_tenant" || info.Admin || info.System {
			t.Fatalf("account root native identity wrong: error=%v canonicalID=%q localID=%q tenant=%q type=%q accountID=%q admin=%t system=%t", err, info.ID, info.LocalID, info.Tenant, info.Type, info.AccountID, info.Admin, info.System)
		}
		rootClients[i] = clientFor(roots[i])
	}
	accountQuota := ceph.RGWQuota{Enabled: true, MaxSizeBytes: -1, MaxObjects: 1}
	accountBucketQuota := ceph.RGWQuota{Enabled: true, MaxSizeBytes: 1 << 20, MaxObjects: 8}
	if err := gateway.SetAccountQuota(ctx, account, accountQuota); err != nil {
		info, readErr := gateway.AccountInfo(ctx, account)
		t.Fatalf("set account quota failed: %v; readback_error=%v actual_account_quota=%+v actual_bucket_quota=%+v requested=%+v", err, readErr, info.AccountQuota, info.BucketQuota, accountQuota)
	}
	if err := gateway.SetAccountBucketQuota(ctx, account, accountBucketQuota); err != nil {
		info, readErr := gateway.AccountInfo(ctx, account)
		t.Fatalf("set account bucket quota failed: %v; readback_error=%v actual_account_quota=%+v actual_bucket_quota=%+v requested=%+v", err, readErr, info.AccountQuota, info.BucketQuota, accountBucketQuota)
	}
	if err := gateway.SetUserQuota(ctx, roots[0], accountQuota); err == nil {
		t.Fatal("account fixture accepted ineffective per-user quota")
	}
	info, err := gateway.AccountInfo(ctx, account)
	if err != nil || info.AccountQuota != accountQuota || info.BucketQuota != accountBucketQuota {
		t.Fatal("native account aggregate and per-bucket quota did not persist")
	}
	const accountBucketA, accountBucketB = "/tc-account-bucket-a", "/tc-account-bucket-b"
	rootClients[0].request(t, ctx, http.MethodPut, accountBucketA, nil, http.StatusOK)
	rootClients[1].request(t, ctx, http.MethodPut, accountBucketB, nil, http.StatusOK)
	accountPayload := []byte("account owns this object across root identities")
	rootClients[0].request(t, ctx, http.MethodPut, accountBucketA+"/one", accountPayload, http.StatusOK)
	if actual := rootClients[1].request(t, ctx, http.MethodGet, accountBucketA+"/one", nil, http.StatusOK); !bytes.Equal(actual, accountPayload) {
		t.Fatal("second account root could not read account-owned data")
	}
	var acl struct {
		Owner struct {
			ID string `xml:"ID"`
		} `xml:"Owner"`
	}
	if xml.Unmarshal(rootClients[1].request(t, ctx, http.MethodGet, accountBucketA+"?acl", nil, http.StatusOK), &acl) != nil || acl.Owner.ID != account.ID() {
		t.Fatal("S3 bucket owner is not the native account ID")
	}
	if _, err := gateway.Admin(ctx, "account", "stats", "--account-id", account.ID(), "--sync-stats"); err != nil {
		t.Fatal(err)
	}
	rgwAccountWaitAggregateQuotaDenied(t, ctx, gateway, account, rootClients[1], accountBucketB)
	if err := gateway.RemoveAccount(ctx, account); err == nil {
		t.Fatal("nonempty account removed without purge")
	}
	if actual := rootClients[0].request(t, ctx, http.MethodGet, accountBucketA+"/one", nil, http.StatusOK); !bytes.Equal(actual, accountPayload) {
		t.Fatal("failed account removal changed data/credentials")
	}
	rootClients[0].request(t, ctx, http.MethodDelete, accountBucketA+"/one", nil, http.StatusNoContent)
	rootClients[0].request(t, ctx, http.MethodDelete, accountBucketA, nil, http.StatusNoContent)
	rootClients[1].request(t, ctx, http.MethodDelete, accountBucketB, nil, http.StatusNoContent)
	// Even with all buckets empty, account removal must retain its root users.
	if err := gateway.RemoveAccount(ctx, account); err == nil {
		t.Fatal("account with remaining roots removed")
	}
	for _, root := range roots {
		if err := gateway.RemoveUser(ctx, root); err != nil {
			t.Fatal(err)
		}
	}
	copyAccount := *account
	if err := gateway.RemoveAccount(ctx, account); err != nil {
		t.Fatal(err)
	}
	if err := gateway.RemoveAccount(ctx, &copyAccount); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.AccountInfo(ctx, account); err == nil {
		t.Fatal("removed account handle remained active")
	}
	t.Log("fresh tenanted account and two non-admin/non-system roots shared account-owned bytes/ACL owner; aggregate one-object quota denied another root's write to another bucket; bucket policy read back; nonpurge removal refused while resources remained, and final cleanup was idempotent")
}

func rgwTenantRequest(t *testing.T, ctx context.Context, client s3HTTPClient, method, path string, body []byte, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.URL.RawPath = strings.ReplaceAll(req.URL.EscapedPath(), ":", "%3A")
	client.sign(req, body, time.Now().UTC())
	response, err := client.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != wantStatus {
		t.Fatalf("S3 explicit tenant path returned HTTP %d expected %d error=%v", response.StatusCode, wantStatus, err)
	}
	return data
}

func rgwAccountWaitAggregateQuotaDenied(t *testing.T, parent context.Context, gateway *ceph.RGWContainer, account *ceph.RGWAccount, client s3HTTPClient, bucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	started := time.Now()
	for attempt := 0; ; attempt++ {
		path := fmt.Sprintf("%s/over-aggregate-%d", bucket, attempt)
		payload := []byte("second root, second bucket, over account aggregate quota")
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, client.endpoint+path, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		client.sign(req, payload, time.Now().UTC())
		response, err := client.http.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode == http.StatusForbidden {
			var result struct{ Code string }
			if xml.Unmarshal(data, &result) != nil || result.Code != "QuotaExceeded" {
				t.Fatalf("over-account-quota write returned unexpected S3 code %q", result.Code)
			}
			client.request(t, ctx, http.MethodGet, path, nil, http.StatusNotFound)
			t.Logf("actual account aggregate quota denied another root/bucket with QuotaExceeded after %s and %d transient accounting-cache grants", time.Since(started).Round(time.Millisecond), attempt)
			return
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("account quota probe returned unexpected HTTP %d", response.StatusCode)
		}
		client.request(t, ctx, http.MethodDelete, path, nil, http.StatusNoContent)
		if attempt%4 == 0 {
			if _, err := gateway.Admin(ctx, "account", "stats", "--account-id", account.ID(), "--sync-stats"); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("account quota did not reject a second bucket/user write: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}
