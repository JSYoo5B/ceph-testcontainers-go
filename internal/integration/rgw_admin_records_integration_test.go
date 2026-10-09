//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"bytes"
	"context"
	"maps"
	"net/http"
	"net/url"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Native server prerequisites composed from public config, RGW identity and
// Admin APIs. HTTP AdminOps remains a consumer, not a new production CRUD API.
func TestRGWAdminRecordsAndRateLimit(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, _ := newServiceCluster(t, opts...)
			// Usage logging is disabled by default and must be prepared before
			// RGW's request environment/background flush thread is constructed.
			for _, setting := range []ceph.ConfigSetting{
				{Section: "client.admin", Name: "rgw_enable_usage_log", Value: "true"},
				{Section: "client.admin", Name: "rgw_usage_log_tick_interval", Value: "1"},
				{Section: "client.admin", Name: "rgw_usage_log_flush_threshold", Value: "1"},
			} {
				change, err := cluster.TemporaryConfig(ctx, setting)
				if change != nil {
					t.Cleanup(func() {
						cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
						defer stop()
						if err := change.Restore(cleanup); err != nil {
							t.Error(err)
						}
					})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{SkipUserCreation: true})
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var users []*ceph.RGWUser
			var clients []s3HTTPClient
			for _, config := range []ceph.RGWUserConfig{
				{ID: "tc-records-a"}, {ID: "tc-records-b"},
				{ID: "tc-records-operator", AdminCaps: "usage=read;ratelimit=read"},
				{ID: "tc-records-no-caps"},
			} {
				user, err := gateway.CreateUser(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				access, secret, err := user.Credentials()
				if err != nil {
					t.Fatal(err)
				}
				users = append(users, user)
				clients = append(clients, s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 15 * time.Second}})
			}
			operator, noCaps := clients[2], clients[3]
			// Negative AdminOps controls use a valid native S3 credential.
			noCaps.request(t, ctx, http.MethodGet, "/", nil, http.StatusOK)
			rgwRecordsRequireCaps(t, ctx, gateway, users[2], "read")
			// Every capability edit is scoped to this fresh, still-owned key.
			addWriteCaps := func() {
				t.Helper()
				rgwRecordsRequireCaps(t, ctx, gateway, users[2], "read")
				rgwBackendAdmin(t, ctx, gateway, "caps", "add", "--uid", users[2].ID(), "--caps", "usage=write;ratelimit=write")
				rgwRecordsRequireCaps(t, ctx, gateway, users[2], "read,write")
			}
			payload := bytes.Repeat([]byte("native-owned-usage-payload\n"), 2048)
			for i, bucket := range []string{"tc-records-a-data", "tc-records-b-data"} {
				clients[i].request(t, ctx, http.MethodPut, "/"+bucket, nil, http.StatusOK)
				clients[i].request(t, ctx, http.MethodPut, "/"+bucket+"/kept", payload, http.StatusOK)
				if actual := clients[i].request(t, ctx, http.MethodGet, "/"+bucket+"/kept", nil, http.StatusOK); !bytes.Equal(actual, payload) {
					t.Fatal("usage producer did not read its exact owned bytes")
				}
			}
			readUsage := func(callCtx context.Context, uid string) rgwRecordsUsage {
				t.Helper()
				query := url.Values{"format": {"json"}, "uid": {uid}, "show-entries": {"true"}, "show-summary": {"true"}}
				response := rgwBackendSigned(t, callCtx, operator, "s3", http.MethodGet, "/admin/usage?"+query.Encode(), nil, nil)
				rgwBackendRequire(t, response, http.StatusOK)
				return rgwRecordsDecodeUsage(t, response.body, uid)
			}
			usagePath := "/admin/usage?" + url.Values{"format": {"json"}, "uid": {users[0].ID()}}.Encode()
			rgwRecordsAccessDenied(t, rgwBackendSigned(t, ctx, noCaps, "s3", http.MethodGet, usagePath, nil, nil))
			// The same valid read credential must be denied a destructive trim.
			rgwRecordsAccessDenied(t, rgwBackendSigned(t, ctx, operator, "s3", http.MethodDelete, usagePath, nil, nil))
			var otherBefore map[string]rgwRecordsCounters
			rgwRecordsWait(t, ctx, "owned native usage flush", func(callCtx context.Context) bool {
				first := readUsage(callCtx, users[0].ID())
				other := readUsage(callCtx, users[1].ID())
				if !first.hasPayload(uint64(len(payload))) || !other.hasPayload(uint64(len(payload))) {
					return false
				}
				otherBefore = other.metrics()
				return true
			})
			// Ensure the completed data operations have reached native storage
			// and the other principal is stable across three flush ticks. Admin
			// probes use a separate principal, so they do not change this usage.
			stable := 0
			rgwRecordsWait(t, ctx, "unrelated principal stable usage", func(callCtx context.Context) bool {
				current := readUsage(callCtx, users[1].ID()).metrics()
				if maps.Equal(current, otherBefore) {
					stable++
				} else {
					otherBefore, stable = current, 0
				}
				return stable >= 3
			})
			addWriteCaps()
			// uid is always explicit. Never send remove-all=true or an unscoped
			// trim, even though this recipe owns the disposable cluster.
			trimQuery := url.Values{"format": {"json"}, "uid": {users[0].ID()}, "remove-all": {"false"}}
			rgwBackendRequire(t, rgwBackendSigned(t, ctx, operator, "s3", http.MethodDelete, "/admin/usage?"+trimQuery.Encode(), nil, nil), http.StatusOK)
			stable = 0
			rgwRecordsWait(t, ctx, "scoped native usage trim", func(callCtx context.Context) bool {
				first := readUsage(callCtx, users[0].ID())
				if !maps.Equal(readUsage(callCtx, users[1].ID()).metrics(), otherBefore) {
					t.Fatal("owned usage trim changed the other principal's counters")
				}
				if len(first.Entries) != 0 || len(first.Summary) != 0 {
					stable = 0
					return false
				}
				stable++
				return stable >= 3
			})
			t.Log("native usage: pre-start enable plus one-second flush; read capability authorized actual counters, unprivileged read/read-only trim denied, exact UID trim removed one principal and preserved the other's stored counters")

			// Restore only the owned operator's write bits to exercise the rate
			// limit's independent read/write capability contract below.
			rgwRecordsRequireCaps(t, ctx, gateway, users[2], "read,write")
			rgwBackendAdmin(t, ctx, gateway, "caps", "rm", "--uid", users[2].ID(), "--caps", "usage=write;ratelimit=write")
			rgwRecordsRequireCaps(t, ctx, gateway, users[2], "read")
			rgwRecordsRateLimit(t, ctx, gateway, users[0], users[2], clients[0], operator, noCaps, payload, addWriteCaps)
			// Successful S3 cleanup can emit new usage. Remaining records belong
			// to this disposable cluster and disappear with its native storage.
			for i, bucket := range []string{"tc-records-a-data", "tc-records-b-data"} {
				clients[i].request(t, ctx, http.MethodDelete, "/"+bucket+"/kept", nil, http.StatusNoContent)
				clients[i].request(t, ctx, http.MethodDelete, "/"+bucket, nil, http.StatusNoContent)
			}
			for _, user := range users {
				if err := gateway.RemoveUser(ctx, user); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
