//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Public composition for caller-created S3 buckets. It keeps the actual bucket
// identity before scoped Admin mutations; this is not a general adoption API.
func TestRGWBucketMaintenance(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		t.Run(network, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, _ := newServiceCluster(t, opts...)
			// Keep the owned queue under this recipe's explicit process step.
			// Otherwise the native background worker can consume it first.
			reshardConfig, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "client.admin", Name: "rgw_dynamic_resharding", Value: "false"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
				defer stop()
				if err := reshardConfig.Restore(cleanup); err != nil {
					t.Error(err)
				}
			})
			gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{SkipUserCreation: true})
			if err != nil {
				t.Fatal(err)
			}
			user, err := gateway.CreateUser(ctx, ceph.RGWUserConfig{ID: "tc-maintenance"})
			if err != nil {
				t.Fatal(err)
			}
			access, secret, err := user.Credentials()
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := gateway.S3Endpoint(ctx)
			if err != nil {
				t.Fatal(err)
			}
			client := s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 15 * time.Second}}
			admin := func(args ...string) []byte {
				t.Helper()
				data, err := gateway.Admin(ctx, args...)
				if err != nil {
					t.Fatalf("bucket fixture native %v: %v", args, err)
				}
				return data
			}
			const limited, outside = "tc-maintenance-limited", "tc-maintenance-outside"
			for _, bucket := range []string{limited, outside} {
				client.request(t, ctx, http.MethodPut, "/"+bucket, nil, http.StatusOK)
			}
			readBucket := func(bucket string) rgwMaintenanceBucket {
				t.Helper()
				var state rgwMaintenanceBucket
				data := admin("bucket", "stats", "--bucket", bucket)
				if err := json.Unmarshal(data, &state); err != nil || state.ID == "" || state.Owner != user.ID() || state.Bucket != bucket {
					t.Fatalf("native owned bucket identity: %s %v", data, err)
				}
				return state
			}
			prior := readBucket(limited)
			otherPrior := readBucket(outside)
			// Quota is for this particular bucket, independent of the user's
			// default bucket policy. Native cache configuration stays unchanged.
			admin("quota", "set", "--quota-scope", "bucket", "--bucket", limited, "--max-objects", "1", "--max-size", "2097152")
			admin("quota", "enable", "--quota-scope", "bucket", "--bucket", limited)
			state := readBucket(limited)
			if state.ID != prior.ID || !state.Quota.Enabled || state.Quota.MaxObjects != 1 || state.Quota.MaxSize != 2<<20 {
				t.Fatalf("individual bucket quota: %+v", state)
			}
			if readBucket(outside).Quota != otherPrior.Quota {
				t.Fatal("individual quota changed another bucket")
			}
			payload := bytes.Repeat([]byte("bucket maintenance durable bytes\n"), 2048)
			client.request(t, ctx, http.MethodPut, "/"+limited+"/kept", payload, http.StatusOK)
			admin("user", "stats", "--uid", user.ID(), "--sync-stats")
			deadline := time.Now().Add(2 * time.Minute)
			for {
				code, body, _ := rgwMaintenanceRequest(t, ctx, client, http.MethodPut, "/"+limited+"/rejected", []byte("over quota"))
				if code == http.StatusForbidden {
					if !bytes.Contains(body, []byte("<Code>QuotaExceeded</Code>")) {
						t.Fatalf("quota rejected for unrelated reason: %s", body)
					}
					break
				}
				if code != http.StatusOK {
					t.Fatalf("quota probe unexpected status %d: %s", code, body)
				}
				client.request(t, ctx, http.MethodDelete, "/"+limited+"/rejected", nil, http.StatusNoContent)
				if time.Now().After(deadline) {
					t.Fatal("native individual quota never enforced")
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Second):
				}
			}
			client.request(t, ctx, http.MethodGet, "/"+limited+"/rejected", nil, http.StatusNotFound)
			for _, key := range []string{"unaffected-a", "unaffected-b"} {
				client.request(t, ctx, http.MethodPut, "/"+outside+"/"+key, payload, http.StatusOK)
			}
			admin("quota", "disable", "--quota-scope", "bucket", "--bucket", limited)
			if state = readBucket(limited); state.ID != prior.ID || state.Quota.Enabled {
				t.Fatal("quota did not restore admission")
			}
			client.request(t, ctx, http.MethodPut, "/"+limited+"/after-disable", payload, http.StatusOK)
			// Queue -> process -> native status. The test owns a fresh cluster;
			// process handles its whole queue, which contains only this task.
			const shards = 17
			if prior.Shards >= shards {
				t.Fatalf("fixture cannot prove shard increase from %d to %d", prior.Shards, shards)
			}
			admin("reshard", "add", "--bucket", limited, "--num-shards", strconv.Itoa(shards))
			queue := admin("reshard", "list")
			if !bytes.Contains(queue, []byte(limited)) {
				t.Fatalf("owned bucket missing native queue: %s", queue)
			}
			admin("reshard", "process")
			state = readBucket(limited)
			if state.Shards != shards || state.Quota.Enabled || state.Quota.MaxObjects != 1 {
				t.Fatalf("reshard did not preserve quota: %+v", state)
			}
			var statuses []struct {
				State string `json:"reshard_status"`
			}
			status := admin("reshard", "status", "--bucket", limited)
			if err := json.Unmarshal(status, &statuses); err != nil || len(statuses) != shards {
				t.Fatalf("native shard readiness: %s %v", status, err)
			}
			for _, shard := range statuses {
				if shard.State != "not-resharding" {
					t.Fatalf("native shard still active: %s", status)
				}
			}
			if queue = admin("reshard", "list"); bytes.Contains(queue, []byte(limited)) {
				t.Fatalf("native queue not completed: %s", queue)
			}
			if other := readBucket(outside); other.ID != otherPrior.ID || other.Shards != otherPrior.Shards || other.Quota != otherPrior.Quota {
				t.Fatal("reshard changed another bucket")
			}
			for _, site := range []struct {
				bucket string
				keys   []string
			}{{limited, []string{"kept", "after-disable"}}, {outside, []string{"unaffected-a", "unaffected-b"}}} {
				keys := client.listKeys(t, ctx, "/"+site.bucket)
				if len(keys) != len(site.keys) {
					t.Fatalf("reshard listing lost or added entries: %v", keys)
				}
				for _, key := range site.keys {
					path := "/" + site.bucket + "/" + key
					if actual := client.request(t, ctx, http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(actual, payload) {
						t.Fatal("reshard changed S3 bytes")
					}
					client.request(t, ctx, http.MethodDelete, path, nil, http.StatusNoContent)
				}
				client.request(t, ctx, http.MethodDelete, "/"+site.bucket, nil, http.StatusNoContent)
			}
			if err := gateway.RemoveUser(ctx, user); err != nil {
				t.Fatal(err)
			}
			t.Logf("native individual quota denied excess object and preserved sibling bucket; disable restored writes; queued reshard %d→%d completed with all shard statuses idle, four payloads/listings/policies preserved, owned S3 cleanup", prior.Shards, shards)
		})
	}
}

type rgwMaintenanceQuota struct {
	Enabled    bool  `json:"enabled"`
	MaxObjects int64 `json:"max_objects"`
	MaxSize    int64 `json:"max_size"`
}
type rgwMaintenanceBucket struct {
	Bucket string              `json:"bucket"`
	ID     string              `json:"id"`
	Owner  string              `json:"owner"`
	Shards int                 `json:"num_shards"`
	Quota  rgwMaintenanceQuota `json:"bucket_quota"`
}

func rgwMaintenanceRequest(t *testing.T, ctx context.Context, client s3HTTPClient, method, path string, payload []byte) (int, []byte, http.Header) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	client.sign(request, payload, time.Now().UTC())
	response, err := client.http.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var data bytes.Buffer
	if _, err := data.ReadFrom(response.Body); err != nil {
		t.Fatal(err)
	}
	// The response metadata is usable by the client recipe without printing
	// the request's Authorization header or secret credentials.
	return response.StatusCode, data.Bytes(), response.Header.Clone()
}
