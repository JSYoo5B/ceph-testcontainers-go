//go:build all || (integration && features)

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func rgwRecordsRateLimit(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, owner, operatorUser *ceph.RGWUser, client, operator, noCaps s3HTTPClient, payload []byte, addWriteCaps func()) {
	t.Helper()
	const limited, outside = "tc-records-rate-limited", "tc-records-rate-outside"
	for _, bucket := range []string{limited, outside} {
		client.request(t, ctx, http.MethodPut, "/"+bucket, nil, http.StatusOK)
		client.request(t, ctx, http.MethodPut, "/"+bucket+"/kept", payload, http.StatusOK)
		if actual := client.request(t, ctx, http.MethodGet, "/"+bucket+"/kept", nil, http.StatusOK); !bytes.Equal(actual, payload) {
			t.Fatal("pre-limit native object read changed exact owned bytes")
		}
	}
	readBucketID := func(bucket string) string {
		t.Helper()
		var native struct {
			ID    string `json:"id"`
			Name  string `json:"bucket"`
			Owner string `json:"owner"`
			Quota struct {
				Enabled bool `json:"enabled"`
			} `json:"bucket_quota"`
		}
		data := rgwBackendAdmin(t, ctx, gateway, "bucket", "stats", "--bucket", bucket)
		if json.Unmarshal(data, &native) != nil || native.ID == "" || native.Name != bucket || native.Owner != owner.ID() || native.Quota.Enabled {
			t.Fatal("rate-limit fixture bucket identity/quota prerequisite changed")
		}
		return native.ID
	}
	limitedID, outsideID := readBucketID(limited), readBucketID(outside)
	info, err := gateway.UserInfo(ctx, owner)
	if err != nil || info.UserQuota.Enabled || info.BucketQuota.Enabled {
		t.Fatal("rate-limit fixture requires owned principal with independent quotas disabled")
	}
	path := func(bucket string, values url.Values) string {
		values.Set("format", "json")
		values.Set("bucket", bucket)
		values.Set("ratelimit-scope", "bucket")
		return "/admin/ratelimit?" + values.Encode()
	}
	readLimit := func(bucket string) rgwRecordsRatePolicy {
		t.Helper()
		response := rgwBackendSigned(t, ctx, operator, "s3", http.MethodGet, path(bucket, url.Values{}), nil, nil)
		rgwBackendRequire(t, response, http.StatusOK)
		var native struct {
			Policy *rgwRecordsRatePolicy `json:"bucket_ratelimit"`
		}
		if json.Unmarshal(response.body, &native) != nil || native.Policy == nil {
			t.Fatal("decode native individual bucket rate-limit policy")
		}
		return *native.Policy
	}
	before, outsideBefore := readLimit(limited), readLimit(outside)
	if before.Enabled || outsideBefore.Enabled {
		t.Fatal("fresh rate-limit recipe refuses an already-enabled bucket policy")
	}
	policy := rgwRecordsRatePolicy{Enabled: true, ReadOps: 1}
	rgwRecordsAccessDenied(t, rgwBackendSigned(t, ctx, noCaps, "s3", http.MethodGet, path(limited, url.Values{}), nil, nil))
	for _, denied := range []s3HTTPClient{noCaps, operator} {
		rgwRecordsAccessDenied(t, rgwBackendSigned(t, ctx, denied, "s3", http.MethodPost, path(limited, policy.values()), nil, nil))
	}
	if readLimit(limited) != before || readLimit(outside) != outsideBefore {
		t.Fatal("denied bucket rate-limit write changed native policies")
	}
	addWriteCaps()
	if readBucketID(limited) != limitedID || readBucketID(outside) != outsideID {
		t.Fatal("rate-limit mutation refuses replaced native buckets")
	}
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, operator, "s3", http.MethodPost, path(limited, policy.values()), nil, nil), http.StatusOK)
	if readLimit(limited) != policy || readLimit(outside) != outsideBefore {
		t.Fatal("individual bucket rate-limit readback/sibling isolation failed")
	}
	rgwRecordsWait(t, ctx, "native bucket SlowDown rejection", func(callCtx context.Context) bool {
		response := rgwBackendSigned(t, callCtx, client, "s3", http.MethodGet, "/"+limited+"/kept", nil, nil)
		if response.code == http.StatusOK {
			if !bytes.Equal(response.body, payload) {
				t.Fatal("rate-limit admitted read changed owned object bytes")
			}
			return false
		}
		if response.code != http.StatusServiceUnavailable || rgwRecordsErrorCode(response.body) != "SlowDown" {
			t.Fatalf("rate-limit negative control was not native 503 SlowDown: status=%d code=%s (body redacted)", response.code, rgwRecordsErrorCode(response.body))
		}
		return true
	})
	// The same identity and gateway must still admit sibling bucket reads.
	for range 3 {
		if actual := client.request(t, ctx, http.MethodGet, "/"+outside+"/kept", nil, http.StatusOK); !bytes.Equal(actual, payload) {
			t.Fatal("individual rate-limit changed sibling bucket bytes")
		}
	}
	if readBucketID(limited) != limitedID || readBucketID(outside) != outsideID || readLimit(limited) != policy {
		t.Fatal("rate-limit restore refuses replaced native identity or outside policy edits")
	}
	rgwRecordsRequireCaps(t, ctx, gateway, operatorUser, "read,write")
	// Restore every native field, not merely enabled=false. Zero disables the
	// corresponding dimension; Tentacle has only read/write ops and bytes.
	rgwBackendRequire(t, rgwBackendSigned(t, ctx, operator, "s3", http.MethodPost, path(limited, before.values()), nil, nil), http.StatusOK)
	if readLimit(limited) != before || readLimit(outside) != outsideBefore {
		t.Fatal("rate-limit did not restore exact prior native policies")
	}
	for range 3 {
		response := rgwBackendWaitStatus(t, ctx, func(callCtx context.Context) rgwBackendResponse {
			return rgwBackendSigned(t, callCtx, client, "s3", http.MethodGet, "/"+limited+"/kept", nil, nil)
		}, http.StatusOK)
		if !bytes.Equal(response.body, payload) {
			t.Fatal("rate-limit restore did not recover exact object bytes")
		}
	}
	for _, bucket := range []string{limited, outside} {
		client.request(t, ctx, http.MethodDelete, "/"+bucket+"/kept", nil, http.StatusNoContent)
		client.request(t, ctx, http.MethodDelete, "/"+bucket, nil, http.StatusNoContent)
	}
	rgwBackendAdmin(t, ctx, gateway, "caps", "rm", "--uid", operatorUser.ID(), "--caps", "usage=write;ratelimit=write")
	rgwRecordsRequireCaps(t, ctx, gateway, operatorUser, "read")
	t.Log("native bucket rate-limit: caps-gated read/write, exact Tentacle policy readback, actual 503 SlowDown on owned reads, same-principal sibling admission, full native policy restore and repeated exact-byte recovery; scoped S3 cleanup")
}

type rgwRecordsRatePolicy struct {
	Enabled    bool  `json:"enabled"`
	ReadOps    int64 `json:"max_read_ops"`
	WriteOps   int64 `json:"max_write_ops"`
	ReadBytes  int64 `json:"max_read_bytes"`
	WriteBytes int64 `json:"max_write_bytes"`
}

func (policy rgwRecordsRatePolicy) values() url.Values {
	return url.Values{
		"enabled":         {strconv.FormatBool(policy.Enabled)},
		"max-read-ops":    {strconv.FormatInt(policy.ReadOps, 10)},
		"max-write-ops":   {strconv.FormatInt(policy.WriteOps, 10)},
		"max-read-bytes":  {strconv.FormatInt(policy.ReadBytes, 10)},
		"max-write-bytes": {strconv.FormatInt(policy.WriteBytes, 10)},
	}
}

func rgwRecordsRequireCaps(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, user *ceph.RGWUser, permission string) {
	t.Helper()
	info, err := gateway.UserInfo(ctx, user)
	if err != nil || info.ID != user.ID() || info.Admin || info.System || len(info.AdminCaps) != 2 {
		t.Fatal("AdminOps operator must remain an owned ordinary user with exactly two explicit capability types")
	}
	seen := map[string]bool{}
	for _, capability := range info.AdminCaps {
		actual := strings.ReplaceAll(capability.Permission, " ", "")
		// Native RGW_CAP_ALL is the read|write bitmask and dumps as "*".
		// This is one capability type's bits, not the global user admin flag.
		if actual == "*" {
			actual = "read,write"
		}
		if seen[capability.Type] || (capability.Type != "usage" && capability.Type != "ratelimit") || actual != permission {
			t.Fatal("AdminOps operator's native capability bits changed")
		}
		seen[capability.Type] = true
	}
}

func rgwRecordsErrorCode(body []byte) string {
	var failure struct {
		Code  string `xml:"Code" json:"Code"`
		Error struct {
			Code string `xml:"Code" json:"Code"`
		} `xml:"Error" json:"Error"`
	}
	if xml.Unmarshal(body, &failure) != nil {
		_ = json.Unmarshal(body, &failure)
	}
	if failure.Code != "" {
		return failure.Code
	}
	return failure.Error.Code
}

func rgwRecordsAccessDenied(t *testing.T, response rgwBackendResponse) {
	t.Helper()
	rgwBackendRequire(t, response, http.StatusForbidden)
	if rgwRecordsErrorCode(response.body) != "AccessDenied" {
		t.Fatal("AdminOps negative control was not native AccessDenied")
	}
}

func rgwRecordsWait(t *testing.T, parent context.Context, operation string, probe func(context.Context) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	for {
		if ctx.Err() != nil {
			t.Fatalf("%s: bounded native proof did not complete", operation)
		}
		ready := probe(ctx)
		if ctx.Err() != nil {
			t.Fatalf("%s: native probe exceeded deadline", operation)
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: bounded native proof did not complete", operation)
		case <-time.After(time.Second):
		}
	}
}

type rgwRecordsCounters struct {
	BytesSent     uint64 `json:"bytes_sent"`
	BytesReceived uint64 `json:"bytes_received"`
	Ops           uint64 `json:"ops"`
	SuccessfulOps uint64 `json:"successful_ops"`
}

type rgwRecordsCategory struct {
	Category string `json:"category"`
	rgwRecordsCounters
}

type rgwRecordsUsage struct {
	Entries []struct {
		User    string `json:"user"`
		Buckets []struct {
			Bucket     string               `json:"bucket"`
			Owner      string               `json:"owner"`
			Epoch      uint64               `json:"epoch"`
			Categories []rgwRecordsCategory `json:"categories"`
		} `json:"buckets"`
	} `json:"entries"`
	Summary []struct {
		User       string               `json:"user"`
		Categories []rgwRecordsCategory `json:"categories"`
		Total      rgwRecordsCounters   `json:"total"`
	} `json:"summary"`
}

func rgwRecordsDecodeUsage(t *testing.T, data []byte, uid string) rgwRecordsUsage {
	t.Helper()
	var usage rgwRecordsUsage
	if json.Unmarshal(data, &usage) != nil || !bytes.Contains(data, []byte(`"entries"`)) || !bytes.Contains(data, []byte(`"summary"`)) {
		t.Fatal("decode native usage entries and summary")
	}
	for _, entry := range usage.Entries {
		if entry.User != uid {
			t.Fatal("native scoped usage included another principal")
		}
		for _, bucket := range entry.Buckets {
			if bucket.Owner != "" && bucket.Owner != uid {
				t.Fatal("native scoped usage bucket owner changed")
			}
		}
	}
	for _, summary := range usage.Summary {
		if summary.User != uid {
			t.Fatal("native scoped usage summary included another principal")
		}
	}
	return usage
}

func (usage rgwRecordsUsage) hasPayload(size uint64) bool {
	if len(usage.Entries) == 0 || len(usage.Summary) != 1 {
		return false
	}
	put, get := false, false
	for _, category := range usage.Summary[0].Categories {
		if category.Category == "put_obj" && category.SuccessfulOps > 0 && category.BytesReceived >= size {
			put = true
		}
		if category.Category == "get_obj" && category.SuccessfulOps > 0 && category.BytesSent >= size {
			get = true
		}
	}
	return put && get
}

func (usage rgwRecordsUsage) metrics() map[string]rgwRecordsCounters {
	values := map[string]rgwRecordsCounters{}
	for _, entry := range usage.Entries {
		for _, bucket := range entry.Buckets {
			for _, category := range bucket.Categories {
				key := entry.User + "/" + bucket.Bucket + "/" + strconv.FormatUint(bucket.Epoch, 10) + "/" + category.Category
				values[key] = category.rgwRecordsCounters
			}
		}
	}
	for _, summary := range usage.Summary {
		values["summary/"+summary.User+"/total"] = summary.Total
		for _, category := range summary.Categories {
			values["summary/"+summary.User+"/"+category.Category] = category.rgwRecordsCounters
		}
	}
	return values
}
