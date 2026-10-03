//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func rgwS3LifecycleAndObjectLock(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, owner s3HTTPClient, user *ceph.RGWUser) {
	t.Helper()
	req := func(method, path string, payload []byte, headers http.Header, want int) ([]byte, http.Header) {
		t.Helper()
		return s3FeatureRequest(t, ctx, owner, method, path, payload, headers, want)
	}
	const lifecycleBucket = "/tc-s3-lifecycle"
	req(http.MethodPut, lifecycleBucket, nil, nil, http.StatusOK)
	config := []byte(`<LifecycleConfiguration><Rule><ID>owned-prefix-expiration</ID><Filter><Prefix>expiring/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	req(http.MethodPut, lifecycleBucket+"?lifecycle", config, nil, http.StatusOK)
	actual, _ := req(http.MethodGet, lifecycleBucket+"?lifecycle", nil, nil, http.StatusOK)
	if !bytes.Contains(actual, []byte("owned-prefix-expiration")) || !bytes.Contains(actual, []byte("expiring/")) {
		t.Fatalf("native lifecycle policy: %s", actual)
	}
	payload := []byte("native lifecycle prefix policy preserves outside bytes")
	for _, key := range []string{"expiring/object", "outside/object"} {
		req(http.MethodPut, lifecycleBucket+"/"+key, payload, nil, http.StatusOK)
		actual, _ := req(http.MethodGet, lifecycleBucket+"/"+key, nil, nil, http.StatusOK)
		if !bytes.Equal(actual, payload) {
			t.Fatal("lifecycle initial bytes differ")
		}
	}
	stats, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", strings.TrimPrefix(lifecycleBucket, "/"))
	var native rgwMaintenanceBucket
	if err != nil || json.Unmarshal(stats, &native) != nil || native.ID == "" || native.Owner != user.ID() {
		t.Fatalf("native lifecycle bucket identity: %v", err)
	}
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(3 * time.Second):
	}
	if _, err := gateway.Admin(ctx, "lc", "process", "--bucket", strings.TrimPrefix(lifecycleBucket, "/")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, data, _ := rgwMaintenanceRequest(t, ctx, owner, http.MethodGet, lifecycleBucket+"/expiring/object", nil)
		if code == http.StatusNotFound {
			break
		}
		if code != http.StatusOK || !bytes.Equal(data, payload) {
			t.Fatalf("lifecycle unexpected response %d: %s", code, data)
		}
		if time.Now().After(deadline) {
			t.Fatal("native lifecycle never expired selected prefix")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	actual, _ = req(http.MethodGet, lifecycleBucket+"/outside/object", nil, nil, http.StatusOK)
	if !bytes.Equal(actual, payload) {
		t.Fatal("lifecycle expired unselected prefix")
	}
	req(http.MethodDelete, lifecycleBucket+"?lifecycle", nil, nil, http.StatusNoContent)
	req(http.MethodDelete, lifecycleBucket+"/outside/object", nil, nil, http.StatusNoContent)
	req(http.MethodDelete, lifecycleBucket, nil, nil, http.StatusNoContent)

	const locked = "/tc-s3-object-lock"
	req(http.MethodPut, locked, nil, http.Header{"X-Amz-Bucket-Object-Lock-Enabled": []string{"true"}}, http.StatusOK)
	req(http.MethodPut, locked+"?object-lock", []byte(`<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`), nil, http.StatusOK)
	actual, _ = req(http.MethodGet, locked+"?object-lock", nil, nil, http.StatusOK)
	if !bytes.Contains(actual, []byte("Enabled")) {
		t.Fatal("native bucket object lock absent")
	}
	retainedUntil := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	_, headers := req(http.MethodPut, locked+"/retained", payload, http.Header{
		"X-Amz-Object-Lock-Mode":              []string{"GOVERNANCE"},
		"X-Amz-Object-Lock-Retain-Until-Date": []string{retainedUntil.Format(time.RFC3339)},
	}, http.StatusOK)
	version := headers.Get("X-Amz-Version-Id")
	if version == "" {
		t.Fatal("locked object version ID missing")
	}
	object := locked + "/retained?versionId=" + url.QueryEscape(version)
	actual, _ = req(http.MethodGet, object+"&retention", nil, nil, http.StatusOK)
	var retention struct {
		Mode  string `xml:"Mode"`
		Until string `xml:"RetainUntilDate"`
	}
	if xml.Unmarshal(actual, &retention) != nil || retention.Mode != "GOVERNANCE" {
		t.Fatalf("native retention: %s", actual)
	}
	until, err := time.Parse(time.RFC3339Nano, retention.Until)
	if err != nil || !until.Equal(retainedUntil) {
		t.Fatal("native retention deadline differs")
	}
	req(http.MethodDelete, object, nil, nil, http.StatusForbidden)
	req(http.MethodPut, object+"&legal-hold", []byte(`<LegalHold><Status>ON</Status></LegalHold>`), nil, http.StatusOK)
	actual, _ = req(http.MethodGet, object+"&legal-hold", nil, nil, http.StatusOK)
	if !bytes.Contains(actual, []byte("<Status>ON</Status>")) {
		t.Fatal("native legal hold absent")
	}
	bypass := http.Header{"X-Amz-Bypass-Governance-Retention": []string{"true"}}
	req(http.MethodDelete, object, nil, bypass, http.StatusForbidden)
	actual, _ = req(http.MethodGet, object, nil, nil, http.StatusOK)
	if !bytes.Equal(actual, payload) {
		t.Fatal("denied version removal changed bytes")
	}
	req(http.MethodPut, object+"&legal-hold", []byte(`<LegalHold><Status>OFF</Status></LegalHold>`), nil, http.StatusOK)
	req(http.MethodDelete, object, nil, bypass, http.StatusNoContent)
	req(http.MethodGet, object, nil, nil, http.StatusNotFound)
	req(http.MethodDelete, locked, nil, nil, http.StatusNoContent)
	t.Log("native lifecycle used explicit two-second debug clock and scoped CLI processing: selected prefix expired, outside bytes retained; GOVERNANCE retention and legal hold denied version removal, explicit hold release plus authorized bypass removed owned data")
}
