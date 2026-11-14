//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_conditional_writes)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_suspended_conditional_writes_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_suspended_conditional_writes_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.5, Umbrella 21.2.0) does not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"

// condResult is the native answer to one conditional write.
type condResult struct {
	Code    int
	ErrCode string
}

// condWriter writes data to path with one optional precondition header.
type condWriter func(t *testing.T, path string, data []byte, header, value string) condResult

// condObject is what a fresh signed GET of a key returns; Data is nil when
// the key has no current version (404 NoSuchKey).
type condObject struct {
	Data      []byte
	ETag      string
	VersionID string
}

type condCase struct {
	ctx    context.Context
	client harness.S3HTTPClient
}

func (c condCase) signed(t *testing.T, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, c.ctx, c.client, "s3", method, path, payload, headers)
}

func (c condCase) get(t *testing.T, path string) condObject {
	t.Helper()
	r := c.signed(t, http.MethodGet, path, nil, nil)
	switch {
	case r.Code == http.StatusOK:
		etag := r.Headers.Get("ETag")
		if !condValidator(etag) {
			// Every If-Match step uses the ETag of the last GET, so a
			// missing one would credit an unconditional write.
			t.Fatalf("condition not established: fresh GET %s returned ETag %q, not a quoted strong validator", path, etag)
		}
		return condObject{Data: r.Body, ETag: etag, VersionID: r.Headers.Get("X-Amz-Version-Id")}
	case r.Code == http.StatusNotFound && harness.RGWBackendErrorCode(r.Body) == "NoSuchKey":
		return condObject{}
	default:
		t.Fatalf("fresh GET %s: status=%d code=%s", path, r.Code, harness.RGWBackendErrorCode(r.Body))
		return condObject{}
	}
}

func (c condCase) versioning(t *testing.T, bucket, status string) {
	t.Helper()
	body := []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>` + status + `</Status></VersioningConfiguration>`)
	harness.RGWBackendRequire(t, c.signed(t, http.MethodPut, bucket+"?versioning", body, nil), http.StatusOK)
	r := c.signed(t, http.MethodGet, bucket+"?versioning", nil, nil)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	var state struct {
		Status string `xml:"Status"`
	}
	if err := xml.Unmarshal(r.Body, &state); err != nil || state.Status != status {
		t.Fatalf("condition not established: bucket %s versioning %q, want %q (%v)", bucket, state.Status, status, err)
	}
}

type condVersion struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId"`
	ETag      string `xml:"ETag"`
	IsLatest  bool   `xml:"IsLatest"`
}

func (c condCase) versions(t *testing.T, bucket, key string) (versions, markers []condVersion) {
	t.Helper()
	r := c.signed(t, http.MethodGet, bucket+"?versions&prefix="+url.QueryEscape(key), nil, nil)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	var listing struct {
		IsTruncated bool          `xml:"IsTruncated"`
		Versions    []condVersion `xml:"Version"`
		Markers     []condVersion `xml:"DeleteMarker"`
	}
	if err := xml.Unmarshal(r.Body, &listing); err != nil || listing.IsTruncated {
		t.Fatalf("version listing of %s/%s: truncated=%t %v", bucket, key, listing.IsTruncated, err)
	}
	t.Logf("RGW_COND versions bucket=%s key=%s versions=%+v markers=%+v", bucket, key, listing.Versions, listing.Markers)
	return listing.Versions, listing.Markers
}

func (c condCase) put(t *testing.T, path string, data []byte, header, value string) condResult {
	t.Helper()
	h := http.Header{}
	if header != "" {
		h.Set(header, value)
	}
	r := c.signed(t, http.MethodPut, path, data, h)
	return condResult{Code: r.Code, ErrCode: harness.RGWBackendErrorCode(r.Body)}
}

// multipart uploads data as the single part of a new upload and sends the
// precondition with CompleteMultipartUpload. An upload whose completion is
// refused is aborted, so no part outlives its step.
func (c condCase) multipart(t *testing.T, path string, data []byte, header, value string) condResult {
	t.Helper()
	started := c.signed(t, http.MethodPost, path+"?uploads", nil, nil)
	harness.RGWBackendRequire(t, started, http.StatusOK)
	var upload struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(started.Body, &upload); err != nil || upload.ID == "" {
		t.Fatalf("multipart upload ID for %s: %v", path, err)
	}
	id := url.QueryEscape(upload.ID)
	part := c.signed(t, http.MethodPut, path+"?partNumber=1&uploadId="+id, data, nil)
	harness.RGWBackendRequire(t, part, http.StatusOK)
	completion := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", part.Headers.Get("ETag"))
	h := http.Header{}
	if header != "" {
		h.Set(header, value)
	}
	done := c.signed(t, http.MethodPost, path+"?uploadId="+id, []byte(completion), h)
	result := condResult{Code: done.Code, ErrCode: harness.RGWBackendErrorCode(done.Body)}
	if done.Code == http.StatusOK && bytes.Contains(done.Body, []byte("<Error>")) {
		// A completion can answer 200 and report its failure in the body.
		result.Code = 0
	}
	if result.Code != http.StatusOK {
		harness.RGWBackendRequire(t, c.signed(t, http.MethodDelete, path+"?uploadId="+id, nil, nil), http.StatusNoContent)
	}
	return result
}

// requireNoPendingUploads fails when an upload the test started is still
// pending in bucket.
func (c condCase) requireNoPendingUploads(t *testing.T, bucket string) {
	t.Helper()
	r := c.signed(t, http.MethodGet, bucket+"?uploads", nil, nil)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	var pending struct {
		Uploads []struct {
			Key string `xml:"Key"`
			ID  string `xml:"UploadId"`
		} `xml:"Upload"`
	}
	if err := xml.Unmarshal(r.Body, &pending); err != nil || len(pending.Uploads) != 0 {
		t.Errorf("pending uploads in %s: %+v (%v)", bucket, pending.Uploads, err)
	}
}

// condSequence runs the AWS conditional-write sequence on path, whose
// current object is initial with ETag initialTag. Every write is followed by
// a fresh signed GET: an accepted write must read back its exact bytes and a
// refused one must leave the key unchanged; either violation is a different
// defect and stops the sequence. A status other than the AWS one is passed
// to mismatch and the sequence continues from the state the GET shows.
func condSequence(t *testing.T, c condCase, write condWriter, path string, initial []byte, suspended bool, mismatch func(string)) condObject {
	t.Helper()
	current := c.get(t, path)
	if !bytes.Equal(current.Data, initial) {
		t.Fatalf("condition not established: %s does not hold its initial bytes", path)
	}
	step := func(name string, data []byte, header, value string, wantCode int, wantErr string) bool {
		t.Helper()
		r := write(t, path, data, header, value)
		after := c.get(t, path)
		t.Logf("RGW_COND step=%s %s=%q status=%d code=%s etag=%s version=%q present=%t", name, header, value, r.Code, r.ErrCode, after.ETag, after.VersionID, after.Data != nil)
		if r.Code == http.StatusOK {
			if !bytes.Equal(after.Data, data) {
				t.Fatalf("%s: the accepted write does not read back its exact bytes", name)
			}
		} else if !bytes.Equal(after.Data, current.Data) || after.ETag != current.ETag {
			t.Fatalf("%s: the refused write (status %d %s) changed the key", name, r.Code, r.ErrCode)
		}
		current = after
		if r.Code != wantCode || r.ErrCode != wantErr {
			mismatch(fmt.Sprintf("%s %s: status %d %q, AWS semantics want %d %q", path, name, r.Code, r.ErrCode, wantCode, wantErr))
			return false
		}
		return true
	}
	const precondition = "PreconditionFailed"
	step("existing-if-none-match", []byte("refused duplicate"), "If-None-Match", "*", http.StatusPreconditionFailed, precondition)
	step("existing-if-match", []byte(path+" v2"), "If-Match", current.ETag, http.StatusOK, "")
	step("wrong-if-match", []byte("refused wrong etag"), "If-Match", condWrongETag, http.StatusPreconditionFailed, precondition)

	lastTag := current.ETag
	deleted := c.signed(t, http.MethodDelete, path, nil, nil)
	harness.RGWBackendRequire(t, deleted, http.StatusNoContent)
	marker := deleted.Headers.Get("X-Amz-Delete-Marker")
	t.Logf("RGW_COND step=delete status=%d delete_marker=%q version=%q", deleted.Code, marker, deleted.Headers.Get("X-Amz-Version-Id"))
	if suspended && (marker != "true" || deleted.Headers.Get("X-Amz-Version-Id") != "null") {
		t.Fatalf("condition not established: DELETE on the suspended bucket did not write a null delete marker")
	}
	if current = c.get(t, path); current.Data != nil {
		t.Fatal("the deleted key still has a current version")
	}

	step("deleted-if-match", []byte("refused missing key"), "If-Match", lastTag, http.StatusNotFound, "NoSuchKey")
	step("deleted-if-none-match", []byte(path+" v3"), "If-None-Match", "*", http.StatusOK, "")
	if current.Data == nil {
		t.Logf("RGW_COND steps duplicate-if-none-match and subsequent-if-match not reached: the key has no current version")
		return current
	}
	step("duplicate-if-none-match", []byte("refused second create"), "If-None-Match", "*", http.StatusPreconditionFailed, precondition)
	step("subsequent-if-match", []byte(path+" v4"), "If-Match", current.ETag, http.StatusOK, "")
	return current
}

// condFreshKey is the positive control: a key first written by a
// conditional create, then refused as a duplicate. It passes on every
// tested release, so a failure here means the gateway does not evaluate the
// headers at all and the sequence's result says nothing.
func condFreshKey(t *testing.T, c condCase, write condWriter, path string) {
	t.Helper()
	created := write(t, path, []byte("fresh"), "If-None-Match", "*")
	duplicate := write(t, path, []byte("fresh duplicate"), "If-None-Match", "*")
	t.Logf("RGW_COND control=%s create=%d duplicate=%d %s", path, created.Code, duplicate.Code, duplicate.ErrCode)
	if created.Code != http.StatusOK || duplicate.Code != http.StatusPreconditionFailed || !bytes.Equal(c.get(t, path).Data, []byte("fresh")) {
		t.Fatalf("control not established: create-once on %s answered %d then %d", path, created.Code, duplicate.Code)
	}
}

// TestRGWSuspendedConditionalWrites reports whether RGW breaks AWS
// conditional-write semantics for keys written while versioning was
// enabled, after versioning is suspended: If-Match with the current ETag
// replaces the null version, and after a delete marker If-None-Match "*"
// creates the key and If-Match answers 404.
//
// Given a never-versioned bucket on which the whole sequence follows AWS
// semantics (the control), and a suspended bucket whose keys each hold one
// version written while versioning was enabled, when the sequence is sent
// with an ordinary PUT and with CompleteMultipartUpload on the suspended
// bucket, then the defect is REPRODUCED if a step answers another status
// than AWS semantics want, and NOT_REPRODUCED if every step does. Tentacle
// 20.2.4 checks the precondition against the OLH head instead of the current
// version, so existing-if-match, deleted-if-match and deleted-if-none-match
// are refused for both writers: it reproduces. Tentacle 20.2.5 and Umbrella
// 21.2.0 do not.
// Squid 19.2.5 fails the control: it compares a quoted If-Match with the
// unquoted stored ETag, and it ignores the precondition of
// CompleteMultipartUpload.
//
// A failure of the given or when steps, of the control, or of an effect
// check (a write whose result does not read back, a refused write that
// changed the key, a final version listing inconsistent with the GET) fails
// the test; the result does not.
func TestRGWSuspendedConditionalWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	t.Logf("RGW_COND image=%s", image)
	cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, image, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_COND ceph=%s", harness.StaticFacts(cluster).CephVersion)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := condCase{ctx: ctx, client: harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}}
	writers := []struct {
		name  string
		write condWriter
	}{{"put", c.put}, {"multipart", c.multipart}}

	t.Run("never-versioned", func(t *testing.T) {
		const bucket = "/tc-cond-never-versioned"
		harness.RGWBackendRequire(t, c.signed(t, http.MethodPut, bucket, nil, nil), http.StatusOK)
		for _, w := range writers {
			t.Run(w.name, func(t *testing.T) {
				path := bucket + "/" + w.name
				initial := []byte(path + " v1")
				harness.RGWBackendRequire(t, c.signed(t, http.MethodPut, path, initial, nil), http.StatusOK)
				condFreshKey(t, c, w.write, bucket+"/fresh-"+w.name)
				condSequence(t, c, w.write, path, initial, false, func(failure string) { t.Errorf("control failed: %s", failure) })
				c.requireNoPendingUploads(t, bucket)
			})
		}
	})

	// Each symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	t.Run("suspended", func(t *testing.T) {
		const bucket = "/tc-cond-suspended"
		harness.RGWBackendRequire(t, c.signed(t, http.MethodPut, bucket, nil, nil), http.StatusOK)
		c.versioning(t, bucket, "Enabled")
		firstIDs := map[string]string{}
		for _, w := range writers {
			path := bucket + "/" + w.name
			r := c.signed(t, http.MethodPut, path, []byte(path+" v1"), nil)
			harness.RGWBackendRequire(t, r, http.StatusOK)
			if firstIDs[w.name] = r.Headers.Get("X-Amz-Version-Id"); firstIDs[w.name] == "" || firstIDs[w.name] == "null" {
				t.Fatalf("condition not established: %s written while versioning was enabled has version %q", path, firstIDs[w.name])
			}
		}
		c.versioning(t, bucket, "Suspended")
		for _, w := range writers {
			t.Run(w.name, func(t *testing.T) {
				path := bucket + "/" + w.name
				versions, markers := c.versions(t, bucket, w.name)
				if len(versions) != 1 || len(markers) != 0 || versions[0].VersionID != firstIDs[w.name] || !versions[0].IsLatest {
					t.Fatalf("condition not established: %s is not a single latest version %q", path, firstIDs[w.name])
				}
				condFreshKey(t, c, w.write, bucket+"/fresh-"+w.name)
				final := condSequence(t, c, w.write, path, []byte(path+" v1"), true, func(symptom string) { symptoms = append(symptoms, symptom) })

				// The version written while versioning was enabled survives
				// every step, and the null entry is the one the GET reads.
				versions, markers = c.versions(t, bucket, w.name)
				var kept, null []condVersion
				for _, v := range versions {
					if v.VersionID == firstIDs[w.name] {
						kept = append(kept, v)
					} else {
						null = append(null, v)
					}
				}
				if len(kept) != 1 || kept[0].IsLatest {
					t.Errorf("version %q of %s: %+v, want it kept and not latest", firstIDs[w.name], path, kept)
				}
				switch {
				case final.Data != nil && (len(null) != 1 || len(markers) != 0 || null[0].VersionID != "null" || !null[0].IsLatest || null[0].ETag != final.ETag):
					t.Errorf("%s: listing %+v %+v, want one latest null version with ETag %s", path, null, markers, final.ETag)
				case final.Data == nil && (len(null) != 0 || len(markers) != 1 || markers[0].VersionID != "null" || !markers[0].IsLatest):
					t.Errorf("%s: listing %+v %+v, want one latest null delete marker", path, null, markers)
				}
				c.requireNoPendingUploads(t, bucket)
			})
		}
	})
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_suspended_conditional_writes result=%s image=%s symptoms=%q", result, image, symptoms)
}
