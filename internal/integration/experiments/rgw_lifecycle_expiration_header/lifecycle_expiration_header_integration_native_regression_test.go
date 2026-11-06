//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_expiration_header)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_lifecycle_expiration_header_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

type expCase struct {
	ctx    context.Context
	client harness.S3HTTPClient
}

func (c expCase) signed(t *testing.T, ctx context.Context, method, path string, payload []byte, headers http.Header) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, ctx, c.client, "s3", method, path, payload, headers)
}

// expVersion is one version written by the test.
type expVersion struct {
	ID    string
	Data  []byte
	MTime time.Time
}

// putVersion writes data to path on a versioned bucket and returns its
// version ID and Last-Modified time read back with a HEAD of that version.
func (c expCase) putVersion(t *testing.T, path string, data []byte) expVersion {
	t.Helper()
	r := c.signed(t, c.ctx, http.MethodPut, path, data, nil)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	id := r.Headers.Get("X-Amz-Version-Id")
	if id == "" || id == "null" {
		t.Fatalf("condition not established: %s written on a versioned bucket has version %q", path, id)
	}
	head := c.signed(t, c.ctx, http.MethodHead, path+"?versionId="+url.QueryEscape(id), nil, nil)
	harness.RGWBackendRequire(t, head, http.StatusOK)
	mtime, err := http.ParseTime(head.Headers.Get("Last-Modified"))
	if err != nil {
		t.Fatalf("Last-Modified of %s version %s: %v", path, id, err)
	}
	t.Logf("RGW_EXP version path=%s id=%s mtime=%s bytes=%d", path, id, mtime.UTC().Format(time.RFC3339), len(data))
	return expVersion{ID: id, Data: data, MTime: mtime}
}

func (c expCase) versioning(t *testing.T, bucket string) {
	t.Helper()
	body := []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
	harness.RGWBackendRequire(t, c.signed(t, c.ctx, http.MethodPut, bucket+"?versioning", body, nil), http.StatusOK)
	r := c.signed(t, c.ctx, http.MethodGet, bucket+"?versioning", nil, nil)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	var state struct {
		Status string `xml:"Status"`
	}
	if err := xml.Unmarshal(r.Body, &state); err != nil || state.Status != "Enabled" {
		t.Fatalf("condition not established: bucket %s versioning %q (%v)", bucket, state.Status, err)
	}
}

// expRule is one lifecycle rule of the case; a zero day count leaves the
// action out.
type expRule struct {
	ID             string
	Prefix         string
	ExpirationDays int
	NoncurrentDays int
}

func (r expRule) xml() string {
	var b strings.Builder
	b.WriteString("<Rule><ID>" + r.ID + "</ID><Filter><Prefix>" + r.Prefix + "</Prefix></Filter><Status>Enabled</Status>")
	if r.ExpirationDays > 0 {
		fmt.Fprintf(&b, "<Expiration><Days>%d</Days></Expiration>", r.ExpirationDays)
	}
	if r.NoncurrentDays > 0 {
		fmt.Fprintf(&b, "<NoncurrentVersionExpiration><NoncurrentDays>%d</NoncurrentDays></NoncurrentVersionExpiration>", r.NoncurrentDays)
	}
	b.WriteString("</Rule>")
	return b.String()
}

// lifecycle installs rules on bucket and requires that the read-back
// configuration holds exactly these rules and actions.
func (c expCase) lifecycle(t *testing.T, bucket string, rules ...expRule) {
	t.Helper()
	var body strings.Builder
	body.WriteString(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	for _, r := range rules {
		body.WriteString(r.xml())
	}
	body.WriteString("</LifecycleConfiguration>")
	digest := md5.Sum([]byte(body.String()))
	headers := http.Header{"Content-Md5": {base64.StdEncoding.EncodeToString(digest[:])}}
	harness.RGWBackendRequire(t, c.signed(t, c.ctx, http.MethodPut, bucket+"?lifecycle", []byte(body.String()), headers), http.StatusOK)

	r := c.signed(t, c.ctx, http.MethodGet, bucket+"?lifecycle", nil, nil)
	harness.RGWBackendRequire(t, r, http.StatusOK)
	var readBack struct {
		Rules []struct {
			ID         string `xml:"ID"`
			Prefix     string `xml:"Filter>Prefix"`
			Status     string `xml:"Status"`
			Expiration *struct {
				Days int `xml:"Days"`
			} `xml:"Expiration"`
			Noncurrent *struct {
				Days int `xml:"NoncurrentDays"`
			} `xml:"NoncurrentVersionExpiration"`
		} `xml:"Rule"`
	}
	if err := xml.Unmarshal(r.Body, &readBack); err != nil || len(readBack.Rules) != len(rules) {
		t.Fatalf("condition not established: lifecycle of %s read back %d rules (%v)", bucket, len(readBack.Rules), err)
	}
	for i, want := range rules {
		got := readBack.Rules[i]
		expiration, noncurrent := 0, 0
		if got.Expiration != nil {
			expiration = got.Expiration.Days
		}
		if got.Noncurrent != nil {
			noncurrent = got.Noncurrent.Days
		}
		t.Logf("RGW_EXP lifecycle bucket=%s rule=%s prefix=%q status=%s expiration-days=%d noncurrent-days=%d", bucket, got.ID, got.Prefix, got.Status, expiration, noncurrent)
		if got.ID != want.ID || got.Prefix != want.Prefix || got.Status != "Enabled" || expiration != want.ExpirationDays || noncurrent != want.NoncurrentDays {
			t.Fatalf("condition not established: lifecycle rule %d of %s reads back %+v, want %+v", i, bucket, got, want)
		}
	}
}

// expRequest is one GET or HEAD of the matrix.
type expRequest struct {
	Name      string
	Path      string
	Version   expVersion
	Header    string
	Present   bool
	VersionID string
}

// query reads path, optionally at an explicit version, with GET and HEAD.
// It requires the version's exact bytes from GET, the same version ID and
// expiration header from both, and returns the GET's header.
func (c expCase) query(t *testing.T, name, path string, explicit bool, v expVersion) expRequest {
	t.Helper()
	target := path
	if explicit {
		target += "?versionId=" + url.QueryEscape(v.ID)
	}
	get := c.signed(t, c.ctx, http.MethodGet, target, nil, nil)
	head := c.signed(t, c.ctx, http.MethodHead, target, nil, nil)
	harness.RGWBackendRequire(t, get, http.StatusOK)
	harness.RGWBackendRequire(t, head, http.StatusOK)
	values, present := get.Headers["X-Amz-Expiration"]
	r := expRequest{Name: name, Path: target, Version: v, Header: strings.Join(values, ","), Present: present, VersionID: get.Headers.Get("X-Amz-Version-Id")}
	headValues, headPresent := head.Headers["X-Amz-Expiration"]
	t.Logf("RGW_EXP request=%s path=%s version=%s get-expiration=%q head-expiration=%q", name, target, r.VersionID, r.Header, strings.Join(headValues, ","))
	if !bytes.Equal(get.Body, v.Data) || r.VersionID != v.ID || head.Headers.Get("X-Amz-Version-Id") != v.ID {
		t.Fatalf("condition not established: %s read version %q (%d bytes), want version %q with its exact bytes", target, r.VersionID, len(get.Body), v.ID)
	}
	if headPresent != present || !slices.Equal(headValues, values) {
		t.Errorf("%s: HEAD expiration %q differs from GET %q", target, strings.Join(headValues, ","), r.Header)
	}
	return r
}

// requireNoHeader requires that r carries no x-amz-expiration.
func requireNoHeader(t *testing.T, r expRequest, why string) {
	t.Helper()
	if r.Present {
		t.Errorf("%s (%s): x-amz-expiration %q, want none: %s", r.Name, r.Path, r.Header, why)
	}
}

// requireRule requires that r reports rule with the expiry date that its
// days give for the version's modification time.
func requireRule(t *testing.T, r expRequest, rule expRule) {
	t.Helper()
	if !r.Present {
		t.Errorf("%s (%s): no x-amz-expiration, want rule %s", r.Name, r.Path, rule.ID)
		return
	}
	got, err := parseExpHeader(r.Header)
	if err != nil {
		t.Errorf("%s: %v", r.Name, err)
		return
	}
	want := expExpected(r.Version.MTime, rule.ExpirationDays)
	if got.RuleID != rule.ID || !got.Date.Equal(want) {
		t.Errorf("%s (%s): rule %q expiring %s, want rule %q expiring %s (Expiration %d days from %s)", r.Name, r.Path, got.RuleID,
			got.Date.Format(time.RFC1123), rule.ID, want.Format(time.RFC1123), rule.ExpirationDays, r.Version.MTime.UTC().Format(time.RFC3339))
	}
}

// cleanup deletes every version and delete marker in the test's buckets,
// then their lifecycle configuration and the buckets.
func (c expCase) cleanup(t *testing.T, buckets ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := false
	for _, bucket := range buckets {
		r := c.signed(t, ctx, http.MethodGet, bucket+"?versions", nil, nil)
		var listing struct {
			Versions []struct {
				Key       string `xml:"Key"`
				VersionID string `xml:"VersionId"`
			} `xml:"Version"`
			Markers []struct {
				Key       string `xml:"Key"`
				VersionID string `xml:"VersionId"`
			} `xml:"DeleteMarker"`
		}
		if r.Code != http.StatusOK || xml.Unmarshal(r.Body, &listing) != nil {
			t.Errorf("RGW_EXP cleanup failed: list %s: %d", bucket, r.Code)
			failed = true
			continue
		}
		for _, v := range append(listing.Versions, listing.Markers...) {
			if d := c.signed(t, ctx, http.MethodDelete, bucket+"/"+v.Key+"?versionId="+url.QueryEscape(v.VersionID), nil, nil); d.Code != http.StatusNoContent {
				t.Errorf("RGW_EXP cleanup failed: delete %s/%s version %s: %d", bucket, v.Key, v.VersionID, d.Code)
				failed = true
			}
		}
		if d := c.signed(t, ctx, http.MethodDelete, bucket+"?lifecycle", nil, nil); d.Code != http.StatusNoContent {
			t.Errorf("RGW_EXP cleanup failed: delete lifecycle of %s: %d", bucket, d.Code)
			failed = true
		}
		if d := c.signed(t, ctx, http.MethodDelete, bucket, nil, nil); d.Code != http.StatusNoContent {
			t.Errorf("RGW_EXP cleanup failed: delete bucket %s: %d %s", bucket, d.Code, harness.RGWBackendErrorCode(d.Body))
			failed = true
		}
	}
	t.Logf("RGW_EXP cleanup buckets=%d failed=%t", len(buckets), failed)
}

// TestRGWLifecycleExpirationHeader requires the S3 rule for
// x-amz-expiration on a versioned bucket: the header reports only the
// current-version Expiration action, for a request without versionId, and a
// request for an explicit version, the latest one included, gets no header.
// A NoncurrentVersionExpiration rule alone never produces it, and a key
// outside the rule's prefix gets none.
//
// Tentacle 20.2.4 derives the header from the instance that RGW resolved
// for the current version, so a plain request on a versioned bucket is
// treated like a versionId request and reports the NoncurrentVersionExpiration
// action, and explicit-version requests get that header too. Tentacle 20.2.5
// and Umbrella 21.2.0 pass.
func TestRGWLifecycleExpirationHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_EXP ceph=%s", harness.StaticFacts(cluster).CephVersion)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := cluster.Ceph(ctx, "versions")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_EXP daemons=%s", strings.Join(strings.Fields(string(versions)), " "))
	t.Logf("RGW_EXP gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := expCase{ctx: ctx, client: harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}}

	const noncurrentOnly, both = "/tc-exp-noncurrent-only", "/tc-exp-current-and-noncurrent"
	for _, bucket := range []string{noncurrentOnly, both} {
		harness.RGWBackendRequire(t, c.signed(t, ctx, http.MethodPut, bucket, nil, nil), http.StatusOK)
	}
	t.Cleanup(func() { c.cleanup(t, noncurrentOnly, both) })

	// writeKey writes two versions of bucket/docs/key and returns the older
	// and the current one.
	writeKey := func(t *testing.T, bucket string) (string, expVersion, expVersion) {
		t.Helper()
		c.versioning(t, bucket)
		path := bucket + "/docs/key"
		older := c.putVersion(t, path, []byte(path+" older"))
		current := c.putVersion(t, path, []byte(path+" current"))
		if older.ID == current.ID {
			t.Fatalf("condition not established: both writes of %s have version %q", path, older.ID)
		}
		return path, older, current
	}

	t.Run("noncurrent-only", func(t *testing.T) {
		path, older, current := writeKey(t, noncurrentOnly)
		c.lifecycle(t, noncurrentOnly, expRule{ID: "tc-noncurrent-only", Prefix: "docs/", NoncurrentDays: 30})
		const why = "a NoncurrentVersionExpiration rule is not a current-version expiration"
		requireNoHeader(t, c.query(t, "plain", path, false, current), why)
		requireNoHeader(t, c.query(t, "explicit-current", path, true, current), "explicit version request")
		requireNoHeader(t, c.query(t, "explicit-older", path, true, older), "explicit version request")
	})

	t.Run("current-and-noncurrent", func(t *testing.T) {
		path, older, current := writeKey(t, both)
		outside := c.putVersion(t, both+"/other/key", []byte("outside the rule prefix"))
		rule := expRule{ID: "tc-current-and-noncurrent", Prefix: "docs/", ExpirationDays: 7, NoncurrentDays: 90}
		c.lifecycle(t, both, rule)
		requireRule(t, c.query(t, "plain", path, false, current), rule)
		requireNoHeader(t, c.query(t, "explicit-current", path, true, current), "explicit version request, although it is the latest")
		requireNoHeader(t, c.query(t, "explicit-older", path, true, older), "explicit version request")
		requireNoHeader(t, c.query(t, "outside-prefix", both+"/other/key", false, outside), "the key is outside the rule's prefix")
	})
}
