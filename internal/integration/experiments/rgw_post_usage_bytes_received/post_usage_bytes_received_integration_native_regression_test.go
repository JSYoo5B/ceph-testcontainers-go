//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rgw_post_usage_bytes_received)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_post_usage_bytes_received_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_post_usage_bytes_received_test.image=<image>"
//
// and a fixed one (Tentacle 20.2.5, Umbrella 21.2.0) does not reproduce it.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all"

const (
	// Each upload has its own user and bucket, so the usage log keeps the
	// two apart by more than the name of the operation.
	putUser    = "tc-usage-put"
	postUser   = "tc-usage-post"
	putBucket  = "tc-usage-put"
	postBucket = "tc-usage-post"
	uploadKey  = "payload"
	// The payload of both uploads. It is larger than any form overhead, so
	// a count of the form without the file cannot pass for the payload.
	uploadSize = 1 << 20
	// How long a presigned request stays valid, in seconds.
	presignValidity = 600
)

// uploader is one user of the case with its bucket: the S3 client of the
// user's key, which signs the requests that are not under test.
type uploader struct {
	user, bucket string
	s3           harness.S3HTTPClient
}

// scope returns the credential scope of a request signed on day.
func (u uploader) scope(day string) string {
	return day + "/" + u.s3.Region + "/s3/aws4_request"
}

// sign returns the SigV4 signature of text on day.
func (u uploader) sign(day, text string) string {
	key := harness.S3HMAC([]byte("AWS4"+u.s3.SecretKey), day)
	for _, part := range []string{u.s3.Region, "s3", "aws4_request"} {
		key = harness.S3HMAC(key, part)
	}
	return hex.EncodeToString(harness.S3HMAC(key, text))
}

// presignedPut returns the URL of a PUT of the upload key that carries its
// authentication in the query, for an unsigned payload.
func (u uploader) presignedPut(t *testing.T, now time.Time) string {
	t.Helper()
	endpoint, err := url.Parse(u.s3.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	stamp, day, path := now.Format("20060102T150405Z"), now.Format("20060102"), "/"+u.bucket+"/"+uploadKey
	query := strings.ReplaceAll(url.Values{
		"X-Amz-Algorithm":     {"AWS4-HMAC-SHA256"},
		"X-Amz-Credential":    {u.s3.AccessKey + "/" + u.scope(day)},
		"X-Amz-Date":          {stamp},
		"X-Amz-Expires":       {strconv.Itoa(presignValidity)},
		"X-Amz-SignedHeaders": {"host"},
	}.Encode(), "+", "%20")
	request := strings.Join([]string{http.MethodPut, harness.S3CanonicalURI(path), query, "host:" + endpoint.Host + "\n", "host", "UNSIGNED-PAYLOAD"}, "\n")
	text := "AWS4-HMAC-SHA256\n" + stamp + "\n" + u.scope(day) + "\n" + harness.S3Hash([]byte(request))
	return u.s3.Endpoint + path + "?" + query + "&X-Amz-Signature=" + u.sign(day, text)
}

// postForm returns the body and the content type of a browser form upload
// of payload to the upload key: the fields of a signed policy, then the
// file.
func (u uploader) postForm(t *testing.T, now time.Time, payload []byte) ([]byte, string) {
	t.Helper()
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	signed := []formField{
		{"x-amz-algorithm", "AWS4-HMAC-SHA256"},
		{"x-amz-credential", u.s3.AccessKey + "/" + u.scope(day)},
		{"x-amz-date", stamp},
	}
	document, err := postPolicy(now.Add(presignValidity*time.Second).Format("2006-01-02T15:04:05Z"), u.bucket, uploadKey, len(payload), signed)
	if err != nil {
		t.Fatal(err)
	}
	policy := base64.StdEncoding.EncodeToString(document)
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	fields := append([]formField{{"key", uploadKey}}, signed...)
	fields = append(fields, formField{"policy", policy}, formField{"x-amz-signature", u.sign(day, policy)})
	for _, field := range fields {
		if err := form.WriteField(field.Name, field.Value); err != nil {
			t.Fatal(err)
		}
	}
	file, err := form.CreateFormFile("file", uploadKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes(), form.FormDataContentType()
}

// TestRGWPostUploadUsageBytesReceived reports whether the usage log counts
// no received bytes for a browser form upload (POST with a signed policy).
//
// Given one gateway with usage logging enabled, two users with one bucket
// each, a usage log that holds the creation of both buckets and no upload,
// and the same 1 MiB payload stored through a presigned PUT in one bucket
// and through a presigned POST in the other, each answered with success and
// read back byte for byte, when the usage log holds one successful put_obj
// for the first bucket and one successful post_obj for the second, then the
// defect is REPRODUCED if the POST's received bytes are zero, and
// NOT_REPRODUCED if they are at least the payload and at most the body of
// the POST request. The PUT is the control: its received bytes must be
// exactly the payload. Tentacle 20.2.4 does not count what it reads of a
// form body, so it reproduces, and so does Squid 19.2.5; Tentacle 20.2.5
// and Umbrella 21.2.0 do not.
//
// A failure of the given or when steps or of the control fails the test,
// and so does a count that is neither; the result does not.
func TestRGWPostUploadUsageBytesReceived(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("RGW_POST_USAGE image=%s", image)
	cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, image, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	// Usage logging is off by default and is read when the gateway starts.
	// The log is flushed every second and after every entry.
	for _, setting := range []ceph.ConfigSetting{
		{Section: "client.admin", Name: "rgw_enable_usage_log", Value: "true"},
		{Section: "client.admin", Name: "rgw_usage_log_tick_interval", Value: "1"},
		{Section: "client.admin", Name: "rgw_usage_log_flush_threshold", Value: "1"},
	} {
		if _, err := override.SetConfig(ctx, cluster, setting); err != nil {
			t.Fatalf("set %s: %v", setting.Name, err)
		}
	}
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_POST_USAGE gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Given: two users with one bucket each. The client opens a connection
	// per request, so no request is sent twice over a connection that the
	// gateway closed.
	client := &http.Client{Timeout: 2 * time.Minute, Transport: &http.Transport{DisableKeepAlives: true}}
	uploaders := make([]uploader, 2)
	for i, name := range []struct{ user, bucket string }{{putUser, putBucket}, {postUser, postBucket}} {
		user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: name.user, DisplayName: "usage of uploads"})
		if err != nil {
			t.Fatal(err)
		}
		access, secret, err := user.CreationKey().Credentials()
		if err != nil {
			t.Fatal(err)
		}
		uploaders[i] = uploader{user: name.user, bucket: name.bucket, s3: harness.S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: gateway.Region, HTTP: client}}
		harness.RGWBackendRequire(t, harness.RGWBackendSigned(t, ctx, uploaders[i].s3, "s3", http.MethodPut, "/"+name.bucket, nil, nil), http.StatusOK)
	}
	put, post := uploaders[0], uploaders[1]
	// usage reads the usage log of an uploader's bucket. Every read of a
	// bucket is compared with the one before it.
	last := map[string]bucketUsage{}
	usage := func(ctx context.Context, u uploader) (bucketUsage, error) {
		report, err := gateway.Admin(ctx, "usage", "show", "--uid", u.user, "--show-log-entries=true", "--show-log-sum=false")
		if err != nil {
			return nil, err
		}
		got, err := parseUsage(report, u.user, u.bucket)
		if err != nil {
			return nil, err
		}
		if err := usageGrew(last[u.bucket], got); err != nil {
			return nil, err
		}
		last[u.bucket] = got
		return got, nil
	}
	// The usage log holds the creation of both buckets: usage logging is
	// enabled, and its flush reaches the log before any upload is sent.
	for _, u := range uploaders {
		seen, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probe context.Context) (bucketUsage, bool, error) {
			got, err := usage(probe, u)
			_, created := got[categoryCreateBucket]
			return got, created, err
		})
		t.Logf("RGW_POST_USAGE usage step=created bucket=%s reads=%d after=%s usage=%+v", u.bucket, seen.Attempts, seen.Elapsed.Round(time.Millisecond), seen.Value)
		if err != nil {
			t.Fatalf("condition not established: the usage log does not hold the creation of %s, so usage logging is not enabled or not flushed: %v", u.bucket, err)
		}
		if err := requireLogging(seen.Value); err != nil {
			t.Fatalf("bucket %s: %v", u.bucket, err)
		}
	}

	// When: the same payload is uploaded through a presigned PUT and through
	// a presigned POST, neither with an Authorization header.
	payload := make([]byte, uploadSize)
	if _, err := rand.NewChaCha8([32]byte{1}).Read(payload); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, put.presignedPut(t, now), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	harness.RGWBackendRequire(t, harness.RGWBackendDo(t, client, request), http.StatusOK)
	form, contentType := post.postForm(t, now, payload)
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/"+post.bucket, bytes.NewReader(form))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	harness.RGWBackendRequire(t, harness.RGWBackendDo(t, client, request), http.StatusNoContent)
	t.Logf("RGW_POST_USAGE uploads payload=%d post-body=%d", len(payload), len(form))
	for _, u := range uploaders {
		stored := harness.RGWBackendSigned(t, ctx, u.s3, "s3", http.MethodGet, "/"+u.bucket+"/"+uploadKey, nil, nil)
		if stored.Code != http.StatusOK || !bytes.Equal(stored.Body, payload) {
			t.Fatalf("condition not established: %s answered %d with %d bytes, want 200 and the %d bytes of the payload", stored.Operation, stored.Code, len(stored.Body), len(payload))
		}
	}

	// Then: the usage log is read until it holds both uploads.
	counters := map[string]*usageCounters{}
	for _, upload := range []struct {
		uploader
		category string
	}{{put, categoryPut}, {post, categoryPost}} {
		seen, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probe context.Context) (*usageCounters, bool, error) {
			got, err := usage(probe, upload.uploader)
			if err != nil {
				return nil, false, err
			}
			found, err := uploaded(got, upload.category)
			return found, found != nil, err
		})
		t.Logf("RGW_POST_USAGE usage step=uploaded bucket=%s category=%s reads=%d after=%s usage=%+v", upload.bucket, upload.category, seen.Attempts, seen.Elapsed.Round(time.Millisecond), last[upload.bucket])
		if err != nil {
			t.Fatalf("usage not observed: the usage log of %s does not hold a %s: %v", upload.bucket, upload.category, err)
		}
		counters[upload.category] = seen.Value
	}
	symptoms, err := judgeUploads(counters[categoryPut], counters[categoryPost], uint64(len(payload)), uint64(len(form)))
	if err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}
	verdict := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		verdict = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_post_usage_bytes_received result=%s image=%s symptoms=%q", verdict, image, symptoms)
}
