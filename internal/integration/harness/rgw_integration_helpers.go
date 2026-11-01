//go:build all || integration

package harness

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// RGWSyncStatsBound bounds one explicit stats synchronization of a few small
// buckets. It is its own phase inside the test's context, so a stalled
// synchronization fails with its own deadline and leaves the test's remaining
// time for the failure's evidence.
const RGWSyncStatsBound = 2 * time.Minute

// RGWSyncStats runs one native stats synchronization, user or account stats
// with --sync-stats, through gateway.Admin under RGWSyncStatsBound. It never
// repeats the command: a stopped or failed synchronization may have changed
// native counters, and its outcome is unknown. A failure names how the command
// ended, its phase deadline and elapsed time, and the gateway container's
// generation and state and the cluster's OSD map summary, each read once
// under its own short bound after the failure.
func RGWSyncStats(t *testing.T, ctx context.Context, cluster *ceph.Container, gateway *rgw.Gateway, args ...string) {
	t.Helper()
	phase, cancel := context.WithTimeout(ctx, RGWSyncStatsBound)
	defer cancel()
	deadline, _ := phase.Deadline()
	started := time.Now()
	_, err := gateway.Admin(phase, args...)
	if err == nil {
		return
	}
	elapsed := time.Since(started)
	end := RGWSyncStatsEnd(err, ctx.Err(), phase.Err())
	t.Fatalf("native %s: %s after %s (phase bound %s, deadline %s); %s; %s; error: %v", strings.Join(args, " "), end,
		elapsed.Round(time.Millisecond), RGWSyncStatsBound, deadline.UTC().Format(time.RFC3339Nano),
		rgwSyncStatsGateway(ctx, gateway), rgwSyncStatsOSDs(ctx, cluster), err)
}

// RGWSyncStatsEnd classifies how a failed synchronization ended: by the
// supervisor at its own limit, by the phase deadline, by the test's context,
// with a stop that could not be confirmed, or by a native exit.
func RGWSyncStatsEnd(err, testErr, phaseErr error) string {
	var unstopped *ceph.NativeCommandUnstoppedError
	var native *ceph.NativeFailure
	switch {
	case errors.As(err, &unstopped):
		return "radosgw-admin may still run: its stop was not confirmed"
	case errors.Is(err, ceph.ErrNativeFailure) && strings.Contains(err.Error(), "still ran after"):
		return "radosgw-admin did not finish within the phase bound and the gateway's supervisor stopped it"
	case testErr != nil:
		return "the test's context ended the call: " + testErr.Error()
	case phaseErr != nil:
		return "the phase deadline ended the call, which stopped radosgw-admin"
	case errors.As(err, &native):
		return fmt.Sprintf("radosgw-admin exited %d (errno %q)", native.ExitCode, native.Errno)
	}
	return "the call failed before a native result"
}

func rgwSyncStatsGateway(parent context.Context, gateway *rgw.Gateway) string {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer cancel()
	state, err := gateway.State(ctx)
	switch {
	case err != nil:
		return fmt.Sprintf("gateway %s: state unavailable: %v", gateway.GetContainerID(), err)
	case state == nil:
		return fmt.Sprintf("gateway %s: state unavailable: no state", gateway.GetContainerID())
	}
	return fmt.Sprintf("gateway %s: status=%s exit=%d oom=%t started=%s finished=%s", gateway.GetContainerID(), state.Status, state.ExitCode, state.OOMKilled, state.StartedAt, state.FinishedAt)
}

func rgwSyncStatsOSDs(parent context.Context, cluster *ceph.Container) string {
	if cluster == nil {
		return "OSD map: not read"
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 15*time.Second)
	defer cancel()
	data, err := cluster.Ceph(ctx, "osd", "stat", "--format", "json")
	if err != nil {
		return "OSD map: unavailable: " + err.Error()
	}
	text := strings.TrimSpace(string(data))
	if len(text) > 512 {
		text = text[:512] + " [truncated]"
	}
	return "OSD map: " + text
}

// Explicit synchronization prepares deterministic native accounting for the
// fixture assertion. User.Usage itself performs only read-only stats queries.
func RGWFixtureUserUsage(t *testing.T, ctx context.Context, gateway *rgw.Gateway, user *rgw.User, stage string, objects, sizeBytes uint64) {
	t.Helper()
	RGWSyncStats(t, ctx, nil, gateway, "user", "stats", "--uid", user.ID(), "--sync-stats")
	usage, err := user.Usage(ctx)
	if err != nil || usage.UserID != user.ID() || usage.Scope != "user" || usage.OwnerID != user.ID() || usage.NumObjects != objects || usage.SizeBytes != sizeBytes {
		t.Fatalf("native user storage assertion %s: usage=%+v error=%v expected_objects=%d expected_bytes=%d", stage, usage, err, objects, sizeBytes)
	}
	t.Logf("RGW_USER_USAGE stage=%s scope=%s owner_id=%q user_id=%q num_objects=%d size_bytes=%d size_actual_bytes=%d", stage, usage.Scope, usage.OwnerID, usage.UserID, usage.NumObjects, usage.SizeBytes, usage.SizeActualBytes)
}

// This deliberately small test-only SigV4 client avoids adding a native client
// or an SDK dependency. Production consumers can use their ordinary S3 SDK.
type S3HTTPClient struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	HTTP      *http.Client
}

func (s S3HTTPClient) Request(t *testing.T, ctx context.Context, method, path string, payload []byte, wantStatus int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, s.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	s.Sign(req, payload, time.Now().UTC())
	response, err := s.HTTP.Do(req)
	if err != nil {
		t.Fatalf("S3 %s %s: %v", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("S3 %s %s returned %d, want %d: %s", method, path, response.StatusCode, wantStatus, body)
	}
	return body
}

func (s S3HTTPClient) ListKeys(t *testing.T, ctx context.Context, bucket string) []string {
	t.Helper()
	var listing struct {
		IsTruncated bool `xml:"IsTruncated"`
		Contents    []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
	}
	if err := xml.Unmarshal(s.Request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &listing); err != nil {
		t.Fatal(err)
	}
	if listing.IsTruncated {
		t.Fatal("small S3 test listing was unexpectedly truncated")
	}
	keys := make([]string, 0, len(listing.Contents))
	for _, object := range listing.Contents {
		keys = append(keys, object.Key)
	}
	slices.Sort(keys)
	return keys
}

func (s S3HTTPClient) Sign(req *http.Request, payload []byte, now time.Time) {
	now = now.UTC()
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	payloadHash := S3Hash(payload)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	const signedHeaders = "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + req.URL.Host + "\nx-amz-content-sha256:" + payloadHash + "\nx-amz-date:" + stamp + "\n"
	canonicalQuery := strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20")
	canonicalRequest := strings.Join([]string{req.Method, S3CanonicalURI(req.URL.Path), canonicalQuery, canonicalHeaders, signedHeaders, payloadHash}, "\n")
	scope := day + "/" + s.Region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + S3Hash([]byte(canonicalRequest))
	key := S3HMAC([]byte("AWS4"+s.SecretKey), day)
	key = S3HMAC(key, s.Region)
	key = S3HMAC(key, "s3")
	key = S3HMAC(key, "aws4_request")
	signature := hex.EncodeToString(S3HMAC(key, stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s.AccessKey, scope, signedHeaders, signature))
}

// AWS4 encodes decoded UTF-8 path bytes using only unreserved characters and
// literal slashes. Go's URL.EscapedPath also permits reserved characters such
// as colon, so it cannot serve as the signed URI. Do not normalize S3 paths or
// encode URL.RawPath a second time; Ceph v20.2.4 rgw_auth_s3.h decodes once and
// applies these same encoding rules to its canonical URI.
func S3CanonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	const uppercaseHex = "0123456789ABCDEF"
	var canonical strings.Builder
	canonical.Grow(len(path))
	for i := range len(path) {
		c := path[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' || c == '/' {
			canonical.WriteByte(c)
		} else {
			canonical.WriteByte('%')
			canonical.WriteByte(uppercaseHex[c>>4])
			canonical.WriteByte(uppercaseHex[c&15])
		}
	}
	return canonical.String()
}

func S3Hash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func S3HMAC(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write([]byte(value))
	return hash.Sum(nil)
}
