//go:build all || (integration && features && (!ci || (ci_topology && (!ci_batch || ci_batch_rgw_single_part_read))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rgw

package rgw_single_part_multipart_read_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	spBucket = "tc-single-part"
	// Each numbered read has its own deadline. The largest answer is 9 MiB
	// from a gateway on the same host.
	spReadDeadline = 30 * time.Second
	// The window over which a gateway that did not answer is measured.
	spStallWindow = 3 * time.Second
)

// image is the Ceph image the case runs on: the Squid release candidate
// 19.1.0, which reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_single_part_multipart_read_test.image=<image>"
//
// and a fixed one (Squid 19.2.0 and every later release) does not reproduce
// it.
var image = "quay.io/ceph/ceph:v19.1.0@sha256:56d1542f98c99516890898f79f1ba6010922be479a1432d7f380203a73405517"

type spCase struct {
	ctx     context.Context
	gateway *rgw.Gateway
	client  harness.S3HTTPClient
	uploads map[string]string
	// unanswered counts the numbered reads that hit their deadline.
	unanswered int
}

func (c *spCase) signed(t *testing.T, ctx context.Context, method, path string, payload []byte) harness.RGWBackendResponse {
	t.Helper()
	return harness.RGWBackendSigned(t, ctx, c.client, "s3", method, path, payload, nil)
}

// upload completes a multipart upload of parts to the object and returns
// the ETag that a HEAD of the object answers.
func (c *spCase) upload(t *testing.T, object string, parts [][]byte) string {
	t.Helper()
	path := "/" + spBucket + "/" + object
	started := c.signed(t, c.ctx, http.MethodPost, path+"?uploads", nil)
	harness.RGWBackendRequire(t, started, http.StatusOK)
	var initiated struct {
		ID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(started.Body, &initiated); err != nil || initiated.ID == "" {
		t.Fatalf("initiate %s: no upload ID (%v)", path, err)
	}
	c.uploads[initiated.ID] = path
	var complete strings.Builder
	complete.WriteString("<CompleteMultipartUpload>")
	for i, part := range parts {
		r := c.signed(t, c.ctx, http.MethodPut, fmt.Sprintf("%s?partNumber=%d&uploadId=%s", path, i+1, url.QueryEscape(initiated.ID)), part)
		harness.RGWBackendRequire(t, r, http.StatusOK)
		fmt.Fprintf(&complete, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, r.Headers.Get("ETag"))
	}
	complete.WriteString("</CompleteMultipartUpload>")
	done := c.signed(t, c.ctx, http.MethodPost, path+"?uploadId="+url.QueryEscape(initiated.ID), []byte(complete.String()))
	harness.RGWBackendRequire(t, done, http.StatusOK)
	classified, err := harness.RGWClassifyResultBody(done.Body, "CompleteMultipartUploadResult")
	if err != nil || classified.Error {
		t.Fatalf("complete %s: %+v %v", path, classified, err)
	}
	delete(c.uploads, initiated.ID)
	head := c.signed(t, c.ctx, http.MethodHead, path, nil)
	harness.RGWBackendRequire(t, head, http.StatusOK)
	return strings.Trim(head.Headers.Get("ETag"), `"`)
}

// manifestParts reads the object's manifest with radosgw-admin and returns
// the first part number of each of its rules.
func (c *spCase) manifestParts(t *testing.T, object string) []int {
	t.Helper()
	var stat struct {
		Manifest struct {
			Rules []struct {
				Val struct {
					StartPart int `json:"start_part_num"`
				} `json:"val"`
			} `json:"rules"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(harness.RGWBackendAdmin(t, c.ctx, c.gateway, "object", "stat", "--bucket", spBucket, "--object", object), &stat); err != nil {
		t.Fatalf("object stat %s: %v", object, err)
	}
	parts := []int{}
	for _, rule := range stat.Manifest.Rules {
		parts = append(parts, rule.Val.StartPart)
	}
	return parts
}

// readPart sends one GET or HEAD of a part number with its own deadline and
// records the answer, or the phase in which the deadline ended.
func (c *spCase) readPart(t *testing.T, method, object string, part int, want []byte) spRead {
	t.Helper()
	request, err := http.NewRequest(method, fmt.Sprintf("%s/%s/%s?partNumber=%d", c.client.Endpoint, spBucket, object, part), nil)
	if err != nil {
		t.Fatal(err)
	}
	harness.S3FeatureSign(request, c.client, nil, time.Now().UTC(), "s3")
	read := sendPartRead(c.ctx, c.client.HTTP, request, spRead{Method: method, Part: part, Deadline: spReadDeadline}, want, harness.RGWBackendErrorCode)
	t.Logf("RGW_SP read object=%s %s", object, read)
	return read
}

// process reads process 1 of the gateway container, the radosgw process.
// It reports a gateway that cannot be read instead of stopping the test.
func (c *spCase) process(t *testing.T) (spProcess, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	code, reader, err := c.gateway.Exec(ctx, []string{"cat", "/proc/1/stat"}, tcexec.Multiplexed())
	if err != nil {
		return spProcess{}, err
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		return spProcess{}, fmt.Errorf("read /proc/1/stat: exit %d: %v", code, err)
	}
	return parseProcessStat(string(data))
}

// stall measures the gateway after a part read that got no answer.
func (c *spCase) stall(t *testing.T, before spProcess, ticks uint64, earlier int) *spStall {
	t.Helper()
	first, err := c.process(t)
	if err != nil {
		t.Logf("RGW_SP stall: the gateway process cannot be read: %v", err)
		return nil
	}
	started := time.Now()
	select {
	case <-time.After(spStallWindow):
	case <-c.ctx.Done():
		return nil
	}
	second, err := c.process(t)
	if err != nil {
		t.Logf("RGW_SP stall: the gateway process cannot be read: %v", err)
		return nil
	}
	s := &spStall{Before: before, First: first, Second: second, Window: time.Since(started), TicksPerSecond: ticks, Earlier: earlier}
	t.Logf("RGW_SP stall start=%d,%d,%d cpu-ticks=%d,%d,%d window=%s ticks-per-second=%d process-busy=%.2f earlier-unanswered=%d",
		before.Start, first.Start, second.Start, before.CPU, first.CPU, second.CPU, s.Window.Round(time.Millisecond), ticks, s.busy(), earlier)
	return s
}

// cleanup aborts the uploads left open, then deletes the objects and the
// bucket.
func (c *spCase) cleanup(t *testing.T, buckets, objects []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	failed := false
	for id, path := range c.uploads {
		if r := c.signed(t, ctx, http.MethodDelete, path+"?uploadId="+url.QueryEscape(id), nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_SP cleanup failed: abort %s: %d", path, r.Code)
			failed = true
		}
	}
	for _, bucket := range buckets {
		for _, object := range objects {
			if r := c.signed(t, ctx, http.MethodDelete, "/"+bucket+"/"+object, nil); r.Code != http.StatusNoContent {
				t.Errorf("RGW_SP cleanup failed: delete %s/%s: %d", bucket, object, r.Code)
				failed = true
			}
		}
		if r := c.signed(t, ctx, http.MethodDelete, "/"+bucket, nil); r.Code != http.StatusNoContent {
			t.Errorf("RGW_SP cleanup failed: delete bucket %s: %d %s", bucket, r.Code, harness.RGWBackendErrorCode(r.Body))
			failed = true
		}
	}
	t.Logf("RGW_SP cleanup buckets=%d failed=%t", len(buckets), failed)
}

// TestRGWSinglePartMultipartNumberedRead reports whether the only part of a
// completed multipart upload in the default storage class cannot be read
// with GET and HEAD ?partNumber=1: the part's length, a part count of 1 and,
// for a GET, its bytes.
//
// Given three completed multipart objects that read back whole, one of a
// single one-stripe part, one of a single three-stripe part and a two-part
// control whose numbered reads answer, when part 1 of each single-part
// object is read with GET and then HEAD, each with its own deadline, then
// the defect is REPRODUCED if a read is answered otherwise or is written to
// the gateway and does not end while the same gateway process runs
// (judgeOnlyPart's "observed defect"), and NOT_REPRODUCED if every read
// answers the part. The process-wide CPU time recorded with a read that did
// not end does not establish the cause. The Squid release candidate 19.1.0
// does not answer the GET, so it reproduces; Squid 19.2.0 and every later
// release do not.
//
// A failure of the given or when steps, of a control (the two-part object,
// the refusal of the part number after the last part), or another fault
// (a read that never reached the gateway, a restarted or unreadable
// gateway) fails the test; the result does not.
func TestRGWSinglePartMultipartNumberedRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	t.Logf("RGW_SP image=%s", image)
	cluster, _ := harness.NewSingleImageCluster(t, ceph.Run, image, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	t.Logf("RGW_SP ceph=%s", harness.StaticFacts(cluster).CephVersion)
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RGW_SP gateway=%s", strings.TrimSpace(harness.ExecOutput(t, ctx, gateway, "radosgw", "--version")))
	ticks, err := strconv.ParseUint(harness.ExecOutput(t, ctx, gateway, "getconf", "CLK_TCK"), 10, 64)
	if err != nil || ticks == 0 {
		t.Fatalf("unavailable prerequisite: the gateway's clock tick rate cannot be read: %v", err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The client itself does not time out, so that a read ends by its own
	// deadline only.
	c := &spCase{ctx: ctx, gateway: gateway, uploads: map[string]string{},
		client: harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{}}}

	type object struct {
		name  string
		parts [][]byte
	}
	fill := func(text string, size int) []byte {
		return bytes.Repeat([]byte(text), size/len(text)+1)[:size]
	}
	// A part is striped in 4 MiB objects: 64 KiB is one stripe, 9 MiB is
	// three. 5 MiB is the smallest valid non-final part.
	small := object{"single-small", [][]byte{fill("the only part, one stripe\n", 64<<10)}}
	large := object{"single-large", [][]byte{fill("THE ONLY PART, THREE STRIPES\n", 9<<20)}}
	control := object{"two-parts", [][]byte{fill("first part of the control\n", 5<<20), fill("SECOND PART\n", 64<<10)}}
	objects := []object{small, large, control}
	names := []string{}
	for _, o := range objects {
		names = append(names, o.name)
	}
	if err := harness.RGWOwnBuckets(t, harness.RGWBackendBucketCreator(t, ctx, c.client), func(created []string) { c.cleanup(t, created, names) }, spBucket); err != nil {
		t.Fatal(err)
	}

	// Condition: every object is a completed multipart upload of the
	// intended number of parts, and reads back whole.
	for _, o := range objects {
		etag := c.upload(t, o.name, o.parts)
		whole := bytes.Join(o.parts, nil)
		got := c.signed(t, ctx, http.MethodGet, "/"+spBucket+"/"+o.name, nil)
		rules := c.manifestParts(t, o.name)
		t.Logf("RGW_SP object=%s parts=%d etag-suffix=%q manifest-rule-start-parts=%v get=%d bytes=%d matches=%t",
			o.name, len(o.parts), etag[strings.LastIndexByte(etag, '-')+1:], rules, got.Code, len(got.Body), bytes.Equal(got.Body, whole))
		if !strings.HasSuffix(etag, fmt.Sprintf("-%d", len(o.parts))) || len(rules) == 0 || rules[0] != 1 || got.Code != http.StatusOK || !bytes.Equal(got.Body, whole) {
			t.Fatalf("condition not established: %s is not a readable multipart object of %d parts", o.name, len(o.parts))
		}
	}

	// Control first: numbered reads work on this gateway at all.
	t.Run("two-part-control", func(t *testing.T) {
		for i, part := range control.parts {
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				if verdict := judgeControlPart(c.readPart(t, method, control.name, i+1, part), len(part)); verdict != "" {
					t.Error(verdict)
				}
			}
		}
		if verdict := judgeBeyond(c.readPart(t, http.MethodGet, control.name, len(control.parts)+1, nil)); verdict != "" {
			t.Error(verdict)
		}
	})
	if t.Failed() {
		t.Fatal("control failed: the single-part objects were not read")
	}

	// Each symptom observed by the then step; none means NOT_REPRODUCED.
	var symptoms []string
	for _, o := range []object{small, large} {
		t.Run(o.name, func(t *testing.T) {
			part := o.parts[0]
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				before, err := c.process(t)
				if err != nil {
					t.Fatalf("another fault: the gateway process cannot be read before the %s: %v", method, err)
				}
				read := c.readPart(t, method, o.name, 1, part)
				var stall *spStall
				if read.TimedOut {
					stall = c.stall(t, before, ticks, c.unanswered)
					c.unanswered++
				}
				verdict := judgeOnlyPart(read, len(part), stall)
				defect := strings.HasPrefix(verdict, "observed defect")
				if defect {
					symptoms = append(symptoms, fmt.Sprintf("%s: %s", o.name, verdict))
				}
				if verdict != "" {
					if read.TimedOut || read.Err != "" {
						// A request that did not end may still hold a
						// gateway thread, so this object is not read
						// again. Every method the subtest leaves out gets
						// its own line.
						if method == http.MethodGet {
							t.Logf("RGW_SP read object=%s %s", o.name, spRead{Method: http.MethodHead, Part: 1,
								NotSent: "the GET of part 1 did not end; HEAD is untested for this object"})
						}
						t.Logf("RGW_SP read object=%s %s", o.name, spRead{Method: http.MethodGet, Part: 2,
							NotSent: "a read of part 1 did not end; the refusal of part 2 is untested for this object"})
						if defect {
							return
						}
						t.Fatal(verdict)
					}
					if !defect {
						t.Error(verdict)
					}
				}
				// Gateway health after the read: the same process still
				// answers the whole object.
				after, err := c.process(t)
				whole := c.signed(t, ctx, http.MethodGet, "/"+spBucket+"/"+o.name, nil)
				if err != nil || after.Start != before.Start || whole.Code != http.StatusOK || !bytes.Equal(whole.Body, part) {
					t.Fatalf("another fault: after the %s of part 1 the gateway is not the same healthy process (process error %v, start %d then %d, whole GET %d)",
						method, err, before.Start, after.Start, whole.Code)
				}
			}
			if verdict := judgeBeyond(c.readPart(t, http.MethodGet, o.name, 2, nil)); verdict != "" {
				t.Error(verdict)
			}
		})
	}
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rgw_single_part_multipart_read result=%s image=%s symptoms=%q", result, image, symptoms)
}
