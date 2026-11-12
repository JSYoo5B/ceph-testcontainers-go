//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_compressed_copy_accounting_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// ownBuckets runs harness.RGWOwnBuckets in a subtest whose creation of
// failAt fails (failAt empty: none fails) and returns the creations
// attempted and the buckets passed to teardown (nil when teardown never
// ran). The subtest ends right after the call, so its cleanups run as they
// do when a native case stops at a failed creation; it passes rather than
// skips, since the tag runner rejects a skipped pure test.
func ownBuckets(t *testing.T, failAt string, names ...string) (attempted, tornDown []string) {
	t.Helper()
	create := func(bucket string) error {
		attempted = append(attempted, bucket)
		if bucket == failAt {
			return errors.New("refused")
		}
		return nil
	}
	t.Run("case", func(t *testing.T) {
		err := harness.RGWOwnBuckets(t, create, func(created []string) { tornDown = append([]string{}, created...) }, names...)
		if (err != nil) != (failAt != "") {
			t.Errorf("RGWOwnBuckets error %v with failAt %q", err, failAt)
		}
	})
	return attempted, tornDown
}

func TestOwnBucketsTearsDownOnlyCreatedBucketsAfterAFailedIntermediateCreate(t *testing.T) {
	attempted, tornDown := ownBuckets(t, "b", "a", "b", "c")
	if !slices.Equal(attempted, []string{"a", "b"}) {
		t.Errorf("attempted creations %v, want [a b]: creation must stop at the failed one", attempted)
	}
	if !slices.Equal(tornDown, []string{"a"}) {
		t.Errorf("teardown got %v, want exactly the created bucket [a]", tornDown)
	}
}

func TestOwnBucketsTearsDownEveryCreatedBucket(t *testing.T) {
	if _, tornDown := ownBuckets(t, "", "a", "b"); !slices.Equal(tornDown, []string{"a", "b"}) {
		t.Errorf("teardown got %v, want [a b]", tornDown)
	}
}

func TestOwnBucketsTearsDownNothingAfterAFailedFirstCreate(t *testing.T) {
	if _, tornDown := ownBuckets(t, "a", "a", "b"); tornDown == nil || len(tornDown) != 0 {
		t.Errorf("teardown got %v, want a call with no bucket", tornDown)
	}
}

func TestClassifyCopyResult(t *testing.T) {
	const ns = `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`
	for _, tc := range []struct {
		body string
		want harness.RGWResultBody
	}{
		{`<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult ` + ns + `><ETag>"e"</ETag></CopyObjectResult>`, harness.RGWResultBody{}},
		{`<CopyObjectResult><ETag>"e"</ETag></CopyObjectResult>`, harness.RGWResultBody{}},
		{`<Error><Code>InternalError</Code><Message>plain</Message></Error>`, harness.RGWResultBody{Error: true, Code: "InternalError", Message: "plain"}},
		{`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<Error ` + ns + `><Code>SlowDown</Code><Message>namespaced</Message></Error>`, harness.RGWResultBody{Error: true, Code: "SlowDown", Message: "namespaced"}},
	} {
		got, err := harness.RGWClassifyResultBody([]byte(tc.body), "CopyObjectResult")
		if err != nil || got != tc.want {
			t.Errorf("RGWClassifyResultBody(%q) = %+v, %v; want %+v", tc.body, got, err, tc.want)
		}
	}
	for _, body := range []string{"", "<", `<CompleteMultipartUploadResult/>`} {
		if got, err := harness.RGWClassifyResultBody([]byte(body), "CopyObjectResult"); err == nil {
			t.Errorf("RGWClassifyResultBody(%q) = %+v, want an error", body, got)
		}
	}
}

func TestJudgeCopyAccounting(t *testing.T) {
	data := []byte("logical bytes of the copy accounting case")
	logical := uint64(len(data))
	const compressedIndex, compressedBlocks = 12, 11
	compressed := []uint64{compressedIndex, compressedBlocks}
	sized := func(accounted uint64, listed int64, stats uint64) ccObservation {
		return ccObservation{Bucket: "b", Index: ccIndexEntry{Size: logical, AccountedSize: accounted}, Stats: ccStats{Size: stats, NumObjects: 1}, ListedSize: listed, Data: data}
	}
	for _, c := range []struct {
		name    string
		o       ccObservation
		symptom bool
		fault   bool
	}{
		{"logical length", sized(logical, int64(logical), logical), false, false},
		{"compressed index length", sized(compressedIndex, compressedIndex, compressedIndex), true, false},
		{"compressed block length", sized(compressedBlocks, compressedBlocks, compressedBlocks), true, false},
		{"sizes disagree", sized(compressedIndex, int64(logical), logical), false, true},
		{"another length", sized(5, 5, 5), false, true},
		{"larger than logical", sized(logical+1, int64(logical)+1, logical+1), false, true},
		{"other bytes", func() ccObservation { o := sized(logical, int64(logical), logical); o.Data = data[1:]; return o }(), false, true},
	} {
		symptom, detail, err := judgeCopyAccounting(c.o, data, compressed)
		if c.fault {
			if err == nil || !strings.HasPrefix(err.Error(), "another fault: ") {
				t.Errorf("%s: %t %q %v, want another fault", c.name, symptom, detail, err)
			}
			continue
		}
		if err != nil || symptom != c.symptom || detail == "" {
			t.Errorf("%s: symptom=%t %q %v, want symptom=%t", c.name, symptom, detail, err, c.symptom)
		}
	}
}
