//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_post_usage_bytes_received_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// The reports below have the form "usage show" prints with its log entries.
// They are synthetic inputs of the reader, not native evidence of what a
// gateway logs.
const (
	usageUser   = "tc-usage-post"
	usageBucket = "tc-usage-post"
)

func usageCategory(name, received string) string {
	return `{"category":"` + name + `","bytes_sent":19,"bytes_received":` + received + `,"ops":1,"successful_ops":1}`
}

func usageRecord(bucket string, categories ...string) string {
	return `{"bucket":"` + bucket + `","time":"2026-01-01T00:00:00.000000Z","epoch":1767225600,"owner":"` + usageUser + `","categories":[` + strings.Join(categories, ",") + `],"s3select":{"bytes_processed":0,"bytes_returned":0}}`
}

func usageReport(records ...string) []byte {
	return []byte(`{"entries":[{"user":"` + usageUser + `","buckets":[` + strings.Join(records, ",") + `]}]}`)
}

func TestParseUsageReadsTheBucketOfTheUser(t *testing.T) {
	report := usageReport(
		usageRecord(usageBucket, usageCategory(categoryCreateBucket, "0"), usageCategory(categoryPost, "0")),
		usageRecord("another", usageCategory(categoryPut, "7")),
		// The record of the next hour of the same bucket.
		usageRecord(usageBucket, usageCategory(categoryPost, "1048576")),
	)
	got, err := parseUsage(report, usageUser, usageBucket)
	if err != nil {
		t.Fatal(err)
	}
	want := bucketUsage{
		categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1},
		categoryPost:         {Sent: 38, Received: 1048576, Ops: 2, Successful: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("usage = %+v, want %+v", got, want)
	}
	// A log without an entry yet, and one without the bucket, are
	// observations of an empty usage.
	for _, report := range []string{`{"entries":[]}`, string(usageReport(usageRecord("another", usageCategory(categoryPut, "7"))))} {
		if got, err := parseUsage([]byte(report), usageUser, usageBucket); err != nil || len(got) != 0 {
			t.Fatalf("%s = %+v, %v", report, got, err)
		}
	}
}

// A counter that is missing or null is not a zero: read as zero, a POST
// whose received bytes the log does not print would be the symptom.
func TestParseUsageRefusesWhatItCannotRead(t *testing.T) {
	for name, report := range map[string]string{
		"not JSON":                `usage log is disabled`,
		"no entry list":           `{"summary":[]}`,
		"null entry list":         `{"entries":null}`,
		"entry without a user":    `{"entries":[{"buckets":[]}]}`,
		"entry of another user":   `{"entries":[{"user":"someone","buckets":[]}]}`,
		"entry without buckets":   `{"entries":[{"user":"` + usageUser + `"}]}`,
		"entry with null buckets": `{"entries":[{"user":"` + usageUser + `","buckets":null}]}`,
		"record without a bucket": `{"entries":[{"user":"` + usageUser + `","buckets":[{"categories":[]}]}]}`,
		"record without a list":   `{"entries":[{"user":"` + usageUser + `","buckets":[{"bucket":"` + usageBucket + `"}]}]}`,
		"record with a null list": `{"entries":[{"user":"` + usageUser + `","buckets":[{"bucket":"` + usageBucket + `","categories":null}]}]}`,
		"category without a name": string(usageReport(usageRecord(usageBucket, `{"bytes_sent":0,"bytes_received":0,"ops":1,"successful_ops":1}`))),
		"no received bytes":       string(usageReport(usageRecord(usageBucket, `{"category":"post_obj","bytes_sent":0,"ops":1,"successful_ops":1}`))),
		"null received bytes":     string(usageReport(usageRecord(usageBucket, usageCategory(categoryPost, "null")))),
		"no sent bytes":           string(usageReport(usageRecord(usageBucket, `{"category":"post_obj","bytes_received":0,"ops":1,"successful_ops":1}`))),
		"no operations":           string(usageReport(usageRecord(usageBucket, `{"category":"post_obj","bytes_sent":0,"bytes_received":0,"successful_ops":1}`))),
		"no successes":            string(usageReport(usageRecord(usageBucket, `{"category":"post_obj","bytes_sent":0,"bytes_received":0,"ops":1}`))),
		"negative received bytes": string(usageReport(usageRecord(usageBucket, usageCategory(categoryPost, "-1")))),
		"category listed twice":   string(usageReport(usageRecord(usageBucket, usageCategory(categoryPost, "0"), usageCategory(categoryPost, "0")))),
	} {
		if got, err := parseUsage([]byte(report), usageUser, usageBucket); err == nil {
			t.Errorf("%s: read as %+v", name, got)
		}
	}
	for _, names := range [][2]string{{"", usageBucket}, {usageUser, ""}} {
		if got, err := parseUsage(usageReport(), names[0], names[1]); err == nil {
			t.Errorf("user %q bucket %q: read as %+v", names[0], names[1], got)
		}
	}
}

func TestUsageGrewRefusesTotalsThatWentDown(t *testing.T) {
	before := bucketUsage{categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}, categoryPut: {Received: 1048576, Ops: 1, Successful: 1}}
	for name, after := range map[string]bucketUsage{
		"the same":       {categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}, categoryPut: {Received: 1048576, Ops: 1, Successful: 1}},
		"more":           {categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}, categoryPut: {Received: 2097152, Ops: 2, Successful: 1}},
		"a new category": {categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}, categoryPut: {Received: 1048576, Ops: 1, Successful: 1}, "get_obj": {Sent: 5, Ops: 1}},
	} {
		if err := usageGrew(before, after); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := usageGrew(nil, before); err != nil {
		t.Errorf("first sample: %v", err)
	}
	for name, after := range map[string]bucketUsage{
		"a category gone":      {categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}},
		"fewer received bytes": {categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}, categoryPut: {Received: 1048575, Ops: 1, Successful: 1}},
		"fewer sent bytes":     {categoryCreateBucket: {Sent: 18, Ops: 1, Successful: 1}, categoryPut: {Received: 1048576, Ops: 1, Successful: 1}},
		"fewer operations":     {categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}, categoryPut: {Received: 1048576, Successful: 1}},
		"fewer successes":      {categoryCreateBucket: {Sent: 19, Ops: 1, Successful: 1}, categoryPut: {Received: 1048576, Ops: 1}},
		"an empty log":         {},
	} {
		if err := usageGrew(before, after); err == nil || !strings.HasPrefix(err.Error(), "usage not observed") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRequireLoggingNeedsTheCreationAndNoUpload(t *testing.T) {
	created := usageCounters{Sent: 19, Ops: 1, Successful: 1}
	if err := requireLogging(bucketUsage{categoryCreateBucket: created, "get_obj": {Ops: 1}}); err != nil {
		t.Fatal(err)
	}
	for name, usage := range map[string]bucketUsage{
		"an empty log":        {},
		"no creation":         {"get_obj": {Ops: 1, Successful: 1}},
		"a failed creation":   {categoryCreateBucket: {Ops: 1}},
		"two creations":       {categoryCreateBucket: {Ops: 2, Successful: 2}},
		"a PUT before upload": {categoryCreateBucket: created, categoryPut: {Ops: 1, Successful: 1}},
		"a POST before it":    {categoryCreateBucket: created, categoryPost: {Ops: 1, Successful: 1}},
	} {
		if err := requireLogging(usage); err == nil || !strings.HasPrefix(err.Error(), "condition not established") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestUploadedReturnsTheCategoryOfTheBucket(t *testing.T) {
	post := usageCounters{Ops: 1, Successful: 1}
	got, err := uploaded(bucketUsage{categoryCreateBucket: {Ops: 1, Successful: 1}, categoryPost: post}, categoryPost)
	if err != nil || got == nil || *got != post {
		t.Fatalf("uploaded = %+v, %v", got, err)
	}
	// Not in the log yet: neither counters nor an error.
	if got, err := uploaded(bucketUsage{categoryCreateBucket: {Ops: 1, Successful: 1}}, categoryPost); got != nil || err != nil {
		t.Fatalf("before the flush = %+v, %v", got, err)
	}
	// The upload of the other kind in this bucket is not this case's upload.
	for _, category := range []string{categoryPut, categoryPost} {
		usage := bucketUsage{categoryPut: {Ops: 1, Successful: 1}, categoryPost: post}
		if got, err := uploaded(usage, category); err == nil {
			t.Errorf("%s beside the other upload: %+v", category, got)
		}
	}
}

func TestJudgeUploads(t *testing.T) {
	const payload, form = 1 << 20, 1<<20 + 1500
	counters := func(received, ops, successful uint64) *usageCounters {
		return &usageCounters{Received: received, Ops: ops, Successful: successful}
	}
	put := counters(payload, 1, 1)
	// The symptom: no received bytes for the POST beside the PUT's payload.
	if symptoms, err := judgeUploads(put, counters(0, 1, 1), payload, form); err != nil || len(symptoms) != 1 {
		t.Fatalf("zero received bytes = %q, %v", symptoms, err)
	}
	// The fixed form: the payload, with any part of the form overhead.
	for _, received := range []uint64{payload, payload + 700, form} {
		if symptoms, err := judgeUploads(put, counters(received, 1, 1), payload, form); err != nil || len(symptoms) != 0 {
			t.Errorf("%d received bytes = %q, %v", received, symptoms, err)
		}
	}
	for name, test := range map[string]struct {
		put, post     *usageCounters
		payload, form uint64
		reason        string
	}{
		"no PUT in the log":            {nil, counters(0, 1, 1), payload, form, "usage not observed"},
		"no POST in the log":           {put, nil, payload, form, "usage not observed"},
		"a failed PUT":                 {counters(payload, 1, 0), counters(0, 1, 1), payload, form, "condition not established"},
		"two PUTs":                     {counters(2*payload, 2, 2), counters(0, 1, 1), payload, form, "condition not established"},
		"a failed POST":                {put, counters(0, 1, 0), payload, form, "condition not established"},
		"two POSTs":                    {put, counters(0, 2, 2), payload, form, "condition not established"},
		"a POST that was not counted":  {put, counters(0, 0, 0), payload, form, "condition not established"},
		"a PUT without received bytes": {counters(0, 1, 1), counters(0, 1, 1), payload, form, "control not established"},
		"a PUT with fewer bytes":       {counters(payload-1, 1, 1), counters(payload, 1, 1), payload, form, "control not established"},
		"a PUT with more bytes":        {counters(payload+1, 1, 1), counters(payload, 1, 1), payload, form, "control not established"},
		"a POST with part of it":       {put, counters(payload-1, 1, 1), payload, form, "another state"},
		"a POST with the form only":    {put, counters(1500, 1, 1), payload, form, "another state"},
		"a POST with more than sent":   {put, counters(form+1, 1, 1), payload, form, "another state"},
		"no payload":                   {counters(0, 1, 1), counters(0, 1, 1), 0, form, "outcome not observed"},
		"a body smaller than payload":  {put, counters(payload, 1, 1), payload, payload - 1, "outcome not observed"},
	} {
		symptoms, err := judgeUploads(test.put, test.post, test.payload, test.form)
		if err == nil || !strings.HasPrefix(err.Error(), test.reason) {
			t.Errorf("%s: %q, %v, want an error that starts with %q", name, symptoms, err, test.reason)
		}
	}
}

func TestPostPolicyNamesEveryCondition(t *testing.T) {
	document, err := postPolicy("2026-01-01T00:10:00Z", "tc-usage-post", "payload", 1<<20, []formField{{"X-Amz-Date", "20260101T000000Z"}})
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Expiration string `json:"expiration"`
		Conditions []any  `json:"conditions"`
	}
	if err := json.Unmarshal(document, &policy); err != nil {
		t.Fatal(err)
	}
	want := []any{
		map[string]any{"bucket": "tc-usage-post"},
		[]any{"eq", "$key", "payload"},
		[]any{"content-length-range", float64(1 << 20), float64(1 << 20)},
		map[string]any{"x-amz-date": "20260101T000000Z"},
	}
	if policy.Expiration != "2026-01-01T00:10:00Z" || !reflect.DeepEqual(policy.Conditions, want) {
		t.Fatalf("policy = %s", document)
	}
}
