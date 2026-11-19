//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_ssec_multipart_completion_test

import (
	"net/http"
	"testing"
)

const s3Namespace = `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`

func TestSSECClassifyCompletion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		want      ssecCompletion
		completed bool
	}{
		{
			name:      "result",
			status:    http.StatusOK,
			body:      `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult ` + s3Namespace + `><Key>k</Key><ETag>"e-1"</ETag><ChecksumSHA256>c-1</ChecksumSHA256></CompleteMultipartUploadResult>`,
			want:      ssecCompletion{Status: http.StatusOK, ETag: `"e-1"`, ChecksumSHA256: "c-1"},
			completed: true,
		},
		{
			name:   "embedded-plain-error",
			status: http.StatusOK,
			body:   `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>plain</Message></Error>`,
			want:   ssecCompletion{Status: http.StatusOK, Embedded: true, Failure: ssecFailure{Code: "InternalError", Message: "plain"}},
		},
		{
			name:   "embedded-namespaced-error",
			status: http.StatusOK,
			body:   `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<Error ` + s3Namespace + `><Code>InvalidPart</Code><Message>namespaced</Message></Error>`,
			want:   ssecCompletion{Status: http.StatusOK, Embedded: true, Failure: ssecFailure{Code: "InvalidPart", Message: "namespaced"}},
		},
		{
			name:   "refusal-plain-error",
			status: http.StatusBadRequest,
			body:   `<Error><Code>InvalidArgument</Code><Message>The calculated MD5 hash of the key did not match the hash that was provided.</Message></Error>`,
			want:   ssecCompletion{Status: http.StatusBadRequest, Failure: ssecFailure{Code: "InvalidArgument", Message: "The calculated MD5 hash of the key did not match the hash that was provided."}},
		},
		{
			name:   "refusal-namespaced-error",
			status: http.StatusBadRequest,
			body:   `<Error ` + s3Namespace + `><Code>InvalidArgument</Code><Message>m</Message></Error>`,
			want:   ssecCompletion{Status: http.StatusBadRequest, Failure: ssecFailure{Code: "InvalidArgument", Message: "m"}},
		},
		{
			name:   "refusal-without-body",
			status: http.StatusForbidden,
			want:   ssecCompletion{Status: http.StatusForbidden},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ssecClassifyCompletion(tc.status, []byte(tc.body))
			if err != nil {
				t.Fatalf("ssecClassifyCompletion: %v", err)
			}
			if got != tc.want || got.completed() != tc.completed {
				t.Fatalf("ssecClassifyCompletion = %+v (completed %t), want %+v (completed %t)", got, got.completed(), tc.want, tc.completed)
			}
		})
	}
}

func TestSSECClassifyCompletionRejectsUnclassified200(t *testing.T) {
	for _, body := range []string{
		"",
		"not xml",
		`<CompleteMultipartUploadResult ` + s3Namespace + `><Key>k</Key></CompleteMultipartUploadResult>`,
		`<CopyObjectResult><ETag>"e"</ETag></CopyObjectResult>`,
	} {
		got, err := ssecClassifyCompletion(http.StatusOK, []byte(body))
		if err == nil {
			t.Errorf("ssecClassifyCompletion(200, %q) = %+v, want an error", body, got)
		}
		if got.Status != http.StatusOK || got.ETag != "" {
			t.Errorf("ssecClassifyCompletion(200, %q) lost the status or reported a result: %+v", body, got)
		}
	}
}

func TestJudgeOmittedKey(t *testing.T) {
	const missing = "<Error><Code>InvalidArgument</Code><Message>Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm.</Message></Error>"
	for _, tc := range []struct {
		name                  string
		status                int
		body                  string
		reproduced, wantError bool
	}{
		{"completed", http.StatusOK, "<CompleteMultipartUploadResult><ETag>etag</ETag></CompleteMultipartUploadResult>", false, false},
		{"missing algorithm refusal", http.StatusBadRequest, missing, true, false},
		{"embedded missing algorithm", http.StatusOK, missing, false, true},
		{"different invalid argument", http.StatusBadRequest, "<Error><Code>InvalidArgument</Code><Message>Bad XML</Message></Error>", false, true},
		{"wrong key", http.StatusBadRequest, "<Error><Code>InvalidArgument</Code><Message>Key MD5 mismatch</Message></Error>", false, true},
		{"authentication failure", http.StatusForbidden, "<Error><Code>AccessDenied</Code></Error>", false, true},
		{"server failure without XML", http.StatusInternalServerError, "unavailable", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			completion, err := ssecClassifyCompletion(tc.status, []byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			got, err := judgeOmittedKey(completion)
			if got != tc.reproduced || (err != nil) != tc.wantError {
				t.Fatalf("reproduced=%t err=%v, want reproduced=%t error=%t", got, err, tc.reproduced, tc.wantError)
			}
		})
	}
}
