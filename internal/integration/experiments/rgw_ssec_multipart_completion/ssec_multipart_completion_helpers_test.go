//go:build all || integration

package rgw_ssec_multipart_completion_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// ssecFailure is the S3 error of a refused request; Message attributes a
// refusal to the native check that made it.
type ssecFailure struct {
	Code    string
	Message string
}

// ssecCompletion is the native answer to a CompleteMultipartUpload.
type ssecCompletion struct {
	// Status is the HTTP status as sent. Embedded marks a 200 whose body is
	// an S3 Error document: the completion failed after the status was
	// sent, and Failure holds that error.
	Status         int
	Embedded       bool
	Failure        ssecFailure
	ETag           string
	ChecksumSHA256 string
}

// completed reports whether the upload became an object. It is meaningful
// only for an answer ssecClassifyCompletion classified without an error.
func (r ssecCompletion) completed() bool {
	return r.Status == http.StatusOK && !r.Embedded
}

// ssecClassifyCompletion classifies a CompleteMultipartUpload answer by its
// status and its XML root. A refusal keeps its status and the S3 error of
// its body. A 200 is a completion only when its root is
// CompleteMultipartUploadResult with an ETag; a 200 with an Error root, in
// any namespace, is an embedded failure. Any other 200 body returns an
// error together with the status, because it is neither.
func ssecClassifyCompletion(status int, body []byte) (ssecCompletion, error) {
	result := ssecCompletion{Status: status}
	classified, err := harness.RGWClassifyResultBody(body, "CompleteMultipartUploadResult")
	if status != http.StatusOK {
		if err == nil && classified.Error {
			result.Failure = ssecFailure{Code: classified.Code, Message: classified.Message}
		}
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("completion answered 200 with an unclassified body: %w", err)
	}
	if classified.Error {
		result.Embedded = true
		result.Failure = ssecFailure{Code: classified.Code, Message: classified.Message}
		return result, nil
	}
	var parsed struct {
		ETag           string `xml:"ETag"`
		ChecksumSHA256 string `xml:"ChecksumSHA256"`
	}
	if err := xml.Unmarshal(body, &parsed); err != nil || parsed.ETag == "" {
		return result, fmt.Errorf("completion answered 200 with a CompleteMultipartUploadResult without an ETag (%v)", err)
	}
	result.ETag, result.ChecksumSHA256 = parsed.ETag, parsed.ChecksumSHA256
	return result, nil
}

// ssecAlgorithmRefusal is the message of the refusal the case reproduces:
// the completion is checked for the customer-key headers.
const ssecAlgorithmRefusal = "must provide a valid encryption algorithm"

// judgeOmittedKeyCompletion judges the completion of an SSE-C upload without
// a checksum algorithm, sent without the customer-key headers. The documented
// symptom is the customer-key refusal: 400 InvalidArgument whose message asks
// for a valid encryption algorithm. A completed upload is its absence (the
// caller then requires the object to read back only with the key). Any other
// answer, an embedded error included, is another fault: no verdict.
func judgeOmittedKeyCompletion(done ssecCompletion) (symptom bool, detail string, err error) {
	switch {
	case done.completed():
		return false, fmt.Sprintf("completed with %d without the customer key", done.Status), nil
	case done.Status == http.StatusBadRequest && !done.Embedded && done.Failure.Code == "InvalidArgument" && strings.Contains(done.Failure.Message, ssecAlgorithmRefusal):
		return true, fmt.Sprintf("refused with %d %s %q", done.Status, done.Failure.Code, done.Failure.Message), nil
	default:
		return false, "", fmt.Errorf("another fault: completion without the customer key answered %d (embedded error %t) %s %q, want 200 or 400 InvalidArgument %q",
			done.Status, done.Embedded, done.Failure.Code, done.Failure.Message, ssecAlgorithmRefusal)
	}
}
