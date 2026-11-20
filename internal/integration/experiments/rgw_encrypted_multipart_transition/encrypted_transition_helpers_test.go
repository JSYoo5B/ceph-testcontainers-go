//go:build all || integration

package rgw_encrypted_multipart_transition_test

import (
	"fmt"
	"net/http"
)

// etWant is the answer a correct gateway gives to one GET of an object:
// the status, for a range the Content-Range, and the bytes that start at
// offset Start of the object.
type etWant struct {
	Status       int
	ContentRange string
	Start        int
	Bytes        []byte
}

// etRead is one GET of an object as the gateway answered it.
type etRead struct {
	Kind         string
	Status       int
	ContentRange string
	Length       int
	// Mismatch is the object offset of the first byte that differs from the
	// expected bytes, or -1 when the body and the expected bytes are equal.
	Mismatch int
	// Err is a transport failure, such as a body cut off after the headers.
	Err string
}

// observe records a GET answer against want. err is the transport failure
// of the request or of reading its body; body holds what arrived before it.
func observe(kind string, want etWant, status int, contentRange string, body []byte, err error) etRead {
	read := etRead{Kind: kind, Status: status, ContentRange: contentRange, Length: len(body), Mismatch: -1}
	if err != nil {
		read.Err = err.Error()
	}
	if i := firstDifference(body, want.Bytes); i >= 0 {
		read.Mismatch = want.Start + i
	}
	return read
}

// firstDifference returns the index of the first byte at which got and want
// differ, the shorter length when one is a prefix of the other, or -1 when
// they are equal.
func firstDifference(got, want []byte) int {
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			return i
		}
	}
	if len(got) != len(want) {
		return min(len(got), len(want))
	}
	return -1
}

// exact reports whether the read is the answer want describes: no transport
// failure, its status, its Content-Range when want names one, and exactly
// its bytes.
func (r etRead) exact(want etWant) bool {
	return r.Err == "" && r.Status == want.Status && (want.ContentRange == "" || r.ContentRange == want.ContentRange) &&
		r.Length == len(want.Bytes) && r.Mismatch < 0
}

// judgeTransitionRead requires a complete response for the requested range
// before attributing divergent plaintext to the transition.
func judgeTransitionRead(r etRead, want etWant) (bool, error) {
	if r.Err != "" || r.Status != want.Status || (want.ContentRange != "" && r.ContentRange != want.ContentRange) || r.Length != len(want.Bytes) {
		return false, fmt.Errorf("unqualified read of transitioned object: %s", r)
	}
	return r.Mismatch >= 0, nil
}

func (r etRead) String() string {
	s := fmt.Sprintf("%s GET status=%d content-range=%q bytes=%d first-mismatch=%d", r.Kind, r.Status, r.ContentRange, r.Length, r.Mismatch)
	if r.Err != "" {
		s += fmt.Sprintf(" transport-error=%q", r.Err)
	}
	return s
}

// fullWant and rangeWant describe the two reads of the case on whole: a
// full GET and a GET of the bytes first..last.
func fullWant(whole []byte) etWant {
	return etWant{Status: http.StatusOK, Bytes: whole}
}

func rangeWant(whole []byte, first, last int) etWant {
	return etWant{Status: http.StatusPartialContent, ContentRange: fmt.Sprintf("bytes %d-%d/%d", first, last, len(whole)), Start: first, Bytes: whole[first : last+1]}
}
