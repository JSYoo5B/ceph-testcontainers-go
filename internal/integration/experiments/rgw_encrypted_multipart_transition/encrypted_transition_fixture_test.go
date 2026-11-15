//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_encrypted_multipart_transition_test

import (
	"bytes"
	"errors"
	"net/http"
	"testing"
)

func TestFirstDifference(t *testing.T) {
	for name, test := range map[string]struct {
		got, want string
		index     int
	}{
		"equal":         {"abcdef", "abcdef", -1},
		"empty":         {"", "", -1},
		"first byte":    {"xbcdef", "abcdef", 0},
		"middle byte":   {"abcXef", "abcdef", 3},
		"short body":    {"abc", "abcdef", 3},
		"long body":     {"abcdefgh", "abcdef", 6},
		"nothing read":  {"", "abcdef", 0},
		"differs early": {"aXcdeY", "abcdef", 1},
	} {
		if got := firstDifference([]byte(test.got), []byte(test.want)); got != test.index {
			t.Errorf("%s: firstDifference(%q, %q) = %d, want %d", name, test.got, test.want, got, test.index)
		}
	}
}

// A read is exact only with the expected status, range header and every
// expected byte, without a transport failure. A corrupted byte, a cut body,
// an error answer or a wrong Content-Range is a symptom; the first
// mismatch is reported as an object offset.
func TestExact(t *testing.T) {
	whole := bytes.Repeat([]byte("0123456789"), 40)
	full := fullWant(whole)
	ranged := rangeWant(whole, 100, 260)
	corrupt := func(b []byte, at int) []byte {
		c := bytes.Clone(b)
		c[at] ^= 0xff
		return c
	}
	for name, test := range map[string]struct {
		want     etWant
		read     etRead
		ok       bool
		mismatch int
	}{
		"full":                  {full, observe("full", full, http.StatusOK, "", whole, nil), true, -1},
		"range":                 {ranged, observe("range", ranged, http.StatusPartialContent, "bytes 100-260/400", whole[100:261], nil), true, -1},
		"full corrupted":        {full, observe("full", full, http.StatusOK, "", corrupt(whole, 333), nil), false, 333},
		"range corrupted":       {ranged, observe("range", ranged, http.StatusPartialContent, "bytes 100-260/400", corrupt(whole[100:261], 50), nil), false, 150},
		"full cut off":          {full, observe("full", full, http.StatusOK, "", whole[:128], errors.New("unexpected EOF")), false, 128},
		"full truncated":        {full, observe("full", full, http.StatusOK, "", whole[:128], nil), false, 128},
		"server error":          {full, observe("full", full, http.StatusInternalServerError, "", []byte("<Error/>"), nil), false, 0},
		"range answered as 200": {ranged, observe("range", ranged, http.StatusOK, "", whole, nil), false, 261},
		"other content range":   {ranged, observe("range", ranged, http.StatusPartialContent, "bytes 100-260/401", whole[100:261], nil), false, -1},
		"transport failure":     {full, observe("full", full, 0, "", nil, errors.New("connection reset")), false, 0},
	} {
		if got := test.read.exact(test.want); got != test.ok {
			t.Errorf("%s: exact(%s) = %t, want %t", name, test.read, got, test.ok)
		}
		if test.read.Mismatch != test.mismatch {
			t.Errorf("%s: first mismatch %d, want %d", name, test.read.Mismatch, test.mismatch)
		}
	}
}
