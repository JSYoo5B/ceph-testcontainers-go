//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_encrypted_multipart_transition_test

import (
	"bytes"
	"context"
	"errors"
	"io"
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
// expected byte. The first mismatch is reported as an object offset, and
// failures of the read are separate from observed plaintext corruption.
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

func TestJudgeTransitionRead(t *testing.T) {
	full := fullWant([]byte("abcdef"))
	rangeRead := rangeWant([]byte("abcdef"), 1, 3)
	for _, tc := range []struct {
		name                  string
		want                  etWant
		status                int
		contentRange, body    string
		err                   error
		reproduced, wantError bool
	}{
		{"exact", full, http.StatusOK, "", "abcdef", nil, false, false},
		{"corruption", full, http.StatusOK, "", "abcXYZ", nil, true, false},
		{"range corruption", rangeRead, http.StatusPartialContent, rangeRead.ContentRange, "bXd", nil, true, false},
		{"wrong range", rangeRead, http.StatusPartialContent, "bytes 2-4/6", "cde", nil, false, true},
		{"short body", full, http.StatusOK, "", "ab", nil, false, true},
		{"body interrupted", full, http.StatusOK, "", "ab", io.ErrUnexpectedEOF, false, true},
		{"expired read", full, http.StatusOK, "", "abcXYZ", context.DeadlineExceeded, false, true},
		{"canceled request", full, 0, "", "", context.Canceled, false, true},
		{"reset before headers", full, 0, "", "", errors.New("connection reset"), false, true},
		{"authentication failure", full, http.StatusForbidden, "", "<Error/>", nil, false, true},
		{"server failure", full, http.StatusInternalServerError, "", "<Error/>", nil, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := judgeTransitionRead(observe(tc.name, tc.want, tc.status, tc.contentRange, []byte(tc.body), tc.err), tc.want)
			if got != tc.reproduced || (err != nil) != tc.wantError {
				t.Fatalf("reproduced=%t err=%v, want reproduced=%t error=%t", got, err, tc.reproduced, tc.wantError)
			}
		})
	}
}
