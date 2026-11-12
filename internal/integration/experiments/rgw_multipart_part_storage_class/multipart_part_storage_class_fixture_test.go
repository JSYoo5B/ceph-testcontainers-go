//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_multipart_part_storage_class_test

import (
	"net/http"
	"strings"
	"testing"
)

// A part read passes only with the part's own length, the object's part
// count and, for a GET, the part's bytes. The 400 InvalidPart that the COLD
// object answers on every tested release, a whole-object length and a
// missing part count are failures.
func TestPartOK(t *testing.T) {
	part := make([]byte, 5<<20)
	good := mpRead{Method: http.MethodGet, Part: 1, Status: http.StatusPartialContent, Length: int64(len(part)), PartsCount: "2", BodyMatches: true}
	for name, test := range map[string]struct {
		read mpRead
		ok   bool
	}{
		"206 part":            {good, true},
		"200 part":            {func() mpRead { r := good; r.Status = http.StatusOK; return r }(), true},
		"head without body":   {func() mpRead { r := good; r.Method, r.BodyMatches = http.MethodHead, false; return r }(), true},
		"invalid part":        {mpRead{Method: http.MethodGet, Part: 1, Status: http.StatusBadRequest, Length: 238, Code: "InvalidPart"}, false},
		"invalid part head":   {mpRead{Method: http.MethodHead, Part: 1, Status: http.StatusBadRequest, Length: 238}, false},
		"whole-object length": {func() mpRead { r := good; r.Length = 5308416; return r }(), false},
		"no parts count":      {func() mpRead { r := good; r.PartsCount = ""; return r }(), false},
		"other bytes":         {func() mpRead { r := good; r.BodyMatches = false; return r }(), false},
	} {
		if got := partOK(test.read, part); got != test.ok {
			t.Errorf("%s: partOK(%s) = %t, want %t", name, test.read, got, test.ok)
		}
	}
}

// The out-of-range control passes only with the refusal every measured build
// gives, 400 InvalidPart; other errors leave the control inconclusive.
func TestBeyondRefused(t *testing.T) {
	refused := mpRead{Method: http.MethodGet, Part: 3, Status: http.StatusBadRequest, Length: 238, Code: "InvalidPart"}
	for name, test := range map[string]struct {
		read mpRead
		ok   bool
	}{
		"invalid part":        {refused, true},
		"access denied":       {mpRead{Method: http.MethodGet, Part: 3, Status: http.StatusForbidden, Code: "AccessDenied"}, false},
		"signature mismatch":  {mpRead{Method: http.MethodGet, Part: 3, Status: http.StatusForbidden, Code: "SignatureDoesNotMatch"}, false},
		"service unavailable": {mpRead{Method: http.MethodGet, Part: 3, Status: http.StatusServiceUnavailable, Code: "ServiceUnavailable"}, false},
		"internal error":      {mpRead{Method: http.MethodGet, Part: 3, Status: http.StatusInternalServerError, Code: "InternalError"}, false},
		"other 400":           {func() mpRead { r := refused; r.Code = "InvalidArgument"; return r }(), false},
		"400 without code":    {func() mpRead { r := refused; r.Code = ""; return r }(), false},
		"answered":            {mpRead{Method: http.MethodGet, Part: 3, Status: http.StatusPartialContent, Length: 1, PartsCount: "2"}, false},
		"head":                {func() mpRead { r := refused; r.Method = http.MethodHead; return r }(), false},
	} {
		if got := beyondRefused(test.read); got != test.ok {
			t.Errorf("%s: beyondRefused(%s) = %t, want %t", name, test.read, got, test.ok)
		}
	}
}

func TestJudgePartRead(t *testing.T) {
	part := make([]byte, 64<<10)
	good := mpRead{Method: http.MethodGet, Part: 2, Status: http.StatusPartialContent, Length: int64(len(part)), PartsCount: "2", BodyMatches: true}
	for name, test := range map[string]struct {
		read    mpRead
		symptom bool
		fault   bool
	}{
		"part":                     {good, false, false},
		"head of the part":         {func() mpRead { r := good; r.Method, r.BodyMatches = http.MethodHead, false; return r }(), false, false},
		"invalid part":             {mpRead{Method: http.MethodGet, Part: 2, Status: http.StatusBadRequest, Length: 238, Code: "InvalidPart"}, true, false},
		"invalid part head":        {mpRead{Method: http.MethodHead, Part: 2, Status: http.StatusBadRequest, Length: 238}, true, false},
		"other 400":                {mpRead{Method: http.MethodGet, Part: 2, Status: http.StatusBadRequest, Code: "InvalidArgument"}, false, true},
		"access denied":            {mpRead{Method: http.MethodGet, Part: 2, Status: http.StatusForbidden, Code: "AccessDenied"}, false, true},
		"service unavailable head": {mpRead{Method: http.MethodHead, Part: 2, Status: http.StatusServiceUnavailable}, false, true},
		"whole-object length":      {func() mpRead { r := good; r.Length = 5308416; return r }(), false, true},
		"other bytes":              {func() mpRead { r := good; r.BodyMatches = false; return r }(), false, true},
	} {
		symptom, detail, err := judgePartRead(test.read, part)
		if test.fault {
			if err == nil || !strings.HasPrefix(err.Error(), "another fault: ") {
				t.Errorf("%s: %t %q %v, want another fault", name, symptom, detail, err)
			}
			continue
		}
		if err != nil || symptom != test.symptom || detail == "" {
			t.Errorf("%s: symptom=%t %q %v, want symptom=%t", name, symptom, detail, err, test.symptom)
		}
	}
}
