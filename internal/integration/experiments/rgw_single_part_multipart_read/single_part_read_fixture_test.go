//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_single_part_multipart_read_test

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestParseProcessStat(t *testing.T) {
	const tail = " S 0 1 1 0 -1 4194560 5210 0 12 0 731 248 0 0 20 0 610 0 1234567 2147483648 30000 18446744073709551615 1 1 0 0 0 0 0 4096 18760 0 0 0 17 3 0 0 0 0 0"
	for name, test := range map[string]struct {
		text    string
		want    spProcess
		wantErr bool
	}{
		"radosgw":               {"1 (radosgw)" + tail, spProcess{Start: 1234567, CPU: 979}, false},
		"name with parentheses": {"1 (a) b (c))" + tail, spProcess{Start: 1234567, CPU: 979}, false},
		"no name":               {"1 radosgw" + tail, spProcess{}, true},
		"short":                 {"1 (radosgw) S 0 1 1", spProcess{}, true},
		"not a number":          {"1 (radosgw)" + strings.Replace(tail, " 731 ", " x ", 1), spProcess{}, true},
		"empty":                 {"", spProcess{}, true},
	} {
		got, err := parseProcessStat(test.text)
		if (err != nil) != test.wantErr || got != test.want {
			t.Errorf("%s: %+v, %v; want %+v, error %t", name, got, err, test.want, test.wantErr)
		}
	}
}

func TestJudgeOnlyPart(t *testing.T) {
	const size = 64 << 10
	good := spRead{Method: http.MethodGet, Part: 1, Deadline: 30 * time.Second, Status: http.StatusPartialContent, Length: size, PartsCount: "1", BodyMatches: true}
	with := func(change func(*spRead)) spRead { r := good; change(&r); return r }
	lost := spRead{Method: http.MethodGet, Part: 1, Deadline: 30 * time.Second, TimedOut: true}
	// One core for three seconds at 100 ticks a second is 300 ticks.
	stalled := func(start2 uint64, ticks uint64) *spStall {
		return &spStall{Before: spProcess{Start: 7, CPU: 100}, First: spProcess{Start: 7, CPU: 3100},
			Second: spProcess{Start: start2, CPU: 3100 + ticks}, Window: 3 * time.Second, TicksPerSecond: 100}
	}
	for name, test := range map[string]struct {
		read  spRead
		stall *spStall
		want  string
	}{
		"206 part":          {good, nil, ""},
		"200 part":          {with(func(r *spRead) { r.Status = http.StatusOK }), nil, ""},
		"head without body": {with(func(r *spRead) { r.Method, r.BodyMatches = http.MethodHead, false }), nil, ""},
		// The answers of a gateway that takes the last part id of a
		// single-part manifest for one more than the count.
		"no answer, a core busy":    {lost, stalled(7, 300), "observed defect: "},
		"no answer, most of a core": {lost, stalled(7, 250), "observed defect: "},
		"part count 0":              {with(func(r *spRead) { r.PartsCount = "0" }), nil, "observed defect: "},
		"no part count":             {with(func(r *spRead) { r.PartsCount = "" }), nil, "observed defect: "},
		"part count of two":         {with(func(r *spRead) { r.PartsCount = "2" }), nil, "observed defect: "},
		"invalid part":              {spRead{Method: http.MethodGet, Part: 1, Status: http.StatusBadRequest, Length: 238, Code: "InvalidPart"}, nil, "observed defect: "},
		"another length":            {with(func(r *spRead) { r.Length = size - 1 }), nil, "observed defect: "},
		"other bytes":               {with(func(r *spRead) { r.BodyMatches = false }), nil, "observed defect: "},
		"head, another length":      {with(func(r *spRead) { r.Method, r.BodyMatches, r.Length = http.MethodHead, false, 0 }), nil, "observed defect: "},
		// A timeout alone does not show a loop in the gateway.
		"no answer, not inspected":   {lost, nil, "another fault: "},
		"no answer, idle gateway":    {lost, stalled(7, 6), "another fault: "},
		"no answer, under the bound": {lost, stalled(7, 230), "another fault: "},
		"no answer, new process":     {lost, stalled(8, 300), "another fault: "},
		"no answer, restarted before the samples": {lost, &spStall{Before: spProcess{Start: 6}, First: spProcess{Start: 7}, Second: spProcess{Start: 7, CPU: 300},
			Window: 3 * time.Second, TicksPerSecond: 100}, "another fault: "},
		"no answer, no tick rate":  {lost, &spStall{Second: spProcess{CPU: 300}, Window: 3 * time.Second}, "another fault: "},
		"no answer, cpu went back": {lost, &spStall{First: spProcess{CPU: 300}, Window: 3 * time.Second, TicksPerSecond: 100}, "another fault: "},
		"connection refused":       {spRead{Method: http.MethodGet, Part: 1, Err: "connection refused"}, nil, "another fault: "},
	} {
		got := judgeOnlyPart(test.read, size, test.stall)
		if test.want == "" && got != "" || test.want != "" && !strings.HasPrefix(got, test.want) {
			t.Errorf("%s: %q, want prefix %q", name, got, test.want)
		}
	}
}

func TestJudgeControls(t *testing.T) {
	const size = 5 << 20
	good := spRead{Method: http.MethodGet, Part: 1, Status: http.StatusPartialContent, Length: size, PartsCount: "2", BodyMatches: true}
	with := func(change func(*spRead)) spRead { r := good; change(&r); return r }
	for name, test := range map[string]struct {
		read spRead
		ok   bool
	}{
		"206 part":          {good, true},
		"200 part":          {with(func(r *spRead) { r.Status = http.StatusOK }), true},
		"head without body": {with(func(r *spRead) { r.Method, r.BodyMatches = http.MethodHead, false }), true},
		"part count of one": {with(func(r *spRead) { r.PartsCount = "1" }), false},
		"another length":    {with(func(r *spRead) { r.Length = 1 }), false},
		"other bytes":       {with(func(r *spRead) { r.BodyMatches = false }), false},
		"invalid part":      {spRead{Method: http.MethodGet, Part: 1, Status: http.StatusBadRequest, Length: 238, Code: "InvalidPart"}, false},
		"no answer":         {spRead{Method: http.MethodGet, Part: 1, TimedOut: true}, false},
		"transport error":   {spRead{Method: http.MethodGet, Part: 1, Err: "reset"}, false},
	} {
		if got := judgeControlPart(test.read, size); (got == "") != test.ok || got != "" && !strings.HasPrefix(got, "control failed: ") {
			t.Errorf("%s: %q, want ok %t", name, got, test.ok)
		}
	}
	refused := spRead{Method: http.MethodGet, Part: 2, Status: http.StatusBadRequest, Length: 238, Code: "InvalidPart"}
	for name, test := range map[string]struct {
		read spRead
		ok   bool
	}{
		"invalid part":        {refused, true},
		"access denied":       {spRead{Method: http.MethodGet, Part: 2, Status: http.StatusForbidden, Code: "AccessDenied"}, false},
		"service unavailable": {spRead{Method: http.MethodGet, Part: 2, Status: http.StatusServiceUnavailable, Code: "ServiceUnavailable"}, false},
		"other 400":           {func() spRead { r := refused; r.Code = "InvalidArgument"; return r }(), false},
		"400 without code":    {func() spRead { r := refused; r.Code = ""; return r }(), false},
		"answered":            {spRead{Method: http.MethodGet, Part: 2, Status: http.StatusPartialContent, Length: 1, PartsCount: "1"}, false},
		"head":                {func() spRead { r := refused; r.Method = http.MethodHead; return r }(), false},
		"no answer":           {spRead{Method: http.MethodGet, Part: 2, TimedOut: true}, false},
	} {
		if got := judgeBeyond(test.read); (got == "") != test.ok || got != "" && !strings.HasPrefix(got, "control failed: ") {
			t.Errorf("%s: %q, want ok %t", name, got, test.ok)
		}
	}
}
