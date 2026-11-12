//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_single_part_multipart_read_test

import (
	"net/http"
	"net/http/httptest"
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
	lost := spRead{Method: http.MethodGet, Part: 1, Deadline: 30 * time.Second, TimedOut: true, Phase: spSent}
	const lostText = "observed defect: GET partNumber=1 not ended within 30s (the request was written to a connection and no response header arrived), and the gateway process is the one that ran before the request. The cause is not established: "
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
		// A read that was written to the gateway and did not end fails
		// the contract whatever the process-wide CPU time is: busy, idle,
		// or busy with an earlier unended request.
		"sent, a core busy":        {lost, stalled(7, 300), lostText},
		"sent, idle gateway":       {lost, stalled(7, 6), lostText},
		"sent, earlier one counts": {lost, func() *spStall { s := stalled(7, 600); s.Earlier = 1; return s }(), lostText + "the whole process used 2.00 of a core in the 3s after the deadline, with 1 earlier unended read(s)"},
		"sent, no tick rate":       {lost, &spStall{Second: spProcess{CPU: 300}, Window: 3 * time.Second}, lostText},
		// So does one whose header arrived; the status received is kept.
		"header, stalled body": {with(func(r *spRead) { r.TimedOut, r.Phase, r.BodyBytes, r.BodyMatches = true, spBody, 3, false }), stalled(7, 6),
			"observed defect: GET partNumber=1 not ended within 30s (the response header arrived and the body did not end): status=206 content-length=65536 parts-count=\"1\" body-bytes-read=3, and the gateway process"},
		// A request that the client never wrote shows nothing about the
		// gateway, however the gateway looks.
		"unsent, idle gateway":  {with(func(r *spRead) { *r = lost; r.Phase = spUnsent }), stalled(7, 6), "another fault: GET partNumber=1 not ended within 30s (the request was not written to a connection). Nothing shows that the request reached the gateway"},
		"unsent, a core busy":   {with(func(r *spRead) { *r = lost; r.Phase = spUnsent }), stalled(7, 300), "another fault: "},
		"unsent, not inspected": {with(func(r *spRead) { *r = lost; r.Phase = spUnsent }), nil, "another fault: "},
		"no phase recorded":     {with(func(r *spRead) { *r = lost; r.Phase = "" }), stalled(7, 300), "another fault: "},
		// Wrong answers.
		"part count 0":         {with(func(r *spRead) { r.PartsCount = "0" }), nil, "observed defect: the only part of the upload is answered with a part count"},
		"no part count":        {with(func(r *spRead) { r.PartsCount = "" }), nil, "observed defect: "},
		"part count of two":    {with(func(r *spRead) { r.PartsCount = "2" }), nil, "observed defect: "},
		"invalid part":         {spRead{Method: http.MethodGet, Part: 1, Status: http.StatusBadRequest, Length: 238, Code: "InvalidPart"}, nil, "observed defect: the only part of the upload is not answered"},
		"another length":       {with(func(r *spRead) { r.Length = size - 1 }), nil, "observed defect: "},
		"other bytes":          {with(func(r *spRead) { r.BodyMatches = false }), nil, "observed defect: "},
		"head, another length": {with(func(r *spRead) { r.Method, r.BodyMatches, r.Length = http.MethodHead, false, 0 }), nil, "observed defect: "},
		// The gateway that should have answered cannot be named.
		"sent, not inspected": {lost, nil, "another fault: "},
		"sent, new process":   {lost, stalled(8, 300), "another fault: "},
		"sent, restarted before the samples": {lost, &spStall{Before: spProcess{Start: 6}, First: spProcess{Start: 7}, Second: spProcess{Start: 7, CPU: 300},
			Window: 3 * time.Second, TicksPerSecond: 100}, "another fault: "},
		"not sent":           {spRead{Method: http.MethodHead, Part: 1, NotSent: "the GET did not end"}, nil, "another fault: HEAD partNumber=1 not sent: the GET did not end"},
		"connection refused": {spRead{Method: http.MethodGet, Part: 1, Err: "connection refused"}, nil, "another fault: "},
	} {
		// The verdict text: the error of another fault, "observed defect: "
		// and the detail of a symptom, and "" for the part's own answer,
		// whose detail must not be empty.
		symptom, detail, err := judgeOnlyPart(test.read, size, test.stall)
		got := ""
		switch {
		case err != nil:
			got = err.Error()
		case symptom:
			got = "observed defect: " + detail
		case detail == "":
			got = "absent symptom without a detail"
		}
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
		"not sent":          {spRead{Method: http.MethodGet, Part: 1, NotSent: "x"}, false},
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
		"not sent":            {spRead{Method: http.MethodGet, Part: 2, NotSent: "x"}, false},
	} {
		if got := judgeBeyond(test.read); (got == "") != test.ok || got != "" && !strings.HasPrefix(got, "control failed: ") {
			t.Errorf("%s: %q, want ok %t", name, got, test.ok)
		}
	}
}

// sendPartRead tells apart a request that never left the client, one that
// was written and got no header, and one whose header arrived before the
// body stalled, and keeps the status of the last.
func TestSendPartReadPhases(t *testing.T) {
	const deadline = 300 * time.Millisecond
	part := []byte("0123456789")
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/silent":
			<-release
		case "/stalled":
			w.Header().Set("Content-Length", "10")
			w.Header().Set("X-Amz-Mp-Parts-Count", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(part[:3])
			w.(http.Flusher).Flush()
			<-release
		case "/refused":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("<Error><Code>InvalidPart</Code></Error>"))
		default:
			w.Header().Set("X-Amz-Mp-Parts-Count", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(part)
		}
	}))
	defer server.Close()
	defer close(release)
	// A transport that never gets as far as a connection.
	stuck := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	code := func(body []byte) string {
		if strings.Contains(string(body), "InvalidPart") {
			return "InvalidPart"
		}
		return ""
	}
	for name, test := range map[string]struct {
		client *http.Client
		path   string
		want   spRead
	}{
		"expires before delivery": {stuck, "/answered", spRead{TimedOut: true, Phase: spUnsent}},
		"written, no header":      {server.Client(), "/silent", spRead{TimedOut: true, Phase: spSent}},
		"header, stalled body":    {server.Client(), "/stalled", spRead{TimedOut: true, Phase: spBody, Status: http.StatusPartialContent, Length: 10, PartsCount: "1", BodyBytes: 3}},
		"answered":                {server.Client(), "/answered", spRead{Status: http.StatusPartialContent, Length: 10, PartsCount: "1", BodyMatches: true}},
		"refused":                 {server.Client(), "/refused", spRead{Status: http.StatusBadRequest, Length: 39, Code: "InvalidPart"}},
	} {
		request, err := http.NewRequest(http.MethodGet, server.URL+test.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		test.want.Method, test.want.Part, test.want.Deadline = http.MethodGet, 1, deadline
		got := sendPartRead(t.Context(), test.client, request, spRead{Method: http.MethodGet, Part: 1, Deadline: deadline}, part, code)
		if got != test.want {
			t.Errorf("%s: %+v, want %+v", name, got, test.want)
		}
	}
	// Another transport failure is not a deadline.
	server.CloseClientConnections()
	request, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1:1/unreachable", nil)
	if got := sendPartRead(t.Context(), &http.Client{}, request, spRead{Method: http.MethodGet, Part: 1, Deadline: deadline}, part, code); got.TimedOut || got.Err == "" {
		t.Errorf("refused connection: %+v", got)
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
