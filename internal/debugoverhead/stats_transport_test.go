package debugoverhead_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	mobycl "github.com/moby/moby/client"
)

// These are source-shaped responses through the actual pinned client, not
// recovered raw bodies of the failed CI run. Moby v28.0.4's direct one-shot
// path constructs Read/counters without assigning ID; the other path adds it.
type debugStatsTransport struct {
	statsBody                 string
	startedAt, afterStartedAt string
	inspects, stats           int
	statsQuery                url.Values
}

func (d *debugStatsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return nil, fmt.Errorf("unexpected method %s", req.Method)
	}
	var body string
	switch {
	case strings.HasSuffix(req.URL.Path, "/containers/osd-container/json"):
		d.inspects++
		start := d.startedAt
		if d.inspects > 1 && d.afterStartedAt != "" {
			start = d.afterStartedAt
		}
		body = fmt.Sprintf(`{"Id":"osd-container","State":{"Running":true,"Paused":false,"StartedAt":%q}}`, start)
	case strings.HasSuffix(req.URL.Path, "/containers/osd-container/stats"):
		d.stats++
		d.statsQuery = req.URL.Query()
		body = d.statsBody
	default:
		return nil, fmt.Errorf("unexpected request path %s", req.URL.Path)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func debugStatsClient(t *testing.T, transport *debugStatsTransport) *mobycl.Client {
	t.Helper()
	client, err := mobycl.New(mobycl.WithHost("http://docker.invalid"), mobycl.WithHTTPClient(&http.Client{Transport: transport}), mobycl.WithAPIVersion("1.48"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestDebugOverheadOneShotStatsResponseShapes(t *testing.T) {
	gen := containerGeneration{"osd-container", "2026-01-01T00:00:00Z"}
	const withoutID = `{"read":"2026-01-01T00:00:01Z","cpu_stats":{"cpu_usage":{"total_usage":123}},"memory_stats":{"usage":456}}`
	for _, tc := range []struct {
		name, body, identity string
	}{
		{"one-shot without ID", withoutID, "endpoint-generation"},
		{"response with ID", `{"id":"osd-container",` + withoutID[1:], "response-and-endpoint-generation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &debugStatsTransport{statsBody: tc.body, startedAt: gen.StartedAt}
			got, err := sampleDebugOverhead(t.Context(), debugStatsClient(t, transport), gen)
			if err != nil || got.Generation != gen || got.StatsIdentity != tc.identity || got.CPUNanos != 123 || got.Memory != 456 {
				t.Fatalf("valid stats shape refused or changed: %+v %v", got, err)
			}
			if transport.inspects != 2 || transport.stats != 1 || transport.statsQuery.Get("one-shot") != "true" || transport.statsQuery.Get("stream") != "false" {
				t.Fatalf("endpoint/generation fencing or pinned one-shot request changed: inspections=%d stats=%d query=%v", transport.inspects, transport.stats, transport.statsQuery)
			}
		})
	}
}

func TestDebugOverheadOneShotRetainsGenerationAndIdentityRefusals(t *testing.T) {
	gen := containerGeneration{"osd-container", "2026-01-01T00:00:00Z"}
	const withoutID = `{"read":"2026-01-01T00:00:01Z","cpu_stats":{"cpu_usage":{"total_usage":123}},"memory_stats":{"usage":456}}`
	for _, tc := range []struct {
		name, body, before, after string
		wantStats                 int
	}{
		{"before request generation changed", withoutID, "another-start", "", 0},
		{"during request generation changed", withoutID, gen.StartedAt, "another-start", 1},
		{"contradictory response identity", `{"id":"another-container",` + withoutID[1:], gen.StartedAt, "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &debugStatsTransport{statsBody: tc.body, startedAt: tc.before, afterStartedAt: tc.after}
			_, err := sampleDebugOverhead(t.Context(), debugStatsClient(t, transport), gen)
			if err == nil || transport.stats != tc.wantStats {
				t.Fatalf("unconfirmed target or contradictory identity accepted: stats=%d error=%v", transport.stats, err)
			}
		})
	}
}
