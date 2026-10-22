//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// diagnosticGateway answers a failure collection and records its calls in
// the order of the shared log.
type diagnosticGateway struct {
	name     string
	calls    *[]string
	mu       *sync.Mutex
	state    *container.State
	stateErr error
	hang     bool // admin queries wait for their context to end
	logs     string
	logsErr  error
	readErr  error
}

func (g *diagnosticGateway) record(call string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	*g.calls = append(*g.calls, g.name+" "+call)
}

func (g *diagnosticGateway) State(ctx context.Context) (*container.State, error) {
	g.record("state")
	return g.state, g.stateErr
}

func (g *diagnosticGateway) Admin(ctx context.Context, args ...string) ([]byte, error) {
	g.record("admin " + strings.Join(args, " "))
	if g.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return []byte(`{"secret_key":"hidden","status":"` + args[0] + ` ok"}`), nil
}

type diagnosticLogs struct {
	io.Reader
	err error
}

func (l diagnosticLogs) Read(p []byte) (int, error) {
	n, err := l.Reader.Read(p)
	if err == io.EOF && l.err != nil {
		return n, l.err
	}
	return n, err
}

func (diagnosticLogs) Close() error { return nil }

func (g *diagnosticGateway) Logs(context.Context) (io.ReadCloser, error) {
	g.record("logs")
	if g.logsErr != nil {
		return nil, g.logsErr
	}
	return diagnosticLogs{Reader: strings.NewReader(g.logs), err: g.readErr}, nil
}

func diagnosticSites(gateways ...*diagnosticGateway) []rgwDiagnosticSite {
	var sites []rgwDiagnosticSite
	for _, g := range gateways {
		sites = append(sites, rgwDiagnosticSite{Name: g.name, Gateway: g, Commands: [][]string{{"sync", "status"}, {"sync", "error", "list"}},
			Redact: func(data []byte) string { return strings.ReplaceAll(string(data), "hidden", "[redacted]") }})
	}
	return sites
}

func newDiagnosticGateways() (*diagnosticGateway, *diagnosticGateway, *[]string) {
	calls, mu := &[]string{}, &sync.Mutex{}
	source := &diagnosticGateway{name: "source", calls: calls, mu: mu, logs: "rgw data sync: started\n",
		state: &container.State{Status: "exited", ExitCode: 139, StartedAt: "t0", FinishedAt: "t1"}}
	destination := &diagnosticGateway{name: "destination", calls: calls, mu: mu, logs: "rgw sync: waiting\n",
		state: &container.State{Status: "running", Running: true, StartedAt: "t0"}}
	return source, destination, calls
}

// Both sites' container states come first: an exited source is recorded
// even when every later admin query of it stalls until its own bound.
func TestRGWFailureDiagnosticsReadStatesBeforeStallingAdminQueries(t *testing.T) {
	source, destination, calls := newDiagnosticGateways()
	source.hang = true
	bounds := rgwDiagnosticBounds{Total: 5 * time.Second, Step: 40 * time.Millisecond, Log: time.Second, LogBytes: 1 << 10}
	started := time.Now()
	lines := rgwFailureDiagnostics(t.Context(), bounds, diagnosticSites(source, destination))
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("stalled admin queries took %s", elapsed)
	}
	if !slices.Equal((*calls)[:2], []string{"source state", "destination state"}) {
		t.Fatalf("calls %v", *calls)
	}
	want := []string{
		"source gateway container: status=exited running=false exit=139 oom=false started=t0 finished=t1",
		"destination gateway container: status=running running=true exit=0 oom=false started=t0 finished=",
		"source native sync status diagnostic: unavailable: context deadline exceeded",
		"source native sync error list diagnostic: unavailable: context deadline exceeded",
		"source recent RGW diagnostics: rgw data sync: started\n",
		`destination native sync status diagnostic: {"secret_key":"[redacted]","status":"sync ok"}`,
		`destination native sync error list diagnostic: {"secret_key":"[redacted]","status":"sync ok"}`,
		"destination recent RGW diagnostics: rgw sync: waiting\n",
	}
	if !slices.Equal(lines, want) {
		t.Fatalf("lines\n%s", strings.Join(lines, "\n"))
	}
}

// A failed state, log or read and a truncated log are each reported; the
// collection runs although the test's own context already ended.
func TestRGWFailureDiagnosticsReportEveryUnavailableField(t *testing.T) {
	source, destination, _ := newDiagnosticGateways()
	source.stateErr = errors.New("inspect timeout")
	source.logsErr = errors.New("logs unavailable")
	destination.state = nil
	destination.logs = strings.Repeat("x", 64)
	destination.readErr = errors.New("connection reset")
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	bounds := rgwDiagnosticBounds{Total: 5 * time.Second, Step: time.Second, Log: time.Second, LogBytes: 16}
	lines := rgwFailureDiagnostics(ended, bounds, diagnosticSites(source, destination))
	for _, want := range []string{
		"source gateway container: state unavailable: inspect timeout",
		"destination gateway container: state unavailable: no state",
		`source native sync status diagnostic: {"secret_key":"[redacted]","status":"sync ok"}`,
		"source recent RGW diagnostics: unavailable: logs unavailable",
		"destination recent RGW diagnostics: xxxxxxxxxxxxxxxx [truncated at 16 bytes]",
	} {
		if !slices.Contains(lines, want) {
			t.Fatalf("lacks %q in\n%s", want, strings.Join(lines, "\n"))
		}
	}
	destination.logs = "short"
	lines = rgwFailureDiagnostics(ended, bounds, diagnosticSites(destination))
	if last := lines[len(lines)-1]; last != "destination recent RGW diagnostics: short [read stopped: connection reset]" {
		t.Fatalf("read error %q", last)
	}
}
