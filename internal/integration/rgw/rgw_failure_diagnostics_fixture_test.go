//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
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
	destination.logs = "headfrst" + strings.Repeat("x", 48) + "lastline"
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
		"destination recent RGW diagnostics: headfrst\n[log bytes omitted here]\nlastline [truncated: kept the first and last 8 bytes, omitted 48 bytes between them; read stopped: connection reset]",
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

// A failed stats synchronization names how it ended; an unconfirmed stop and
// the supervisor's own stop are not the test's deadline, and a native exit
// keeps its code and errno.
func TestRGWSyncStatsEndClassifiesTheEnd(t *testing.T) {
	stopped := &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "radosgw-admin", Err: errors.New("radosgw-admin still ran after 105s and was stopped")}
	exited := fmt.Errorf("radosgw-admin command failed: %w", &ceph.NativeFailure{Program: "radosgw-admin", ExitCode: 2, Errno: "ENOENT"})
	for _, tc := range []struct {
		err, test, phase error
		want             string
	}{
		{&ceph.NativeCommandUnstoppedError{}, nil, context.DeadlineExceeded, "its stop was not confirmed"},
		{stopped, nil, nil, "the gateway's supervisor stopped it"},
		{context.DeadlineExceeded, context.DeadlineExceeded, context.DeadlineExceeded, "the test's context ended the call"},
		{context.DeadlineExceeded, nil, context.DeadlineExceeded, "the phase deadline ended the call"},
		{exited, nil, nil, `radosgw-admin exited 2 (errno "ENOENT")`},
		{errors.New("inspect failed"), nil, nil, "before a native result"},
	} {
		if got := harness.RGWSyncStatsEnd(tc.err, tc.test, tc.phase); !strings.Contains(got, tc.want) {
			t.Errorf("%v: %q, want %q", tc.err, got, tc.want)
		}
	}
}

// rgwDataSyncCrashLog is the shape of a gateway's data-sync SIGSEGV report:
// the untimestamped standard-error rendering, then the same report in the
// dump of recent events with the daemon's time.
const rgwDataSyncCrashLog = `2026-10-28T23:20:39.986+0000 7f22707e3640  0 rgw data sync: ERROR: no valid endpoint
*** Caught signal (Segmentation fault) **
 in thread 7f22707e3640 thread_name:data-sync
 ceph version 20.2.4 (0000000000000000000000000000000000000000) tentacle (stable)
 1: /lib64/libc.so.6(+0x3e730) [0x7f2296a3e730]
 2: (RGWCoroutinesStack::operate(DoutPrefixProvider const*, RGWCoroutinesEnv*)+0x3d) [0x55d1c0000001]
 3: (RGWCoroutinesManager::run(DoutPrefixProvider const*, std::__cxx11::list<RGWCoroutinesStack*>&)+0x2a1) [0x55d1c0000002]
 4: (RGWRemoteDataLog::run_sync(DoutPrefixProvider const*, int)+0x3f6) [0x55d1c0000003]
 NOTE: a copy of the executable, or ` + "`objdump -rdS <executable>`" + ` is needed to interpret this.
--- begin dump of recent events ---
2026-10-28T23:21:09.821+0000 7f22707e3640 -1 *** Caught signal (Segmentation fault) **
 in thread 7f22707e3640 thread_name:data-sync
 ceph version 20.2.4 (0000000000000000000000000000000000000000) tentacle (stable)
 1: /lib64/libc.so.6(+0x3e730) [0x7f2296a3e730]
 2: (RGWCoroutinesStack::operate(DoutPrefixProvider const*, RGWCoroutinesEnv*)+0x3d) [0x55d1c0000001]
 3: (RGWCoroutinesManager::run(DoutPrefixProvider const*, std::__cxx11::list<RGWCoroutinesStack*>&)+0x2a1) [0x55d1c0000002]
 4: (RGWRemoteDataLog::run_sync(DoutPrefixProvider const*, int)+0x3f6) [0x55d1c0000003]
 NOTE: a copy of the executable is needed to interpret this.
`

// The excerpt keeps the daemon's clock apart from Docker's, counts
// renderings rather than crashes, binds them to the inspected generation and
// classifies a data-sync crash only by its frames.
func TestRGWCrashExcerptKeepsClocksAndRenderingsApart(t *testing.T) {
	excerpt := rgwCrashExcerpt([]byte(rgwDataSyncCrashLog), false, "2026-10-28T23:19:31.63Z", "2026-10-28T23:21:17.455Z")
	for _, want := range []string{
		"2 fatal-signal renderings in the captured log (one crash prints two; renderings are not a crash count)",
		"[1] signal=Segmentation fault thread=data-sync no native timestamp",
		"[2] signal=Segmentation fault thread=data-sync native time 2026-10-28T23:21:09.821Z, in the bound generation, 7.634s before Docker's finish time",
		"RGWRemoteDataLog::run_sync(DoutPrefixProvider const*, int)+0x3f6",
		"classification: native RGW data-sync crash",
	} {
		if !strings.Contains(excerpt, want) {
			t.Fatalf("lacks %q in %s", want, excerpt)
		}
	}
	renderings := rgwCrashRenderings([]byte(rgwDataSyncCrashLog))
	if len(renderings) != 2 || renderings[0].fingerprint() != renderings[1].fingerprint() || !renderings[0].Complete || strings.Contains(excerpt, "0x55d1") {
		t.Fatalf("one report, two renderings, same fingerprint, no load addresses: %+v", renderings)
	}
	// A report of an earlier generation is not this generation's.
	if excerpt := rgwCrashExcerpt([]byte(rgwDataSyncCrashLog), false, "2026-10-28T23:25:00Z", "0001-01-01T00:00:00Z"); !strings.Contains(excerpt, "before the bound generation started") || strings.Contains(excerpt, "Docker's finish") {
		t.Fatalf("earlier generation: %s", excerpt)
	}
	// No rendering is not proof of no crash, and truncation is named.
	if excerpt := rgwCrashExcerpt([]byte("rgw sync: ok\n"), true, "", ""); excerpt != "crash excerpt: no fatal-signal rendering in the captured log; the capture was truncated" {
		t.Fatalf("no signal: %s", excerpt)
	}
	// A backtrace cut by the log window, and a crash in another thread.
	cut := "*** Caught signal (Aborted) **\n in thread 7f thread_name:rgw_frontend\n 1: (abort()+0x1) [0x1]\n" + rgwLogOmitted + "tail\n"
	excerpt = rgwCrashExcerpt([]byte(cut), true, "2026-10-28T23:19:31.63Z", "")
	if !strings.Contains(excerpt, "thread=rgw_frontend no native timestamp") || !strings.Contains(excerpt, "(abort()+0x1) [backtrace cut]") || !strings.Contains(excerpt, "unclassified cause") {
		t.Fatalf("cut backtrace: %s", excerpt)
	}
}
