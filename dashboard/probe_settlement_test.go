package dashboard

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/moby/moby/api/types/container"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// probeSettlementCLI models an already-authorized server handler independently
// of its HTTP client. Canceling the native client cannot cancel serverWrite.
// This is a lifetime/cleanup control, not a native Dashboard reproduction.
type probeSettlementCLI struct {
	*lateWriteCLI
	definitive   bool
	inspectErr   error
	attachCalls  int
	serverWrite  func()
	stopCalls    int
	cleanupCalls int
}

func (f *probeSettlementCLI) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, f.inspectErr
}

func (f *probeSettlementCLI) AttachExec(_ context.Context, args, _ []string) (supervised.Attachment, error) {
	f.attachCalls++
	if !slices.Contains(args, probeScript) {
		return supervised.Attachment{}, errors.New("expected readiness probe")
	}
	reader, writer := io.Pipe()
	var input interface {
		io.Writer
		CloseWrite() error
		Close() error
	}
	if f.definitive {
		input = &settledProbeInput{inputLifetimeStream: inputLifetimeStream{output: writer}, complete: func() { f.keys[multiclusterOption] = fixtureMulticlusterRecord("fixture-session") }}
	} else {
		input = &lateCommandInput{output: writer, accepted: func(string) {
			f.serverWrite = func() { f.keys[multiclusterOption] = fixtureMulticlusterRecord("late-session") }
		}}
	}
	return supervised.Attachment{Input: input, Output: reader, ExitCode: func(context.Context) (int, error) {
		if f.definitive {
			return 0, nil
		}
		return 0, errors.New("native client outcome unavailable")
	}, Release: func() { _ = reader.Close() }}, nil
}

type settledProbeInput struct {
	inputLifetimeStream
	complete func()
}

func (in *settledProbeInput) CloseWrite() error {
	in.complete()
	return in.inputLifetimeStream.CloseWrite()
}

func (f *probeSettlementCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 2 && args[2] == supervised.CancelScript {
		f.stopCalls++
		return f.lateWriteCLI.Exec(ctx, args, opts...)
	}
	f.cleanupCalls++
	return f.restoreCLI.Exec(ctx, args, opts...)
}

func probeSettlementState(f *probeSettlementCLI) *state {
	s := dashboardRestoreState(f.restoreCLI)
	s.cluster = testhelper.TestCluster(f, testhelper.TestConfig{StartupTimeout: time.Second})
	s.accountMarker = "fixture-owned-account"
	f.accountName = s.accountMarker
	f.keys = map[string]string{"mgr/dashboard/crt": string(s.cert), "mgr/dashboard/key": string(s.key), multiclusterOption: fixtureMulticlusterRecord("initial-session")}
	s.certificate = &configKey{name: "mgr/dashboard/crt", installed: s.cert}
	s.privateKey = &configKey{name: "mgr/dashboard/key", installed: s.key}
	s.multicluster = &multiclusterState{}
	return s
}

func TestRestoreRetainsUncertainProbeServerWork(t *testing.T) {
	for _, failures := range []int{0, 2} {
		t.Run(map[int]string{0: "client-stop-succeeds", 2: "client-stop-retry"}[failures], func(t *testing.T) {
			f := &probeSettlementCLI{lateWriteCLI: &lateWriteCLI{restoreCLI: &restoreCLI{}, stopFailures: failures}}
			s := probeSettlementState(f)
			report, err := runProbe(t.Context(), s.cluster, "https://dashboard.invalid:8443", "fixture-fingerprint", s.username, s.password, summaryPath)
			if err == nil || !report.LoginPending || f.serverWrite == nil {
				t.Fatalf("lost response did not retain server uncertainty: %+v %v", report, err)
			}
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(err, &unstopped) != (failures > 0) {
				t.Fatalf("native stop outcome: %v", err)
			}
			s.trackProbe(report, err)
			baseline := maps.Clone(f.keys)
			certificate, key, multicluster := s.certificate, s.privateKey, s.multicluster
			retained := func() {
				t.Helper()
				if s.registry.active != s || s.restored || s.accountMarker != f.accountName || f.accountName == "" || s.certificate != certificate || s.privateKey != key || s.multicluster != multicluster || !maps.Equal(f.keys, baseline) || f.cleanupCalls != 0 {
					t.Fatalf("uncertain login allowed cleanup: active=%t restored=%t cleanup calls=%d", s.registry.active == s, s.restored, f.cleanupCalls)
				}
			}
			if err := s.restore(t.Context()); !errors.Is(err, errLoginUnsettled) {
				t.Fatalf("first restore: %v", err)
			}
			retained()
			if failures > 0 && len(s.probeCommands) != 1 {
				t.Fatal("failed Stop lost its retry handle")
			}
			if err := s.restore(t.Context()); !errors.Is(err, errLoginUnsettled) {
				t.Fatalf("retry restore: %v", err)
			}
			retained()
			if len(s.probeCommands) != 0 || f.stopCalls != failures+1 {
				t.Fatalf("stop retry: pending=%d calls=%d", len(s.probeCommands), f.stopCalls)
			}
			// Even after a confirmed client stop, its already-authorized server handler
			// can finish. Neither a retry nor a different completed probe proves that
			// this old request has finished, so fixture ownership must remain held.
			f.serverWrite()
			if f.keys[multiclusterOption] == baseline[multiclusterOption] {
				t.Fatal("late server model did not write")
			}
			baseline = maps.Clone(f.keys)
			s.trackProbe(probeReport{Finished: true}, nil)
			if err := s.restore(t.Context()); !errors.Is(err, errLoginUnsettled) {
				t.Fatalf("restore after unrelated completed probe: %v", err)
			}
			retained()
		})
	}
}

func TestRestoreAfterDefinitiveProbeCompletion(t *testing.T) {
	f := &probeSettlementCLI{lateWriteCLI: &lateWriteCLI{restoreCLI: &restoreCLI{}}, definitive: true}
	s := probeSettlementState(f)
	report, err := runProbe(t.Context(), s.cluster, "https://dashboard.invalid:8443", "fixture-fingerprint", s.username, s.password, summaryPath)
	if err != nil || !report.Finished || report.LoginPending {
		t.Fatalf("definitive probe: %+v %v", report, err)
	}
	s.trackProbe(report, err)
	if err := s.restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !s.restored || s.registry.active != nil || f.accountName != "" || len(f.keys) != 0 || s.certificate != nil || s.privateKey != nil || s.multicluster != nil || f.stopCalls != 0 {
		t.Fatal("definitive completion did not allow full cleanup")
	}
	if err := s.restore(t.Context()); err != nil {
		t.Fatalf("idempotent restore: %v", err)
	}
}

func TestProbeInspectionRefusalKeepsReadinessAndRestoreRetryable(t *testing.T) {
	refusal := errors.New("transient target inspection refused")
	f := &probeSettlementCLI{lateWriteCLI: &lateWriteCLI{restoreCLI: &restoreCLI{}}, definitive: true, inspectErr: refusal}
	s := probeSettlementState(f)
	probe := func() (probeReport, error) {
		return runProbe(t.Context(), s.cluster, "https://dashboard.invalid:8443", "fixture-fingerprint", s.username, s.password, summaryPath)
	}
	report, err := probe()
	if !errors.Is(err, ceph.ErrEnvironmentUnavailable) || !errors.Is(err, refusal) || f.attachCalls != 0 {
		t.Fatalf("inspection refusal: %+v %v, attach calls %d", report, err, f.attachCalls)
	}
	s.trackProbe(report, err)
	if report.LoginPending || s.loginUncertain {
		t.Fatal("a refused inspection retained a server login that never started")
	}
	f.inspectErr = nil
	report, err = probe()
	if err != nil || !report.Finished || report.LoginPending || f.attachCalls != 1 {
		t.Fatalf("probe retry: %+v %v, attach calls %d", report, err, f.attachCalls)
	}
	s.trackProbe(report, err)
	if err := s.restore(t.Context()); err != nil || !s.restored || s.registry.active != nil {
		t.Fatalf("restore after probe retry: %v", err)
	}
}
