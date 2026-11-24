package supervised

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// finishedAttachment is an exec that already ended with a complete report.
// Closing its connection runs onClose, which can cancel the caller's
// context just as RunInput takes the report.
type finishedAttachment struct {
	onClose func()
	once    sync.Once
}

func (a *finishedAttachment) Write(p []byte) (int, error) { return len(p), nil }

func (a *finishedAttachment) CloseWrite() error { return nil }

func (a *finishedAttachment) Close() error {
	a.once.Do(a.onClose)
	return nil
}

// finishedContainer controls attached output and counts marker cancellations.
// Like Docker, inspection fails once its context ended.
type finishedContainer struct {
	testcontainers.Container
	onClose   func()
	noReport  bool
	code      int
	unknown   bool
	exitCode  func(context.Context) (int, error)
	output    io.Reader
	cancelRun func(context.Context) error
	cancelErr error
	mu        sync.Mutex
	cancels   int
}

func (c *finishedContainer) AttachExec(context.Context, []string, []string) (Attachment, error) {
	report, _ := json.Marshal(map[string]any{"schema_version": 1, "stdout": base64.StdEncoding.EncodeToString([]byte("done")), "stderr": "",
		"exit_code": 0, "timed_out": false, "stdout_truncated": false, "stderr_truncated": false,
		"input_received": 0, "input_delivered": 0, "input_eof": false})
	output := append(binary.BigEndian.AppendUint32([]byte{1, 0, 0, 0}, uint32(len(report))), report...)
	if c.noReport {
		output = nil
	}
	var reportStream io.Reader = bytes.NewReader(output)
	if c.output != nil {
		reportStream = c.output
	}
	return Attachment{
		Input:  &finishedAttachment{onClose: c.onClose},
		Output: reportStream,
		ExitCode: func(ctx context.Context) (int, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if c.exitCode != nil {
				return c.exitCode(ctx)
			}
			if c.unknown {
				return 0, errors.New("engine unreachable")
			}
			return c.code, nil
		},
		Release: func() {},
	}, nil
}

func (c *finishedContainer) Exec(ctx context.Context, _ []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.mu.Lock()
	c.cancels++
	c.mu.Unlock()
	if c.cancelRun != nil {
		return 0, nil, c.cancelRun(ctx)
	}
	return 0, bytes.NewReader(nil), c.cancelErr
}

// A cancellation that becomes visible after RunInput took the complete
// output must not make it wait for that output again.
func TestRunInputCancellationAfterTheOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctr := &finishedContainer{onClose: cancel}
	writes, closed := make(chan InputWrite), make(chan struct{})
	finished := make(chan struct{})
	returned := make(chan error, 1)
	go func() {
		_, err := RunInput(ctx, ctr, "probe", time.Minute, 1024, 1024,
			Input{Limit: 8, Writes: writes, Closed: closed, Finished: finished}, "true")
		returned <- err
	}()
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunInput returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunInput stuck after its output completion coincided with cancellation")
	}
	select {
	case <-finished:
	default:
		t.Fatal("RunInput returned without finishing its input")
	}
	if ctr.cancels != 0 {
		t.Fatalf("a supervisor that had ended was canceled %d times", ctr.cancels)
	}
}

func TestRunInputEOFCancellationBudgets(t *testing.T) {
	for _, tc := range []struct {
		name            string
		outputOpen      bool
		terminalInspect bool
		elapsed         time.Duration
	}{
		{name: "output still open", outputOpen: true, elapsed: 10 * time.Second},
		{name: "inspection times out first", elapsed: 20 * time.Second},
		{name: "terminal inspection after cancellation", terminalInspect: true, elapsed: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctr := &finishedContainer{
					onClose: func() {},
					exitCode: func(inspectCtx context.Context) (int, error) {
						if tc.outputOpen {
							t.Error("cancellation with input still attached inspected an exit")
						}
						cancel()
						if tc.terminalInspect {
							time.Sleep(time.Second)
							return 0, nil
						}
						<-inspectCtx.Done()
						return 0, inspectCtx.Err()
					},
					cancelRun: func(ctx context.Context) error {
						<-ctx.Done()
						return ctx.Err()
					},
				}
				cancelAt := time.Now()
				if tc.outputOpen {
					reader, writer := io.Pipe()
					defer func() { _ = reader.Close() }()
					ctr.output = reader
					ctr.onClose = func() { _ = writer.Close() }
					cancelAt = cancelAt.Add(time.Second)
					go func() {
						time.Sleep(time.Second)
						cancel()
					}()
				}
				report, err := RunInput(ctx, ctr, "probe", time.Minute, 1024, 1024, Input{
					Limit: 8, Writes: make(chan InputWrite), Closed: make(chan struct{}), Finished: make(chan struct{}),
				}, "true")
				if !errors.Is(err, context.Canceled) || len(report.Stdout) != 0 || time.Since(cancelAt) != tc.elapsed {
					t.Fatalf("RunInput after %v returned %+v, %v; want canceled after %s", time.Since(cancelAt), report, err, tc.elapsed)
				}
				marker, unconfirmed := Unconfirmed(err)
				if tc.terminalInspect {
					if ctr.cancels != 0 || unconfirmed {
						t.Fatalf("terminal exit used the canceler or retained a marker: %d, %q", ctr.cancels, marker)
					}
				} else if ctr.cancels != 1 || !unconfirmed || marker == "" {
					t.Fatalf("failed canceler lost its marker: %d, %q", ctr.cancels, marker)
				}
			})
		})
	}
}

// Output that ends without an error does not show that the supervisor cleaned
// up: a signal may have killed it first. When cancellation arrives as that
// output ends, RunInput still classifies the supervisor's exit, and keeps the
// invocation unconfirmed together with the cancellation.
func TestRunInputCancellationAfterAKilledSupervisor(t *testing.T) {
	for name, test := range map[string]struct {
		ctr     *finishedContainer
		cancels int
		check   func(error) bool
	}{
		"killed": {&finishedContainer{noReport: true, code: 137}, 0, func(err error) bool {
			var exit *ExitError
			return errors.As(err, &exit) && exit.Code == 137
		}},
		"cleanup failed": {&finishedContainer{noReport: true, code: CleanupFailedExit}, 0, func(err error) bool {
			var exit *ExitError
			return errors.As(err, &exit) && exit.Code == CleanupFailedExit
		}},
		// An exit that cannot be inspected is stopped like a run that ended
		// first.
		"not inspected": {&finishedContainer{noReport: true, unknown: true}, 1, func(err error) bool {
			var backend *BackendError
			return errors.As(err, &backend)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			test.ctr.onClose = cancel
			writes, closed, finished := make(chan InputWrite), make(chan struct{}), make(chan struct{})
			returned := make(chan error, 1)
			go func() {
				_, err := RunInput(ctx, test.ctr, "probe", time.Minute, 1024, 1024,
					Input{Limit: 8, Writes: writes, Closed: closed, Finished: finished}, "true")
				returned <- err
			}()
			var err error
			select {
			case err = <-returned:
			case <-time.After(5 * time.Second):
				t.Fatal("RunInput stuck after its supervisor ended")
			}
			if !errors.Is(err, context.Canceled) || !test.check(err) || test.ctr.cancels != test.cancels {
				t.Fatalf("RunInput returned %v after %d cancelers", err, test.ctr.cancels)
			}
			if _, ok := Unconfirmed(err); ok != (test.cancels == 0) {
				t.Fatalf("unconfirmed %t for %v", ok, err)
			}
		})
	}
}

// execInspectionTransport supplies one Docker ExecInspect response through the
// Moby client selected by production. The test controls only the attachment's
// EOF and the outcome of the separate marker cancellation command.
type execInspectionTransport struct{ responses []string }

func (t *execInspectionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || !strings.HasSuffix(req.URL.Path, "/exec/attached/json") {
		return nil, fmt.Errorf("unexpected exec inspection request %s %s", req.Method, req.URL.Path)
	}
	body := t.responses[0]
	if len(t.responses) > 1 {
		t.responses = t.responses[1:]
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func TestRunInputInspectsExecAfterOutputEOF(t *testing.T) {
	const running = `{"Running":true,"ExitCode":null}`
	const stopped = `{"Running":false,"ExitCode":0}`
	cancelErr := errors.New("canceler unavailable")
	for _, tc := range []struct {
		name          string
		inspections   []string
		noReport      bool
		cancelCaller  bool
		cancelFailure error
		wantCancels   int
	}{
		{name: "running after EOF", inspections: []string{running}, noReport: true, wantCancels: 1},
		{name: "caller canceled at EOF", inspections: []string{running}, noReport: true, cancelCaller: true, wantCancels: 1},
		{name: "running with unsuccessful stop", inspections: []string{running}, noReport: true, cancelCaller: true, cancelFailure: cancelErr, wantCancels: 1},
		{name: "terminal without report", inspections: []string{stopped}, noReport: true},
		{name: "terminal with completed report", inspections: []string{stopped}},
		{name: "terminal state arrives just after completed report", inspections: []string{running, stopped}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, err := mobycl.New(
					mobycl.WithHost("http://docker.invalid"), mobycl.WithAPIVersion("1.56"),
					mobycl.WithHTTPClient(&http.Client{Transport: &execInspectionTransport{responses: tc.inspections}}),
				)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctr := &finishedContainer{
					onClose: func() {
						if tc.cancelCaller {
							cancel()
						}
					},
					noReport:  tc.noReport,
					cancelErr: tc.cancelFailure,
					exitCode:  func(ctx context.Context) (int, error) { return inspectAttachedExit(ctx, client, "attached") },
				}
				finished := make(chan struct{})
				report, err := RunInput(ctx, ctr, "probe", time.Minute, 1024, 1024,
					Input{Limit: 8, Writes: make(chan InputWrite), Closed: make(chan struct{}), Finished: finished}, "true")
				if tc.noReport {
					if err == nil {
						t.Fatal("accepted EOF without a supervisor report")
					}
				} else if err != nil || string(report.Stdout) != "done" {
					t.Fatalf("completed output changed: %+v, %v", report, err)
				}
				if ctr.cancels != tc.wantCancels || errors.Is(err, errAttachedExecRunning) != (tc.wantCancels > 0) {
					t.Fatalf("RunInput canceled %d times, want %d; error %v", ctr.cancels, tc.wantCancels, err)
				}
				if errors.Is(err, context.Canceled) != tc.cancelCaller {
					t.Fatalf("caller cancellation state was not preserved: %v", err)
				}
				marker, unconfirmed := Unconfirmed(err)
				if tc.cancelFailure != nil {
					if !unconfirmed || !strings.HasPrefix(marker, MarkerDir+"/") || !errors.Is(err, tc.cancelFailure) {
						t.Fatalf("failed cancellation lost its marker or cause: %q, %v", marker, err)
					}
				} else if unconfirmed {
					t.Fatalf("settled execution retained a marker: %q, %v", marker, err)
				}
				select {
				case <-finished:
				default:
					t.Fatal("RunInput returned before releasing input")
				}
			})
		})
	}
}
