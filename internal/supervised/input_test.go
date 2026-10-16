package supervised

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

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

// finishedContainer attaches finishedAttachment and counts cancelers. A
// killed supervisor ends its output without a report and exits with code; an
// unknown one cannot be inspected. Like Docker, inspection fails once its
// context ended.
type finishedContainer struct {
	testcontainers.Container
	onClose func()
	killed  bool
	code    int
	unknown bool
	mu      sync.Mutex
	cancels int
}

func (c *finishedContainer) AttachExec(context.Context, []string, []string) (Attachment, error) {
	report, _ := json.Marshal(map[string]any{"schema_version": 1, "stdout": base64.StdEncoding.EncodeToString([]byte("done")), "stderr": "",
		"exit_code": 0, "timed_out": false, "stdout_truncated": false, "stderr_truncated": false,
		"input_received": 0, "input_delivered": 0, "input_eof": false})
	output := append(binary.BigEndian.AppendUint32([]byte{1, 0, 0, 0}, uint32(len(report))), report...)
	if c.killed {
		output = nil
	}
	return Attachment{
		Input:  &finishedAttachment{onClose: c.onClose},
		Output: bytes.NewReader(output),
		ExitCode: func(ctx context.Context) (int, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if c.unknown {
				return 0, errors.New("engine unreachable")
			}
			return c.code, nil
		},
		Release: func() {},
	}, nil
}

func (c *finishedContainer) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.mu.Lock()
	c.cancels++
	c.mu.Unlock()
	return 0, bytes.NewReader(nil), nil
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
		"killed": {&finishedContainer{killed: true, code: 137}, 0, func(err error) bool {
			var exit *ExitError
			return errors.As(err, &exit) && exit.Code == 137
		}},
		"cleanup failed": {&finishedContainer{killed: true, code: CleanupFailedExit}, 0, func(err error) bool {
			var exit *ExitError
			return errors.As(err, &exit) && exit.Code == CleanupFailedExit
		}},
		// An exit that cannot be inspected is stopped like a run that ended
		// first.
		"not inspected": {&finishedContainer{killed: true, unknown: true}, 1, func(err error) bool {
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
