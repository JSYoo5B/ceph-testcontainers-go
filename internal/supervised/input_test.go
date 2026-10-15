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

// finishedContainer attaches finishedAttachment and counts cancelers.
type finishedContainer struct {
	testcontainers.Container
	onClose func()
	mu      sync.Mutex
	cancels int
}

func (c *finishedContainer) AttachExec(context.Context, []string, []string) (Attachment, error) {
	report, _ := json.Marshal(map[string]any{"schema_version": 1, "stdout": base64.StdEncoding.EncodeToString([]byte("done")), "stderr": "",
		"exit_code": 0, "timed_out": false, "stdout_truncated": false, "stderr_truncated": false,
		"input_received": 0, "input_delivered": 0, "input_eof": false})
	frame := binary.BigEndian.AppendUint32([]byte{1, 0, 0, 0}, uint32(len(report)))
	return Attachment{
		Input:    &finishedAttachment{onClose: c.onClose},
		Output:   bytes.NewReader(append(frame, report...)),
		ExitCode: func(context.Context) (int, error) { return 0, nil },
		Release:  func() {},
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
