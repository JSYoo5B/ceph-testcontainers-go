package dashboard

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// inputLifetimeTarget refuses before input is claimed, or accepts the attached
// input and returns a complete supervisor envelope. It models no Ceph behavior.
type inputLifetimeTarget struct {
	testcontainers.Container
	id         string
	state      container.State
	inspectErr error
	beforeID   func()
	attached   bool
	input      *inputLifetimeStream
}

func (f *inputLifetimeTarget) GetContainerID() string {
	if f.beforeID != nil {
		before := f.beforeID
		f.beforeID = nil
		before()
	}
	return f.id
}

func (f *inputLifetimeTarget) State(context.Context) (*container.State, error) {
	return &f.state, f.inspectErr
}

func (f *inputLifetimeTarget) AttachExec(context.Context, []string, []string) (supervised.Attachment, error) {
	f.attached = true
	reader, writer := io.Pipe()
	f.input = &inputLifetimeStream{output: writer}
	return supervised.Attachment{
		Input: f.input, Output: reader,
		ExitCode: func(context.Context) (int, error) { return 0, nil },
		Release:  func() { _ = reader.Close() },
	}, nil
}

// The envelope appears only after EOF, so accepted calls must send the whole
// input and close it before they can return successfully.
type inputLifetimeStream struct {
	bytes.Buffer
	output *io.PipeWriter
}

func (in *inputLifetimeStream) CloseWrite() error {
	report, err := json.Marshal(map[string]any{
		"schema_version": 1, "stdout": base64.StdEncoding.EncodeToString([]byte(`{"finished":true,"login_pending":false}`)), "stderr": "",
		"exit_code": 0, "timed_out": false, "stdout_truncated": false, "stderr_truncated": false,
		"input_received": in.Len(), "input_delivered": in.Len(), "input_eof": true,
	})
	if err != nil {
		return err
	}
	frame := append(binary.BigEndian.AppendUint32([]byte{1, 0, 0, 0}, uint32(len(report))), report...)
	if _, err := in.output.Write(frame); err != nil {
		return err
	}
	return in.output.Close()
}
func (in *inputLifetimeStream) Close() error { return in.output.Close() }

// Count only Dashboard NativeInput writers, not unrelated test/runtime workers.
// The target's first ID lookup waits for the writer to reach Write, removing
// scheduler timing from the refusal control without a production test seam.
func dashboardInputWriters() int {
	data := make([]byte, 64<<10)
	for {
		n := runtime.Stack(data, true)
		if n < len(data) {
			count := 0
			for _, stack := range strings.Split(string(data[:n]), "\n\n") {
				if strings.Contains(stack, "ceph.(*NativeInput).Write(") && strings.Contains(stack, "/dashboard.") {
					count++
				}
			}
			return count
		}
		data = make([]byte, len(data)*2)
	}
}

func waitDashboardInputWriters(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for dashboardInputWriters() != want {
		if time.Now().After(deadline) {
			t.Fatalf("Dashboard input writers = %d, want %d", dashboardInputWriters(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDashboardInputLifetime(t *testing.T) {
	for _, operation := range []string{"mutation", "probe"} {
		for _, refusal := range []string{"missing-target", "closed-cluster", "stopped", "paused", "inspect-error", "accepted"} {
			t.Run(operation+"/"+refusal, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(func() { cancel(); waitDashboardInputWriters(t, 0) })
				target := &inputLifetimeTarget{id: "dashboard-input-control", state: container.State{Running: true}}
				switch refusal {
				case "missing-target":
					target.id = ""
				case "stopped":
					target.state.Running = false
				case "paused":
					target.state.Paused = true
				case "inspect-error":
					target.inspectErr = errors.New("inspection refused")
				}
				cluster := testhelper.TestCluster(target, testhelper.TestConfig{Closed: refusal == "closed-cluster"})
				const password = "fixture-input-password"
				expected := []byte(password)
				if operation == "probe" {
					var err error
					expected, err = newProbeRequest("https://dashboard.invalid:8443", "fixture-fingerprint", "fixture-user", password, []string{summaryPath})
					if err != nil {
						t.Fatal(err)
					}
				}
				// Repeated readiness failures must not accumulate writers until ctx ends.
				for attempt := range 8 {
					target.beforeID = func() { waitDashboardInputWriters(t, 1) }
					var err error
					if operation == "mutation" {
						err = runWithInput(ctx, cluster, "input lifetime", []byte(password), "dashboard", "ac-user-set-password", "fixture-user", "-i", "-")
					} else {
						_, err = runProbe(ctx, cluster, "https://dashboard.invalid:8443", "fixture-fingerprint", "fixture-user", password, summaryPath)
					}
					if refusal == "accepted" {
						if err != nil {
							t.Fatal(err)
						}
						if !target.attached || !bytes.Equal(target.input.Bytes(), expected) {
							t.Fatal("accepted command did not receive exact input")
						}
					} else {
						if err == nil || target.attached {
							t.Fatalf("attempt %d: early refusal = %v, attached = %v", attempt, err, target.attached)
						}
						switch refusal {
						case "missing-target", "stopped", "paused":
							if !errors.Is(err, ceph.ErrInvalidRequest) {
								t.Fatalf("target refusal classification: %v", err)
							}
						case "inspect-error":
							if !errors.Is(err, ceph.ErrEnvironmentUnavailable) {
								t.Fatalf("inspection refusal classification: %v", err)
							}
						}

					}
					if ctx.Err() != nil {
						t.Fatal("helper canceled its caller")
					}
					if count := dashboardInputWriters(); count != 0 {
						t.Fatalf("attempt %d left %d writers while caller remains alive", attempt, count)
					}
				}
			})
		}
	}
}
