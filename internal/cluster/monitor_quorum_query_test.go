package cluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type monitorQuorumTestEnvelope struct {
	SchemaVersion   int    `json:"schema_version"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	ExitCode        int    `json:"exit_code"`
	TimedOut        bool   `json:"timed_out"`
	StdoutTruncated bool   `json:"stdout_truncated"`
	StderrTruncated bool   `json:"stderr_truncated"`
}

func monitorQuorumTestResult(stdout, stderr []byte, code int, timeout, clipped bool) []byte {
	data, _ := json.Marshal(monitorQuorumTestEnvelope{SchemaVersion: 1,
		Stdout: base64.StdEncoding.EncodeToString(stdout), Stderr: base64.StdEncoding.EncodeToString(stderr),
		ExitCode: code, TimedOut: timeout, StdoutTruncated: clipped})
	return data
}

func monitorQuorumTestCommand(args []string) bool {
	return len(args) == 12 && slices.Equal(args[:3], []string{"python3", "-c", monitorQuorumCommandScript}) &&
		args[3] == strconv.Itoa(monitorQuorumStdoutLimit) && args[4] == strconv.Itoa(monitorQuorumStderrLimit) &&
		slices.Equal(args[6:], []string{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"})
}

// Existing native fixtures keep their command assertions, while representing
// the supervised quorum read's separate, complete stdout transport faithfully.
func monitorQuorumTestModuleArgs(args []string) []string {
	if monitorQuorumTestCommand(args) {
		return args[9:]
	}
	return args[3:]
}

func monitorQuorumTestReader(args []string, code int, output []byte) (int, io.Reader, error) {
	if monitorQuorumTestCommand(args) {
		return 0, bytes.NewReader(monitorQuorumTestResult(output, nil, code, false, false)), nil
	}
	var stream bytes.Buffer
	header := make([]byte, 8)
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	stream.Write(header)
	stream.Write(output)
	return code, &stream, nil
}

func TestMonitorQuorumSupervisorRejectsIncompleteObservations(t *testing.T) {
	native := []byte(monitorConfigQuorum)
	valid := monitorQuorumTestResult(native, []byte("warning on stderr\n"), 0, false, false)
	invalidField := func(field string, value any) []byte {
		var fields map[string]any
		_ = json.Unmarshal(valid, &fields)
		fields[field] = value
		data, _ := json.Marshal(fields)
		return data
	}
	cases := []struct {
		name string
		data []byte
		code int
	}{
		{"native exit", monitorQuorumTestResult(native, []byte("native failure"), 2, false, false), 0},
		{"complete JSON but process timeout", monitorQuorumTestResult(native, nil, -9, true, false), 0},
		{"stdout truncation", monitorQuorumTestResult(native, nil, 0, false, true), 0},
		{"stderr truncation", invalidField("stderr_truncated", true), 0},
		{"supervisor exit", valid, 7},
		{"invalid protocol JSON", []byte("{"), 0},
		{"wrong schema", invalidField("schema_version", 2), 0},
		{"invalid stdout encoding", invalidField("stdout", "???"), 0},
		{"invalid stderr encoding", invalidField("stderr", "???"), 0},
		{"invalid native JSON", monitorQuorumTestResult([]byte("{"), nil, 0, false, false), 0},
		{"missing native arrays", monitorQuorumTestResult([]byte(`{}`), nil, 0, false, false), 0},
		{"null native arrays", monitorQuorumTestResult([]byte(`{"quorum_names":null,"monmap":{"mons":null}}`), nil, 0, false, false), 0},
		{"wrong native array shape", monitorQuorumTestResult([]byte(`{"quorum_names":[1],"monmap":{"mons":[]}}`), nil, 0, false, false), 0},
		{"wrong native member shape", monitorQuorumTestResult([]byte(`{"quorum_names":[],"monmap":{"mons":["invalid"]}}`), nil, 0, false, false), 0},
		{"missing native member name", monitorQuorumTestResult([]byte(`{"quorum_names":[],"monmap":{"mons":[{}]}}`), nil, 0, false, false), 0},
		{"duplicate native members", monitorQuorumTestResult([]byte(`{"quorum_names":["a"],"monmap":{"mons":[{"name":"a"},{"name":"a"}]}}`), nil, 0, false, false), 0},
		{"foreign quorum member", monitorQuorumTestResult([]byte(`{"quorum_names":["foreign"],"monmap":{"mons":[{"name":"a"}]}}`), nil, 0, false, false), 0},
		{"duplicate quorum member", monitorQuorumTestResult([]byte(`{"quorum_names":["a","a"],"monmap":{"mons":[{"name":"a"}]}}`), nil, 0, false, false), 0},
	}
	for _, field := range []string{"schema_version", "stdout", "stderr", "exit_code", "timed_out", "stdout_truncated", "stderr_truncated"} {
		cases = append(cases, struct {
			name string
			data []byte
			code int
		}{"null " + field, invalidField(field, nil), 0})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			control := &diagnosticsTestContainer{exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
				return tc.code, bytes.NewReader(tc.data), nil
			}}
			if status, err := queryMonitorQuorum(t.Context(), control); err == nil || len(status.QuorumNames) != 0 || len(status.MonMap.Mons) != 0 {
				t.Fatalf("incomplete native observation accepted: %+v, %v", status, err)
			}
		})
	}
	control := &diagnosticsTestContainer{exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
		return 0, bytes.NewReader(valid), nil
	}}
	status, err := queryMonitorQuorum(t.Context(), control)
	if err != nil || !slices.Equal(status.QuorumNames, []string{"a", "b", "c"}) || len(status.MonMap.Mons) != 3 {
		t.Fatalf("separate stderr changed a complete native observation: %+v, %v", status, err)
	}
	control.exec = func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
		return 0, bytes.NewReader(monitorQuorumTestResult([]byte(`{"quorum_names":[],"monmap":{"mons":[]}}`), nil, 0, false, false)), nil
	}
	if _, err := queryMonitorQuorum(t.Context(), control); err != nil {
		t.Fatalf("complete empty native arrays are unavailable to quorum wait predicates: %v", err)
	}
}

func TestMonitorQuorumAttemptStaysInsideCallerBudget(t *testing.T) {
	for _, timeout := range []time.Duration{40 * time.Millisecond, 10 * time.Second} {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		control := &diagnosticsTestContainer{exec: func(attempt context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
			if !monitorQuorumTestCommand(args) || len(opts) != 1 {
				t.Fatalf("quorum read lost its fixed supervised command: %v", args)
			}
			callerDeadline, _ := ctx.Deadline()
			attemptDeadline, _ := attempt.Deadline()
			seconds, err := strconv.ParseFloat(args[5], 64)
			if err != nil || seconds <= 0 || seconds > 5 || attemptDeadline.After(callerDeadline) || seconds >= time.Until(attemptDeadline).Seconds() {
				t.Fatalf("child/reap budget exceeds attempt or caller: seconds=%f, attempt=%v, caller=%v, %v", seconds, attemptDeadline, callerDeadline, err)
			}
			return 0, bytes.NewReader(monitorQuorumTestResult([]byte(monitorConfigQuorum), nil, 0, false, false)), nil
		}}
		if _, err := queryMonitorQuorum(ctx, control); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	control := &diagnosticsTestContainer{exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
		t.Fatal("expired caller reached native Exec")
		return 0, nil, nil
	}}
	if _, err := queryMonitorQuorum(ctx, control); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation cause lost: %v", err)
	}
}

func TestMonitorQuorumTimeoutPreservesCauseAndNativeFailure(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		control := &diagnosticsTestContainer{exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
			return 0, bytes.NewReader(monitorQuorumTestResult(nil, []byte("native failure"), 2, timeout, false)), nil
		}}
		_, err := queryMonitorQuorum(t.Context(), control)
		if err == nil || errors.Is(err, context.DeadlineExceeded) != timeout || !timeout && !strings.Contains(err.Error(), "native failure") {
			t.Fatalf("timeout/native exit was hidden: %v", err)
		}
	}
}

type monitorQuorumCancelOnEOFReader struct {
	io.Reader
	cancel context.CancelFunc
}

func (reader monitorQuorumCancelOnEOFReader) Read(data []byte) (int, error) {
	n, err := reader.Reader.Read(data)
	if err == io.EOF {
		reader.cancel()
	}
	return n, err
}

func TestMonitorQuorumEOFReaderCancellationReturnsNoStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	control := &diagnosticsTestContainer{exec: func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
		return 0, monitorQuorumCancelOnEOFReader{
			Reader: bytes.NewReader(monitorQuorumTestResult([]byte(monitorConfigQuorum), nil, 0, false, false)),
			cancel: cancel,
		}, nil
	}}
	cluster := &Container{Container: control}
	status, err := cluster.QuorumStatus(ctx)
	if !errors.Is(err, context.Canceled) || len(status.QuorumNames) != 0 || len(status.MonMap.Mons) != 0 {
		t.Fatalf("complete native JSON after caller cancellation became a valid observation: %+v, %v", status, err)
	}
}
