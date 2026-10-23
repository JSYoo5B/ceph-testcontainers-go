package capture

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

func init() { pollInterval = time.Millisecond }

const (
	targetID  = "target-id"
	targetMAC = "02:42:ac:11:00:02"
	helperID  = "helper-id"
	probeID   = "probe-id"
	imageID   = "sha256:helper"
)

type fakeTarget struct{ testcontainers.Container }

func (fakeTarget) GetContainerID() string { return targetID }

// pcapFile builds a little-endian microsecond classic pcap with records of
// the given captured lengths.
func pcapFile(snaplen uint32, records ...int) []byte {
	var b bytes.Buffer
	for _, v := range []any{uint32(0xa1b2c3d4), uint16(2), uint16(4), int32(0), uint32(0), snaplen, uint32(1)} {
		_ = binary.Write(&b, binary.LittleEndian, v)
	}
	for _, n := range records {
		for _, v := range []uint32{1, 0, uint32(n), uint32(n)} {
			_ = binary.Write(&b, binary.LittleEndian, v)
		}
		b.Write(bytes.Repeat([]byte{0xab}, n))
	}
	return b.Bytes()
}

func goodProbe() map[string]any {
	return map[string]any{
		"uid": 104, "euid": 104, "missing": []string{},
		"version":   []any{0, "tcpdump version 4.99.3\nlibpcap version 1.10.3\n", ""},
		"filter":    []any{0, "(000) ldh [12]\n", ""},
		"namespace": "net:[4026532000]", "mac": targetMAC,
	}
}

// fakeBackend models one target and one helper. Its helper becomes ready
// after readyAfter state reads, and ends on SIGTERM unless ignoreTerm.
type fakeBackend struct {
	mu sync.Mutex

	targetState     targetState
	targetErr       error
	targetAfter     *targetState // the target from the second inspection on
	targetGone      bool         // removed from the second inspection on
	targetCalls     int
	probeReport     map[string]any
	probeStdout     string
	probeStderr     string
	probeErr        error
	runErr          error
	readyLine       string
	stderrExtra     string
	noFile          bool
	exitBefore      *int // ends before readiness with this status
	ignoreTerm      bool
	termStatus      int
	selfEnded       bool // tcpdump ended at its packet limit before Stop
	file            []byte
	copyErr         error
	removeErrs      []error
	running         bool
	exitCode        int
	signals         []string
	removed         []string
	probeRemoved    int
	probeRemoveErrs []error
	trailer         *string // tcpdump's statistics after its end, when not the default
	probeArgv       []string
	runArgv         []string
	networkModes    []string
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		targetState: targetState{running: true, networkMode: "tc-net", sandboxID: "sandbox-1", startedAt: "t1", driver: "bridge", mac: targetMAC},
		probeReport: goodProbe(),
		readyLine:   "tcpdump: listening on eth0, link-type EN10MB (Ethernet), snapshot length 128 bytes",
		file:        pcapFile(128, 60, 128),
	}
}

func (f *fakeBackend) target(_ context.Context, id, network string) (targetState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targetCalls++
	if f.targetErr != nil {
		return targetState{}, f.targetErr
	}
	if f.targetCalls > 1 {
		if f.targetGone {
			return targetState{}, errdefs.ErrNotFound
		}
		if f.targetAfter != nil {
			return *f.targetAfter, nil
		}
	}
	return f.targetState, nil
}

func (f *fakeBackend) probe(_ context.Context, image, networkMode string, argv []string) (probeResult, string, error) {
	f.probeArgv, f.networkModes = argv, append(f.networkModes, networkMode)
	if f.probeErr != nil {
		return probeResult{}, probeID, f.probeErr
	}
	stdout := f.probeStdout
	if stdout == "" && f.probeReport != nil {
		data, _ := json.Marshal(f.probeReport)
		stdout = string(data) + "\n"
	}
	return probeResult{imageID: imageID, stdout: []byte(stdout), stderr: []byte(f.probeStderr)}, probeID, nil
}

func (f *fakeBackend) platform(context.Context, string) (string, error) { return "linux/amd64", nil }

func (f *fakeBackend) run(_ context.Context, image, networkMode string, argv []string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runArgv, f.networkModes = argv, append(f.networkModes, networkMode)
	if image != imageID {
		return "", errors.New("the helper must run the probed image ID")
	}
	if f.runErr != nil {
		return "", f.runErr
	}
	f.running = true
	if f.exitBefore != nil {
		f.running, f.exitCode = false, *f.exitBefore
	}
	return helperID, nil
}

func (f *fakeBackend) state(context.Context, string) (helperState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.selfEnded && f.running && len(f.signals) == 0 && f.targetCalls > 1 {
		f.running, f.exitCode = false, 0
	}
	return helperState{running: f.running, exitCode: f.exitCode, finishedAt: time.Unix(10, 0).UTC()}, nil
}

func (f *fakeBackend) logs(context.Context, string, int) ([]byte, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	text := f.readyLine + "\n" + f.stderrExtra
	if !f.running && f.exitCode == 0 && f.exitBefore == nil {
		if f.trailer != nil {
			text += *f.trailer
		} else {
			text += "2 packets captured\n2 packets received by filter\n0 packets dropped by kernel\n"
		}
	}
	return nil, []byte(text), nil
}

func (f *fakeBackend) exists(context.Context, string, string) (bool, error) { return !f.noFile, nil }

func (f *fakeBackend) signal(_ context.Context, _ string, signal string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.running {
		return errdefs.ErrConflict
	}
	f.signals = append(f.signals, signal)
	switch {
	case signal == "SIGKILL":
		f.running, f.exitCode = false, 137
	case !f.ignoreTerm:
		f.running, f.exitCode = false, f.termStatus
	}
	return nil
}

func (f *fakeBackend) copy(context.Context, string, string) (io.ReadCloser, error) {
	if f.copyErr != nil {
		return nil, f.copyErr
	}
	return io.NopCloser(bytes.NewReader(f.file)), nil
}

func (f *fakeBackend) remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id == probeID {
		if len(f.probeRemoveErrs) > 0 {
			err := f.probeRemoveErrs[0]
			f.probeRemoveErrs = f.probeRemoveErrs[1:]
			return err
		}
		f.probeRemoved++
		return nil
	}
	if len(f.removeErrs) > 0 {
		err := f.removeErrs[0]
		f.removeErrs = f.removeErrs[1:]
		if err != nil {
			return err
		}
	}
	f.removed = append(f.removed, id)
	return nil
}

func request(t *testing.T) Request {
	return Request{
		HelperImage: "example.invalid/capture:dev", Network: "tc-net", Interface: "eth0",
		Filter: "tcp port 7000 and host 192.0.2.1", SnapLen: 128, MaxPackets: 10,
		Output: filepath.Join(t.TempDir(), "capture.pcap"), ReadyTimeout: time.Second, StopGrace: 20 * time.Millisecond,
	}
}

func startFake(t *testing.T, f *fakeBackend, req Request) (*Session, error) {
	t.Helper()
	return start(context.Background(), f, fakeTarget{}, req)
}

func exists(path string) bool { _, err := os.Lstat(path); return err == nil }

func TestRequestValidation(t *testing.T) {
	valid := request(t)
	if err := valid.validate(); err != nil {
		t.Fatalf("valid request refused: %v", err)
	}
	if valid.maxBytes() != 24+10*(16+128) {
		t.Fatalf("file bound = %d", valid.maxBytes())
	}
	for name, change := range map[string]func(*Request){
		"network":         func(r *Request) { r.Network = "" },
		"default bridge":  func(r *Request) { r.Network = "bridge" },
		"host":            func(r *Request) { r.Network = "host" },
		"interface":       func(r *Request) { r.Interface = "" },
		"long interface":  func(r *Request) { r.Interface = "eth0123456789abc" },
		"path interface":  func(r *Request) { r.Interface = "../eth0" },
		"any":             func(r *Request) { r.Interface = "any" },
		"loopback":        func(r *Request) { r.Interface = "lo" },
		"filter":          func(r *Request) { r.Filter = "  " },
		"long filter":     func(r *Request) { r.Filter = strings.Repeat("a", MaxFilterBytes+1) },
		"NUL filter":      func(r *Request) { r.Filter = "tcp\x00" },
		"snaplen":         func(r *Request) { r.SnapLen = 0 },
		"large snaplen":   func(r *Request) { r.SnapLen = MaxSnapLen + 1 },
		"packets":         func(r *Request) { r.MaxPackets = 0 },
		"file bound":      func(r *Request) { r.SnapLen, r.MaxPackets = MaxSnapLen, MaxFileBytes/MaxSnapLen },
		"output":          func(r *Request) { r.Output = "" },
		"ready timeout":   func(r *Request) { r.ReadyTimeout = -1 },
		"long ready wait": func(r *Request) { r.ReadyTimeout = MaxTimeout + 1 },
		"stop grace":      func(r *Request) { r.StopGrace = -1 },
	} {
		req := valid
		change(&req)
		f := newFakeBackend()
		if _, err := startFake(t, f, req); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: err = %v, want ceph.ErrInvalidRequest", name, err)
		}
		if f.targetCalls != 0 || f.probeArgv != nil {
			t.Errorf("%s: an invalid request reached Docker", name)
		}
	}
	req := valid
	req.HelperImage = ""
	if _, err := startFake(t, newFakeBackend(), req); !errors.Is(err, ceph.ErrImageCapability) || errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("missing helper image: err = %v, want only ceph.ErrImageCapability", err)
	}
	if _, err := start(context.Background(), newFakeBackend(), nil, valid); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("nil target: err = %v", err)
	}
}

func TestStartRefusesTargets(t *testing.T) {
	for name, change := range map[string]func(*targetState){
		"stopped":        func(s *targetState) { s.running = false },
		"paused":         func(s *targetState) { s.paused = true },
		"host":           func(s *targetState) { s.networkMode = "host" },
		"shared":         func(s *targetState) { s.networkMode = "container:other" },
		"no sandbox":     func(s *targetState) { s.sandboxID = "" },
		"not attached":   func(s *targetState) { s.mac, s.driver = "", "" },
		"overlay driver": func(s *targetState) { s.driver = "overlay" },
	} {
		f := newFakeBackend()
		change(&f.targetState)
		req := request(t)
		if _, err := startFake(t, f, req); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: err = %v", name, err)
		}
		if f.probeArgv != nil || exists(req.Output) {
			t.Errorf("%s: a refused target started a probe or created the output", name)
		}
	}
	f := newFakeBackend()
	f.targetErr = errdefs.ErrNotFound
	if _, err := startFake(t, f, request(t)); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("missing target: err = %v", err)
	}
}

func TestStartRefusesExistingOutput(t *testing.T) {
	req := request(t)
	if err := os.WriteFile(req.Output, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFakeBackend()
	if _, err := startFake(t, f, req); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("err = %v", err)
	}
	if data, _ := os.ReadFile(req.Output); string(data) != "keep" || f.probeArgv != nil {
		t.Fatal("an existing output was replaced or a probe ran")
	}
}

func TestProbeClassification(t *testing.T) {
	cases := map[string]struct {
		change func(map[string]any, *fakeBackend)
		want   error
	}{
		"missing tcpdump": {func(p map[string]any, _ *fakeBackend) { p["missing"] = []string{"tcpdump"} }, ErrImageUnsupported},
		"incomplete":      {func(p map[string]any, _ *fakeBackend) { delete(p, "missing") }, ErrImageUnsupported},
		"root":            {func(p map[string]any, _ *fakeBackend) { p["uid"], p["euid"] = 0, 0 }, ErrImageUnsupported},
		"version broken": {func(p map[string]any, _ *fakeBackend) {
			p["version"] = []any{1, "", "error while loading shared libraries"}
		}, ErrImageUnsupported},
		"version wrong": {func(p map[string]any, _ *fakeBackend) { p["version"] = []any{0, "something else", ""} }, ErrImageUnsupported},
		"exec refused": {func(p map[string]any, _ *fakeBackend) {
			p["version"] = []any{-1, "", "[Errno 1] Operation not permitted"}
		}, ErrPermissionDenied},
		"filter syntax": {func(p map[string]any, _ *fakeBackend) {
			p["filter"] = []any{1, "", "tcpdump: can't parse filter expression: syntax error"}
		}, ceph.ErrInvalidRequest},
		"filter broken":   {func(p map[string]any, _ *fakeBackend) { p["filter"] = []any{1, "", "tcpdump: unknown option"} }, ErrImageUnsupported},
		"no interface":    {func(p map[string]any, _ *fakeBackend) { delete(p, "mac"); p["mac_error"] = "[Errno 19] No such device" }, ceph.ErrInvalidRequest},
		"other interface": {func(p map[string]any, _ *fakeBackend) { p["mac"] = "02:42:ac:11:00:09" }, ceph.ErrInvalidRequest},
		"no report":       {func(_ map[string]any, f *fakeBackend) { f.probeStdout, f.probeStderr = "garbage", "Traceback" }, ErrImageUnsupported},
		"refused report": {func(_ map[string]any, f *fakeBackend) {
			f.probeStdout, f.probeStderr = "x", "exec /usr/bin/python3: operation not permitted"
		}, ErrPermissionDenied},
		"no python": {func(_ map[string]any, f *fakeBackend) {
			f.probeErr = errors.New(`exec: "python3": executable file not found in $PATH`)
		}, ErrImageUnsupported},
		"engine refused": {func(_ map[string]any, f *fakeBackend) {
			f.probeErr = errors.New("OCI runtime create failed: operation not permitted")
		}, ErrPermissionDenied},
		"engine available": {func(_ map[string]any, f *fakeBackend) { f.probeErr = errors.New("connection refused") }, ceph.ErrEnvironmentUnavailable},
	}
	for name, tc := range cases {
		f := newFakeBackend()
		tc.change(f.probeReport, f)
		req := request(t)
		session, err := startFake(t, f, req)
		if session != nil || !errors.Is(err, tc.want) {
			t.Errorf("%s: session = %v, err = %v, want %v", name, session, err, tc.want)
		}
		for _, other := range []error{ErrImageUnsupported, ErrPermissionDenied, ErrFailed, ceph.ErrInvalidRequest} {
			if other != tc.want && errors.Is(err, other) {
				t.Errorf("%s: err %v also matches %v", name, err, other)
			}
		}
		if f.runArgv != nil || exists(req.Output) {
			t.Errorf("%s: a failed probe started the capture or kept the output", name)
		}
	}
}

func TestProbeArgumentsAndCaptureCommand(t *testing.T) {
	f := newFakeBackend()
	req := request(t)
	req.Filter = "-w /etc/passwd"
	session, err := startFake(t, f, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.probeArgv[3:], []string{"eth0", "-w /etc/passwd"}) || f.probeArgv[0] != "python3" {
		t.Errorf("probe argv = %q", f.probeArgv)
	}
	want := []string{"tcpdump", "-p", "-i", "eth0", "-s", "128", "-c", "10", "--immediate-mode", "-U", "-w", capturePath, "--", "-w /etc/passwd"}
	if !slices.Equal(f.runArgv, want) {
		t.Errorf("capture argv = %q", f.runArgv)
	}
	if !slices.Equal(f.networkModes, []string{"container:" + targetID, "container:" + targetID}) {
		t.Errorf("network modes = %q", f.networkModes)
	}
	if !strings.Contains(probeScript, `"-d","--",filt`) {
		t.Error("the probe does not end the options before the filter")
	}
}

func TestHelperOptions(t *testing.T) {
	req := testcontainers.GenericContainerRequest{}
	for _, option := range helperOptions("container:"+targetID, []string{"tcpdump", "-i", "eth0"}) {
		if err := option.Customize(&req); err != nil {
			t.Fatal(err)
		}
	}
	host := &container.HostConfig{}
	req.HostConfigModifier(host)
	config := &container.Config{}
	if req.ConfigModifier != nil {
		req.ConfigModifier(config)
	}
	if string(host.NetworkMode) != "container:"+targetID || !slices.Equal(host.CapDrop, []string{"ALL"}) || !slices.Equal(host.CapAdd, []string{"NET_RAW"}) {
		t.Fatalf("host config = %+v", host)
	}
	if host.Privileged || len(host.SecurityOpt) > 0 || config.User != "" {
		t.Fatalf("the helper must run as the image's user without no-new-privileges: %+v %+v", host, config)
	}
	if !slices.Equal(req.Entrypoint, []string{"tcpdump"}) || !slices.Equal(req.Cmd, []string{"-i", "eth0"}) {
		t.Fatalf("entrypoint %q cmd %q", req.Entrypoint, req.Cmd)
	}
}

func TestReadiness(t *testing.T) {
	status := 1
	cases := map[string]struct {
		change func(*fakeBackend)
		want   error
	}{
		"socket refused": {func(f *fakeBackend) {
			f.exitBefore, f.readyLine = &status, "tcpdump: eth0: You don't have permission to capture on that device (socket: Operation not permitted)"
		}, ErrPermissionDenied},
		"exec refused": {func(f *fakeBackend) {
			status255 := 255
			f.exitBefore, f.readyLine = &status255, "exec /usr/bin/tcpdump: operation not permitted"
		}, ErrPermissionDenied},
		"no device": {func(f *fakeBackend) {
			f.exitBefore, f.readyLine = &status, "tcpdump: eth0: No such device exists"
		}, ErrFailed},
		"no line":         {func(f *fakeBackend) { f.readyLine = "" }, ErrFailed},
		"other interface": {func(f *fakeBackend) { f.readyLine = strings.Replace(f.readyLine, "eth0", "eth1", 1) }, ErrFailed},
		"no file":         {func(f *fakeBackend) { f.noFile = true }, ErrFailed},
		"not ethernet":    {func(f *fakeBackend) { f.readyLine = strings.Replace(f.readyLine, "EN10MB (Ethernet)", "RAW", 1) }, ErrFailed},
		"restarted": {func(f *fakeBackend) {
			after := f.targetState
			after.startedAt = "t2"
			f.targetAfter = &after
		}, ErrFailed},
		"run refused": {func(f *fakeBackend) { f.runErr = errors.New("operation not permitted") }, ErrPermissionDenied},
	}
	for name, tc := range cases {
		f := newFakeBackend()
		tc.change(f)
		req := request(t)
		req.ReadyTimeout = 50 * time.Millisecond
		session, err := startFake(t, f, req)
		if session != nil || !errors.Is(err, tc.want) {
			t.Errorf("%s: session = %v, err = %v, want %v", name, session, err, tc.want)
		}
		if f.runErr == nil && !slices.Equal(f.removed, []string{helperID}) {
			t.Errorf("%s: helper removals = %q", name, f.removed)
		}
		if exists(req.Output) {
			t.Errorf("%s: the output of a capture that never started was kept", name)
		}
	}
}

func TestStartKeepsHelperThatCannotBeRemoved(t *testing.T) {
	f := newFakeBackend()
	f.noFile = true
	f.removeErrs = []error{errors.New("engine busy")}
	req := request(t)
	req.ReadyTimeout = 20 * time.Millisecond
	session, err := startFake(t, f, req)
	if session == nil || !errors.Is(err, ErrFailed) {
		t.Fatalf("session = %v, err = %v", session, err)
	}
	report, err := session.Stop(context.Background())
	if err != nil || !report.HelperRemoved || report.End != EndFailed || len(f.signals) != 0 {
		t.Fatalf("retry: report = %+v, err = %v, signals = %q", report, err, f.signals)
	}
}

func TestStopDeliversValidatedCapture(t *testing.T) {
	f := newFakeBackend()
	req := request(t)
	session, err := startFake(t, f, req)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // An expired workload context still finishes and removes the helper.
	report, err := session.Stop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := os.ReadFile(req.Output)
	if readErr != nil || !bytes.Equal(data, f.file) {
		t.Fatalf("delivered %d bytes, want the helper's %d: %v", len(data), len(f.file), readErr)
	}
	if !slices.Equal(f.signals, []string{"SIGTERM"}) || report.End != EndStopped || report.ExitCode != 0 {
		t.Errorf("end = %s/%d after %q", report.End, report.ExitCode, f.signals)
	}
	if report.Target != TargetUnchanged || !report.HelperRemoved || !report.Retrieved || report.RetrievedBytes != int64(len(f.file)) {
		t.Errorf("report = %+v", report)
	}
	if file := report.File; file.Status != FileValid || file.Packets != 2 || !file.Complete || file.LinkType != 1 || file.SnapLen != 128 {
		t.Errorf("file = %+v", file)
	}
	if s := report.Statistics; s.Captured == nil || *s.Captured != 2 || s.ReceivedByFilter == nil || s.DroppedByKernel == nil || s.DroppedByInterface != nil {
		t.Errorf("statistics = %+v", s)
	}
	if report.HelperImageID != imageID || report.HelperPlatform != "linux/amd64" || report.ToolVersion != "tcpdump version 4.99.3 libpcap version 1.10.3" ||
		report.Namespace != "net:[4026532000]" || report.SandboxID != "sandbox-1" || report.MaxBytes != 24+10*144 || report.ReadyAt.IsZero() || report.StopRequestedAt.IsZero() {
		t.Errorf("identity = %+v", report)
	}
	if _, err := json.Marshal(report); err != nil {
		t.Fatal(err)
	}
	again, err := session.Stop(context.Background())
	if err != nil || len(f.signals) != 1 || again.File.SHA256 != report.File.SHA256 {
		t.Fatalf("a second Stop repeated the capture's end: %v %q", err, f.signals)
	}
}

func TestStopOutcomes(t *testing.T) {
	cases := map[string]struct {
		change func(*fakeBackend)
		check  func(Report) bool
		err    error
	}{
		// The fake file and counter hold two packets of the requested ten.
		"premature end": {func(f *fakeBackend) { f.selfEnded = true },
			func(r Report) bool { return r.End == EndPremature && r.File.Status == FileValid && r.File.Packets == 2 }, ErrFailed},
		"self end without a file": {func(f *fakeBackend) { f.selfEnded, f.copyErr = true, errdefs.ErrNotFound },
			func(r Report) bool { return r.End == EndUnknown && !r.Retrieved }, ErrFailed},
		"empty capture": {func(f *fakeBackend) { f.file = pcapFile(128) },
			func(r Report) bool { return r.End == EndStopped && r.File.Status == FileValid && r.File.Packets == 0 }, nil},
		"snaplen truncation": {func(f *fakeBackend) {
			f.file = pcapFile(128, 128)
			binary.LittleEndian.PutUint32(f.file[24+12:], 1500)
		}, func(r Report) bool { return r.File.Status == FileValid && r.File.TruncatedPackets == 1 }, nil},
		"killed": {func(f *fakeBackend) { f.ignoreTerm = true },
			func(r Report) bool { return r.End == EndKilled && r.ExitCode == 137 && r.Retrieved }, ErrFailed},
		"nonzero": {func(f *fakeBackend) { f.termStatus = 1 },
			func(r Report) bool { return r.End == EndFailed && r.ExitCode == 1 && r.Retrieved }, ErrFailed},
		"target gone": {func(f *fakeBackend) { f.targetGone = true },
			func(r Report) bool { return r.Target == TargetGone && r.File.Status == FileValid }, ErrFailed},
		"cut off": {func(f *fakeBackend) { f.file = f.file[:len(f.file)-5] },
			func(r Report) bool { return r.File.Status == FileMalformed && r.Retrieved }, ErrFailed},
		"pcapng": {func(f *fakeBackend) { binary.LittleEndian.PutUint32(f.file, 0x0a0d0d0a) },
			func(r Report) bool { return r.File.Status == FileUnsupported }, ErrFailed},
		"over bound": {func(f *fakeBackend) {
			f.file = append(pcapFile(128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128), 0)
		},
			func(r Report) bool {
				return r.File.Status == FileLimitExceeded && r.RetrievedBytes == r.MaxBytes && !r.File.Complete
			}, ErrFailed},
		"not retrieved": {func(f *fakeBackend) { f.copyErr = errdefs.ErrNotFound },
			func(r Report) bool { return !r.Retrieved && r.File.Status == "" && r.HelperRemoved }, ErrFailed},
	}
	for name, tc := range cases {
		f := newFakeBackend()
		req := request(t)
		session, err := startFake(t, f, req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		tc.change(f)
		report, err := session.Stop(context.Background())
		if (tc.err == nil) != (err == nil) || (tc.err != nil && !errors.Is(err, tc.err)) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.err)
		}
		if !tc.check(report) || !report.HelperRemoved {
			t.Errorf("%s: report = %+v", name, report)
		}
		if name == "over bound" {
			if info, err := os.Stat(req.Output); err != nil || info.Size() != report.MaxBytes {
				t.Errorf("over bound: kept %v bytes, want %d", info, report.MaxBytes)
			}
		}
	}
}

func TestStopReportsPacketLimitOnlyWhenReached(t *testing.T) {
	for name, tc := range map[string]struct {
		maxPackets int
		trailer    *string
		want       EndReason
	}{
		"reached":            {2, nil, EndPacketLimit},
		"file short":         {3, nil, EndPremature},
		"counter disagrees":  {2, ptr("3 packets captured\n"), EndPremature},
		"no counter printed": {2, ptr(""), EndPacketLimit},
	} {
		f := newFakeBackend()
		req := request(t)
		req.MaxPackets = tc.maxPackets
		session, err := startFake(t, f, req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		f.selfEnded, f.trailer = true, tc.trailer
		report, err := session.Stop(context.Background())
		if report.End != tc.want || (tc.want == EndPacketLimit) != (err == nil) || (err != nil && !errors.Is(err, ErrFailed)) {
			t.Errorf("%s: end = %s, err = %v", name, report.End, err)
		}
	}
}

func ptr(s string) *string { return &s }

func TestRequestRefusesWrappingPacketBound(t *testing.T) {
	limit := int((MaxFileBytes - 24) / (16 + 128))
	for name, tc := range map[string]struct {
		packets int
		ok      bool
	}{
		"largest": {limit, true}, "one more": {limit + 1, false}, "MaxInt": {math.MaxInt, false},
		"wraps to small": {int(math.MaxInt64/144) + 1, false},
	} {
		req := request(t)
		req.SnapLen, req.MaxPackets = 128, tc.packets
		if err := req.validate(); (err == nil) != tc.ok {
			t.Errorf("%s: validate = %v", name, err)
		}
		if tc.ok {
			if req.maxBytes() > MaxFileBytes || req.maxBytes() <= 0 {
				t.Errorf("%s: file bound = %d", name, req.maxBytes())
			}
			continue
		}
		f := newFakeBackend()
		if _, err := startFake(t, f, req); !errors.Is(err, ceph.ErrInvalidRequest) || f.targetCalls != 0 || f.probeArgv != nil || exists(req.Output) {
			t.Errorf("%s: err = %v, target inspections %d, probe %v, output kept %v", name, err, f.targetCalls, f.probeArgv != nil, exists(req.Output))
		}
	}
}

func TestStartKeepsProbeThatCannotBeRemoved(t *testing.T) {
	for name, probeErr := range map[string]error{"probe passed": nil, "probe failed": errors.New("connection reset")} {
		f := newFakeBackend()
		f.probeErr = probeErr
		f.probeRemoveErrs = []error{errors.New("engine busy")}
		req := request(t)
		session, err := startFake(t, f, req)
		if session == nil || err == nil || !strings.Contains(err.Error(), "remove the capture probe") || f.runArgv != nil {
			t.Fatalf("%s: session = %v, err = %v, helper started %v", name, session, err, f.runArgv != nil)
		}
		report, err := session.Stop(context.Background())
		if err != nil || f.probeRemoved != 1 || f.removed != nil || len(f.signals) != 0 || report.Retrieved {
			t.Fatalf("%s: retry: report = %+v, err = %v, removed %q", name, report, err, f.removed)
		}
		if again, err := session.Stop(context.Background()); err != nil || f.probeRemoved != 1 || again.End != report.End {
			t.Fatalf("%s: a second Stop: %v, removed %q", name, err, f.removed)
		}
	}
}

func TestStopRetriesRemoval(t *testing.T) {
	f := newFakeBackend()
	session, err := startFake(t, f, request(t))
	if err != nil {
		t.Fatal(err)
	}
	f.removeErrs = []error{errors.New("engine busy")}
	first, err := session.Stop(context.Background())
	if err == nil || first.HelperRemoved || first.File.Status != FileValid {
		t.Fatalf("first Stop: %+v, %v", first, err)
	}
	second, err := session.Stop(context.Background())
	if err != nil || !second.HelperRemoved || len(f.signals) != 1 || second.File.SHA256 != first.File.SHA256 {
		t.Fatalf("second Stop: %+v, %v, signals %q", second, err, f.signals)
	}
}

func TestParseStatistics(t *testing.T) {
	stats := parseStatistics([]byte("tcpdump: listening on eth0\n1 packet captured\n3 packets received by filter\n0 packets dropped by kernel\n4 packets dropped by interface\n"))
	if *stats.Captured != 1 || *stats.ReceivedByFilter != 3 || *stats.DroppedByKernel != 0 || *stats.DroppedByInterface != 4 {
		t.Fatalf("statistics = %+v", stats)
	}
	unknown := parseStatistics([]byte("5 packets captured\nsomething 7 packets received by filter\n"))
	if unknown.Captured == nil || unknown.ReceivedByFilter != nil || unknown.DroppedByKernel != nil || unknown.DroppedByInterface != nil {
		t.Fatalf("absent counters must stay unknown: %+v", unknown)
	}
}

func TestErrorCategories(t *testing.T) {
	for err, kind := range map[error]error{
		ErrImageUnsupported: ceph.ErrImageCapability,
		ErrPermissionDenied: ceph.ErrEnvironmentUnavailable,
		ErrFailed:           ceph.ErrNativeFailure,
	} {
		if !errors.Is(err, kind) {
			t.Errorf("%v is not %v", err, kind)
		}
	}
}
