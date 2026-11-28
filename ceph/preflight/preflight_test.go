package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
)

// Synthetic sensitive engine details. No report text, JSON or error may
// carry any of them.
var secrets = []string{"secret-host.example.internal", "/home/secret-user", "s3cr3t-token", "10.9.8.7"}

// testEngine returns fixed engine information: a substitute whose behavior
// is the Info reply below, read where each test sets it.
type testEngine struct {
	info   system.Info
	err    error
	host   string
	closed bool
}

func (e *testEngine) Info(context.Context) (system.Info, error) { return e.info, e.err }
func (e *testEngine) DaemonHost() string                        { return e.host }
func (e *testEngine) Close() error                              { e.closed = true; return nil }

func linuxEngine() *testEngine {
	return &testEngine{host: "tcp://secret-host.example.internal:2376", info: system.Info{
		Name: "secret-host.example.internal", DockerRootDir: "/home/secret-user/docker",
		HTTPProxy: "http://user:s3cr3t-token@10.9.8.7:3128", Labels: []string{"owner=/home/secret-user"},
		ServerVersion: "29.1.0", OSType: "linux", Architecture: "x86_64", KernelVersion: "6.8.0-90-generic",
		OperatingSystem: "Ubuntu 24.04.3 LTS", CgroupVersion: "2", SecurityOptions: []string{"name=seccomp,profile=builtin", "name=cgroupns"},
	}}
}

// testHelper replays one helper run: Start returns startErr, Probe returns
// probe or probeErr, Terminate returns terminateErr. It records the calls
// and whether Terminate ran with a live context.
type testHelper struct {
	startErr, probeErr, terminateErr error
	probe                            string
	image                            string
	probed, terminated               int
	terminateCtxLive                 bool
	onProbe                          func()
}

func (h *testHelper) Start(_ context.Context, image string) (string, error) {
	h.image = image
	if h.startErr != nil && errdefs.IsNotFound(h.startErr) {
		return "", h.startErr
	}
	return "0123456789ab", h.startErr
}

func (h *testHelper) Probe(ctx context.Context) ([]byte, error) {
	h.probed++
	if h.onProbe != nil {
		h.onProbe()
	}
	if h.probeErr != nil {
		return nil, h.probeErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []byte(h.probe), nil
}

func (h *testHelper) Terminate(ctx context.Context) error {
	h.terminated++
	h.terminateCtxLive = ctx.Err() == nil
	return h.terminateErr
}

func probeOutput(targets ...string) string {
	gates := `[{"key":"tools","state":"observed"},{"key":"drivers","state":"observed"},{"key":"control","state":"observed"},{"key":"targets","state":"observed"}]`
	list, _ := json.Marshal(targets)
	return `{"gates":` + gates + `,"targets":` + string(list) + `}`
}

func helperChecker(engine *testEngine, helper *testHelper) checker {
	return checker{
		engine: func(context.Context) (engineClient, error) { return engine, nil },
		helper: func(engineClient) helperRunner { return helper },
	}
}

var both = []Capability{CapabilityBlockIODelay, CapabilityBlockIOEIO}

func helperRequest() Request {
	return Request{Capabilities: both, Level: LevelHelper, HelperImage: "registry.example/helper:v1"}
}

func states(r *Report) map[CheckKey]State {
	out := map[CheckKey]State{}
	for _, check := range r.Checks {
		out[check.Key] = check.State
	}
	return out
}

// requireShareable fails when the report text, its JSON or err carries a
// synthetic sensitive detail.
func requireShareable(t *testing.T, r *Report, err error) {
	t.Helper()
	data, marshalErr := json.Marshal(r)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	texts := map[string]string{"json": string(data), "text": r.String()}
	if err != nil {
		texts["error"] = err.Error()
	}
	for name, text := range texts {
		for _, secret := range secrets {
			if strings.Contains(text, secret) {
				t.Errorf("%s carries %q:\n%s", name, secret, text)
			}
		}
	}
}

func TestInspectStartsNoHelperAndLeavesHelperChecksNotRun(t *testing.T) {
	engine := linuxEngine()
	c := checker{
		engine: func(context.Context) (engineClient, error) { return engine, nil },
		helper: func(engineClient) helperRunner { t.Fatal("inspect level started a helper"); return nil },
	}
	report, err := c.check(t.Context(), Request{Capabilities: both})
	if err != nil {
		t.Fatal(err)
	}
	if report.Level != LevelInspect || report.Helper != nil || !engine.closed {
		t.Fatalf("report %+v, engine closed %t", report, engine.closed)
	}
	want := map[CheckKey]State{CheckKeyEngineInfo: StateObserved, CheckKeyBlockIOEngineOS: StateObserved, CheckKeyBlockIOHelperStart: StateNotRun, CheckKeyBlockIOImageTools: StateNotRun, CheckKeyBlockIODrivers: StateNotRun,
		CheckKeyBlockIOControlAccess: StateNotRun, CheckKeyBlockIOTargetLinear: StateNotRun, CheckKeyBlockIOTargetDelay: StateNotRun, CheckKeyBlockIOTargetError: StateNotRun}
	if got := states(report); !reflect.DeepEqual(got, want) {
		t.Fatalf("states %v, want %v", got, want)
	}
	if check, _ := report.Lookup(CheckKeyBlockIOTargetDelay); check.Reason != "needs a helper level check" {
		t.Fatalf("delay check: %+v", check)
	}
	for capability, want := range map[Capability]State{CapabilityEngine: StateObserved, CapabilityBlockIODelay: StateUnknown, CapabilityBlockIOEIO: StateUnknown} {
		if got := report.Verdict(capability); got != want {
			t.Errorf("%s: %s, want %s", capability, got, want)
		}
	}
	if report.Engine.ServerVersion != "29.1.0" || report.Engine.Transport != "tcp" || *report.Engine.Rootless || report.Engine.Desktop == nil || *report.Engine.Desktop {
		t.Fatalf("engine facts: %+v", report.Engine)
	}
	if text := report.String(); !strings.Contains(text, "desktop no") {
		t.Fatalf("a Linux engine read without error is not reported as no Desktop:\n%s", text)
	}
	if report.Process.OS == "" || report.Process.Architecture == "" {
		t.Fatalf("process platform: %+v", report.Process)
	}
	requireShareable(t, report, err)
}

func TestEngineOnlyRequestChecksTheEngine(t *testing.T) {
	report, err := helperChecker(linuxEngine(), nil).check(t.Context(), Request{Capabilities: []Capability{CapabilityEngine}})
	if err != nil || len(report.Checks) != 1 || report.Verdict(CapabilityEngine) != StateObserved || report.Verdict(CapabilityBlockIODelay) != StateNotRun {
		t.Fatalf("%+v %v", report, err)
	}
}

func TestHelperVerdicts(t *testing.T) {
	gates := func(tools, drivers, control, targets string) string {
		kind := func(state, k string) string {
			if state == "unavailable" || state == "unknown" {
				return k
			}
			return ""
		}
		return fmt.Sprintf(`{"gates":[{"key":"tools","state":%q,"kind":%q,"missing":["blockdev"]},{"key":"drivers","state":%q,"kind":%q},{"key":"control","state":%q,"kind":%q},{"key":"targets","state":%q,"kind":%q,"error":"dmsetup exited 1: /home/secret-user"}],"targets":[]}`,
			tools, kind(tools, "image"), drivers, kind(drivers, "environment"), control, kind(control, "environment"), targets, kind(targets, "environment"))
	}
	for _, tc := range []struct {
		name         string
		helper       testHelper
		delay, eio   State
		checks       map[CheckKey]State
		kind         Kind
		wantErr      error
		wantReleased bool
	}{
		{name: "supported targets", helper: testHelper{probe: probeOutput("striped", "linear", "error", "delay")}, delay: StateObserved, eio: StateObserved, wantReleased: true},
		{name: "delay missing, EIO available", helper: testHelper{probe: probeOutput("linear", "error")}, delay: StateUnavailable, eio: StateObserved,
			checks: map[CheckKey]State{CheckKeyBlockIOTargetDelay: StateUnavailable, CheckKeyBlockIOTargetError: StateObserved}, kind: KindEnvironment, wantReleased: true},
		{name: "common prerequisite missing", helper: testHelper{probe: gates("observed", "unavailable", "not-run", "not-run")}, delay: StateUnavailable, eio: StateUnavailable,
			checks: map[CheckKey]State{CheckKeyBlockIODrivers: StateUnavailable, CheckKeyBlockIOControlAccess: StateNotRun, CheckKeyBlockIOTargetLinear: StateNotRun, CheckKeyBlockIOTargetDelay: StateNotRun}, kind: KindEnvironment, wantReleased: true},
		{name: "image tool missing", helper: testHelper{probe: gates("unavailable", "not-run", "not-run", "not-run")}, delay: StateUnavailable, eio: StateUnavailable,
			checks: map[CheckKey]State{CheckKeyBlockIOImageTools: StateUnavailable, CheckKeyBlockIODrivers: StateNotRun}, kind: KindImage, wantReleased: true},
		{name: "python3 missing", helper: testHelper{probeErr: &supervised.ExitError{Label: "preflight helper", Code: 127}}, delay: StateUnavailable, eio: StateUnavailable,
			checks: map[CheckKey]State{CheckKeyBlockIOHelperStart: StateObserved, CheckKeyBlockIOImageTools: StateUnavailable, CheckKeyBlockIODrivers: StateNotRun}, kind: KindImage, wantReleased: true},
		{name: "targets unreadable", helper: testHelper{probe: gates("observed", "observed", "observed", "unknown")}, delay: StateUnknown, eio: StateUnknown,
			checks: map[CheckKey]State{CheckKeyBlockIOControlAccess: StateObserved, CheckKeyBlockIOTargetLinear: StateUnknown, CheckKeyBlockIOTargetDelay: StateUnknown, CheckKeyBlockIOTargetError: StateUnknown}, wantErr: ceph.ErrNativeFailure, wantReleased: true},
		{name: "helper configuration refused", helper: testHelper{startErr: fmt.Errorf("create container: %w", errdefs.ErrPermissionDenied.WithMessage("authorization denied by plugin for /home/secret-user"))},
			delay: StateUnavailable, eio: StateUnavailable, checks: map[CheckKey]State{CheckKeyBlockIOHelperStart: StateUnavailable, CheckKeyBlockIOImageTools: StateNotRun, CheckKeyBlockIOTargetDelay: StateNotRun}, kind: KindEnvironment, wantReleased: true},
		{name: "helper image not found", helper: testHelper{startErr: fmt.Errorf("pull: %w", errdefs.ErrNotFound.WithMessage("secret-host.example.internal/helper"))},
			delay: StateUnavailable, eio: StateUnavailable, checks: map[CheckKey]State{CheckKeyBlockIOHelperStart: StateUnavailable, CheckKeyBlockIOImageTools: StateNotRun}, kind: KindImage, wantReleased: true},
		{name: "helper start inconclusive", helper: testHelper{startErr: errors.New("start: exec failed on 10.9.8.7")}, delay: StateUnknown, eio: StateUnknown,
			checks: map[CheckKey]State{CheckKeyBlockIOHelperStart: StateUnknown, CheckKeyBlockIOImageTools: StateNotRun}, wantErr: ceph.ErrEnvironmentUnavailable, wantReleased: true},
		{name: "probe inconclusive", helper: testHelper{probeErr: errors.New("exec: s3cr3t-token")}, delay: StateUnknown, eio: StateUnknown,
			checks: map[CheckKey]State{CheckKeyBlockIOHelperStart: StateObserved, CheckKeyBlockIOImageTools: StateUnknown, CheckKeyBlockIODrivers: StateNotRun}, wantErr: ceph.ErrNativeFailure, wantReleased: true},
		{name: "probe output invalid", helper: testHelper{probe: `{"gates":[]}`}, delay: StateUnknown, eio: StateUnknown,
			checks: map[CheckKey]State{CheckKeyBlockIOImageTools: StateUnknown}, wantErr: ceph.ErrNativeFailure, wantReleased: true},
		{name: "cleanup failure", helper: testHelper{probe: probeOutput("linear", "error", "delay"), terminateErr: errors.New("remove on /home/secret-user failed")},
			delay: StateObserved, eio: StateObserved, wantErr: ceph.ErrEnvironmentUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			helper := tc.helper
			report, err := helperChecker(linuxEngine(), &helper).check(t.Context(), helperRequest())
			if tc.wantErr == nil && err != nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("error %v, want %v", err, tc.wantErr)
			}
			if got := report.Verdict(CapabilityBlockIODelay); got != tc.delay {
				t.Errorf("delay verdict %s, want %s\n%s", got, tc.delay, report)
			}
			if got := report.Verdict(CapabilityBlockIOEIO); got != tc.eio {
				t.Errorf("EIO verdict %s, want %s\n%s", got, tc.eio, report)
			}
			got := states(report)
			for key, want := range tc.checks {
				if got[key] != want {
					t.Errorf("%s: %s, want %s", key, got[key], want)
				}
				if want == StateUnavailable {
					if check, _ := report.Lookup(key); check.Kind != tc.kind || check.Reason == "" || check.Guidance == "" {
						t.Errorf("%s: %+v, want kind %s with reason and guidance", key, check, tc.kind)
					}
				}
			}
			for key, state := range got {
				if state == "" {
					t.Errorf("%s has no state", key)
				}
			}
			if helper.image != "registry.example/helper:v1" || helper.terminated != 1 || !helper.terminateCtxLive {
				t.Fatalf("helper image %q, terminated %d, live context %t", helper.image, helper.terminated, helper.terminateCtxLive)
			}
			if report.Helper == nil || report.Helper.Released != tc.wantReleased || (report.Helper.Cleanup != "") == tc.wantReleased {
				t.Fatalf("helper run %+v", report.Helper)
			}
			requireShareable(t, report, err)
		})
	}
}

func TestCancellationKeepsCauseAndStillRemovesHelper(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	helper := &testHelper{probe: probeOutput("linear", "error", "delay"), onProbe: cancel}
	report, err := helperChecker(linuxEngine(), helper).check(ctx, helperRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want context.Canceled", err)
	}
	if report == nil || report.Verdict(CapabilityBlockIODelay) != StateUnknown || report.Verdict(CapabilityBlockIOEIO) != StateUnknown {
		t.Fatalf("partial report:\n%s", report)
	}
	if helper.terminated != 1 || !helper.terminateCtxLive || !report.Helper.Released {
		t.Fatalf("helper not removed with a live context: %+v %+v", helper, report.Helper)
	}
}

func TestCancelledEngineSelectionKeepsCause(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c := checker{engine: func(ctx context.Context) (engineClient, error) { return nil, ctx.Err() }}
	report, err := c.check(ctx, Request{Capabilities: both})
	if !errors.Is(err, context.Canceled) || report.Verdict(CapabilityEngine) != StateUnknown || report.Verdict(CapabilityBlockIODelay) != StateUnknown {
		t.Fatalf("%v\n%s", err, report)
	}
}

// A real client on a socket that nothing listens on: the connection
// failure is conclusive, and neither the socket path nor the client's
// message leaves the report or the error.
func TestUnreachableEngineIsUnavailable(t *testing.T) {
	socket := "/nonexistent/secret-host.example.internal/docker.sock"
	docker, err := client.New(client.WithHost("unix://" + socket))
	if err != nil {
		t.Fatal(err)
	}
	engine := clientEngine{docker}
	c := checker{engine: func(context.Context) (engineClient, error) { return engine, nil }}
	report, err := c.check(t.Context(), Request{Capabilities: both})
	if err != nil {
		t.Fatal(err)
	}
	check, _ := report.Lookup(CheckKeyEngineInfo)
	if check.State != StateUnavailable || check.Kind != KindEnvironment || report.Verdict(CapabilityBlockIODelay) != StateUnavailable || report.Engine.Transport != "unix" {
		t.Fatalf("%+v\n%s", check, report)
	}
	if got := states(report); got[CheckKeyBlockIOEngineOS] != StateNotRun || got[CheckKeyBlockIOTargetDelay] != StateNotRun {
		t.Fatalf("checks after an unreachable engine ran: %v", got)
	}
	requireShareable(t, report, err)
	for _, text := range []string{report.String()} {
		if strings.Contains(text, socket) {
			t.Fatalf("socket path in report: %s", text)
		}
	}
}

// A selection failure that is not a typed connection failure proves
// nothing, even when its text reads like one.
func TestEngineSelectionFailureIsUnknown(t *testing.T) {
	c := checker{engine: func(context.Context) (engineClient, error) {
		return nil, errors.New(`check host "unix:///home/secret-user/docker.sock": connection refused`)
	}}
	report, err := c.check(t.Context(), Request{Capabilities: both})
	if !errors.Is(err, ceph.ErrEnvironmentUnavailable) || report.Verdict(CapabilityEngine) != StateUnknown || report.Verdict(CapabilityBlockIOEIO) != StateUnknown {
		t.Fatalf("%v\n%s", err, report)
	}
	requireShareable(t, report, err)
}

func TestEngineInfoFailureIsUnknown(t *testing.T) {
	engine := linuxEngine()
	engine.err = errors.New("Error response from daemon: secret-host.example.internal: 500")
	report, err := helperChecker(engine, nil).check(t.Context(), Request{Capabilities: both})
	if !errors.Is(err, ceph.ErrEnvironmentUnavailable) || report.Verdict(CapabilityEngine) != StateUnknown || report.Verdict(CapabilityBlockIODelay) != StateUnknown {
		t.Fatalf("%v\n%s", err, report)
	}
	if report.Engine.Desktop != nil || !strings.Contains(report.String(), "desktop unknown") {
		t.Fatalf("an engine whose information was not read has a Desktop fact: %+v\n%s", report.Engine, report)
	}
	requireShareable(t, report, err)
}

func TestNonLinuxEngineIsUnavailableWithoutHelper(t *testing.T) {
	engine := linuxEngine()
	engine.info.OSType = "windows"
	c := helperChecker(engine, nil)
	c.helper = func(engineClient) helperRunner { t.Fatal("a helper started on a Windows engine"); return nil }
	report, err := c.check(t.Context(), helperRequest())
	if err != nil || report.Verdict(CapabilityBlockIODelay) != StateUnavailable || report.Helper != nil {
		t.Fatalf("%v\n%s", err, report)
	}
}

func TestEngineWithoutOperatingSystemIsUnknownWithError(t *testing.T) {
	engine := linuxEngine()
	engine.info.OSType = ""
	c := helperChecker(engine, nil)
	c.helper = func(engineClient) helperRunner { t.Fatal("a helper started without a known engine OS"); return nil }
	report, err := c.check(t.Context(), helperRequest())
	if !errors.Is(err, ceph.ErrEnvironmentUnavailable) || report.Verdict(CapabilityBlockIODelay) != StateUnknown || report.Verdict(CapabilityEngine) != StateObserved {
		t.Fatalf("%v\n%s", err, report)
	}
}

func TestEngineFacts(t *testing.T) {
	info := system.Info{OperatingSystem: "Docker Desktop", KernelVersion: "6.6.87.2-microsoft-standard-WSL2", SecurityOptions: []string{"name=rootless", "name=userns"}}
	f := facts(info, "npipe")
	if f.Desktop == nil || !*f.Desktop || f.Backend != "wsl2" || !*f.Rootless || !*f.UserNamespaces || f.Transport != "npipe" {
		t.Fatalf("%+v", f)
	}
	if f := facts(system.Info{}, ""); f.Desktop == nil || *f.Desktop || f.Rootless != nil || f.UserNamespaces != nil {
		t.Fatalf("read information without Docker Desktop or security options: %+v", f)
	}
	for host, want := range map[string]string{"unix:///var/run/docker.sock": "unix", "ssh://user@secret-host.example.internal": "ssh", "tcp://10.9.8.7:2376": "tcp", "fd://": "", "::": ""} {
		if got := transport(host); got != want {
			t.Errorf("%s: %q, want %q", host, got, want)
		}
	}
}

func TestRequestValidation(t *testing.T) {
	for name, request := range map[string]Request{
		"unknown capability":     {Capabilities: []Capability{"blockio.netem"}},
		"duplicate capability":   {Capabilities: []Capability{CapabilityBlockIOEIO, CapabilityBlockIOEIO}},
		"unknown level":          {Level: "active"},
		"helper without image":   {Capabilities: both, Level: LevelHelper},
		"image at inspect level": {Capabilities: both, HelperImage: "helper:v1"},
		"image without blockio":  {Level: LevelHelper, HelperImage: "helper:v1"},
	} {
		t.Run(name, func(t *testing.T) {
			c := checker{engine: func(context.Context) (engineClient, error) {
				t.Fatal("an invalid request reached the engine")
				return nil, nil
			}}
			report, err := c.check(t.Context(), request)
			if report != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("%v %v", report, err)
			}
		})
	}
}

func TestReportJSONRoundTrip(t *testing.T) {
	report, err := helperChecker(linuxEngine(), &testHelper{probe: probeOutput("linear", "error")}).check(t.Context(), helperRequest())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Report
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&decoded, report) || decoded.Verdict(CapabilityBlockIODelay) != StateUnavailable || decoded.Verdict(CapabilityBlockIOEIO) != StateObserved {
		t.Fatalf("decoded %+v\nwant %+v", decoded, report)
	}
	for _, want := range []string{`"schema_version":1`, `"state":"unavailable"`, `"key":"blockio.target.delay"`, `"level":"helper"`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("JSON lacks %s: %s", want, data)
		}
	}
	text := report.String()
	for _, want := range []string{"blockio.delay: unavailable", "blockio.eio: observed", "blockio.target.delay: unavailable (environment)", "helper: started true, released true"} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
	}
}

// Verdict keeps a proven absence apart from an unknown one whatever the
// order of the checks.
func TestVerdictKeepsUnavailableAndUnknownApart(t *testing.T) {
	for _, tc := range []struct {
		states []State
		want   State
	}{
		{[]State{StateObserved, StateObserved}, StateObserved},
		{[]State{StateObserved, StateNotRun}, StateUnknown},
		{[]State{StateUnknown, StateObserved}, StateUnknown},
		{[]State{StateUnknown, StateUnavailable}, StateUnavailable},
		{[]State{StateUnavailable, StateNotRun}, StateUnavailable},
		{[]State{StateNotRun, StateUnknown}, StateUnknown},
	} {
		r := &Report{Capabilities: []Capability{CapabilityBlockIODelay}}
		for i, state := range tc.states {
			r.Checks = append(r.Checks, Check{Key: CheckKey(fmt.Sprint(i)), Capabilities: []Capability{CapabilityBlockIODelay}, State: state})
		}
		if got := r.Verdict(CapabilityBlockIODelay); got != tc.want {
			t.Errorf("%v: %s, want %s", tc.states, got, tc.want)
		}
	}
	if (*Report)(nil).Verdict(CapabilityEngine) != StateNotRun {
		t.Fatal("nil report")
	}
}

// Only a selection whose every candidate failed to connect proves the
// engine unreachable; one candidate that answered keeps it inconclusive.
func TestOnlyConnectionFailures(t *testing.T) {
	docker, err := client.New(client.WithHost("unix:///nonexistent/secret-host.example.internal/docker.sock"))
	if err != nil {
		t.Fatal(err)
	}
	_, refused := docker.Info(t.Context(), client.InfoOptions{})
	if !client.IsErrConnectionFailed(refused) {
		t.Fatalf("not a connection failure: %v", refused)
	}
	answered := errors.New("Error response from daemon: 500")
	candidate := func(err error) error { return fmt.Errorf("check host: docker info: %w", err) }
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"one refused":           {candidate(refused), true},
		"all refused":           {errors.Join(candidate(refused), candidate(refused)), true},
		"refused and answered":  {errors.Join(candidate(refused), candidate(answered)), false},
		"answered":              {candidate(answered), false},
		"wrapped join":          {fmt.Errorf("connect: %w", errors.Join(candidate(refused), candidate(answered))), false},
		"wrapped refused joins": {fmt.Errorf("connect: %w", errors.Join(candidate(refused), candidate(refused))), true},
		"empty join":            {errors.Join(), false},
		"no candidate":          {errors.New("socket not found"), false},
	} {
		if got := onlyConnectionFailures(tc.err); got != tc.want {
			t.Errorf("%s: %t, want %t", name, got, tc.want)
		}
	}
}

// Every key that a report of all capabilities at the helper level holds is
// one of the exported check keys, and each of them is reported.
func TestReportedKeysAreExported(t *testing.T) {
	exported := []CheckKey{CheckKeyEngineInfo, CheckKeyBlockIOEngineOS, CheckKeyBlockIOHelperStart, CheckKeyBlockIOImageTools,
		CheckKeyBlockIODrivers, CheckKeyBlockIOControlAccess, CheckKeyBlockIOTargetLinear, CheckKeyBlockIOTargetDelay, CheckKeyBlockIOTargetError}
	report, err := helperChecker(linuxEngine(), &testHelper{probe: probeOutput("linear", "error", "delay")}).check(t.Context(), helperRequest())
	if err != nil {
		t.Fatal(err)
	}
	var keys []CheckKey
	for _, check := range report.Checks {
		if !slices.Contains(exported, check.Key) {
			t.Errorf("report key %q is not an exported CheckKey", check.Key)
		}
		if !slices.Contains([]Source{SourceEngineInfo, SourceHelperStart, SourceHelperProbe}, check.Source) {
			t.Errorf("%s has source %q", check.Key, check.Source)
		}
		keys = append(keys, check.Key)
	}
	if !slices.Equal(keys, exported) {
		t.Fatalf("report keys %v, want %v", keys, exported)
	}
}

// The helper names a missing tool from the probe's tool list and maps the
// probe's gates by key, so a tool added to the probe and gates in another
// order are reported as the probe concluded.
func TestRecordFollowsTheProbeToolsAndGateKeys(t *testing.T) {
	t.Run("added tool", func(t *testing.T) {
		tools := control.ProbeTools
		control.ProbeTools = append(slices.Clone(tools), "mkfs.xfs")
		t.Cleanup(func() { control.ProbeTools = tools })
		probe := `{"gates":[{"key":"tools","state":"unavailable","kind":"image","missing":["mkfs.xfs"]},{"key":"drivers","state":"not-run"},{"key":"control","state":"not-run"},{"key":"targets","state":"not-run"}],"targets":[]}`
		report, err := helperChecker(linuxEngine(), &testHelper{probe: probe}).check(t.Context(), helperRequest())
		if err != nil {
			t.Fatal(err)
		}
		if check, _ := report.Lookup(CheckKeyBlockIOImageTools); check.State != StateUnavailable || check.Kind != KindImage || check.Reason != "the helper image lacks mkfs.xfs" {
			t.Fatalf("added tool: %+v", check)
		}
	})
	t.Run("reordered gates", func(t *testing.T) {
		report := &Report{Checks: plan([]Capability{CapabilityBlockIODelay, CapabilityBlockIOEIO})}
		probe := control.Probe{Gates: []control.Gate{
			{Key: control.GateTargets, State: control.GateObserved},
			{Key: control.GateControl, State: control.GateUnavailable, Kind: control.GateEnvironment},
			{Key: control.GateTools, State: control.GateObserved},
			{Key: control.GateDrivers, State: control.GateObserved},
		}, Targets: []string{"linear", "error"}}
		if err := record(probe, report); err != nil {
			t.Fatal(err)
		}
		for key, want := range map[CheckKey]State{
			CheckKeyBlockIOImageTools: StateObserved, CheckKeyBlockIODrivers: StateObserved, CheckKeyBlockIOControlAccess: StateUnavailable,
			CheckKeyBlockIOTargetLinear: StateObserved, CheckKeyBlockIOTargetDelay: StateUnavailable, CheckKeyBlockIOTargetError: StateObserved,
		} {
			if check, _ := report.Lookup(key); check.State != want {
				t.Errorf("%s: %+v, want %s", key, check, want)
			}
		}
		if check, _ := report.Lookup(CheckKeyBlockIOControlAccess); check.Kind != KindEnvironment {
			t.Errorf("control access kind: %+v", check)
		}
	})
}

// The helper runs with the block I/O keeper's privileges and host
// configuration: it starts from the request that fault/blockio's keeper
// starts from, and adds only its owner label.
func TestHelperRequestIsTheKeeperConfiguration(t *testing.T) {
	request := helperContainerRequest("registry.example/helper:v1")
	shared := control.HelperRequest("registry.example/helper:v1", request.Name)
	if !strings.HasPrefix(request.Name, "tcpre-") || request.Labels[ownerLabel] != request.Name || !request.Started {
		t.Fatalf("helper identity: %+v", request)
	}
	if request.Image != shared.Image || !slices.Equal(request.Entrypoint, shared.Entrypoint) || !slices.Equal(request.Cmd, shared.Cmd) || request.User != shared.User {
		t.Fatalf("helper request %+v, keeper request %+v", request.ContainerRequest, shared)
	}
	var got, want container.HostConfig
	request.HostConfigModifier(&got)
	shared.HostConfigModifier(&want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("helper host configuration %+v, keeper %+v", got, want)
	}
}
