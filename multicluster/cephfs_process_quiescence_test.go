package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/system"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const processDraftStart = "2026-10-07T01:00:00Z"
const processDraftLater = "2026-10-07T01:01:00Z"

type processHandleFake struct {
	id         string
	state      container.State
	inspectErr error
	hook       func(int)
	calls      int
}

func (f *processHandleFake) GetContainerID() string { return f.id }
func (f *processHandleFake) Inspect(ctx context.Context) (*container.InspectResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.calls++
	if f.hook != nil {
		f.hook(f.calls)
	}
	if f.inspectErr != nil {
		return nil, f.inspectErr
	}
	s := f.state
	return &container.InspectResponse{ID: f.id, State: &s}, nil
}

type processRawFake struct {
	engineID, id                  string
	state                         container.State
	infoErr, inspectErr, closeErr error
	infoHook                      func(int)
	inspectHook                   func(int)
	infos, inspects, closes       int
	requests                      []string
}

func (f *processRawFake) Info(ctx context.Context, _ mobycl.InfoOptions) (mobycl.SystemInfoResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.SystemInfoResult{}, err
	}
	f.infos++
	if f.infoHook != nil {
		f.infoHook(f.infos)
	}
	return mobycl.SystemInfoResult{Info: system.Info{ID: f.engineID}}, f.infoErr
}
func (f *processRawFake) ContainerInspect(ctx context.Context, id string, opts mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.ContainerInspectResult{}, err
	}
	f.inspects++
	f.requests = append(f.requests, id)
	if opts.Size {
		return mobycl.ContainerInspectResult{}, errors.New("unexpected Size")
	}
	if f.inspectHook != nil {
		f.inspectHook(f.inspects)
	}
	if f.inspectErr != nil {
		return mobycl.ContainerInspectResult{}, f.inspectErr
	}
	s := f.state
	return mobycl.ContainerInspectResult{Container: container.InspectResponse{ID: f.id, State: &s}}, nil
}
func (f *processRawFake) Close() error { f.closes++; return f.closeErr }
func draftRunning(start string) container.State {
	return container.State{Status: container.StateRunning, Running: true, Pid: 42, StartedAt: start}
}
func draftExited(start string) container.State {
	return container.State{Status: container.StateExited, StartedAt: start, FinishedAt: "2026-10-07T01:02:00Z"}
}
func newProcessBindingFakes() (*processHandleFake, *processRawFake) {
	id := strings.Repeat("a", 64)
	return &processHandleFake{id: id, state: draftRunning(processDraftStart)}, &processRawFake{engineID: "opaque-engine-A", id: id, state: draftRunning(processDraftStart)}
}

func TestCephFSOriginalProcessPositiveBindingAndCandidateOwnership(t *testing.T) {
	for _, name := range []string{"bound", "foreign404", "wrongCID", "wrongStart", "engineDrift", "handleRestart", "nilClient", "factoryError", "factoryErrorWithClient", "cancel"} {
		t.Run(name, func(t *testing.T) {
			h, raw := newProcessBindingFakes()
			var candidate cephFSOriginalProcessDocker = raw
			var factoryErr error
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "foreign404":
				raw.inspectErr = errdefs.ErrNotFound
			case "wrongCID":
				raw.id = strings.Repeat("b", 64)
			case "wrongStart":
				raw.state.StartedAt = processDraftLater
			case "engineDrift":
				raw.infoHook = func(n int) {
					if n == 3 {
						raw.engineID = "engine-B"
					}
				}
			case "handleRestart":
				h.hook = func(n int) {
					if n == 2 {
						h.state.StartedAt = processDraftLater
					}
				}
			case "nilClient":
				candidate = nil
			case "factoryError":
				candidate = nil
				factoryErr = errors.New("factory secret 4829")
			case "factoryErrorWithClient":
				factoryErr = errors.New("factory secret 4829")
			case "cancel":
				cancel()
			}
			d := &CephFSMirrorDaemon{}
			b := bindOriginalProcessCandidate(ctx, d, h, candidate, factoryErr)
			st := d.ProcessObserverBindingStatus()
			if name == "bound" {
				if b == nil || !st.Available || st.State != "bound" || raw.closes != 0 || raw.infos < 5 || raw.inspects != 2 {
					t.Fatalf("positive binding lost: %+v %v", st, b)
				}
				for _, id := range raw.requests {
					if id != h.id {
						t.Fatal("not exact full ID")
					}
				}
				if err := b.close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if raw.closes != 1 || d.ProcessObserverBindingStatus().State != "closed" {
					t.Fatal("cleanup boundary lost")
				}
			} else {
				if b != nil || st.Available {
					t.Fatalf("unavailable binding adopted: %+v", st)
				}
				if candidate != nil && raw.closes != 1 {
					t.Fatal("failed/unused candidate not closed")
				}
				if strings.Contains(st.State, "4829") {
					t.Fatal("factory secret leaked")
				}
			}
		})
	}
}

func TestCephFSOriginalProcessBoundEngineMissingAndErrorGuards(t *testing.T) {
	for _, name := range []string{"removed", "inspectText", "inspectMixedJoin", "info404", "engineBefore", "engineAfter", "emptyEngine", "permission", "transport", "nilState", "wrongCID", "cancelAfterInspect"} {
		t.Run(name, func(t *testing.T) {
			h, raw := newProcessBindingFakes()
			b, err := bindCephFSOriginalProcess(t.Context(), h, raw)
			if err != nil {
				t.Fatal(err)
			}
			raw.infos, raw.inspects = 0, 0
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "removed":
				raw.inspectErr = errdefs.ErrNotFound
			case "inspectText":
				raw.inspectErr = errors.New("not found secret 4829")
			case "inspectMixedJoin":
				raw.inspectErr = errors.Join(errdefs.ErrNotFound, errors.New("secret 4829"))
			case "info404":
				raw.infoErr = errdefs.ErrNotFound
				raw.inspectErr = errdefs.ErrNotFound
			case "engineBefore":
				raw.engineID = "engine-B"
				raw.inspectErr = errdefs.ErrNotFound
			case "engineAfter":
				raw.infoHook = func(n int) {
					if n == 2 {
						raw.engineID = "engine-B"
					}
				}
				raw.inspectErr = errdefs.ErrNotFound
			case "emptyEngine":
				raw.engineID = ""
			case "permission":
				raw.inspectErr = errdefs.ErrPermissionDenied
			case "transport":
				raw.inspectErr = errors.New("endpoint secret 4829")
			case "nilState": // A separate minimal raw implementation supplies null State.
				b.docker = processNullStateFake{raw}
			case "wrongCID":
				raw.id = strings.Repeat("b", 64)
			case "cancelAfterInspect":
				raw.inspectHook = func(int) { cancel() }
			}
			i, err := b.inspect(ctx)
			if name == "removed" {
				if err != nil || !i.missing || raw.infos != 2 {
					t.Fatalf("matching bound removal missing: %+v %v", i, err)
				}
			} else {
				if err == nil || i.missing {
					t.Fatalf("invalid absence accepted: %+v %v", i, err)
				}
				if strings.Contains(err.Error(), "4829") {
					t.Fatal("query secret leaked")
				}
				if name == "cancelAfterInspect" && !errors.Is(err, context.Canceled) {
					t.Fatal("cancellation lost")
				}
			}
		})
	}
}

type processNullStateFake struct{ *processRawFake }

func (f processNullStateFake) ContainerInspect(ctx context.Context, id string, opts mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	v, e := f.processRawFake.ContainerInspect(ctx, id, opts)
	v.Container.State = nil
	return v, e
}

type processRoundTripper struct {
	engine          string
	infos, inspects int
	id              string
	inspectStatus   int
}

func (f *processRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var value any
	status := 200
	switch {
	case strings.HasSuffix(req.URL.Path, "/info"):
		f.infos++
		value = system.Info{ID: f.engine}
	case strings.HasSuffix(req.URL.Path, "/containers/"+f.id+"/json"):
		f.inspects++
		s := draftRunning(processDraftStart)
		value = container.InspectResponse{ID: f.id, State: &s}
		if f.inspectStatus != 0 {
			status = f.inspectStatus
			value = map[string]string{"message": "No such container: private endpoint 4829"}
		}
	default:
		return nil, errors.New("unexpected raw API path")
	}
	data, _ := json.Marshal(value)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(data))), Request: req}, nil
}

func TestCephFSOriginalProcessUsesUncachedRawClientAndRetainsEndpoint(t *testing.T) {
	h, _ := newProcessBindingFakes()
	transport := &processRoundTripper{engine: "engine-A", id: h.id}
	raw, err := mobycl.New(mobycl.WithHost("http://original.invalid"), mobycl.WithAPIVersion("1.55"), mobycl.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	// TC wrapper is deliberately present. Binding receives its exported raw Client.
	wrapper := &testcontainers.DockerClient{Client: raw}
	b, err := bindCephFSOriginalProcess(t.Context(), h, wrapper.Client)
	if err != nil {
		t.Fatal(err)
	}
	startInfos := transport.infos
	t.Setenv("DOCKER_HOST", "http://foreign.invalid")
	t.Setenv("DOCKER_CONTEXT", "foreign-context")
	_, err = b.inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if transport.infos != startInfos+2 || raw.DaemonHost() != "http://original.invalid" {
		t.Fatal("raw Info cached or endpoint reseated")
	}
	transport.inspectStatus = http.StatusNotFound
	probe, err := b.inspect(t.Context())
	if err != nil || !probe.missing {
		t.Fatalf("real raw SDK typed404 did not preserve bound absence: %+v %v", probe, err)
	}
	transport.engine = "engine-B"
	_, err = b.inspect(t.Context())
	if !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("fresh raw Info did not observe engine drift")
	}
}

func TestCephFSOriginalProcessTaskEvidence(t *testing.T) {
	for _, name := range []string{"original", "equivalent", "exited", "later", "earlier", "staleExit", "zeroExit", "paused", "restarting", "dead", "created", "removing", "zeroPID", "negativePID", "runningError", "runningFalse", "runningPaused", "zeroStart", "emptyStart", "badStart", "removed"} {
		t.Run(name, func(t *testing.T) {
			s := draftRunning(processDraftStart)
			missing, ended, guard := false, false, false
			switch name {
			case "equivalent":
				s.StartedAt = "2026-10-07T10:00:00+09:00"
			case "exited":
				s = draftExited(processDraftStart)
				ended = true
			case "later":
				s.StartedAt = processDraftLater
				ended = true
			case "earlier":
				s.StartedAt = "2026-10-07T00:59:00Z"
				guard = true
			case "staleExit":
				s = draftExited(processDraftStart)
				s.FinishedAt = "2026-10-07T00:59:00Z"
				guard = true
			case "zeroExit":
				s = draftExited(processDraftStart)
				s.FinishedAt = "0001-01-01T00:00:00Z"
				guard = true
			case "paused":
				s.Status = container.StatePaused
				s.Paused = true
			case "restarting":
				s.Status = container.StateRestarting
				s.Restarting = true
			case "dead":
				s.Status = container.StateDead
				s.Dead = true
			case "created":
				s.Status = container.StateCreated
				s.Running = false
			case "removing":
				s.Status = container.StateRemoving
				s.Running = false
			case "zeroPID":
				s.Pid = 0
				guard = true
			case "negativePID":
				s.Pid = -1
				guard = true
			case "runningError":
				s.Error = "private error 4829"
				guard = true
			case "runningFalse":
				s.Running = false
				guard = true
			case "runningPaused":
				s.Paused = true
				guard = true
			case "zeroStart":
				s.StartedAt = "0001-01-01T00:00:00Z"
				guard = true
			case "emptyStart":
				s.StartedAt = ""
				guard = true
			case "badStart":
				s.StartedAt = "private invalid timestamp 4829"
				guard = true
			case "removed":
				missing = true
				ended = true
			}
			_, got, err := classifyCephFSOriginalTask(processDraftStart, cephFSOriginalProcessProbe{response: container.InspectResponse{State: &s}, missing: missing})
			if got != ended || errors.Is(err, errCephFSObservationGuard) != guard {
				t.Fatalf("evidence changed: ended=%v error=%v", got, err)
			}
		})
	}
}

func TestCephFSOriginalProcessPositiveBindingRejectsIncoherentLiveState(t *testing.T) {
	for _, name := range []string{"zeroPID", "negativePID", "Error", "runningFalse", "paused", "restarting", "dead", "created", "removing", "zeroStart", "badStart"} {
		t.Run(name, func(t *testing.T) {
			h, raw := newProcessBindingFakes()
			switch name {
			case "zeroPID":
				raw.state.Pid = 0
			case "negativePID":
				raw.state.Pid = -1
			case "Error":
				raw.state.Error = "private error 4829"
			case "runningFalse":
				raw.state.Running = false
			case "paused":
				raw.state.Paused = true
			case "restarting":
				raw.state.Restarting = true
			case "dead":
				raw.state.Dead = true
			case "created":
				raw.state.Status = container.StateCreated
			case "removing":
				raw.state.Status = container.StateRemoving
			case "zeroStart":
				raw.state.StartedAt = "0001-01-01T00:00:00Z"
			case "badStart":
				raw.state.StartedAt = "private invalid timestamp 4829"
			}
			b, err := bindCephFSOriginalProcess(t.Context(), h, raw)
			if b != nil || !errors.Is(err, errCephFSObservationGuard) || strings.Contains(err.Error(), "4829") {
				t.Fatalf("incoherent binding accepted or leaked: %v %v", b, err)
			}
		})
	}
}

type processCancellationDuringCause struct{ cancel context.CancelFunc }

func (e processCancellationDuringCause) Error() string { return "private cancellation 4829" }
func (e processCancellationDuringCause) Is(target error) bool {
	e.cancel()
	return target == context.Canceled
}

func TestCephFSOriginalProcessSanitizerPreservesCanonicalJoinedCauses(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	native := errors.Join(errors.New("private backend 4829"), context.Canceled, context.DeadlineExceeded)
	err := processQueryError(ctx, "query original process", native)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "4829") {
		t.Fatal("canonical joined causes or secret masking lost")
	}
	err = processQueryError(ctx, "query original process", processCancellationDuringCause{cancel})
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "4829") {
		t.Fatal("cancellation during single-snapshot sanitizing was lost")
	}
}

func enableProcessDraftBindings(m *CephFSMirror, daemons ...*peerRemovalDaemonFake) []*processRawFake {
	m.originalProcessClientFactory = func(context.Context) (*mobycl.Client, error) { return nil, nil }
	var result []*processRawFake
	for i, d := range daemons {
		d.state.Status = container.StateRunning
		d.state.Pid = 42
		raw := &processRawFake{engineID: "engine-A", id: d.id, state: d.state}
		b := &cephFSBoundOriginalProcess{docker: raw, originalHandle: m.daemons[i].Container, engineID: raw.engineID, containerID: d.id, creationStartedAt: processDraftStart}
		m.daemons[i].originalProcess, m.daemons[i].processBindingState = b, "bound"
		m.owned.addCleanup("close draft original process observer", b.close)
		result = append(result, raw)
	}
	return result
}

func TestCephFSOriginalProcessBeginRequiresCompleteOptionalBindingBeforeMutation(t *testing.T) {
	m, s, _, a, b := newPeerRemovalFixture()
	raws := enableProcessDraftBindings(m, a, b)
	m.daemons[1].originalProcess = nil
	r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if r != nil || !errors.Is(err, errCephFSObservationGuard) || s.removals != 0 {
		t.Fatal("partial capability mutated policy")
	}
	if raws[0].closes != 0 {
		t.Fatal("existing retained client was discarded")
	}
}

func TestCephFSOriginalProcessBeginRejectsSubstitutionAndNumericDuplicate(t *testing.T) {
	for _, name := range []string{"sameCIDHandle", "numericGID"} {
		t.Run(name, func(t *testing.T) {
			m, s, _, a, b := newPeerRemovalFixture()
			enableProcessDraftBindings(m, a, b)
			if name == "sameCIDHandle" {
				copy := *a
				m.daemons[0].Container = &copy
			} else {
				s.watchers = "watcher=172.20.0.8:0/1234 client.4262 cookie=1\nwatcher=172.20.0.9:0/5678 client.04262 cookie=2\n"
			}
			r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
			if r != nil || !errors.Is(err, errCephFSObservationGuard) || s.removals != 0 {
				t.Fatal("optional original ownership guard followed mutation")
			}
		})
	}
}

func TestCephFSOriginalProcessReceiptRejectsSameCIDHandleSubstitution(t *testing.T) {
	m, s, _, a, b := newPeerRemovalFixture()
	raws := enableProcessDraftBindings(m, a, b)
	r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range raws {
		raw.state = draftExited(processDraftStart)
	}
	s.watchers = ""
	copy := *a
	m.daemons[0].Container = &copy
	st, err := r.ProcessQuiescence(t.Context())
	if st.OriginalQuiescent || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("same-CID handle substitution became original proof")
	}
}

func TestCephFSOriginalProcessReceiptBaselineCanFollowPreBeginRestart(t *testing.T) {
	m, _, _, a, b := newPeerRemovalFixture()
	raws := enableProcessDraftBindings(m, a, b)
	a.state.StartedAt = processDraftLater
	raws[0].state.StartedAt = processDraftLater
	r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if err != nil {
		t.Fatal(err)
	}
	if r.cohort[0].startedAt != processDraftLater || r.cohort[0].originalProcess.creationStartedAt != processDraftStart {
		t.Fatal("creation/run provenance confused")
	}
}

func TestCephFSOriginalProcessPeerStopRestartRemovalDoesNotPromoteDrain(t *testing.T) {
	m, s, _, a, b := newPeerRemovalFixture()
	raws := enableProcessDraftBindings(m, a, b)
	r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range raws {
		raw.state = draftExited(processDraftStart)
	}
	a.state, b.state = draftExited(processDraftStart), draftExited(processDraftStart)
	st, err := r.ProcessQuiescence(t.Context())
	if err != nil || st.OriginalQuiescent || !st.PolicyRemoved || !st.Daemons["a"].OriginalTaskEnded || st.Daemons["a"].OriginalWatcherRetired {
		t.Fatalf("exit alone became proof: %+v %v", st, err)
	}
	s.watchers = ""
	st, err = r.ProcessQuiescence(t.Context())
	if err != nil || !st.OriginalQuiescent || r.completed {
		t.Fatalf("stopped original proof/promotion: %+v %v", st, err)
	}
	for _, raw := range raws {
		raw.state = draftRunning(processDraftLater)
	}
	a.state, b.state = draftRunning(processDraftLater), draftRunning(processDraftLater)
	// Same address/new numeric GID does not adopt the new session as original.
	s.watchers = "watcher=172.20.0.8:0/1234 client.5000 cookie=1\n"
	st, err = r.ProcessQuiescence(t.Context())
	if err != nil || !st.OriginalQuiescent || st.Daemons["a"].Evidence != "later-run" || st.Daemons["a"].InstanceID != "4262" {
		t.Fatalf("restart baseline substituted: %+v %v", st, err)
	}
	for _, raw := range raws {
		raw.inspectErr = errdefs.ErrNotFound
	}
	for _, d := range m.daemons {
		d.removed = true
	}
	m.daemons = nil
	m.Container = nil
	s.watchers = ""
	st, err = r.ProcessQuiescence(t.Context())
	if err != nil || !st.OriginalQuiescent || st.Daemons["a"].Evidence != "container-removed" {
		t.Fatalf("removed original client discarded: %+v %v", st, err)
	}
	for _, raw := range raws {
		if raw.closes != 0 {
			t.Fatal("removal closed observer")
		}
	}
	if r.completed || m.guardPeerRemovalOverlap() == nil {
		t.Fatal("diagnostic proof secretly unblocked drain gate")
	}
	if err := m.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, raw := range raws {
		if raw.closes != 1 {
			t.Fatal("fixture cleanup did not close retained observer exactly once")
		}
	}
	st, err = r.ProcessQuiescence(t.Context())
	if st.OriginalQuiescent || !errors.Is(err, errCephFSObservationGuard) {
		t.Fatal("closed fixture reused old process proof")
	}
}

func TestCephFSOriginalProcessWatcherNumericIdentityAndFinalEvidence(t *testing.T) {
	for _, name := range []string{"moved", "numericAlias", "newGID", "finalPolicy", "finalCancel", "processDrift", "watcherDrift", "foreignMember", "FSID"} {
		t.Run(name, func(t *testing.T) {
			m, s, _, a, b := newPeerRemovalFixture()
			raws := enableProcessDraftBindings(m, a, b)
			r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
			if err != nil {
				t.Fatal(err)
			}
			for _, raw := range raws {
				raw.state = draftExited(processDraftStart)
			}
			s.watchers = ""
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			queries := 0
			switch name {
			case "moved":
				s.watchers = "watcher=new-address client.4262 cookie=1\n"
			case "numericAlias":
				s.watchers = "watcher=new-address client.04262 cookie=1\n"
			case "newGID":
				s.watchers = "watcher=new-address client.5000 cookie=1\n"
			case "finalPolicy", "finalCancel":
				s.hook = func(f *peerRemovalControlFake, args []string) {
					if slices.Contains(args, "peer_list") {
						queries++
						if queries == 2 {
							if name == "finalCancel" {
								cancel()
							} else {
								f.fsPeers = peerRemovalRemotePeers()
								f.managerPeers = peerRemovalManagerPeers()
							}
						}
					}
				}
			case "processDrift":
				raws[0].inspects = 0
				raws[0].inspectHook = func(n int) {
					if n == 2 {
						raws[0].state = draftRunning(processDraftLater)
					}
				}
			case "watcherDrift":
				s.hook = func(f *peerRemovalControlFake, args []string) {
					if slices.Contains(args, "listwatchers") {
						queries++
						if queries == 2 {
							f.watchers = "watcher=new-address client.5000 cookie=1\n"
						}
					}
				}
			case "foreignMember":
				m.daemons = append(m.daemons, &CephFSMirrorDaemon{DaemonName: "foreign"})
			case "FSID":
				r.readClusters = func(context.Context) ([2]string, error) {
					return [2]string{peerRemovalDestinationFSID, peerRemovalSourceFSID}, nil
				}
			}
			st, err := r.ProcessQuiescence(ctx)
			if name == "newGID" {
				if err != nil || !st.OriginalQuiescent {
					t.Fatalf("unrelated GID became original: %+v %v", st, err)
				}
			} else {
				if st.OriginalQuiescent {
					t.Fatalf("invalid final proof: %+v %v", st, err)
				}
				for _, entry := range st.Daemons {
					if entry.OriginalQuiescent && err != nil {
						t.Fatal("partial success published before final evidence")
					}
				}
				if name == "finalCancel" && !errors.Is(err, context.Canceled) {
					t.Fatal("final cancel lost")
				}
			}
			if r.completed || s.removals != 1 {
				t.Fatal("observation mutated intent")
			}
		})
	}
}

func TestCephFSOriginalProcessDirectoryAuthorityRemovalAndContract(t *testing.T) {
	for _, name := range []string{"quiescent", "pathPresent", "peerAbsent", "generation", "retired", "unconfigured"} {
		t.Run(name, func(t *testing.T) {
			m, s, _, a, b := newDirectoryRemovalFixture()
			var raws []*processRawFake
			if name != "unconfigured" {
				raws = enableProcessDraftBindings(m, a.peerRemovalDaemonFake, b.peerRemovalDaemonFake)
			}
			r, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader)
			if err != nil {
				t.Fatal(err)
			}
			for _, raw := range raws {
				raw.state = draftExited(processDraftStart)
			}
			s.watchers = ""
			a.state, b.state = draftExited(processDraftStart), draftExited(processDraftStart)
			switch name {
			case "pathPresent":
				s.directories = []string{"/owned"}
			case "peerAbsent":
				s.fsPeers = "{}"
				s.managerPeers = "{}"
			case "generation":
				m.directoryGenerations["/owned"]++
			case "retired":
				m.daemons = nil
				m.Container = nil
				for _, raw := range raws {
					raw.inspectErr = errdefs.ErrNotFound
				}
			}
			st, err := r.ProcessQuiescence(t.Context())
			if name == "quiescent" || name == "retired" {
				if err != nil || !st.OriginalQuiescent || !st.PolicyRemoved || st.Directory != "/owned" {
					t.Fatalf("directory original proof: %+v %v", st, err)
				}
			} else {
				if st.OriginalQuiescent {
					t.Fatalf("invalid authority became proof: %+v %v", st, err)
				}
			}
			if r.completed || m.peerID != cephFSObserverDraftPeer || s.removals != 1 || m.guardDirectoryRemovalOverlap() == nil {
				t.Fatal("quiescence changed Released or overlap gates")
			}
			old, oldErr := r.Status(t.Context())
			if old.Released || (name != "pathPresent" && oldErr == nil) {
				t.Fatal("strict same-session Released changed")
			}
		})
	}
}

func TestCephFSOriginalProcessBindingBusyLockHonorsContext(t *testing.T) {
	h, raw := newProcessBindingFakes()
	b, err := bindCephFSOriginalProcess(t.Context(), h, raw)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	locked, joined := true, false
	done := make(chan error, 1)
	t.Cleanup(func() {
		if locked {
			b.mu.Unlock()
			locked = false
		}
		if !joined {
			select {
			case <-done:
				joined = true
			case <-time.After(500 * time.Millisecond):
				t.Error("observer did not join after cleanup unlock")
			}
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	t.Cleanup(cancel)
	go func() { _, err := b.inspect(ctx); done <- err }()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("retained client lock ignored caller context")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("retained client lock ignored bounded watchdog")
	}
}

func TestCephFSOriginalProcessBusyFixtureAndMemberLocksHaveWatchdog(t *testing.T) {
	for _, name := range []string{"fixture", "member"} {
		t.Run(name, func(t *testing.T) {
			m, _, _, a, b := newPeerRemovalFixture()
			enableProcessDraftBindings(m, a, b)
			r, err := m.beginPeerRemoval(t.Context(), cephFSObserverDraftPeer, peerRemovalClusterReader)
			if err != nil {
				t.Fatal(err)
			}
			var unlock func()
			if name == "fixture" {
				m.mu.Lock()
				unlock = m.mu.Unlock
			} else {
				m.daemons[0].mu.Lock()
				unlock = m.daemons[0].mu.Unlock
			}
			locked, joined := true, false
			done := make(chan error, 1)
			t.Cleanup(func() {
				if locked {
					unlock()
					locked = false
				}
				if !joined {
					select {
					case <-done:
						joined = true
					case <-time.After(500 * time.Millisecond):
						t.Error("quiescence worker did not join after cleanup unlock")
					}
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			t.Cleanup(cancel)
			go func() { _, err := r.ProcessQuiescence(ctx); done <- err }()
			select {
			case err := <-done:
				joined = true
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("process receipt lock ignored caller context")
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("process receipt lock exceeded bounded watchdog")
			}
		})
	}
}
