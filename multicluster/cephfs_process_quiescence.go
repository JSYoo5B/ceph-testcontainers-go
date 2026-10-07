package multicluster

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// A raw binding is a positive association to a trusted ordinary Docker API
// endpoint, not engine authentication, daemon incarnation or restore lineage.
type cephFSOriginalProcessDocker interface {
	Info(context.Context, mobycl.InfoOptions) (mobycl.SystemInfoResult, error)
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	Close() error
}

type cephFSOriginalProcessHandle interface {
	GetContainerID() string
	Inspect(context.Context) (*container.InspectResponse, error)
}

type cephFSBoundOriginalProcess struct {
	mu                                       sync.Mutex
	docker                                   cephFSOriginalProcessDocker
	originalHandle                           cephFSOriginalProcessHandle
	engineID, containerID, creationStartedAt string
	closed                                   bool
}

// CephFSMirrorProcessBindingStatus is a local snapshot of optional capability.
// State contains a finite reason; factory errors and endpoint bodies are omitted.
type CephFSMirrorProcessBindingStatus struct {
	Available                    bool
	State, EngineID, ContainerID string
}

// ProcessObserverBindingStatus reports local optional binding availability.
// It does not query Docker or establish task cessation.
func (d *CephFSMirrorDaemon) ProcessObserverBindingStatus() CephFSMirrorProcessBindingStatus {
	if d == nil {
		return CephFSMirrorProcessBindingStatus{State: "unavailable"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	state := d.processBindingState
	if state == "" {
		state = "disabled"
	}
	result := CephFSMirrorProcessBindingStatus{State: state}
	if b := d.originalProcess; b != nil {
		b.mu.Lock()
		defer b.mu.Unlock()
		result.EngineID, result.ContainerID = b.engineID, b.containerID
		result.Available = !b.closed
		if b.closed {
			result.State = "closed"
		}
	}
	return result
}

// Called after ordinary Run succeeds, under mirror.mu, before publication.
// Failure is finite observable capability state; ordinary Run's result survives.
// The configured public factory returns a raw *moby.Client, so TC cached Info
// cannot accidentally be selected as the production identity implementation.
func (m *CephFSMirror) bindOriginalProcessClient(ctx context.Context, d *CephFSMirrorDaemon, c *testcontainers.DockerContainer) {
	d.processBindingState = "positive-binding-failed"
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		d.processBindingState = "context-unavailable"
		return
	}
	raw, err := m.originalProcessClientFactory(ctx)
	var candidate cephFSOriginalProcessDocker
	if raw != nil {
		candidate = raw
	}
	b := bindOriginalProcessCandidate(ctx, d, c, candidate, err)
	if b == nil {
		return
	}
	// RemoveDaemon/direct daemon Terminate deliberately do not close this client.
	m.owned.addCleanup("close original CephFS process observer", b.close)
}

func bindOriginalProcessCandidate(ctx context.Context, d *CephFSMirrorDaemon, h cephFSOriginalProcessHandle, raw cephFSOriginalProcessDocker, factoryErr error) *cephFSBoundOriginalProcess {
	d.processBindingState = "positive-binding-failed"
	if factoryErr != nil {
		if raw != nil {
			_ = raw.Close()
		}
		d.processBindingState = "factory-error"
		return nil
	}
	if raw == nil {
		d.processBindingState = "empty-client"
		return nil
	}
	b, err := bindCephFSOriginalProcess(ctx, h, raw)
	if err != nil {
		_ = raw.Close()
		return nil
	}
	d.originalProcess, d.processBindingState = b, "bound"
	return b
}

func bindCephFSOriginalProcess(ctx context.Context, h cephFSOriginalProcessHandle, raw cephFSOriginalProcessDocker) (*cephFSBoundOriginalProcess, error) {
	if h == nil || raw == nil || !validCephFSRemovalContainerID(h.GetContainerID()) {
		return nil, cephFSObserveGuard("original process binding input is unavailable")
	}
	id := h.GetContainerID()
	before, err := h.Inspect(ctx)
	if err != nil {
		return nil, processQueryError(ctx, "inspect returned original process", err)
	}
	if !validOriginalLiveInspection(before, id) {
		return nil, cephFSObserveGuard("returned original live process is unavailable")
	}
	info, err := raw.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		return nil, processQueryError(ctx, "read original engine candidate", err)
	}
	if info.Info.ID == "" {
		return nil, cephFSObserveGuard("original engine candidate identity is unavailable")
	}
	b := &cephFSBoundOriginalProcess{docker: raw, originalHandle: h, engineID: info.Info.ID, containerID: id, creationStartedAt: before.State.StartedAt}
	for range 2 {
		probe, err := b.inspect(ctx)
		if err != nil {
			return nil, err
		}
		if probe.missing || !validOriginalLiveInspection(&probe.response, id) || !sameCephFSRemovalStartedAt(probe.response.State.StartedAt, before.State.StartedAt) {
			return nil, cephFSObserveGuard("candidate did not positively match the original live run")
		}
		after, err := h.Inspect(ctx)
		if err != nil {
			return nil, processQueryError(ctx, "confirm returned original process", err)
		}
		if h.GetContainerID() != id || !validOriginalLiveInspection(after, id) || !sameCephFSRemovalStartedAt(after.State.StartedAt, before.State.StartedAt) {
			return nil, cephFSObserveGuard("returned original process changed during binding")
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b, nil
}

func validOriginalLiveInspection(i *container.InspectResponse, id string) bool {
	return i != nil && i.ID == id && normalOriginalRawProcess(i.State) && validProcessTime(i.State.StartedAt)
}

type cephFSOriginalProcessProbe struct {
	response container.InspectResponse
	missing  bool
}

// Inspect only the positively bound full CID, never a name/current default.
// Missing is admitted only between two uncached matching raw Info responses.
func (b *cephFSBoundOriginalProcess) inspect(ctx context.Context) (cephFSOriginalProcessProbe, error) {
	var result cephFSOriginalProcessProbe
	if b == nil || b.docker == nil || b.engineID == "" || !validCephFSRemovalContainerID(b.containerID) {
		return result, cephFSObserveGuard("original raw process binding is unavailable")
	}
	if err := lockRGWSyncObservation(ctx, &b.mu); err != nil {
		return result, err
	}
	defer b.mu.Unlock()
	if b.closed {
		return result, cephFSObserveGuard("original raw process binding is closed")
	}
	checkEngine := func() error {
		info, err := b.docker.Info(ctx, mobycl.InfoOptions{})
		if err != nil {
			return processQueryError(ctx, "query retained original engine identity", err)
		}
		if info.Info.ID == "" || info.Info.ID != b.engineID {
			return cephFSObserveGuard("retained original engine identity differs")
		}
		return ctx.Err()
	}
	if err := checkEngine(); err != nil {
		return result, err
	}
	i, inspectErr := b.docker.ContainerInspect(ctx, b.containerID, mobycl.ContainerInspectOptions{})
	if err := checkEngine(); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if inspectErr != nil {
		if entirelyMissing(inspectErr) {
			result.missing = true
			return result, nil
		}
		return result, processQueryError(ctx, "query retained original container", inspectErr)
	}
	if i.Container.ID != b.containerID || i.Container.State == nil {
		return result, cephFSObserveGuard("retained original container response is incomplete or substituted")
	}
	result.response = i.Container
	copy := *i.Container.State
	result.response.State = &copy
	return result, nil
}

func (b *cephFSBoundOriginalProcess) close(ctx context.Context) error {
	if err := lockRGWSyncObservation(ctx, &b.mu); err != nil {
		return err
	}
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	if err := b.docker.Close(); err != nil {
		return processQueryError(ctx, "close original process observer", err)
	}
	b.closed = true
	return nil
}

func processQueryError(ctx context.Context, operation string, native error) error {
	ctxErr := ctx.Err()
	var causes []error
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(ctxErr, cause) || errors.Is(native, cause) {
			causes = append(causes, cause)
		}
	}
	// Raw endpoint bodies/custom errors are never retained. Both canonical causes
	// survive, including cancellation after the single context snapshot.
	return cephFSObserveQuery(operation, errors.Join(causes...))
}

func normalOriginalRawProcess(s *container.State) bool {
	return normalCephFSRemovalProcess(s) && s.Status == container.StateRunning && s.Pid > 0 && s.Error == ""
}

func validProcessTime(value string) bool {
	v, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && !v.IsZero()
}

func confirmCephFSOriginalRun(ctx context.Context, w cephFSPeerRemovalWitness) error {
	if w.daemon == nil || w.originalProcess == nil || w.originalProcess.containerID != w.containerID || w.originalProcess.originalHandle != w.daemon.Container {
		return cephFSObserveGuard("original process receipt binding differs")
	}
	i, err := w.originalProcess.inspect(ctx)
	if err != nil {
		return err
	}
	if i.missing || !validOriginalLiveInspection(&i.response, w.containerID) || !sameCephFSRemovalStartedAt(i.response.State.StartedAt, w.startedAt) {
		return cephFSObserveGuard("original receipt run did not positively match its retained binding")
	}
	return ctx.Err()
}

// CephFSMirrorProcessQuiescenceStatus is original-cohort evidence only.
// It does not assert graceful teardown, current fixture silence, successful
// remote unlock, irreversibility against checkpoint restore, Drained or Released.
type CephFSMirrorProcessQuiescenceStatus struct {
	PeerID, Directory, SourceFilesystem, DestinationFilesystem string
	SourceFilesystemID, DestinationFilesystemID                int
	PolicyRemoved, OriginalQuiescent                           bool
	Daemons                                                    map[string]CephFSMirrorOriginalProcessStatus
}

// CephFSMirrorOriginalProcessStatus retains one original run and watcher.
// New runs and unrelated watchers never replace these receipt identities.
type CephFSMirrorOriginalProcessStatus struct {
	EngineID, ContainerID, StartedAt, InstanceID                 string
	OriginalTaskEnded, OriginalWatcherRetired, OriginalQuiescent bool
	Evidence, Problem                                            string
}

// ProcessQuiescence does not mutate completed or relax existing overlap gates.
func (r *CephFSMirrorPeerRemoval) ProcessQuiescence(ctx context.Context) (CephFSMirrorProcessQuiescenceStatus, error) {
	if r == nil {
		return CephFSMirrorProcessQuiescenceStatus{}, cephFSObserveGuard("original peer process receipt is unavailable")
	}
	return observeCephFSReceiptProcessQuiescence(ctx, r, "", r.checkHandle, func(ctx context.Context) (bool, error) {
		present, fsPresent, err := r.policy(ctx)
		return !present && !fsPresent && err == nil, err
	})
}

// ProcessQuiescence retains the peer tuple and original path generation.
// Path policy absence is the existing exact native mirror ls observation.
func (r *CephFSMirrorDirectoryRemoval) ProcessQuiescence(ctx context.Context) (CephFSMirrorProcessQuiescenceStatus, error) {
	if r == nil || r.original == nil {
		return CephFSMirrorProcessQuiescenceStatus{}, cephFSObserveGuard("original directory process receipt is unavailable")
	}
	return observeCephFSReceiptProcessQuiescence(ctx, r.original, r.directory, func() error { return r.checkDirectoryRemovalHandle(true) }, func(ctx context.Context) (bool, error) {
		if err := r.peerPolicy(ctx); err != nil {
			return false, err
		}
		present, err := r.policy(ctx)
		return !present && err == nil, err
	})
}

func observeCephFSReceiptProcessQuiescence(ctx context.Context, o *CephFSMirrorPeerRemoval, directory string, owner func() error, authority func(context.Context) (bool, error)) (CephFSMirrorProcessQuiescenceStatus, error) {
	result := initialCephFSProcessQuiescenceStatus(o, directory)
	if o == nil || o.mirror == nil || owner == nil || authority == nil {
		return result, cephFSObserveGuard("original process receipt owner is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &o.mirror.mu); err != nil {
		return result, err
	}
	defer o.mirror.mu.Unlock()
	return observeCephFSReceiptProcessQuiescenceLocked(ctx, o, directory, owner, authority)
}

func initialCephFSProcessQuiescenceStatus(o *CephFSMirrorPeerRemoval, directory string) CephFSMirrorProcessQuiescenceStatus {
	result := CephFSMirrorProcessQuiescenceStatus{Directory: directory, Daemons: make(map[string]CephFSMirrorOriginalProcessStatus)}
	if o == nil {
		return result
	}
	result.PeerID, result.SourceFilesystem, result.DestinationFilesystem = o.peerID, o.sourceFilesystem, o.destinationFilesystem
	result.SourceFilesystemID, result.DestinationFilesystemID = o.sourceID, o.destinationID
	for _, w := range o.cohort {
		entry := CephFSMirrorOriginalProcessStatus{ContainerID: w.containerID, StartedAt: w.startedAt, InstanceID: w.instanceID, Evidence: "unobserved"}
		if w.originalProcess != nil {
			entry.EngineID = w.originalProcess.engineID
		}
		result.Daemons[w.name] = entry
	}
	return result
}

// Called with mirror.mu held. Proof and an explicit acknowledgment decision can
// share one fixture gate without admitting a local generation change between.
func observeCephFSReceiptProcessQuiescenceLocked(ctx context.Context, o *CephFSMirrorPeerRemoval, directory string, owner func() error, authority func(context.Context) (bool, error)) (CephFSMirrorProcessQuiescenceStatus, error) {
	result := initialCephFSProcessQuiescenceStatus(o, directory)
	if o == nil || o.mirror == nil || owner == nil || authority == nil {
		return result, cephFSObserveGuard("original process receipt owner is unavailable")
	}
	if err := owner(); err != nil {
		return result, err
	}
	if err := validateCephFSProcessCohort(ctx, o); err != nil {
		return result, err
	}
	absent, err := authority(ctx)
	if err != nil {
		return result, err
	}
	result.PolicyRemoved = absent
	if !absent {
		return result, ctx.Err()
	}
	before, err := o.watchers(ctx)
	if err != nil {
		return result, err
	}
	if err := validateQuiescenceWatchers(before); err != nil {
		return result, err
	}
	first := make(map[string]cephFSOriginalProcessProbe)
	for _, w := range o.cohort {
		i, err := w.originalProcess.inspect(ctx)
		if err != nil {
			return result, annotateProcessProblem(&result, w.name, err)
		}
		kind, ended, err := classifyCephFSOriginalTask(w.startedAt, i)
		if err != nil {
			return result, annotateProcessProblem(&result, w.name, err)
		}
		first[w.name] = i
		entry := result.Daemons[w.name]
		entry.Evidence, entry.OriginalTaskEnded = kind, ended
		entry.OriginalWatcherRetired = originalNumericWatcherRetired(w.instanceID, before)
		result.Daemons[w.name] = entry
	}
	after, err := o.watchers(ctx)
	if err != nil {
		return result, err
	}
	if err := validateQuiescenceWatchers(after); err != nil {
		return result, err
	}
	if !equalCephFSRemovalWatchers(before, after) {
		return result, cephFSObserveQuery("original process watcher set changed during observation", nil)
	}
	for _, w := range o.cohort {
		i, err := w.originalProcess.inspect(ctx)
		if err != nil {
			return result, annotateProcessProblem(&result, w.name, err)
		}
		if !sameOriginalProcessProbe(first[w.name], i) {
			return result, cephFSObserveQuery("original process state changed during observation", nil)
		}
	}
	// Policy/owner/context rechecks precede every published per-member success.
	absent, err = authority(ctx)
	if err != nil {
		return result, err
	}
	result.PolicyRemoved = absent
	if err := owner(); err != nil {
		return result, err
	}
	if err := validateCephFSProcessCohort(ctx, o); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	all := absent
	for name, entry := range result.Daemons {
		entry.OriginalQuiescent = absent && entry.OriginalTaskEnded && entry.OriginalWatcherRetired
		result.Daemons[name] = entry
		all = all && entry.OriginalQuiescent
	}
	result.OriginalQuiescent = all
	return result, nil
}

func annotateProcessProblem(result *CephFSMirrorProcessQuiescenceStatus, name string, err error) error {
	entry := result.Daemons[name]
	entry.Problem = "original process evidence unavailable"
	if errors.Is(err, errCephFSObservationGuard) {
		entry.Problem = "original process identity guard"
	}
	result.Daemons[name] = entry
	return err
}

func validateCephFSProcessCohort(ctx context.Context, o *CephFSMirrorPeerRemoval) error {
	if o == nil || o.mirror == nil || len(o.cohort) == 0 {
		return cephFSObserveGuard("original process cohort is empty")
	}
	owned := make(map[*CephFSMirrorDaemon]bool)
	names, ids, gids := make(map[string]bool), make(map[string]bool), make(map[uint64]bool)
	for _, w := range o.cohort {
		gid, err := strconv.ParseUint(w.instanceID, 10, 64)
		if w.daemon == nil {
			return cephFSObserveGuard("original daemon identity is unavailable")
		}
		if err := lockRGWSyncObservation(ctx, &w.daemon.mu); err != nil {
			return err
		}
		sameOwner := w.originalProcess != nil && w.originalProcess.originalHandle == w.daemon.Container && w.daemon.originalProcess == w.originalProcess && w.daemon.DaemonName == w.name
		w.daemon.mu.Unlock()
		if !sameOwner || w.daemon == nil || w.originalProcess == nil || w.daemon.originalProcess != w.originalProcess || w.daemon.DaemonName != w.name || w.originalProcess.containerID != w.containerID || w.address == "" || !validProcessTime(w.startedAt) || err != nil || gid == 0 || names[w.name] || ids[w.containerID] || gids[gid] || owned[w.daemon] {
			return cephFSObserveGuard("original process cohort binding or numeric watcher identity differs")
		}
		owned[w.daemon], names[w.name], ids[w.containerID], gids[gid] = true, true, true, true
	}
	for _, d := range o.mirror.daemons {
		if !owned[d] {
			return cephFSObserveGuard("current process membership is outside the original cohort")
		}
	}
	return nil
}

func validateQuiescenceWatchers(values map[string]string) error {
	if values == nil {
		return cephFSObserveGuard("complete original process watcher set is unavailable")
	}
	for address, value := range values {
		gid, err := strconv.ParseUint(value, 10, 64)
		if address == "" || err != nil || gid == 0 {
			return cephFSObserveGuard("original process watcher identity is invalid")
		}
	}
	return nil
}

func originalNumericWatcherRetired(baseline string, values map[string]string) bool {
	old, _ := strconv.ParseUint(baseline, 10, 64)
	for _, value := range values {
		gid, _ := strconv.ParseUint(value, 10, 64)
		if gid == old {
			return false
		}
	}
	return true
}

func classifyCephFSOriginalTask(original string, i cephFSOriginalProcessProbe) (string, bool, error) {
	start, err := time.Parse(time.RFC3339Nano, original)
	if err != nil || start.IsZero() {
		return "", false, cephFSObserveGuard("original process start is invalid")
	}
	if i.missing {
		return "container-removed", true, nil
	}
	s := i.response.State
	if s == nil {
		return "", false, cephFSObserveGuard("original process state is unavailable")
	}
	current, err := time.Parse(time.RFC3339Nano, s.StartedAt)
	if err != nil || current.IsZero() {
		return "", false, cephFSObserveGuard("original process observed start is invalid")
	}
	if current.Before(start) {
		return "", false, cephFSObserveGuard("original process start regressed")
	}
	if normalOriginalRawProcess(s) {
		if current.After(start) {
			return "later-run", true, nil
		}
		return "original-running", false, nil
	}
	if s.Status == container.StateRunning {
		return "", false, cephFSObserveGuard("original raw running state is incoherent")
	}
	if s.Status == container.StateExited && !s.Running && !s.Paused && !s.Restarting && !s.Dead && s.Pid == 0 && s.Error == "" {
		finished, err := time.Parse(time.RFC3339Nano, s.FinishedAt)
		if err != nil || finished.IsZero() || finished.Before(start) || finished.Before(current) {
			return "", false, cephFSObserveGuard("original process exit evidence is incoherent")
		}
		return "exited", true, nil
	}
	return "state-pending", false, nil
}

func sameOriginalProcessProbe(a, b cephFSOriginalProcessProbe) bool {
	if a.missing || b.missing {
		return a.missing && b.missing
	}
	if a.response.ID != b.response.ID || a.response.State == nil || b.response.State == nil {
		return false
	}
	x, y := a.response.State, b.response.State
	return x.Status == y.Status && x.Running == y.Running && x.Paused == y.Paused && x.Restarting == y.Restarting && x.Dead == y.Dead && x.Pid == y.Pid && x.Error == y.Error && sameCephFSRemovalStartedAt(x.StartedAt, y.StartedAt) && x.FinishedAt == y.FinishedAt
}
