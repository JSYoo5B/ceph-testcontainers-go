//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The receipt of one MGR transition: a baseline with an active and a standby,
// the promotion of that standby after the active's process stopped, and the
// return of the restarted process as a standby with a new registration.
const mgrReceiptSchema = "ceph-testcontainers-go/mgr-transition-receipt/v1"

// The status of one bounded query. Only a complete query is an observation;
// an absence is observed only in a complete one.
const (
	mgrQueryComplete  = "complete"
	mgrQueryMissing   = "missing"
	mgrQueryTimedOut  = "timeout"
	mgrQueryParse     = "parse_failure"
	mgrQueryTruncated = "truncated"
	mgrQueryRejected  = "native_rejection"
	mgrQueryFailed    = "failed"
)

// The state of one sample against its phase.
const (
	mgrSampleAccepted     = "accepted"
	mgrSamplePending      = "pending"
	mgrSampleTransitional = "transitional"
	mgrSampleIncomplete   = "incomplete"
	mgrSampleViolated     = "violated"
)

// The phases of a transition.
const (
	mgrPhaseBaseline = "baseline"
	mgrPhasePromoted = "promoted"
	mgrPhaseRestored = "restored"
)

// mgrDumpLimit bounds the "mgr dump" output that is decoded. The dump holds
// every module's option descriptions, a few hundred KB; only selected fields
// are retained.
const mgrDumpLimit = 4 << 20

var (
	errMgrMissing   = errors.New("output without the selected fields")
	errMgrMalformed = errors.New("malformed selected fields")
	errMgrOversized = errors.New("output over its bound")
)

// mgrQuery is the outcome of one query.
type mgrQuery struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func (q mgrQuery) complete() bool { return q.Status == mgrQueryComplete }

// mgrQueryOutcome classifies the error of a query.
func mgrQueryOutcome(err error) mgrQuery {
	var native *ceph.NativeFailure
	var syntax *json.SyntaxError
	var kind *json.UnmarshalTypeError
	status := mgrQueryFailed
	switch {
	case err == nil:
		return mgrQuery{Status: mgrQueryComplete}
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		status = mgrQueryTimedOut
	case errors.As(err, &native):
		status = mgrQueryRejected
	case errors.Is(err, errMgrMissing):
		status = mgrQueryMissing
	case errors.Is(err, errMgrOversized):
		status = mgrQueryTruncated
	case errors.As(err, &syntax) || errors.As(err, &kind) || errors.Is(err, errMgrMalformed):
		status = mgrQueryParse
	}
	text := err.Error()
	if len(text) > 256 {
		text = text[:256]
	}
	return mgrQuery{Status: status, Error: text}
}

// mgrRegistration is one MgrMap registration.
type mgrRegistration struct {
	Name string `json:"name"`
	GID  uint64 `json:"gid"`
}

// mgrMapObservation holds the selected fields of one MgrMap.
type mgrMapObservation struct {
	Query     mgrQuery          `json:"query"`
	Epoch     uint64            `json:"epoch,omitempty"`
	Available bool              `json:"available"`
	Active    mgrRegistration   `json:"active"`
	Standbys  []mgrRegistration `json:"standbys"`
}

// same reports whether two complete maps hold the same epoch and
// registrations.
func (m mgrMapObservation) same(other mgrMapObservation) bool {
	return m.Epoch == other.Epoch && m.Available == other.Available && m.Active == other.Active && slices.Equal(m.Standbys, other.Standbys)
}

// registered returns the GID that the map holds for name.
func (m mgrMapObservation) registered(name string) (uint64, bool) {
	if m.Active.Name == name && m.Active.GID != 0 {
		return m.Active.GID, true
	}
	for _, standby := range m.Standbys {
		if standby.Name == name {
			return standby.GID, true
		}
	}
	return 0, false
}

// decodeMgrMap decodes the selected fields of "mgr dump" output. Output
// without them, an empty one included, is missing, and truncated JSON is a
// parse failure: neither is an empty map.
func decodeMgrMap(data []byte) (mgrMapObservation, error) {
	if len(data) > mgrDumpLimit {
		return mgrMapObservation{}, errMgrOversized
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return mgrMapObservation{}, errMgrMissing
	}
	var native struct {
		Epoch      *uint64 `json:"epoch"`
		Available  *bool   `json:"available"`
		ActiveName *string `json:"active_name"`
		ActiveGID  *uint64 `json:"active_gid"`
		Standbys   *[]struct {
			Name *string `json:"name"`
			GID  *uint64 `json:"gid"`
		} `json:"standbys"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return mgrMapObservation{}, err
	}
	if native.Epoch == nil || native.Available == nil || native.ActiveName == nil || native.ActiveGID == nil || native.Standbys == nil {
		return mgrMapObservation{}, errMgrMissing
	}
	observed := mgrMapObservation{Query: mgrQuery{Status: mgrQueryComplete}, Epoch: *native.Epoch, Available: *native.Available,
		Active: mgrRegistration{Name: *native.ActiveName, GID: *native.ActiveGID}, Standbys: []mgrRegistration{}}
	if (observed.Active.Name == "") != (observed.Active.GID == 0) {
		return mgrMapObservation{}, fmt.Errorf("%w: active %q with GID %d", errMgrMalformed, observed.Active.Name, observed.Active.GID)
	}
	for _, standby := range *native.Standbys {
		if standby.Name == nil || standby.GID == nil || *standby.Name == "" || *standby.GID == 0 {
			return mgrMapObservation{}, fmt.Errorf("%w: a standby without its name or GID", errMgrMalformed)
		}
		observed.Standbys = append(observed.Standbys, mgrRegistration{Name: *standby.Name, GID: *standby.GID})
	}
	slices.SortFunc(observed.Standbys, func(a, b mgrRegistration) int { return strings.Compare(a.Name, b.Name) })
	return observed, nil
}

// mgrSession is one MGR session that a MON holds.
type mgrSession struct {
	Entity   string `json:"entity"`
	GlobalID uint64 `json:"global_id"`
}

// mgrMonSessions holds the MGR sessions of one MON: the beacon sessions,
// which a MGR daemon opens as con_type "mgr", and the sessions of its other
// MonClients, whose global IDs are their own.
type mgrMonSessions struct {
	Monitor string       `json:"monitor"`
	Query   mgrQuery     `json:"query"`
	Beacons []mgrSession `json:"beacons"`
	Modules []mgrSession `json:"modules"`
}

// decodeMgrSessions decodes a MON's "sessions" output and keeps its MGR
// sessions.
func decodeMgrSessions(monitor string, data []byte) (mgrMonSessions, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return mgrMonSessions{}, errMgrMissing
	}
	var list []monSession
	if err := json.Unmarshal(data, &list); err != nil {
		return mgrMonSessions{}, err
	}
	if list == nil {
		return mgrMonSessions{}, errMgrMissing
	}
	observed := mgrMonSessions{Monitor: monitor, Query: mgrQuery{Status: mgrQueryComplete}, Beacons: []mgrSession{}, Modules: []mgrSession{}}
	for _, session := range list {
		if !strings.HasPrefix(session.EntityName, "mgr.") {
			continue
		}
		entry := mgrSession{Entity: session.EntityName, GlobalID: session.GlobalID}
		if session.ConType == "mgr" {
			observed.Beacons = append(observed.Beacons, entry)
		} else {
			observed.Modules = append(observed.Modules, entry)
		}
	}
	return observed, nil
}

// beaconSources returns the MONs that hold the beacon session of
// registration: entity mgr.<name>, con_type "mgr" and the MgrMap GID as its
// global ID. A session of the same entity with another global ID is not it.
func beaconSources(monitors []mgrMonSessions, registration mgrRegistration) []string {
	var sources []string
	for _, monitor := range monitors {
		for _, beacon := range monitor.Beacons {
			if beacon.Entity == "mgr."+registration.Name && beacon.GlobalID == registration.GID {
				sources = append(sources, monitor.Monitor)
				break
			}
		}
	}
	return sources
}

// mgrInstance is the container instance of one owned MGR.
type mgrInstance struct {
	Name        string   `json:"name"`
	Query       mgrQuery `json:"query"`
	ContainerID string   `json:"container_id"`
	ImageID     string   `json:"image_id,omitempty"`
	StartedAt   string   `json:"started_at,omitempty"`
	Running     bool     `json:"running"`
}

// mgrQuorum is one fresh quorum_status.
type mgrQuorum struct {
	Query  mgrQuery `json:"query"`
	FSID   string   `json:"fsid,omitempty"`
	Quorum []string `json:"quorum,omitempty"`
}

// mgrModuleControl is the outcome of "rbd task list", a command of the
// rbd_support MGR module; it is no RBD object I/O.
type mgrModuleControl struct {
	Query mgrQuery `json:"query"`
	Tasks int      `json:"tasks"`
}

// decodeModuleTasks decodes the task list of "rbd task list".
func decodeModuleTasks(data []byte) (int, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return 0, errMgrMissing
	}
	var tasks []json.RawMessage
	if err := json.Unmarshal(data, &tasks); err != nil {
		return 0, err
	}
	if tasks == nil {
		return 0, errMgrMissing
	}
	return len(tasks), nil
}

// mgrPhaseSample is one observation of a phase, bracketed by two MgrMaps.
type mgrPhaseSample struct {
	Started   time.Time         `json:"started"`
	Finished  time.Time         `json:"finished"`
	Before    mgrMapObservation `json:"map_before"`
	Monitors  []mgrMonSessions  `json:"monitors"`
	Instances []mgrInstance     `json:"instances"`
	Quorum    mgrQuorum         `json:"quorum"`
	Module    mgrModuleControl  `json:"module"`
	After     mgrMapObservation `json:"map_after"`
	State     string            `json:"state"`
	Reason    string            `json:"reason,omitempty"`
}

// instance returns the sample's instance of the MGR name.
func (s mgrPhaseSample) instance(name string) (mgrInstance, bool) {
	for _, instance := range s.Instances {
		if instance.Name == name {
			return instance, true
		}
	}
	return mgrInstance{}, false
}

// mgrBaseline holds the identities that the baseline froze.
type mgrBaseline struct {
	Active          mgrRegistration `json:"active"`
	Standby         mgrRegistration `json:"standby"`
	ActiveInstance  mgrInstance     `json:"active_instance"`
	StandbyInstance mgrInstance     `json:"standby_instance"`
}

// mgrExpectation is what a phase requires. Baseline is unset for the
// baseline phase itself.
type mgrExpectation struct {
	Phase    string
	FSID     string
	Monitors []string
	Managers []string
	Baseline mgrBaseline
}

// classifyMgrSample judges one sample against its phase. A query that is not
// complete leaves the sample incomplete, and a MgrMap that moved between the
// two brackets leaves it transitional; neither is an observed absence. A
// sample that the transition can no longer turn into the expected state, such
// as a changed standby process or a reused GID, is violated. Otherwise it is
// accepted, or pending with the first condition that is not met yet.
func classifyMgrSample(expect mgrExpectation, sample mgrPhaseSample) (string, string) {
	type named struct {
		name  string
		query mgrQuery
	}
	queries := []named{{"mgr dump before", sample.Before.Query}}
	for _, monitor := range sample.Monitors {
		queries = append(queries, named{"sessions of mon." + monitor.Monitor, monitor.Query})
	}
	for _, instance := range sample.Instances {
		queries = append(queries, named{"container of mgr." + instance.Name, instance.Query})
	}
	queries = append(queries, named{"quorum_status", sample.Quorum.Query}, named{"rbd task list", sample.Module.Query}, named{"mgr dump after", sample.After.Query})
	for _, query := range queries {
		if !query.query.complete() {
			return mgrSampleIncomplete, query.name + ": " + query.query.Status
		}
	}
	if len(sample.Monitors) != len(expect.Monitors) || len(sample.Instances) != len(expect.Managers) {
		return mgrSampleIncomplete, fmt.Sprintf("%d of %d MONs and %d of %d MGR containers observed",
			len(sample.Monitors), len(expect.Monitors), len(sample.Instances), len(expect.Managers))
	}
	if !sample.Before.same(sample.After) {
		return mgrSampleTransitional, fmt.Sprintf("the MgrMap moved from epoch %d to %d during the sample", sample.Before.Epoch, sample.After.Epoch)
	}
	if sample.Quorum.FSID != expect.FSID {
		return mgrSampleViolated, fmt.Sprintf("FSID %s, frozen %s", sample.Quorum.FSID, expect.FSID)
	}
	mgrMap := sample.After
	if expect.Phase != mgrPhaseBaseline {
		if state, reason := mgrViolation(expect.Phase, expect.Baseline, mgrMap, sample); state != "" {
			return state, reason
		}
	}
	for _, monitor := range expect.Monitors {
		if !slices.Contains(sample.Quorum.Quorum, monitor) {
			return mgrSamplePending, "mon." + monitor + " is out of the quorum"
		}
	}
	if sample.Module.Tasks != 0 {
		return mgrSamplePending, fmt.Sprintf("the rbd_support task queue holds %d tasks", sample.Module.Tasks)
	}
	if !mgrMap.Available {
		return mgrSamplePending, "no MGR is available"
	}
	var want []mgrRegistration
	switch expect.Phase {
	case mgrPhaseBaseline:
		if mgrMap.Active.Name == "" || len(mgrMap.Standbys) != 1 {
			return mgrSamplePending, fmt.Sprintf("active %+v with standbys %+v", mgrMap.Active, mgrMap.Standbys)
		}
		want = []mgrRegistration{mgrMap.Active, mgrMap.Standbys[0]}
		for _, registration := range want {
			if instance, ok := sample.instance(registration.Name); !ok || !instance.Running {
				return mgrSamplePending, "mgr." + registration.Name + " is not an owned running container"
			}
		}
	case mgrPhasePromoted:
		base := expect.Baseline
		if mgrMap.Active != base.Standby {
			return mgrSamplePending, fmt.Sprintf("active %+v, not the captured standby %+v", mgrMap.Active, base.Standby)
		}
		if _, ok := mgrMap.registered(base.Active.Name); ok {
			return mgrSamplePending, "the stopped mgr." + base.Active.Name + " is still registered"
		}
		if instance, _ := sample.instance(base.Active.Name); instance.Running {
			return mgrSamplePending, "the container of mgr." + base.Active.Name + " still runs"
		}
		want = []mgrRegistration{mgrMap.Active}
	case mgrPhaseRestored:
		base := expect.Baseline
		if mgrMap.Active != base.Standby {
			return mgrSamplePending, fmt.Sprintf("active %+v, not the promoted %+v", mgrMap.Active, base.Standby)
		}
		if len(mgrMap.Standbys) != 1 || mgrMap.Standbys[0].Name != base.Active.Name {
			return mgrSamplePending, fmt.Sprintf("standbys %+v, not mgr.%s alone", mgrMap.Standbys, base.Active.Name)
		}
		if instance, _ := sample.instance(base.Active.Name); !instance.Running || instance.StartedAt == base.ActiveInstance.StartedAt {
			return mgrSamplePending, fmt.Sprintf("the container of mgr.%s runs %t since %s", base.Active.Name, instance.Running, instance.StartedAt)
		}
		want = []mgrRegistration{mgrMap.Active, mgrMap.Standbys[0]}
	default:
		return mgrSampleViolated, "unknown phase " + expect.Phase
	}
	for _, registration := range want {
		if len(beaconSources(sample.Monitors, registration)) == 0 {
			return mgrSamplePending, fmt.Sprintf("no MON holds the beacon of mgr.%s GID %d", registration.Name, registration.GID)
		}
	}
	return mgrSampleAccepted, ""
}

// mgrViolation returns a violated state when a sample after the baseline
// contradicts it: the captured standby's process or registration changed,
// the old active's container was replaced, or, once restarted, it holds its
// old GID again. Before the restart, the stopped MGR's registration is only
// not gone yet.
func mgrViolation(phase string, base mgrBaseline, mgrMap mgrMapObservation, sample mgrPhaseSample) (string, string) {
	if gid, ok := mgrMap.registered(base.Standby.Name); ok && gid != base.Standby.GID {
		return mgrSampleViolated, fmt.Sprintf("mgr.%s registered again as GID %d, captured %d", base.Standby.Name, gid, base.Standby.GID)
	}
	if mgrMap.Active.Name != "" && mgrMap.Active.Name != base.Standby.Name && mgrMap.Active.Name != base.Active.Name {
		return mgrSampleViolated, "an unowned mgr." + mgrMap.Active.Name + " is active"
	}
	standby, _ := sample.instance(base.Standby.Name)
	if standby.ContainerID != base.StandbyInstance.ContainerID || standby.StartedAt != base.StandbyInstance.StartedAt || !standby.Running {
		return mgrSampleViolated, fmt.Sprintf("the captured standby's process changed: %s since %s running %t, captured %s since %s",
			standby.ContainerID, standby.StartedAt, standby.Running, base.StandbyInstance.ContainerID, base.StandbyInstance.StartedAt)
	}
	if active, _ := sample.instance(base.Active.Name); active.ContainerID != base.ActiveInstance.ContainerID {
		return mgrSampleViolated, fmt.Sprintf("the container of mgr.%s was replaced", base.Active.Name)
	}
	if phase != mgrPhaseRestored {
		return "", ""
	}
	if gid, ok := mgrMap.registered(base.Active.Name); ok && gid == base.Active.GID {
		return mgrSampleViolated, fmt.Sprintf("mgr.%s holds its old GID %d in a later map", base.Active.Name, gid)
	}
	return "", ""
}

// mgrPhaseRecord is one phase of the receipt: its window, the count of
// samples by state, and the samples it retains, which are the last few that
// were not accepted and the final one.
type mgrPhaseRecord struct {
	Phase    string           `json:"phase"`
	Started  time.Time        `json:"started"`
	Finished time.Time        `json:"finished"`
	Deadline string           `json:"deadline"`
	Counts   map[string]int   `json:"counts"`
	Samples  []mgrPhaseSample `json:"samples"`
	Accepted bool             `json:"accepted"`
}

// mgrRetainedSamples bounds the samples that a phase retains.
const mgrRetainedSamples = 4

func (r *mgrPhaseRecord) add(sample mgrPhaseSample) {
	r.Counts[sample.State]++
	r.Samples = append(r.Samples, sample)
	if len(r.Samples) > mgrRetainedSamples {
		r.Samples = slices.Delete(r.Samples, 0, len(r.Samples)-mgrRetainedSamples)
	}
}

// mgrTransitionReceipt is the selected record of one transition.
type mgrTransitionReceipt struct {
	Schema   string            `json:"schema"`
	Source   string            `json:"source"`
	Run      string            `json:"run"`
	Images   map[string]string `json:"images"`
	FSID     string            `json:"fsid"`
	Monitors []string          `json:"monitors"`
	Managers []string          `json:"managers"`
	Baseline *mgrBaseline      `json:"baseline,omitempty"`
	Phases   []mgrPhaseRecord  `json:"phases"`
	Result   string            `json:"result"`
}

// mgrSources is what a sample reads from a cluster.
type mgrSources interface {
	Ceph(ctx context.Context, args ...string) ([]byte, error)
	AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error)
	QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error)
}

// mgrQueryMap reads one MgrMap within bound.
func mgrQueryMap(ctx context.Context, cluster mgrSources, bound time.Duration) mgrMapObservation {
	queryCtx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	data, err := cluster.Ceph(queryCtx, "mgr", "dump", "--format", "json")
	if err == nil {
		var observed mgrMapObservation
		if observed, err = decodeMgrMap(data); err == nil {
			return observed
		}
	}
	return mgrMapObservation{Query: mgrQueryOutcome(err)}
}

// collectMgrSample takes one sample: a MgrMap, each frozen MON's sessions
// through its admin socket, each owned MGR's container, a fresh quorum, the
// module command, and a second MgrMap. Each query has its own bound.
func collectMgrSample(ctx context.Context, cluster mgrSources, monitors []string, managers []*ceph.ManagerContainer, bound time.Duration) mgrPhaseSample {
	sample := mgrPhaseSample{Started: time.Now().UTC(), Before: mgrQueryMap(ctx, cluster, bound)}
	bounded := func(query func(context.Context) error) mgrQuery {
		queryCtx, cancel := context.WithTimeout(ctx, bound)
		defer cancel()
		return mgrQueryOutcome(query(queryCtx))
	}
	for _, monitor := range monitors {
		observed := mgrMonSessions{Monitor: monitor}
		observed.Query = bounded(func(ctx context.Context) error {
			data, err := cluster.AdminSocket(ctx, "mon."+monitor, "sessions")
			if err == nil {
				observed, err = decodeMgrSessions(monitor, data)
			}
			return err
		})
		sample.Monitors = append(sample.Monitors, observed)
	}
	for _, manager := range managers {
		instance := mgrInstance{Name: manager.DaemonName, ContainerID: manager.GetContainerID()}
		instance.Query = bounded(func(ctx context.Context) error {
			inspection, err := manager.Inspect(ctx)
			if err == nil && inspection.State == nil {
				err = errMgrMissing
			}
			if err == nil {
				instance.ImageID, instance.StartedAt, instance.Running = inspection.Image, inspection.State.StartedAt, inspection.State.Running
			}
			return err
		})
		sample.Instances = append(sample.Instances, instance)
	}
	sample.Quorum.Query = bounded(func(ctx context.Context) error {
		status, err := cluster.QuorumStatus(ctx)
		if err == nil {
			sample.Quorum.FSID, sample.Quorum.Quorum = status.MonMap.FSID, slices.Sorted(slices.Values(status.QuorumNames))
		}
		return err
	})
	sample.Module.Query = bounded(func(ctx context.Context) error {
		data, err := cluster.Ceph(ctx, "rbd", "task", "list", "--format", "json")
		if err == nil {
			sample.Module.Tasks, err = decodeModuleTasks(data)
		}
		return err
	})
	sample.After = mgrQueryMap(ctx, cluster, bound)
	sample.Finished = time.Now().UTC()
	return sample
}

// awaitMgrPhase samples a phase until one sample is accepted, one violates
// it, or deadline passes, pausing between samples. Every sample counts, and
// the record keeps the last few.
func awaitMgrPhase(ctx context.Context, cluster mgrSources, expect mgrExpectation, managers []*ceph.ManagerContainer,
	deadline, bound, pause time.Duration) (mgrPhaseRecord, mgrPhaseSample) {
	record := mgrPhaseRecord{Phase: expect.Phase, Started: time.Now().UTC(), Deadline: deadline.String(), Counts: map[string]int{}}
	phaseCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	var sample mgrPhaseSample
	for {
		sample = collectMgrSample(phaseCtx, cluster, expect.Monitors, managers, bound)
		sample.State, sample.Reason = classifyMgrSample(expect, sample)
		record.add(sample)
		if sample.State == mgrSampleAccepted || sample.State == mgrSampleViolated || phaseCtx.Err() != nil {
			break
		}
		select {
		case <-phaseCtx.Done():
		case <-time.After(pause):
		}
		if phaseCtx.Err() != nil {
			break
		}
	}
	record.Finished, record.Accepted = time.Now().UTC(), sample.State == mgrSampleAccepted
	return record, sample
}

// testManagerTransitionReceipt stops the active MGR's process, requires the
// captured standby, with the same process, to take over, and starts the old
// process again until it returns as a standby with a new GID. Each phase is
// accepted only on a sample whose two MgrMaps agree, whose registrations each
// have a MON beacon session, and whose fresh quorum and rbd_support module
// command succeed. The receipt keeps selected fields only.
func testManagerTransitionReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	cluster, _ := newServiceClusterWithOptions(t, image, append(slices.Clone(roles), ceph.WithMonitorCount(3), ceph.WithManagerCount(2), ceph.WithNoInitialOSDs())...)
	waitProfileTopology(t, ctx, cluster, 3, 1)
	receipt := mgrTransitionReceipt{Schema: mgrReceiptSchema, Source: "ceph-testcontainers-go internal/integration", Run: t.Name(),
		Images: map[string]string{"control": image}}
	quorum, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	receipt.FSID = quorum.MonMap.FSID
	for _, monitor := range cluster.Monitors() {
		receipt.Monitors = append(receipt.Monitors, monitor.DaemonName)
	}
	slices.Sort(receipt.Monitors)
	managers, err := cluster.ManagersContext(ctx)
	if err != nil || len(managers) != 2 {
		t.Fatalf("owned MGRs %v: %v", managers, err)
	}
	handles := map[string]*ceph.ManagerContainer{}
	for _, manager := range managers {
		receipt.Managers = append(receipt.Managers, manager.DaemonName)
		handles[manager.DaemonName] = manager
	}
	expect := mgrExpectation{Phase: mgrPhaseBaseline, FSID: receipt.FSID, Monitors: receipt.Monitors, Managers: receipt.Managers}
	const bound, pause = 20 * time.Second, 2 * time.Second
	fail := func(format string, args ...any) {
		t.Helper()
		receipt.Result = fmt.Sprintf(format, args...)
		data, _ := json.Marshal(receipt)
		t.Fatalf("%s\nMGR_TRANSITION_RECEIPT %s", receipt.Result, data)
	}
	phase := func(name string, deadline time.Duration) mgrPhaseSample {
		t.Helper()
		expect.Phase = name
		record, sample := awaitMgrPhase(ctx, cluster, expect, managers, deadline, bound, pause)
		receipt.Phases = append(receipt.Phases, record)
		if !record.Accepted {
			fail("phase %s ended %s: %s", name, sample.State, sample.Reason)
		}
		return sample
	}

	baseline := phase(mgrPhaseBaseline, 3*time.Minute)
	base := mgrBaseline{Active: baseline.After.Active, Standby: baseline.After.Standbys[0]}
	base.ActiveInstance, _ = baseline.instance(base.Active.Name)
	base.StandbyInstance, _ = baseline.instance(base.Standby.Name)
	receipt.Baseline, expect.Baseline = &base, base

	stopGrace := 10 * time.Second
	stopCtx, stopCancel := context.WithTimeout(ctx, time.Minute)
	err = handles[base.Active.Name].Stop(stopCtx, &stopGrace)
	stopCancel()
	if err != nil {
		fail("stop mgr.%s: %v", base.Active.Name, err)
	}
	promoted := phase(mgrPhasePromoted, 4*time.Minute)

	startCtx, startCancel := context.WithTimeout(ctx, 2*time.Minute)
	err = handles[base.Active.Name].Start(startCtx)
	startCancel()
	if err != nil {
		fail("start mgr.%s: %v", base.Active.Name, err)
	}
	restored := phase(mgrPhaseRestored, 4*time.Minute)

	receipt.Result = mgrSampleAccepted
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	rejoined := restored.After.Standbys[0]
	activeAfter, _ := restored.instance(base.Active.Name)
	t.Logf("MGR_TRANSITION epochs %d -> %d -> %d: mgr.%s/%d active -> same mgr.%s/%d active (container %.12s since %s) -> mgr.%s/%d standby (container %.12s, started %s -> %s); beacons on %v, %v, %v",
		baseline.After.Epoch, promoted.After.Epoch, restored.After.Epoch, base.Active.Name, base.Active.GID, base.Standby.Name, base.Standby.GID,
		base.StandbyInstance.ContainerID, base.StandbyInstance.StartedAt, rejoined.Name, rejoined.GID, activeAfter.ContainerID,
		base.ActiveInstance.StartedAt, activeAfter.StartedAt, beaconSources(baseline.Monitors, base.Active), beaconSources(promoted.Monitors, base.Standby),
		beaconSources(restored.Monitors, rejoined))
	t.Logf("MGR_TRANSITION_RECEIPT %s", data)
}
