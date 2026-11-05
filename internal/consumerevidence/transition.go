package consumerevidence

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// TransitionSchema names the consumer's MGR transition receipt.
const TransitionSchema = "consumer-example/mgr-transition-receipt/v1"

// The state of one transition sample.
const (
	SampleAccepted     = "accepted"
	SamplePending      = "pending"
	SampleTransitional = "transitional"
	SampleIncomplete   = "incomplete"
	SampleViolated     = "violated"
)

// transitionRetained bounds the samples that a receipt keeps.
const transitionRetained = 4

// TransitionBounds bounds a transition: the whole wait for an accepted
// sample, each read, the pause between samples and the grace of the stop.
type TransitionBounds struct {
	Deadline, Query, Pause, StopGrace time.Duration
}

// TransitionFrozen is what the receipt froze before the stop: the MgrMap and
// every owned MGR's container instance.
type TransitionFrozen struct {
	Map       ManagerMap `json:"map"`
	Instances []Instance `json:"instances"`
}

// TransitionSample is one observation after the restart, bracketed by two
// MgrMap reads.
type TransitionSample struct {
	Started   time.Time         `json:"started"`
	Finished  time.Time         `json:"finished"`
	Before    ManagerMap        `json:"map_before"`
	Monitors  []MonitorSessions `json:"monitors"`
	Instances []Instance        `json:"instances"`
	After     ManagerMap        `json:"map_after"`
	State     string            `json:"state"`
	Reason    string            `json:"reason,omitempty"`
}

// TransitionReceipt records one restart of the active MGR. Changes keeps
// every distinct MgrMap that a read observed, in order, starting with the
// frozen one: the generation and owner changes, not only the final state.
// The reads sample; EpochsNotObserved counts the epochs between two observed
// ones that no read saw, so the receipt never claims to hold every map.
type TransitionReceipt struct {
	Schema            string             `json:"schema"`
	Monitors          []string           `json:"monitors"`
	Managers          []string           `json:"managers"`
	Restarted         string             `json:"restarted"`
	Frozen            TransitionFrozen   `json:"frozen"`
	Started           time.Time          `json:"started"`
	Finished          time.Time          `json:"finished"`
	Deadline          string             `json:"deadline"`
	Changes           []ManagerMap       `json:"changes"`
	EpochsNotObserved uint64             `json:"epochs_not_observed"`
	Counts            map[string]int     `json:"counts"`
	Samples           []TransitionSample `json:"samples"`
	Result            string             `json:"result"`
}

// observe adds the maps of a sample to Changes, and keeps the sample.
func (r *TransitionReceipt) observe(sample TransitionSample) {
	for _, observed := range []ManagerMap{sample.Before, sample.After} {
		if observed.Query.Complete() && (len(r.Changes) == 0 || !r.Changes[len(r.Changes)-1].same(observed)) {
			r.Changes = append(r.Changes, observed)
		}
	}
	r.Counts[sample.State]++
	r.Samples = append(r.Samples, sample)
	if len(r.Samples) > transitionRetained {
		r.Samples = slices.Delete(r.Samples, 0, len(r.Samples)-transitionRetained)
	}
}

// epochsNotObserved counts the epochs that lie between two observed ones.
func epochsNotObserved(changes []ManagerMap) uint64 {
	var missed uint64
	for i := 1; i < len(changes); i++ {
		if changes[i].Epoch > changes[i-1].Epoch+1 {
			missed += changes[i].Epoch - changes[i-1].Epoch - 1
		}
	}
	return missed
}

// ClassifyTransitionSample judges one sample after the restart of the MGR
// restarted against the frozen identities. An incomplete read leaves the
// sample incomplete and a MgrMap that moved between its two reads leaves it
// transitional; neither is an observed absence. A sample that the restart can
// no longer turn into the expected state is violated: an unowned MGR is
// registered, a container was replaced, or the MGR that was not restarted
// changed its process or GID. It is accepted when both MGRs are registered,
// one active and one standby, the restarted one with a new GID and a new
// start time of the same container, and every registration has its beacon
// session on some MON. A registration of the restarted MGR that still holds
// its frozen GID is the stopped process's, kept until its beacon expires, so
// it is pending.
func ClassifyTransitionSample(frozen TransitionFrozen, monitors []string, restarted string, sample TransitionSample) (string, string) {
	queries := []Query{sample.Before.Query}
	for _, monitor := range sample.Monitors {
		queries = append(queries, monitor.Query)
	}
	for _, instance := range sample.Instances {
		queries = append(queries, instance.Query)
	}
	queries = append(queries, sample.After.Query)
	for _, query := range queries {
		if !query.Complete() {
			return SampleIncomplete, query.Name + ": " + query.Status
		}
	}
	if len(sample.Monitors) != len(monitors) || len(sample.Instances) != len(frozen.Instances) {
		return SampleIncomplete, fmt.Sprintf("%d of %d MONs and %d of %d MGR containers observed",
			len(sample.Monitors), len(monitors), len(sample.Instances), len(frozen.Instances))
	}
	if !sample.Before.same(sample.After) {
		return SampleTransitional, fmt.Sprintf("the MgrMap moved from epoch %d to %d during the sample", sample.Before.Epoch, sample.After.Epoch)
	}
	mgrMap := sample.After
	for _, name := range mgrMap.names() {
		if _, owned := instanceOf(frozen.Instances, name); !owned {
			return SampleViolated, "an unowned mgr." + name + " is registered"
		}
	}
	for _, before := range frozen.Instances {
		now, _ := instanceOf(sample.Instances, before.Name)
		if now.ContainerID != before.ContainerID {
			return SampleViolated, "the container of mgr." + before.Name + " was replaced"
		}
		if before.Name == restarted {
			continue
		}
		if now.StartedAt != before.StartedAt || !now.Running {
			return SampleViolated, fmt.Sprintf("mgr.%s, not restarted, runs %t since %s, frozen %s", before.Name, now.Running, now.StartedAt, before.StartedAt)
		}
		if old, ok := frozen.Map.registered(before.Name); ok {
			if gid, ok := mgrMap.registered(before.Name); ok && gid != old {
				return SampleViolated, fmt.Sprintf("mgr.%s, not restarted, registered again as GID %d, frozen %d", before.Name, gid, old)
			}
		}
	}
	if !mgrMap.Available || mgrMap.Active.Name == "" || len(mgrMap.Standbys) != len(frozen.Instances)-1 {
		return SamplePending, fmt.Sprintf("available %t, active %+v, standbys %+v", mgrMap.Available, mgrMap.Active, mgrMap.Standbys)
	}
	for _, before := range frozen.Instances {
		gid, ok := mgrMap.registered(before.Name)
		if !ok {
			return SamplePending, "mgr." + before.Name + " is not registered"
		}
		if before.Name == restarted {
			if old, _ := frozen.Map.registered(restarted); gid == old {
				return SamplePending, fmt.Sprintf("mgr.%s still holds the stopped process's GID %d", restarted, gid)
			}
			if now, _ := instanceOf(sample.Instances, restarted); !now.Running || now.StartedAt == before.StartedAt {
				return SamplePending, fmt.Sprintf("the container of mgr.%s runs %t since %s, frozen %s", restarted, now.Running, now.StartedAt, before.StartedAt)
			}
		}
		if len(beaconMonitors(sample.Monitors, Registration{Name: before.Name, GID: gid})) == 0 {
			return SamplePending, fmt.Sprintf("no MON holds the beacon of mgr.%s GID %d", before.Name, gid)
		}
	}
	return SampleAccepted, ""
}

// RecordTransition freezes the MgrMap and the owned MGRs' containers, stops
// and starts the active MGR's container once, and samples until one sample is
// accepted, one is violated, or bounds.Deadline passes. A failed stop or start
// is not repeated. The receipt is returned in every case, with its result.
func RecordTransition(ctx context.Context, cluster *ceph.Container, bounds TransitionBounds) (TransitionReceipt, error) {
	receipt := TransitionReceipt{Schema: TransitionSchema, Deadline: bounds.Deadline.String(), Counts: map[string]int{}, Changes: []ManagerMap{}}
	fail := func(err error) (TransitionReceipt, error) {
		receipt.Finished, receipt.Result = time.Now().UTC(), err.Error()
		receipt.EpochsNotObserved = epochsNotObserved(receipt.Changes)
		return receipt, err
	}
	monitors, err := cluster.Monitors(ctx)
	if err != nil {
		return fail(fmt.Errorf("list MONs: %w", err))
	}
	managers, err := cluster.Managers(ctx)
	if err != nil {
		return fail(fmt.Errorf("list MGRs: %w", err))
	}
	receipt.Monitors = daemonNames(monitors, func(m *ceph.MonitorContainer) string { return m.DaemonName })
	receipt.Managers = daemonNames(managers, func(m *ceph.ManagerContainer) string { return m.DaemonName })
	receipt.Frozen = TransitionFrozen{Map: readManagerMap(ctx, cluster, bounds.Query), Instances: readInstances(ctx, managers, bounds.Query)}
	if !receipt.Frozen.Map.Query.Complete() || !receipt.Frozen.Map.Available || receipt.Frozen.Map.Active.Name == "" {
		return fail(fmt.Errorf("freeze: no complete MgrMap with an active MGR: %+v", receipt.Frozen.Map))
	}
	for _, instance := range receipt.Frozen.Instances {
		if !instance.Query.Complete() || instance.StartedAt == "" {
			return fail(fmt.Errorf("freeze: %s: %s", instance.Query.Name, instance.Query.Status))
		}
	}
	receipt.Changes = append(receipt.Changes, receipt.Frozen.Map)
	receipt.Restarted = receipt.Frozen.Map.Active.Name
	index := slices.IndexFunc(managers, func(m *ceph.ManagerContainer) bool { return m.DaemonName == receipt.Restarted })
	if index < 0 {
		return fail(errors.New("the active mgr." + receipt.Restarted + " is not an owned MGR"))
	}
	receipt.Started = time.Now().UTC()
	grace := bounds.StopGrace
	if err := managers[index].Stop(ctx, &grace); err != nil {
		return fail(fmt.Errorf("stop mgr.%s: %w", receipt.Restarted, err))
	}
	if err := managers[index].Start(ctx); err != nil {
		return fail(fmt.Errorf("start mgr.%s: %w", receipt.Restarted, err))
	}
	phase, cancel := context.WithTimeout(ctx, bounds.Deadline)
	defer cancel()
	for {
		sample := TransitionSample{Started: time.Now().UTC(), Before: readManagerMap(phase, cluster, bounds.Query)}
		sample.Monitors = readSessions(phase, monitors, bounds.Query)
		sample.Instances = readInstances(phase, managers, bounds.Query)
		sample.After = readManagerMap(phase, cluster, bounds.Query)
		sample.Finished = time.Now().UTC()
		sample.State, sample.Reason = ClassifyTransitionSample(receipt.Frozen, receipt.Monitors, receipt.Restarted, sample)
		receipt.observe(sample)
		switch sample.State {
		case SampleAccepted:
			receipt.Finished, receipt.Result = time.Now().UTC(), SampleAccepted
			receipt.EpochsNotObserved = epochsNotObserved(receipt.Changes)
			return receipt, nil
		case SampleViolated:
			return fail(errors.New("MGR transition violated: " + sample.Reason))
		}
		if phase.Err() != nil || wait(phase, bounds.Pause) != nil {
			return fail(fmt.Errorf("no accepted sample within %v: %s %s", bounds.Deadline, sample.State, sample.Reason))
		}
	}
}
