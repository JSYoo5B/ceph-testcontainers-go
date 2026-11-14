//go:build all || integration

package mgr_pg_merge_primary_override_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// The PG counts of the pools: each pool starts with two PGs, and the
// reduction to one merges the source PG <pool>.1 into its parent <pool>.0.
const (
	mergeFrom = 2
	mergeTo   = 1
)

// mergeHold is how long the pool with the override must stay blocked,
// sample after sample, to be reported as blocked. mergeLimit is how long the
// test samples a pool that has neither merged nor been blocked that long.
const (
	mergeHold  = 2 * time.Minute
	mergeLimit = 6 * time.Minute
)

// poolCounts are the PG counts of one pool in the OSDMap: the current count
// and its target, the placement count, and the count the pool is merging
// towards (equal to the current count while no merge is under way).
type poolCounts struct {
	ID      int64
	PGNum   int
	Target  int
	PGPNum  int
	Pending int
}

// mergeMap is what a decision reads from one OSDMap: the PG counts of every
// pool by name and every primary override (pg-upmap-primary entry) by PG.
type mergeMap struct {
	Epoch     int
	Pools     map[string]poolCounts
	Primaries map[string]int
}

// parseMergeMap reads an "osd dump" report. The report must carry its epoch,
// its pool list and its list of primary overrides, which may be empty but
// not absent or null, every count of every pool, and the PG and the OSD of
// every override. OSD 0 is an OSD; a missing or null one, a negative one and
// the placeholder of a missing OSD are not.
func parseMergeMap(dump []byte) (mergeMap, error) {
	var raw struct {
		Epoch *int `json:"epoch"`
		Pools *[]struct {
			Name    *string `json:"pool_name"`
			ID      *int64  `json:"pool"`
			PGNum   *int    `json:"pg_num"`
			Target  *int    `json:"pg_num_target"`
			PGPNum  *int    `json:"pg_placement_num"`
			Pending *int    `json:"pg_num_pending"`
		} `json:"pools"`
		Primaries *[]struct {
			PG  *string `json:"pgid"`
			OSD *int    `json:"primary_osd"`
		} `json:"pg_upmap_primaries"`
	}
	if err := json.Unmarshal(dump, &raw); err != nil {
		return mergeMap{}, fmt.Errorf("osd dump: %w", err)
	}
	if raw.Epoch == nil || raw.Pools == nil || raw.Primaries == nil {
		return mergeMap{}, fmt.Errorf("osd dump: the epoch, the pool list or the list of primary overrides is missing")
	}
	got := mergeMap{Epoch: *raw.Epoch, Pools: map[string]poolCounts{}, Primaries: map[string]int{}}
	for _, pool := range *raw.Pools {
		if pool.Name == nil || *pool.Name == "" || pool.ID == nil || pool.PGNum == nil || pool.Target == nil || pool.PGPNum == nil || pool.Pending == nil {
			return mergeMap{}, fmt.Errorf("osd dump: a pool lacks its name, its ID or a PG count")
		}
		if _, twice := got.Pools[*pool.Name]; twice {
			return mergeMap{}, fmt.Errorf("osd dump: two pools named %s", *pool.Name)
		}
		got.Pools[*pool.Name] = poolCounts{ID: *pool.ID, PGNum: *pool.PGNum, Target: *pool.Target, PGPNum: *pool.PGPNum, Pending: *pool.Pending}
	}
	for _, entry := range *raw.Primaries {
		if entry.PG == nil || *entry.PG == "" || entry.OSD == nil || *entry.OSD < 0 || *entry.OSD >= noOSD {
			return mergeMap{}, fmt.Errorf("osd dump: a primary override lacks its PG or its OSD")
		}
		if _, twice := got.Primaries[*entry.PG]; twice {
			return mergeMap{}, fmt.Errorf("osd dump: two primary overrides of PG %s", *entry.PG)
		}
		got.Primaries[*entry.PG] = *entry.OSD
	}
	return got, nil
}

// noOSD is the placeholder Ceph reports in place of an OSD where a PG has
// none (CRUSH_ITEM_NONE).
const noOSD = 0x7fffffff

// pgReport is the state, the acting primary and the acting set that the
// manager reports for one PG. The primary is -1 for a PG without one, and
// the acting set may be empty or hold noOSD while the PG is placed anew.
type pgReport struct {
	State   string
	Primary int
	Acting  []int
}

// activeClean reports whether the PG state names both active and clean,
// which is what the manager requires of a PG before it merges it.
func (r pgReport) activeClean() bool {
	flags := strings.Split(r.State, "+")
	return slices.Contains(flags, "active") && slices.Contains(flags, "clean")
}

// served reports whether the PG is active and clean with a primary that is
// a member of an acting set of OSDs only.
func (r pgReport) served() bool {
	return r.activeClean() && r.Primary >= 0 && r.Primary < noOSD && slices.Contains(r.Acting, r.Primary) &&
		!slices.ContainsFunc(r.Acting, func(osd int) bool { return osd < 0 || osd >= noOSD })
}

// mergePrimaries returns the acting primaries the reports of a pool name for
// its merge source and its parent. Both PGs must be in the reports and
// served: a PG that is missing, not active and clean, or without a primary
// in its acting set has no primary to compare.
func mergePrimaries(pgs map[string]pgReport, source, parent string) (sourcePrimary, parentPrimary int, err error) {
	primaries := [2]int{}
	for i, pg := range []string{source, parent} {
		report, reported := pgs[pg]
		if !reported {
			return 0, 0, fmt.Errorf("PG %s is not among the reported PGs: %+v", pg, pgs)
		}
		if !report.served() {
			return 0, 0, fmt.Errorf("PG %s is not active and clean with a primary in its acting set: %+v", pg, report)
		}
		primaries[i] = report.Primary
	}
	return primaries[0], primaries[1], nil
}

// mergeSample is one observation of the pool whose PG count was reduced:
// the time since the first sample, the epoch, the pool's counts and the
// primary overrides of the whole OSDMap, read from one map, and the PGs the
// manager reports for the pool, with its word that it has the PGs' reports
// (Ready).
type mergeSample struct {
	At        time.Duration
	Epoch     int
	Ready     bool
	Counts    poolCounts
	Primaries map[string]int
	PGs       map[string]pgReport
}

// mergePlan names what the case set up: the merge source of the reduced
// pool and its parent, which is the merge target, the OSD the parent's
// override names, and the PG and the OSD of the override in the pool that
// keeps its PG count.
type mergePlan struct {
	Source, Parent string
	ParentOSD      int
	Control        string
	ControlOSD     int
}

// mergeOutcome is what the reduced pool showed. Held are its samples, in
// order, from the moment the pool without an override finished the same
// merge. AfterRemoval is the sample taken after the test itself removed the
// parent's override and waited for the merge; nil when the test did not
// have to.
type mergeOutcome struct {
	Held         []mergeSample
	AfterRemoval *mergeSample
}

// The states of a sample.
const (
	mergeMerged  = "merged"
	mergeBlocked = "blocked"
	mergeMoving  = "moving"
)

// state classifies one sample.
//
// It is merged when the pool has reached the reduced count in every count,
// the manager reports exactly the parent PG, served, and the OSDMap has no
// override of the parent.
//
// It is blocked when the pool still has both PGs and no merge under way
// although its placement count is already reduced, the parent's override is
// still in the OSDMap, and both PGs are served on the same OSDs with
// different reported primaries, the parent's being the OSD of its override:
// the state in which the manager, which merges two PGs only when their
// acting sets are equal, waits for an override it does not remove.
//
// Everything else is moving, and so is every sample for which the manager
// does not say that it has the PGs' reports. In every state the control
// pool's override must be exactly as the case made it, and the OSDMap may
// hold no override the case did not make; a sample that breaks either is an
// error.
func (plan mergePlan) state(sample mergeSample) (string, error) {
	for pg, osd := range sample.Primaries {
		if (pg != plan.Control || osd != plan.ControlOSD) && (pg != plan.Parent || osd != plan.ParentOSD) {
			return "", fmt.Errorf("another fault: the OSDMap has the primary override %s -> osd.%d, which the case did not make: %v", pg, osd, sample.Primaries)
		}
	}
	if osd, held := sample.Primaries[plan.Control]; !held || osd != plan.ControlOSD {
		return "", fmt.Errorf("control not established: the override %s -> osd.%d of the pool that keeps its PG count is gone: %v", plan.Control, plan.ControlOSD, sample.Primaries)
	}
	_, overridden := sample.Primaries[plan.Parent]
	source, hasSource := sample.PGs[plan.Source]
	parent, hasParent := sample.PGs[plan.Parent]
	counts := sample.Counts
	switch {
	case !sample.Ready:
	case counts.PGNum == mergeTo && counts.Target == mergeTo && counts.PGPNum == mergeTo && counts.Pending == mergeTo &&
		!overridden && len(sample.PGs) == mergeTo && hasParent && parent.served():
		return mergeMerged, nil
	case counts.PGNum == mergeFrom && counts.Pending == mergeFrom && counts.Target == mergeTo && counts.PGPNum == mergeTo &&
		overridden && len(sample.PGs) == mergeFrom && hasSource && hasParent && source.served() && parent.served() &&
		parent.Primary == plan.ParentOSD && source.Primary != plan.ParentOSD && len(source.Acting) == len(parent.Acting) &&
		slices.Equal(slices.Sorted(slices.Values(source.Acting)), slices.Sorted(slices.Values(parent.Acting))):
		return mergeBlocked, nil
	}
	return mergeMoving, nil
}

// hold decides what the samples show: mergeMerged when the last sample is
// merged, and mergeBlocked when the samples end in a run of at least two
// blocked samples that spans mergeHold. Samples before that run may be in
// any state: the PGs are placed anew when the placement count is reduced,
// and are not clean while they peer. It returns the samples of the blocked
// run with mergeBlocked. Everything else is an error: a pool that neither
// finished nor stayed blocked that long observed neither outcome, and
// samples whose times do not increase or whose OSDMap epochs decrease are
// not one series of observations.
func (plan mergePlan) hold(held []mergeSample) (string, []mergeSample, error) {
	if len(held) == 0 {
		return "", nil, fmt.Errorf("outcome not observed: no sample of the reduced pool")
	}
	states := make([]string, len(held))
	for i, sample := range held {
		state, err := plan.state(sample)
		if err != nil {
			return "", nil, fmt.Errorf("sample %d: %w", i, err)
		}
		states[i] = state
		if i > 0 && (sample.At <= held[i-1].At || sample.Epoch < held[i-1].Epoch) {
			return "", nil, fmt.Errorf("outcome not observed: sample %d, after %v at epoch %d, does not follow sample %d, after %v at epoch %d",
				i, sample.At, sample.Epoch, i-1, held[i-1].At, held[i-1].Epoch)
		}
	}
	if states[len(states)-1] == mergeMerged {
		return mergeMerged, nil, nil
	}
	first := len(held)
	for first > 0 && states[first-1] == mergeBlocked {
		first--
	}
	run := held[first:]
	if len(run) < 2 || run[len(run)-1].At-run[0].At < mergeHold {
		return "", nil, fmt.Errorf("outcome not observed: the pool neither merged nor stayed blocked by the override of %s for %v; its samples were %v and the last, after %v, is %+v",
			plan.Parent, mergeHold, states, held[len(held)-1].At.Round(time.Second), held[len(held)-1])
	}
	return mergeBlocked, run, nil
}

// judgeMerge decides what the reduced pool showed.
//
// It returns the symptoms of the defect only when the samples end blocked
// for the hold and the pool merged once the test removed the parent's
// override: the merge waited for the override and for nothing else. It
// returns neither symptoms nor an error when the last sample is merged,
// which includes that the parent's override is gone although the test did
// not remove it. Everything else is an error: samples that show neither,
// and a pool that stays unmerged without the override, which has another
// fault.
func judgeMerge(plan mergePlan, got mergeOutcome) ([]string, error) {
	held, run, err := plan.hold(got.Held)
	if err != nil {
		return nil, err
	}
	if held == mergeMerged {
		if got.AfterRemoval != nil {
			return nil, fmt.Errorf("outcome not observed: the pool merged, but the test had removed the override of %s", plan.Parent)
		}
		return nil, nil
	}
	if got.AfterRemoval == nil {
		return nil, fmt.Errorf("outcome not observed: the pool was blocked, but the test did not remove the override of %s to see the merge finish", plan.Parent)
	}
	state, err := plan.state(*got.AfterRemoval)
	if err != nil {
		return nil, fmt.Errorf("sample after the removal: %w", err)
	}
	if state != mergeMerged {
		return nil, fmt.Errorf("another fault: the pool is %s after the override of %s was removed: %+v", state, plan.Parent, *got.AfterRemoval)
	}
	last := run[len(run)-1]
	return []string{
		fmt.Sprintf("the merge of %s into %s did not start in %d samples over %v after a pool without an override finished the same merge: pg_num %d, pg_num_pending %d, pgp_num %d, target %d",
			plan.Source, plan.Parent, len(run), (last.At - run[0].At).Round(time.Second), last.Counts.PGNum, last.Counts.Pending, last.Counts.PGPNum, last.Counts.Target),
		fmt.Sprintf("the manager left the override %s -> osd.%d in place, so the acting sets of %s and %s stayed %v and %v", plan.Parent, plan.ParentOSD, plan.Source, plan.Parent, last.PGs[plan.Source].Acting, last.PGs[plan.Parent].Acting),
		"the pool merged once the test removed the override",
	}, nil
}
