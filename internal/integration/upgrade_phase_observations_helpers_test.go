//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The types below model, without Ceph or Docker, how a private mixed-version
// upgrade harness keeps its observations fresh. Observations belong to one
// phase and to the generation of every target they describe. A phase never
// inherits an answer from an earlier phase, and an answer about a target is
// not reused after that target's image or executor changed. The ordinary
// uniform-release helpers (requireUniformRelease, cachedCapability) are not
// involved and keep their policy.

var (
	errUpgradePhaseClosed = errors.New("upgrade phase is closed")
	errUpgradeStale       = errors.New("upgrade observation is stale")
)

// upgradeInventory maps a daemon role to a destination-or-source release and
// the number of daemons "ceph versions" reported for it.
type upgradeInventory map[string]map[string]int

// parseUpgradeInventory decodes "ceph versions" JSON. The monitor emits one
// object per role (mon always; mgr, osd and mds only when metadata exists)
// plus "overall". A missing or null "mon" or "overall", a non-object role, a
// count that is not a positive integer, a version description that does not
// start with a release, or an "overall" that disagrees with the roles is
// unusable evidence and is returned as an error, never as an empty inventory.
func parseUpgradeInventory(data []byte) (upgradeInventory, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil || top == nil {
		return nil, fmt.Errorf("ceph versions is not a JSON object: %q", data)
	}
	for _, required := range []string{"mon", "overall"} {
		raw, ok := top[required]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil, fmt.Errorf("ceph versions has no %q object", required)
		}
	}
	inventory := upgradeInventory{}
	overall := map[string]int{}
	roles := make([]string, 0, len(top))
	for role := range top {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		counts, err := decodeUpgradeCounts(role, top[role])
		if err != nil {
			return nil, err
		}
		if role == "overall" {
			overall = counts
			continue
		}
		if len(counts) == 0 {
			return nil, fmt.Errorf("ceph versions role %q has no daemons", role)
		}
		inventory[role] = counts
	}
	sum := map[string]int{}
	for _, counts := range inventory {
		for release, n := range counts {
			sum[release] += n
		}
	}
	if len(sum) != len(overall) {
		return nil, fmt.Errorf("ceph versions overall %v disagrees with roles %v", overall, sum)
	}
	for release, n := range sum {
		if overall[release] != n {
			return nil, fmt.Errorf("ceph versions overall %v disagrees with roles %v", overall, sum)
		}
	}
	return inventory, nil
}

// decodeUpgradeCounts folds version descriptions to releases. Distinct
// descriptions of one release (different builds) add up.
func decodeUpgradeCounts(role string, raw json.RawMessage) (map[string]int, error) {
	var described map[string]json.RawMessage
	if trimmed := bytes.TrimSpace(raw); bytes.Equal(trimmed, []byte("null")) || json.Unmarshal(raw, &described) != nil || described == nil {
		return nil, fmt.Errorf("ceph versions role %q is not an object", role)
	}
	counts := map[string]int{}
	for description, rawCount := range described {
		match := harness.DaemonVersionPattern.FindStringSubmatch(description + " ")
		if match == nil {
			return nil, fmt.Errorf("ceph versions role %q has unrecognized release %s", role, strconv.Quote(description))
		}
		n, err := strconv.Atoi(strings.TrimSpace(string(rawCount)))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("ceph versions role %q count for %s is %q, want a positive integer", role, match[1], rawCount)
		}
		counts[match[1]] += n
	}
	return counts, nil
}

// upgradeCheckpoint is the explicitly allowed per-role release inventory of
// one phase. Roles absent from it must be absent from the observation.
type upgradeCheckpoint map[string]map[string]int

// check compares a fresh inventory with the checkpoint.
func (c upgradeCheckpoint) Check(got upgradeInventory) error {
	var problems []string
	for role, want := range c {
		have, ok := got[role]
		if !ok {
			problems = append(problems, fmt.Sprintf("role %q missing, want %v", role, want))
			continue
		}
		if fmt.Sprint(have) != fmt.Sprint(want) { // fmt prints maps in key order
			problems = append(problems, fmt.Sprintf("role %q has %v, want %v", role, have, want))
		}
	}
	for role, have := range got {
		if _, ok := c[role]; !ok {
			problems = append(problems, fmt.Sprintf("unexpected role %q with %v", role, have))
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		return errors.New("version inventory differs from the checkpoint: " + strings.Join(problems, "; "))
	}
	return nil
}

// upgradeTargets tracks the generation of each observed target (a daemon or
// a CLI executor). A generation advances when its image or executor changes.
type upgradeTargets struct {
	generation map[string]uint64
	phase      uint64
}

func newUpgradeTargets(targets ...string) *upgradeTargets {
	u := &upgradeTargets{generation: map[string]uint64{}}
	for _, target := range targets {
		u.generation[target] = 1
	}
	return u
}

// advance records that target now runs a different image or executor.
func (u *upgradeTargets) advance(target string) error {
	if _, ok := u.generation[target]; !ok {
		return fmt.Errorf("unknown upgrade target %q", target)
	}
	u.generation[target]++
	return nil
}

// begin opens a phase and closes the previous one.
func (u *upgradeTargets) begin(name string) *upgradePhase {
	u.phase++
	snapshot := make(map[string]uint64, len(u.generation))
	for target, g := range u.generation {
		snapshot[target] = g
	}
	return &upgradePhase{owner: u, name: name, seq: u.phase, opened: snapshot, caps: map[upgradeCapKey]bool{}}
}

type upgradeCapKey struct {
	target     string
	generation uint64
	id         string
}

// upgradePhase holds the observations of one checkpoint.
type upgradePhase struct {
	owner     *upgradeTargets
	name      string
	seq       uint64
	opened    map[string]uint64
	inventory upgradeInventory
	caps      map[upgradeCapKey]bool
}

func (p *upgradePhase) Open() error {
	if p.owner.phase != p.seq {
		return fmt.Errorf("%w: %q was superseded by a later phase", errUpgradePhaseClosed, p.name)
	}
	return nil
}

// current rejects the phase when any target moved since it began.
func (p *upgradePhase) current() error {
	if err := p.Open(); err != nil {
		return err
	}
	var moved []string
	for target, g := range p.opened {
		if p.owner.generation[target] != g {
			moved = append(moved, target)
		}
	}
	sort.Strings(moved)
	if len(moved) > 0 {
		return fmt.Errorf("%w: targets %v changed after phase %q began", errUpgradeStale, moved, p.name)
	}
	return nil
}

// observeInventory parses fresh "ceph versions" output, checks it against the
// phase checkpoint and keeps it only when both succeed.
func (p *upgradePhase) observeInventory(data []byte, want upgradeCheckpoint) error {
	if err := p.current(); err != nil {
		return err
	}
	got, err := parseUpgradeInventory(data)
	if err != nil {
		return err
	}
	if err := want.Check(got); err != nil {
		return err
	}
	p.inventory = got
	return nil
}

// satisfied reports the accepted inventory of this phase.
func (p *upgradePhase) satisfied() (upgradeInventory, error) {
	if err := p.current(); err != nil {
		return nil, err
	}
	if p.inventory == nil {
		return nil, fmt.Errorf("phase %q has no accepted version inventory", p.name)
	}
	return p.inventory, nil
}

// capability answers whether target provides id. An answer is reused only
// inside this phase and for the generation of target it was probed at. A
// probe error is returned and never cached, so an unavailable observation is
// not a false answer.
func (p *upgradePhase) capability(target, id string, probe func() (bool, error)) (bool, error) {
	if err := p.Open(); err != nil {
		return false, err
	}
	g, ok := p.owner.generation[target]
	if !ok {
		return false, fmt.Errorf("unknown upgrade target %q", target)
	}
	if p.opened[target] != g {
		return false, fmt.Errorf("%w: target %q changed after phase %q began", errUpgradeStale, target, p.name)
	}
	key := upgradeCapKey{target: target, generation: g, id: id}
	if answer, ok := p.caps[key]; ok {
		return answer, nil
	}
	answer, err := probe()
	if err != nil {
		return false, fmt.Errorf("probe %q on %q: %w", id, target, err)
	}
	// The probe may have moved the target or opened a later phase; its answer
	// then describes a state that no longer belongs to this phase.
	if err := p.Open(); err != nil {
		return false, err
	}
	if p.owner.generation[target] != g {
		return false, fmt.Errorf("%w: target %q changed during the probe of %q", errUpgradeStale, target, id)
	}
	p.caps[key] = answer
	return answer, nil
}
