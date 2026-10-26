//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// The types below model, without Ceph or Docker, the sequence planning and
// release-pair admission of a private mixed-version upgrade harness. They sit
// on top of upgradeLedger (which owns the per-daemon step order and store
// ownership) and add what the ledger deliberately does not know:
//
//   - the experiment preflight: approved pair, immutable image identities of
//     both sides, and the baseline topology, pool and flag facts. A fact that
//     was not observed is unknown, and unknown refuses like a failure;
//   - the non-cephadm order MON -> MGR -> OSD, one daemon at a time;
//   - the settled evidence required before the next daemon: the previous
//     transition is native-confirmed, the fresh version inventory equals the
//     checkpoint implied by the confirmed transitions, role availability is
//     restored and PGs are settled;
//   - a halt for an unclassified state, after which nothing is admitted and
//     nothing is retried by creating another daemon;
//   - finalization, and only after it the release requirement.
//
// It models admission and ordering over synthetic observations. It does not
// observe Ceph, it is not a rollback and it is not a public upgrade API.

// upgradeTri is an observed boolean. The zero value is unknown.
type upgradeTri int

const (
	upgradeUnknown upgradeTri = iota
	upgradeNo
	upgradeYes
)

// upgradeRoleRank is the order of the non-cephadm procedure. cephadm's own
// MGR-first order is not this harness's contract.
var upgradeRoleRank = map[string]int{"mon": 0, "mgr": 1, "osd": 2}

// upgradeReleaseNames maps the major release to the name Ceph reports for
// min_mon_release_name and require_osd_release.
var upgradeReleaseNames = map[int]string{19: "squid", 20: "tentacle"}

// upgradeMajor returns the major number of a full release such as "20.2.4".
func upgradeMajor(release string) (int, error) {
	major, _, _ := strings.Cut(release, ".")
	n, err := strconv.Atoi(major)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("release %q has no major number", release)
	}
	return n, nil
}

// upgradeClusterFacts are the observations the preflight judges. Pointers and
// tri-states distinguish "not observed" from a bad value.
type upgradeClusterFacts struct {
	SourceImage, DestImage string // immutable digests recorded for the receipt
	Baseline               upgradeInventory
	PoolSize, PoolMinSize  *int
	AutoscaleOff           upgradeTri
	NooutOwned             upgradeTri
	HealthSettled          upgradeTri // PGs settled and quorum restored at the baseline
}

func intFact(n int) *int { return &n }

// judge returns the preflight outcome the ledger takes. Anything not observed
// is unknown; any observed bad value fails; only a complete good set passes.
func (f upgradeClusterFacts) judge(plan []upgradeDaemonID, pair upgradeReleasePair) (upgradePreflight, error) {
	var unknown, failed []string
	if f.SourceImage == "" || f.DestImage == "" {
		unknown = append(unknown, "image identity")
	} else if f.SourceImage == f.DestImage {
		failed = append(failed, "source and destination images are the same")
	}
	if f.Baseline == nil {
		unknown = append(unknown, "baseline inventory")
	} else {
		want, err := upgradeCheckpointAfter(plan, pair, 0)
		if err != nil {
			return upgradePreflightFailed, err
		}
		if err := want.check(f.Baseline); err != nil {
			failed = append(failed, "baseline: "+err.Error())
		}
	}
	if f.PoolSize == nil || f.PoolMinSize == nil {
		unknown = append(unknown, "pool size")
	} else if *f.PoolSize != 3 || *f.PoolMinSize != 2 {
		failed = append(failed, fmt.Sprintf("pool size/min_size %d/%d, want 3/2", *f.PoolSize, *f.PoolMinSize))
	}
	for name, v := range map[string]upgradeTri{"pg autoscale off": f.AutoscaleOff, "noout lease owned": f.NooutOwned, "baseline health settled": f.HealthSettled} {
		switch v {
		case upgradeUnknown:
			unknown = append(unknown, name)
		case upgradeNo:
			failed = append(failed, name+" is false")
		}
	}
	sort.Strings(unknown)
	sort.Strings(failed)
	switch {
	case len(failed) > 0:
		return upgradePreflightFailed, errors.New("preflight failed: " + strings.Join(failed, "; "))
	case len(unknown) > 0:
		return upgradePreflightUnknown, errors.New("preflight unknown: " + strings.Join(unknown, "; "))
	}
	return upgradePreflightPassed, nil
}

// upgradeCheckpointAfter is the inventory implied when the first confirmed
// transitions of the plan are at the destination and the rest at the source.
func upgradeCheckpointAfter(plan []upgradeDaemonID, pair upgradeReleasePair, confirmed int) (upgradeCheckpoint, error) {
	if confirmed < 0 || confirmed > len(plan) {
		return nil, fmt.Errorf("%d confirmed transitions for a plan of %d", confirmed, len(plan))
	}
	c := upgradeCheckpoint{}
	for i, d := range plan {
		release := pair.Source
		if i < confirmed {
			release = pair.Dest
		}
		if c[d.Role] == nil {
			c[d.Role] = map[string]int{}
		}
		c[d.Role][release]++
	}
	return c, nil
}

// validateUpgradePlan checks the order and shape of the daemon sequence.
func validateUpgradePlan(plan []upgradeDaemonID) error {
	if len(plan) == 0 {
		return errors.New("upgrade plan has no daemon")
	}
	seen := map[string]bool{}
	last := -1
	for _, d := range plan {
		rank, ok := upgradeRoleRank[d.Role]
		switch {
		case !ok:
			return fmt.Errorf("role %q is outside the core experiment", d.Role)
		case d.ID == "":
			return fmt.Errorf("plan has a %s without an ID", d.Role)
		case seen[d.key()]:
			return fmt.Errorf("daemon %s is planned twice", d.key())
		case rank < last:
			return fmt.Errorf("daemon %s is out of the MON, MGR, OSD order", d.key())
		}
		seen[d.key()] = true
		last = rank
	}
	if plan[0].Role != "mon" {
		return errors.New("upgrade plan does not start with a MON")
	}
	return nil
}

// upgradeSettled is the evidence that a confirmed transition left the cluster
// ready for the next one. Daemon names the transition it describes.
type upgradeSettled struct {
	Daemon         string
	Inventory      upgradeInventory // fresh, from a current phase
	Quorum         []string         // MON IDs in quorum
	MgrActive      string
	MgrStandbys    []string
	OSDsUpIn       *int
	PGsSettled     upgradeTri
	MinMonRelease  *int // major number, judged once every MON is at the destination
	ObservationErr error
}

// upgradeSequence drives a plan through the ledger.
type upgradeSequence struct {
	ledger       *upgradeLedger
	pair         upgradeReleasePair
	plan         []upgradeDaemonID
	preflight    upgradePreflight
	preflightErr error
	began        int // transitions admitted
	settled      int // transitions followed by accepted settled evidence
	halted       error
	done         bool
	released     bool
}

func newUpgradeSequence(l *upgradeLedger, pair upgradeReleasePair, plan []upgradeDaemonID, facts upgradeClusterFacts) (*upgradeSequence, error) {
	if l == nil {
		return nil, errors.New("upgrade sequence has no ledger")
	}
	if err := validateUpgradePlan(plan); err != nil {
		return nil, err
	}
	for _, v := range []string{pair.Source, pair.Dest} {
		major, err := upgradeMajor(v)
		if err != nil {
			return nil, err
		}
		if upgradeReleaseNames[major] == "" {
			return nil, fmt.Errorf("release %q has no known release name", v)
		}
	}
	pre, err := facts.judge(plan, pair)
	// A failed or unknown preflight is kept with its reason: admit hands it
	// to the ledger, which refuses the first begin before any fence,
	// allocation or write.
	return &upgradeSequence{ledger: l, pair: pair, plan: append([]upgradeDaemonID(nil), plan...), preflight: pre, preflightErr: err}, nil
}

// admit begins the next transition. It refuses while the sequence is halted,
// while another transition is in flight, and until the previous transition
// has accepted settled evidence.
func (s *upgradeSequence) admit() (string, upgradeDaemonID, error) {
	switch {
	case s.halted != nil:
		return "", upgradeDaemonID{}, fmt.Errorf("sequence halted: %w", s.halted)
	case s.done:
		return "", upgradeDaemonID{}, errors.New("sequence is finalized")
	case s.began >= len(s.plan):
		return "", upgradeDaemonID{}, errors.New("every planned daemon already has a transition")
	case s.settled != s.began:
		return "", upgradeDaemonID{}, fmt.Errorf("transition %d is not settled", s.began)
	}
	d := s.plan[s.began]
	id := fmt.Sprintf("t%d-%s", s.began+1, d.key())
	if err := s.ledger.begin(upgradeBegin{Transition: id, Daemon: d, Pair: s.pair, Preflight: s.preflight}); err != nil {
		if s.preflightErr != nil {
			err = fmt.Errorf("%w (%v)", err, s.preflightErr)
		}
		return "", upgradeDaemonID{}, err
	}
	s.began++
	return id, d, nil
}

// halt records an unclassified state. Nothing is admitted afterwards; the
// instances the ledger still owns stay for disposal.
func (s *upgradeSequence) halt(reason error) {
	if s.halted == nil && reason != nil {
		s.halted = reason
	}
}

// settle accepts the evidence for the latest transition. It requires that
// transition to be native-confirmed. Rejected evidence changes nothing.
func (s *upgradeSequence) settle(ev upgradeSettled) error {
	if s.halted != nil {
		return fmt.Errorf("sequence halted: %w", s.halted)
	}
	if s.began == 0 || s.settled != s.began-1 {
		return errors.New("no unsettled transition")
	}
	d := s.plan[s.began-1]
	id := fmt.Sprintf("t%d-%s", s.began, d.key())
	if step, err := s.ledger.stepOf(id); err != nil || step != upgradeNativeConfirmed {
		return fmt.Errorf("transition %s is not native-confirmed", id)
	}
	switch {
	case ev.ObservationErr != nil:
		return fmt.Errorf("settled observation failed: %w", ev.ObservationErr)
	case ev.Daemon != d.key():
		return fmt.Errorf("evidence describes %q, the latest transition is %q", ev.Daemon, d.key())
	case ev.Inventory == nil:
		return errors.New("settled evidence has no version inventory")
	case ev.PGsSettled != upgradeYes:
		return fmt.Errorf("PGs are not settled (%d)", ev.PGsSettled)
	}
	want, err := upgradeCheckpointAfter(s.plan, s.pair, s.began)
	if err != nil {
		return err
	}
	if err := want.check(ev.Inventory); err != nil {
		return err
	}
	if err := s.roleAvailable(d.Role, ev); err != nil {
		return err
	}
	if d.Role == "mon" && s.roleDone("mon") {
		major, _ := upgradeMajor(s.pair.Dest)
		if ev.MinMonRelease == nil {
			return errors.New("min_mon_release was not observed after the MON phase")
		}
		if *ev.MinMonRelease != major {
			return fmt.Errorf("min_mon_release %d, want %d", *ev.MinMonRelease, major)
		}
	}
	s.settled++
	return nil
}

// roleDone reports whether every planned daemon of the role has a transition.
func (s *upgradeSequence) roleDone(role string) bool {
	for i, d := range s.plan {
		if d.Role == role && i >= s.began {
			return false
		}
	}
	return true
}

func (s *upgradeSequence) ids(role string) []string {
	var out []string
	for _, d := range s.plan {
		if d.Role == role {
			out = append(out, d.ID)
		}
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	a, b = append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	return strings.Join(a, "\x00") == strings.Join(b, "\x00") && len(a) == len(b)
}

// roleAvailable checks that the role of the daemon just moved is available
// again with its full intended membership.
func (s *upgradeSequence) roleAvailable(role string, ev upgradeSettled) error {
	switch role {
	case "mon":
		if len(ev.Quorum) == 0 || !sameStrings(ev.Quorum, s.ids("mon")) {
			return fmt.Errorf("quorum %v is not the full MON set %v", ev.Quorum, s.ids("mon"))
		}
	case "mgr":
		all := append([]string{ev.MgrActive}, ev.MgrStandbys...)
		if ev.MgrActive == "" || !sameStrings(all, s.ids("mgr")) {
			return fmt.Errorf("MGR active %q with standbys %v is not the full set %v", ev.MgrActive, ev.MgrStandbys, s.ids("mgr"))
		}
	case "osd":
		if ev.OSDsUpIn == nil || *ev.OSDsUpIn != len(s.ids("osd")) {
			return fmt.Errorf("OSDs up and in not observed or not %d", len(s.ids("osd")))
		}
	}
	return nil
}

// finalize accepts completion only after every daemon is settled, through the
// ledger's own guard, and makes the release requirement available.
func (s *upgradeSequence) finalize() error {
	if s.halted != nil {
		return fmt.Errorf("sequence halted: %w", s.halted)
	}
	if s.began != len(s.plan) || s.settled != s.began {
		return fmt.Errorf("%d of %d daemons are settled", s.settled, len(s.plan))
	}
	keys := make([]string, 0, len(s.plan))
	for _, d := range s.plan {
		keys = append(keys, d.key())
	}
	if err := s.ledger.finalize(keys...); err != nil {
		return err
	}
	s.done = true
	return nil
}

// requireOSDRelease returns the release name to set, once, and only after
// finalization and for the destination release.
func (s *upgradeSequence) requireOSDRelease(release string) (string, error) {
	switch {
	case !s.done:
		return "", errors.New("release requirement before finalization")
	case s.released:
		return "", errors.New("release requirement already issued")
	case release != s.pair.Dest:
		return "", fmt.Errorf("release requirement for %q, destination is %q", release, s.pair.Dest)
	}
	major, _ := upgradeMajor(release)
	s.released = true
	return upgradeReleaseNames[major], nil
}
