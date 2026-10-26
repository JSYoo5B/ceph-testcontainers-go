//go:build all || integration

package integration_test

import (
	"errors"
	"fmt"
	"sort"
)

// The types below model, without Ceph or Docker, the per-daemon transition
// ledger of a private mixed-version upgrade harness. A transition moves one
// retained daemon store from a source launch recipe to a destination one and
// advances strictly through
//
//	not-attempted -> fenced -> old-container-absent -> start-attempted -> native-confirmed
//
// The attempted destination instance identity is recorded before the creation
// call, so an uncertain creation keeps its cleanup owner. Resuming a
// transition that reached start-attempted means observing that attempt; the
// ledger never offers a second writer of the same store. A request the ledger
// rejects changes no state. It models admission and ordering only: it does not
// observe containers, versions or Ceph, and it is not a rollback or a public
// mixed-version API.

type upgradeStep int

const (
	upgradeNotAttempted upgradeStep = iota
	upgradeFenced
	upgradeOldAbsent
	upgradeStartAttempted
	upgradeNativeConfirmed
)

func (s upgradeStep) String() string {
	switch s {
	case upgradeNotAttempted:
		return "not-attempted"
	case upgradeFenced:
		return "fenced"
	case upgradeOldAbsent:
		return "old-container-absent"
	case upgradeStartAttempted:
		return "start-attempted"
	case upgradeNativeConfirmed:
		return "native-confirmed"
	}
	return fmt.Sprintf("step(%d)", int(s))
}

// upgradePreflight is the outcome of the preflight check. The zero value is
// unknown and is refused like a failure.
type upgradePreflight int

const (
	upgradePreflightUnknown upgradePreflight = iota
	upgradePreflightFailed
	upgradePreflightPassed
)

// upgradeReleasePair is one approved source and destination release.
type upgradeReleasePair struct{ Source, Dest string }

// upgradeDaemonID is the identity a transition owns. StoreID is the retained
// store (for an OSD its numeric ID and UUID are part of it) and Arch the
// resolved architecture of the selected image manifests.
type upgradeDaemonID struct {
	Role, ID, FSID, StoreID, Arch string
}

func (d upgradeDaemonID) key() string { return d.Role + "." + d.ID }

// upgradeBegin is the admission request of one transition.
type upgradeBegin struct {
	Transition string
	Daemon     upgradeDaemonID
	Pair       upgradeReleasePair
	Preflight  upgradePreflight
}

// upgradeNativeEvidence is what a native observation of the destination
// instance reported.
type upgradeNativeEvidence struct {
	Instance string
	Version  string
	FSID     string
	StoreID  string
}

// upgradeAction is what a driver may do next for a transition.
type upgradeAction int

const (
	upgradeActFence upgradeAction = iota
	upgradeActVerifyOldAbsent
	upgradeActStart
	// upgradeActObserveAttempt is the only action after a start attempt:
	// observe the recorded instance, never create another one.
	upgradeActObserveAttempt
	upgradeActNone
)

type upgradeTransition struct {
	begin    upgradeBegin
	step     upgradeStep
	instance string
}

// upgradeLedger holds the transitions of one experiment.
type upgradeLedger struct {
	approved    map[upgradeReleasePair]bool
	arch        string
	transitions map[string]*upgradeTransition // by transition identity
	byDaemon    map[string]string             // daemon key -> transition identity
	byStore     map[string]string             // store identity -> daemon key
	instances   map[string]string             // destination instance -> transition identity, kept after confirmation
	finalized   bool
}

// newUpgradeLedger fixes the approved release pairs and the one architecture
// the experiment runs on.
func newUpgradeLedger(arch string, approved ...upgradeReleasePair) (*upgradeLedger, error) {
	if arch == "" {
		return nil, errors.New("upgrade ledger has no architecture")
	}
	if len(approved) == 0 {
		return nil, errors.New("upgrade ledger has no approved release pair")
	}
	l := &upgradeLedger{
		approved:    map[upgradeReleasePair]bool{},
		arch:        arch,
		transitions: map[string]*upgradeTransition{},
		byDaemon:    map[string]string{},
		byStore:     map[string]string{},
		instances:   map[string]string{},
	}
	for _, p := range approved {
		if p.Source == "" || p.Dest == "" || p.Source == p.Dest {
			return nil, fmt.Errorf("release pair %q -> %q is not a transition", p.Source, p.Dest)
		}
		l.approved[p] = true
	}
	return l, nil
}

func (l *upgradeLedger) get(id string) (*upgradeTransition, error) {
	t := l.transitions[id]
	if t == nil {
		return nil, fmt.Errorf("unknown transition %q", id)
	}
	return t, nil
}

func (l *upgradeLedger) expect(id string, want upgradeStep) (*upgradeTransition, error) {
	if l.finalized {
		return nil, errors.New("upgrade ledger is finalized")
	}
	t, err := l.get(id)
	if err != nil {
		return nil, err
	}
	if t.step != want {
		return nil, fmt.Errorf("transition %q is %s, this step needs %s", id, t.step, want)
	}
	return t, nil
}

// begin admits a transition in the not-attempted step, before any fence,
// allocation or write.
func (l *upgradeLedger) begin(req upgradeBegin) error {
	d := req.Daemon
	switch {
	case l.finalized:
		return errors.New("upgrade ledger is finalized")
	case req.Transition == "":
		return errors.New("transition has no identity")
	case d.Role == "" || d.ID == "" || d.FSID == "" || d.StoreID == "" || d.Arch == "":
		return fmt.Errorf("daemon identity %+v is incomplete", d)
	case l.transitions[req.Transition] != nil:
		return fmt.Errorf("transition %q already exists", req.Transition)
	case l.byDaemon[d.key()] != "":
		return fmt.Errorf("daemon %s already has transition %q", d.key(), l.byDaemon[d.key()])
	case l.byStore[d.StoreID] != "":
		return fmt.Errorf("store %q is owned by %s", d.StoreID, l.byStore[d.StoreID])
	case !l.approved[req.Pair]:
		return fmt.Errorf("release pair %q -> %q is not approved", req.Pair.Source, req.Pair.Dest)
	case d.Arch != l.arch:
		return fmt.Errorf("daemon architecture %q differs from the experiment architecture %q", d.Arch, l.arch)
	case req.Preflight != upgradePreflightPassed:
		return errors.New("preflight did not pass (failed or unknown)")
	}
	l.transitions[req.Transition] = &upgradeTransition{begin: req}
	l.byDaemon[d.key()] = req.Transition
	l.byStore[d.StoreID] = d.key()
	return nil
}

// fence records that the source writer was fenced. The observed identity must
// be the one the transition owns.
func (l *upgradeLedger) fence(id string, observed upgradeDaemonID) error {
	t, err := l.expect(id, upgradeNotAttempted)
	if err != nil {
		return err
	}
	if observed != t.begin.Daemon {
		return fmt.Errorf("fenced daemon %+v is not the transition's %+v", observed, t.begin.Daemon)
	}
	t.step = upgradeFenced
	return nil
}

// oldAbsent records the observation that no source container exists. A
// failed observation or a present container does not advance.
func (l *upgradeLedger) oldAbsent(id string, present bool, observeErr error) error {
	t, err := l.expect(id, upgradeFenced)
	if err != nil {
		return err
	}
	if observeErr != nil {
		return fmt.Errorf("source container observation failed: %w", observeErr)
	}
	if present {
		return errors.New("source container is still present")
	}
	t.step = upgradeOldAbsent
	return nil
}

// startAttempted records the destination instance identity before the
// creation call is made. An instance identity belongs to one transition for the
// life of the ledger, also after that transition is confirmed. From here the instance has a cleanup owner whatever
// the call returns.
func (l *upgradeLedger) startAttempted(id, instance string) error {
	t, err := l.expect(id, upgradeOldAbsent)
	if err != nil {
		return err
	}
	if instance == "" {
		return errors.New("start attempt has no instance identity")
	}
	if owner := l.instances[instance]; owner != "" {
		return fmt.Errorf("instance %q is already reserved by transition %q", instance, owner)
	}
	l.instances[instance] = id
	t.step = upgradeStartAttempted
	t.instance = instance
	return nil
}

// confirm promotes a start attempt only on native evidence of the recorded
// instance at the destination version with the owned FSID and store. Evidence
// that does not match leaves the attempt as it was.
func (l *upgradeLedger) confirm(id string, ev upgradeNativeEvidence) error {
	t, err := l.expect(id, upgradeStartAttempted)
	if err != nil {
		return err
	}
	d := t.begin.Daemon
	switch {
	case ev.Instance != t.instance:
		return fmt.Errorf("evidence is for instance %q, the attempt is %q", ev.Instance, t.instance)
	case ev.Version != t.begin.Pair.Dest:
		return fmt.Errorf("instance runs %q, destination is %q", ev.Version, t.begin.Pair.Dest)
	case ev.FSID != d.FSID:
		return fmt.Errorf("instance FSID %q differs from the owned %q", ev.FSID, d.FSID)
	case ev.StoreID != d.StoreID:
		return fmt.Errorf("instance store %q differs from the owned %q", ev.StoreID, d.StoreID)
	}
	t.step = upgradeNativeConfirmed
	return nil
}

// next says what a driver may do for a transition now, including after an
// interruption. After start-attempted it never says start.
func (l *upgradeLedger) next(id string) (upgradeAction, error) {
	t, err := l.get(id)
	if err != nil {
		return upgradeActNone, err
	}
	switch t.step {
	case upgradeNotAttempted:
		return upgradeActFence, nil
	case upgradeFenced:
		return upgradeActVerifyOldAbsent, nil
	case upgradeOldAbsent:
		return upgradeActStart, nil
	case upgradeStartAttempted:
		return upgradeActObserveAttempt, nil
	}
	return upgradeActNone, nil
}

// stepOf reports the current step of a transition.
func (l *upgradeLedger) stepOf(id string) (upgradeStep, error) {
	t, err := l.get(id)
	if err != nil {
		return 0, err
	}
	return t.step, nil
}

// owned lists the destination instances that were attempted but not
// confirmed, in order. Disposal owns them whatever the creation call returned.
func (l *upgradeLedger) owned() []string {
	var out []string
	for _, t := range l.transitions {
		if t.step == upgradeStartAttempted {
			out = append(out, t.instance)
		}
	}
	sort.Strings(out)
	return out
}

// finalize accepts completion only when exactly the expected daemons have a
// transition and every one is native-confirmed. It is accepted once.
func (l *upgradeLedger) finalize(expected ...string) error {
	if l.finalized {
		return errors.New("upgrade ledger is already finalized")
	}
	want := map[string]bool{}
	for _, k := range expected {
		want[k] = true
	}
	if len(want) == 0 {
		return errors.New("finalization names no expected daemon")
	}
	for k := range want {
		id := l.byDaemon[k]
		if id == "" {
			return fmt.Errorf("expected daemon %s has no transition", k)
		}
		if t := l.transitions[id]; t.step != upgradeNativeConfirmed {
			return fmt.Errorf("daemon %s is %s, not native-confirmed", k, t.step)
		}
	}
	for k := range l.byDaemon {
		if !want[k] {
			return fmt.Errorf("transition for %s is not in the expected set", k)
		}
	}
	l.finalized = true
	return nil
}
