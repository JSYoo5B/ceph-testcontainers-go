package expect

import (
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"slices"
	"strings"
)

// ExclusiveLock names the state of an image's managed exclusive lock that an
// ImageClients expectation requires.
type ExclusiveLock string

// The lock states an ImageClients expectation can require. The zero value
// does not compare the lock.
const (
	// ExclusiveLockHeld requires a managed exclusive lock.
	ExclusiveLockHeld ExclusiveLock = "held"
	// ExclusiveLockFree requires no managed exclusive lock. Advisory locks
	// added through rbd lock add are not read.
	ExclusiveLockFree ExclusiveLock = "free"
)

// ImageClients states what a test expects of one rbd.ImageClientStatus
// observation of an image.
//
// Lock, when set, requires the managed exclusive lock to be held or free.
// Owner, when set, names the entity (client.4235) that must hold it, and
// OwnerWatches requires the holder's address among the watchers; both need
// ExclusiveLockHeld. Departed lists addresses, such as a former owner's,
// that must not watch the image. Unwatched requires no watcher at all.
// Watchers and locks that the expectation does not name are not compared, so
// selecting one client leaves unrelated clients alone.
//
// The watcher and lock lists come from separate native reads. During a lock
// handoff an observation can report the new owner without its watch, so an
// owner-watch mismatch is a possible observed state, not proof of a fault;
// the test decides whether and when to read again. A match does not prove
// that the owner can write.
type ImageClients struct {
	Lock         ExclusiveLock
	Owner        string
	OwnerWatches bool
	Departed     []string
	Unwatched    bool
}

// ImageClientsMismatchReason names why an observation does not satisfy an
// ImageClients expectation.
type ImageClientsMismatchReason string

// The reasons an ImageClientsEvaluation reports.
const (
	// ImageClientsMismatchLock: the managed exclusive lock is held although
	// the expectation wants it free, or the reverse. A held expectation then
	// compares neither the owner nor its watch.
	ImageClientsMismatchLock ImageClientsMismatchReason = "lock"
	// ImageClientsMismatchOwner: another entity holds the managed lock. Its
	// watch is then not compared.
	ImageClientsMismatchOwner ImageClientsMismatchReason = "owner"
	// ImageClientsMismatchOwnerWatch: the holder's address is not among the
	// watchers, which a handoff between the two native reads can also show.
	ImageClientsMismatchOwnerWatch ImageClientsMismatchReason = "owner-watch"
	// ImageClientsMismatchDeparted: an address that must not watch the image
	// still does.
	ImageClientsMismatchDeparted ImageClientsMismatchReason = "departed"
	// ImageClientsMismatchWatched: an Unwatched expectation found watchers.
	ImageClientsMismatchWatched ImageClientsMismatchReason = "watched"
)

// ImageClientsMismatch is one reason an observation does not satisfy an
// expectation. Want and Observed are the compared values as text: "held" or
// "free" for ImageClientsMismatchLock, with the holder's entity name after an
// observed "held by"; entity names for ImageClientsMismatchOwner; the
// holder's address (Want) and the watcher addresses (Observed) for
// ImageClientsMismatchOwnerWatch; the address that still watches (Observed)
// for ImageClientsMismatchDeparted; and the watcher addresses (Observed) for
// ImageClientsMismatchWatched. Watcher addresses are in observation order,
// separated by spaces.
type ImageClientsMismatch struct {
	Reason         ImageClientsMismatchReason
	Want, Observed string
}

// String names the reason and the evaluated values.
func (m ImageClientsMismatch) String() string {
	switch m.Reason {
	case ImageClientsMismatchOwnerWatch:
		return fmt.Sprintf("owner-watch: want %s among watchers [%s]", m.Want, m.Observed)
	case ImageClientsMismatchDeparted:
		return fmt.Sprintf("departed: %s still watches", m.Observed)
	case ImageClientsMismatchWatched:
		return fmt.Sprintf("watched: want no watchers, observed [%s]", m.Observed)
	}
	return fmt.Sprintf("%s: want %s, observed %s", m.Reason, quoteEmpty(m.Want), quoteEmpty(m.Observed))
}

// ImageClientsEvaluation is the result of evaluating one observation.
// Expectation and Observed are copies of what was evaluated, so the
// diagnostics describe that observation even when the caller later reuses
// its slices. Mismatches are in the order of the reason constants, departed
// addresses in the order of the expectation; Matched means there are none.
type ImageClientsEvaluation struct {
	Expectation ImageClients
	Observed    rbd.ImageClientStatus
	Mismatches  []ImageClientsMismatch
	Matched     bool
}

// String summarizes the expectation and at most eight mismatches.
func (e ImageClientsEvaluation) String() string {
	var b strings.Builder
	b.WriteString("image clients want")
	x := e.Expectation
	if x.Lock != "" {
		fmt.Fprintf(&b, " lock=%s", x.Lock)
	}
	if x.Owner != "" {
		fmt.Fprintf(&b, " owner=%s", x.Owner)
	}
	if x.OwnerWatches {
		b.WriteString(" owner-watches")
	}
	if len(x.Departed) != 0 {
		fmt.Fprintf(&b, " departed=[%s]", strings.Join(x.Departed, " "))
	}
	if x.Unwatched {
		b.WriteString(" unwatched")
	}
	b.WriteString(": ")
	if e.Matched {
		b.WriteString("match")
		return b.String()
	}
	for i, m := range e.Mismatches {
		if i == evaluationShown {
			fmt.Fprintf(&b, "; %d more", len(e.Mismatches)-i)
			break
		}
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(m.String())
	}
	return b.String()
}

// Evaluate checks one rbd.ImageClientStatus observation (from
// rbd.ImageClients) against the expectation. It reads only its arguments: no
// query, wait or retry, so a test composes it with its own polling or
// assertion helpers. A mismatch is a result; an error means no verdict. An
// expectation that states nothing, an unknown Lock, an Owner or OwnerWatches
// without ExclusiveLockHeld, an Owner that is not a client entity name, an
// empty or repeated Departed address, and Unwatched together with
// OwnerWatches or Departed are ceph.ErrInvalidRequest. An observation with a
// watcher lacking its address or client ID, a lock lacking its ID, locker or
// address, a Managed flag that disagrees with the lock ID, or more than one
// managed lock is a malformed observation, never a mismatch. The result
// shares no slice with its arguments.
//
//purpose:observation
func (e ImageClients) Evaluate(observed rbd.ImageClientStatus) (ImageClientsEvaluation, error) {
	if err := e.validate(); err != nil {
		return ImageClientsEvaluation{}, err
	}
	owner, err := validateImageClientObservation(observed)
	if err != nil {
		return ImageClientsEvaluation{}, err
	}
	e.Departed = slices.Clone(e.Departed)
	result := ImageClientsEvaluation{
		Expectation: e,
		Observed:    rbd.ImageClientStatus{Watchers: slices.Clone(observed.Watchers), Locks: slices.Clone(observed.Locks)},
	}
	result.Mismatches = e.mismatches(observed, owner)
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

func (e ImageClients) validate() error {
	switch e.Lock {
	case "", ExclusiveLockHeld, ExclusiveLockFree:
	default:
		return fmt.Errorf("%w: image clients expectation wants unknown lock state %q", ceph.ErrInvalidRequest, e.Lock)
	}
	if e.Lock == "" && e.Owner == "" && !e.OwnerWatches && len(e.Departed) == 0 && !e.Unwatched {
		return fmt.Errorf("%w: image clients expectation states nothing", ceph.ErrInvalidRequest)
	}
	if (e.Owner != "" || e.OwnerWatches) && e.Lock != ExclusiveLockHeld {
		return fmt.Errorf("%w: image clients expectation names a lock owner without a held lock", ceph.ErrInvalidRequest)
	}
	if e.Owner != "" {
		id, ok := strings.CutPrefix(e.Owner, "client.")
		if !ok || id == "" || strings.ContainsAny(id, " \t\r\n\x00") {
			return fmt.Errorf("%w: image clients expectation wants owner %q that is not a client entity name", ceph.ErrInvalidRequest, e.Owner)
		}
	}
	for i, address := range e.Departed {
		if address == "" || slices.Contains(e.Departed[:i], address) {
			return fmt.Errorf("%w: image clients expectation lists an empty or repeated departed address %q", ceph.ErrInvalidRequest, address)
		}
	}
	if e.Unwatched && (e.OwnerWatches || len(e.Departed) != 0) {
		return fmt.Errorf("%w: image clients expectation wants no watchers and names watchers", ceph.ErrInvalidRequest)
	}
	return nil
}

// validateImageClientObservation returns the managed lock of a well-formed
// observation, or nil when no client holds it.
func validateImageClientObservation(observed rbd.ImageClientStatus) (*rbd.ImageLock, error) {
	for _, w := range observed.Watchers {
		if w.Address == "" || w.ClientID == 0 {
			return nil, fmt.Errorf("malformed image clients observation: a watcher lacks its address or client ID")
		}
	}
	var owner *rbd.ImageLock
	for i, lock := range observed.Locks {
		if lock.ID == "" || lock.Locker == "" || lock.Address == "" {
			return nil, fmt.Errorf("malformed image clients observation: a lock lacks its ID, locker or address")
		}
		if lock.Managed != strings.HasPrefix(lock.ID, "auto ") {
			return nil, fmt.Errorf("malformed image clients observation: lock %q has a managed flag that its ID contradicts", lock.ID)
		}
		if !lock.Managed {
			continue
		}
		if owner != nil {
			return nil, fmt.Errorf("malformed image clients observation: more than one managed lock")
		}
		owner = &observed.Locks[i]
	}
	return owner, nil
}

func (e ImageClients) mismatches(observed rbd.ImageClientStatus, owner *rbd.ImageLock) []ImageClientsMismatch {
	var result []ImageClientsMismatch
	watching := func(address string) bool {
		return slices.ContainsFunc(observed.Watchers, func(w rbd.ImageWatcher) bool { return w.Address == address })
	}
	switch {
	case e.Lock == ExclusiveLockHeld && owner == nil:
		result = append(result, ImageClientsMismatch{Reason: ImageClientsMismatchLock, Want: string(ExclusiveLockHeld), Observed: string(ExclusiveLockFree)})
	case e.Lock == ExclusiveLockFree && owner != nil:
		result = append(result, ImageClientsMismatch{Reason: ImageClientsMismatchLock, Want: string(ExclusiveLockFree), Observed: "held by " + owner.Locker})
	case e.Lock == ExclusiveLockHeld && e.Owner != "" && owner.Locker != e.Owner:
		result = append(result, ImageClientsMismatch{Reason: ImageClientsMismatchOwner, Want: e.Owner, Observed: owner.Locker})
	case e.OwnerWatches && !watching(owner.Address):
		result = append(result, ImageClientsMismatch{Reason: ImageClientsMismatchOwnerWatch, Want: owner.Address, Observed: watcherAddresses(observed.Watchers)})
	}
	for _, address := range e.Departed {
		if watching(address) {
			result = append(result, ImageClientsMismatch{Reason: ImageClientsMismatchDeparted, Observed: address})
		}
	}
	if e.Unwatched && len(observed.Watchers) != 0 {
		result = append(result, ImageClientsMismatch{Reason: ImageClientsMismatchWatched, Observed: watcherAddresses(observed.Watchers)})
	}
	return result
}

func watcherAddresses(watchers []rbd.ImageWatcher) string {
	addresses := make([]string, len(watchers))
	for i, w := range watchers {
		addresses[i] = w.Address
	}
	return strings.Join(addresses, " ")
}
