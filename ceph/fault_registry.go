package ceph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// faultKind is one family of faults that a cluster tracks. Its phase, its
// overlap refusal and its Terminate message are fixed here: callers cannot
// choose a phase, an order or an arbitrary cleanup.
type faultKind int

const (
	faultNetworkDelay faultKind = iota + 1
	faultNetworkInterruption
	faultContainerPause
	faultBlocklist
	faultFullRatios
	faultConfigOverride
	faultOSDFlag
	faultMGRModule
)

// faultPhase is when, and under which lock mode, terminate undoes a kind.
type faultPhase int

const (
	// faultTracked entries are only bookkeeping for overlap and identity:
	// their state ends with the cluster, so terminate leaves them alone.
	faultTracked faultPhase = iota
	// faultShaping entries are undone first, each with c.mu released,
	// because their undo takes c.mu inside its own mutex.
	faultShaping
	// faultEndpoint entries are undone second, under c.mu. A failure
	// keeps the entry's network, which the undo still needs.
	faultEndpoint
	// faultFreeze entries are undone third, under c.mu, before native
	// commands are stopped: a frozen process cannot handle a stop signal.
	faultFreeze
)

// faultPhases is terminate's order.
var faultPhases = []faultPhase{faultShaping, faultEndpoint, faultFreeze}

type faultKindSpec struct {
	phase faultPhase
	// refuse returns the overlap refusal, for a key that is reserved or
	// for one whose held fault is not complete.
	refuse func(reserved bool) error
	// undone prefixes an error of the undo that terminate collects.
	undone string
}

// The families disagree on purpose: delay refusals are ErrInvalidRequest,
// the others plain errors; see docs/design/fault-package.md.
var faultKinds = map[faultKind]faultKindSpec{
	faultNetworkDelay: {phase: faultShaping, undone: "restore network delay", refuse: func(reserved bool) error {
		if reserved {
			return invalidRequest("another network delay of this endpoint is being applied")
		}
		return invalidRequest("restore the existing network delay of this endpoint first")
	}},
	faultNetworkInterruption: {phase: faultEndpoint, undone: "restore interrupted network", refuse: func(bool) error {
		return errors.New("restore the existing endpoint interruption first")
	}},
	faultContainerPause: {phase: faultFreeze, undone: "resume paused container", refuse: func(bool) error {
		return errors.New("resume the existing container pause first")
	}},
	faultBlocklist: {phase: faultTracked, refuse: func(bool) error {
		return errors.New("restore the existing client blocklist override first")
	}},
	faultFullRatios: {phase: faultTracked, refuse: func(bool) error {
		return errors.New("restore the existing full ratios override first")
	}},
	faultConfigOverride: {phase: faultTracked, refuse: func(bool) error {
		return errors.New("restore the existing configuration override first")
	}},
	faultOSDFlag: {phase: faultTracked, refuse: func(bool) error {
		return errors.New("restore the existing OSD flag override first")
	}},
	faultMGRModule: {phase: faultTracked, refuse: func(bool) error {
		return errors.New("restore the existing MGR module override first")
	}},
}

type faultID struct {
	kind faultKind
	key  string
}

// faultEntry is one reserved or held fault. The pointer is the identity
// token of its reservation: only the token that a key still maps to may
// settle or release it.
type faultEntry struct {
	id faultID
	// network is the network that an endpoint fault needs back.
	network string
	// handle is the public handle; nil while the fault is reserved.
	handle any
	// undo is what terminate calls in the kind's phase; nil for tracked
	// kinds.
	undo func(context.Context) error
	// complete reports a held fault that no longer needs undo. A kind with
	// it returns the previous handle with an overlap refusal and replaces a
	// complete one; a kind without it holds the key until releaseFault and
	// refuses with a nil handle.
	complete func(context.Context) (bool, error)
	// replaced is the complete entry that this reservation replaced; it is
	// put back when the reservation settles without a handle.
	replaced *faultEntry
}

// faultRegistry holds the faults of one cluster. It is the state component
// described in docs/design/container-decomposition.md ("Ninth internal
// slice") and is guarded by Container.mu: it has no mutex and takes none. The
// one lock operation it performs is the release of the caller's lock around
// a network-shaping undo, through the sync.Locker that undo receives. It
// calls into no Container method and reads no Container field; the closed
// flag that settle used to return stays with the callers, which hold the lock
// that guards it. The guards in fault_registry_state_test.go enforce this.
type faultRegistry struct {
	entries map[faultID]*faultEntry
}

// reserve reserves key for a fault of kind. The caller holds Container.mu and
// has refused a closed cluster in the same critical section: each family
// does so in its own order and with its own error, so the reservation is
// atomic with closed without normalizing those errors. A refusal of a kind
// with a completion check returns the previous handle, which still needs
// its undo.
func (r *faultRegistry) reserve(ctx context.Context, kind faultKind, key string) (*faultEntry, any, error) {
	spec := faultKinds[kind]
	id := faultID{kind: kind, key: key}
	previous := r.entries[id]
	if previous != nil {
		if previous.handle == nil {
			return nil, nil, spec.refuse(true)
		}
		// A fault without a completion check is held until it is
		// released. Its handle's own mutex is not taken here: a lease's
		// Restore holds it while it takes Container.mu.
		if previous.complete == nil {
			return nil, nil, spec.refuse(false)
		}
		done, err := previous.complete(ctx)
		if err != nil {
			return nil, previous.handle, err
		}
		if !done {
			return nil, previous.handle, spec.refuse(false)
		}
	}
	if r.entries == nil {
		r.entries = make(map[faultID]*faultEntry)
	}
	token := &faultEntry{id: id, replaced: previous}
	r.entries[id] = token
	return token, nil, nil
}

// settle records the handle of a reservation, or ends the reservation when
// there is no handle. A token that its key no longer maps to changes nothing.
// The caller holds Container.mu and reads Container.closed itself to learn
// whether terminate has started, which then has not seen the handle.
func (r *faultRegistry) settle(token *faultEntry, network string, handle any, undo func(context.Context) error, complete func(context.Context) (bool, error)) {
	if r.entries[token.id] != token {
		return
	}
	if handle == nil {
		if token.replaced != nil {
			r.entries[token.id] = token.replaced
		} else {
			delete(r.entries, token.id)
		}
		return
	}
	token.network, token.handle, token.undo, token.complete, token.replaced = network, handle, undo, complete, nil
}

// release forgets a fault only while its key still maps to token, so a
// stale handle cannot release its successor. The caller holds Container.mu.
func (r *faultRegistry) release(token *faultEntry) bool {
	if token == nil || r.entries[token.id] != token {
		return false
	}
	delete(r.entries, token.id)
	return true
}

// tracks reports whether token is still the fault of its key. The caller
// holds Container.mu.
func (r *faultRegistry) tracks(token *faultEntry) bool {
	return token != nil && r.entries[token.id] == token
}

// holds reports whether kind has an entry for key, reserved or held. The
// caller holds Container.mu.
func (r *faultRegistry) holds(kind faultKind, key string) bool {
	return r.entries[faultID{kind: kind, key: key}] != nil
}

// handles returns the handle of every entry of kind by key, nil for a
// reservation. The caller holds Container.mu, or the cluster is not shared.
func (r *faultRegistry) handles(kind faultKind) map[string]any {
	result := make(map[string]any)
	for id, entry := range r.entries {
		if id.kind == kind {
			result[id.key] = entry.handle
		}
	}
	return result
}

// openInstanceFault finds an entry of a kind in instanceFaultNames that
// targets containerID, alone or after its network name, and is not complete.
// It returns the kind's name for the refusal, and found false when every
// matching entry is complete or none matches. A failed completion check
// returns its error. The caller holds Container.mu, which the check runs
// under as it always has.
func (r *faultRegistry) openInstanceFault(ctx context.Context, containerID string) (name string, found bool, err error) {
	for fid, entry := range r.entries {
		kindName, ok := instanceFaultNames[fid.kind]
		if !ok {
			continue
		}
		if fid.key != containerID && !strings.HasSuffix(fid.key, "/"+containerID) {
			continue
		}
		if entry.handle != nil && entry.complete != nil {
			done, err := entry.complete(ctx)
			if err != nil {
				return "", false, err
			}
			if done {
				continue
			}
		}
		return kindName, true, nil
	}
	return "", false, nil
}

// held returns the held faults of phase, without reservations.
func (r *faultRegistry) held(phase faultPhase) []*faultEntry {
	var held []*faultEntry
	for _, entry := range r.entries {
		if entry.handle != nil && entry.undo != nil && faultKinds[entry.id.kind].phase == phase {
			held = append(held, entry)
		}
	}
	return held
}

// undo runs terminate's fault phases in order. The caller holds mu, the
// cluster mutex, which only faultShaping releases around each undo. Failed
// entries stay registered, so terminate is retryable, and a failed endpoint
// fault blocks the removal of its network.
func (r *faultRegistry) undo(ctx context.Context, mu sync.Locker) ([]error, map[string]bool) {
	var errs []error
	blockedNetworks := make(map[string]bool)
	for _, phase := range faultPhases {
		for _, entry := range r.held(phase) {
			var err error
			if phase == faultShaping {
				mu.Unlock()
				err = entry.undo(ctx)
				mu.Lock()
			} else {
				err = entry.undo(ctx)
			}
			if err == nil {
				continue
			}
			errs = append(errs, fmt.Errorf("%s: %w", faultKinds[entry.id.kind].undone, err))
			if phase == faultEndpoint {
				blockedNetworks[entry.network] = true
			}
		}
	}
	return errs, blockedNetworks
}
