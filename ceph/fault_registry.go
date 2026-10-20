package ceph

import (
	"context"
	"errors"
	"fmt"
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

// faultRegistry holds the faults of one cluster. It is guarded by c.mu.
type faultRegistry struct {
	entries map[faultID]*faultEntry
}

// reserveFault reserves key for a fault of kind. The caller holds c.mu and
// has refused a closed cluster in the same critical section: each family
// does so in its own order and with its own error, so the reservation is
// atomic with closed without normalizing those errors. A refusal of a kind
// with a completion check returns the previous handle, which still needs
// its undo.
func (c *Container) reserveFault(ctx context.Context, kind faultKind, key string) (*faultEntry, any, error) {
	spec := faultKinds[kind]
	id := faultID{kind: kind, key: key}
	previous := c.faults.entries[id]
	if previous != nil {
		if previous.handle == nil {
			return nil, nil, spec.refuse(true)
		}
		// A fault without a completion check is held until it is
		// released. Its handle's own mutex is not taken here: a lease's
		// Restore holds it while it takes c.mu.
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
	if c.faults.entries == nil {
		c.faults.entries = make(map[faultID]*faultEntry)
	}
	token := &faultEntry{id: id, replaced: previous}
	c.faults.entries[id] = token
	return token, nil, nil
}

// settleFault records the handle of a reservation, or ends the reservation
// when there is no handle. It reports whether terminate has started, which
// then has not seen the handle. The caller holds c.mu.
func (c *Container) settleFault(token *faultEntry, network string, handle any, undo func(context.Context) error, complete func(context.Context) (bool, error)) bool {
	if c.faults.entries[token.id] != token {
		return c.closed
	}
	if handle == nil {
		if token.replaced != nil {
			c.faults.entries[token.id] = token.replaced
		} else {
			delete(c.faults.entries, token.id)
		}
		return c.closed
	}
	token.network, token.handle, token.undo, token.complete, token.replaced = network, handle, undo, complete, nil
	return c.closed
}

// releaseFault forgets a fault only while its key still maps to token, so a
// stale handle cannot release its successor. The caller holds c.mu.
func (c *Container) releaseFault(token *faultEntry) bool {
	if token == nil || c.faults.entries[token.id] != token {
		return false
	}
	delete(c.faults.entries, token.id)
	return true
}

// tracksFault reports whether token is still the fault of its key. The
// caller holds c.mu.
func (c *Container) tracksFault(token *faultEntry) bool {
	return token != nil && c.faults.entries[token.id] == token
}

// heldFaults returns the held faults of phase, without reservations.
func (c *Container) heldFaults(phase faultPhase) []*faultEntry {
	var held []*faultEntry
	for _, entry := range c.faults.entries {
		if entry.handle != nil && entry.undo != nil && faultKinds[entry.id.kind].phase == phase {
			held = append(held, entry)
		}
	}
	return held
}

// undoFaults runs terminate's fault phases in order. The caller holds c.mu,
// which only faultShaping releases around each undo. Failed entries stay
// registered, so terminate is retryable, and a failed endpoint fault blocks
// the removal of its network.
func (c *Container) undoFaults(ctx context.Context) ([]error, map[string]bool) {
	var errs []error
	blockedNetworks := make(map[string]bool)
	for _, phase := range faultPhases {
		for _, entry := range c.heldFaults(phase) {
			var err error
			if phase == faultShaping {
				c.mu.Unlock()
				err = entry.undo(ctx)
				c.mu.Lock()
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
