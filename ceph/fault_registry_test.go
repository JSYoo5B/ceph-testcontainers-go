package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// modelFault is a handle whose undo records the order, the lock mode and the
// result, and whose state the test reads instead of a command sequence.
type modelFault struct {
	name     string
	c        *Container
	token    *faultEntry
	failures int
	done     bool
	// releases models a network delay lease: a successful undo releases the
	// registry entry, as NetworkProfileLease.Restore does through release.
	releases bool
	log      *[]string
}

func (f *modelFault) undo(context.Context) error {
	locked := !f.c.mu.TryLock()
	if !locked {
		f.c.mu.Unlock()
	}
	mode := "unlocked"
	if locked {
		mode = "locked"
	}
	*f.log = append(*f.log, f.name+":"+mode)
	if f.failures > 0 {
		f.failures--
		return errors.New(f.name + " undo failed")
	}
	f.done = true
	if f.releases {
		f.c.mu.Lock()
		f.c.faults.release(f.token)
		f.c.mu.Unlock()
	}
	return nil
}

func (f *modelFault) complete(context.Context) (bool, error) { return f.done, nil }

// hold reserves and settles a model fault of kind the way its family does:
// delays without a completion check and releasing themselves, endpoint and
// freeze faults with one.
func hold(t *testing.T, c *Container, kind faultKind, key, network string, log *[]string) *modelFault {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	token, _, err := c.faults.reserve(t.Context(), kind, key)
	if err != nil {
		t.Fatalf("reserve %v %s: %v", kind, key, err)
	}
	f := &modelFault{name: key, c: c, token: token, log: log}
	switch faultKinds[kind].phase {
	case faultShaping:
		f.releases = true
		c.faults.settle(token, network, f, f.undo, nil)
	case faultEndpoint, faultFreeze:
		c.faults.settle(token, network, f, f.undo, f.complete)
	default:
		c.faults.settle(token, network, f, nil, nil)
	}
	return f
}

func TestFaultRegistryReserveSettleRelease(t *testing.T) {
	c := &Container{}
	ctx := t.Context()
	token, previous, err := c.faults.reserve(ctx, faultNetworkDelay, "net/osd-0")
	if err != nil || token == nil || previous != nil {
		t.Fatalf("first reservation: %v %v %v", token, previous, err)
	}
	if got := c.faultsOf(faultNetworkDelay); len(got) != 1 || got["net/osd-0"] != nil {
		t.Fatalf("a reservation must be a key without a handle: %v", got)
	}
	_, previous, err = c.faults.reserve(ctx, faultNetworkDelay, "net/osd-0")
	if !errors.Is(err, ErrInvalidRequest) || previous != nil || !strings.Contains(err.Error(), "being applied") {
		t.Fatalf("a reserved key was not refused as being applied: %v %v", previous, err)
	}
	// Another key and another kind with the same key are independent.
	other, _, err := c.faults.reserve(ctx, faultNetworkDelay, "net/osd-1")
	if err != nil {
		t.Fatal(err)
	}
	pause, _, err := c.faults.reserve(ctx, faultContainerPause, "net/osd-0")
	if err != nil {
		t.Fatal(err)
	}
	lease := &modelFault{}
	c.faults.settle(token, "", lease, lease.undo, nil)
	if c.closed {
		t.Fatal("the cluster reports a terminate that did not happen")
	}
	_, previous, err = c.faults.reserve(ctx, faultNetworkDelay, "net/osd-0")
	if !errors.Is(err, ErrInvalidRequest) || previous != nil || !strings.Contains(err.Error(), "restore the existing") {
		t.Fatalf("a held delay was not refused with a nil handle: %v %v", previous, err)
	}
	// Settling without a handle frees only that reservation.
	c.faults.settle(other, "", nil, nil, nil)
	c.faults.settle(pause, "", nil, nil, nil)
	if got := c.faultsOf(faultNetworkDelay); len(got) != 1 || got["net/osd-0"] != lease || len(c.faultsOf(faultContainerPause)) != 0 {
		t.Fatalf("handle-less settles changed other keys: %v", c.faults.entries)
	}
	if !c.faults.tracks(token) || !c.faults.release(token) || c.faults.tracks(token) || c.faults.release(token) {
		t.Fatal("release of the current token did not forget it exactly once")
	}
	if _, _, err := c.faults.reserve(ctx, faultNetworkDelay, "net/osd-0"); err != nil {
		t.Fatalf("a released key stayed refused: %v", err)
	}
}

func TestFaultRegistryStaleTokenCannotTouchSuccessor(t *testing.T) {
	c := &Container{}
	ctx := t.Context()
	first, _, _ := c.faults.reserve(ctx, faultBlocklist, "10.0.0.1:0/1")
	c.faults.settle(first, "", "first", nil, nil)
	if !c.faults.release(first) {
		t.Fatal("release failed")
	}
	second, _, err := c.faults.reserve(ctx, faultBlocklist, "10.0.0.1:0/1")
	if err != nil {
		t.Fatal(err)
	}
	c.faults.settle(second, "", "second", nil, nil)
	if c.faults.release(first) || c.faults.tracks(first) || !c.faults.tracks(second) {
		t.Fatal("a stale token released or claimed its successor")
	}
	c.faults.settle(first, "", "stale", nil, nil)
	c.faults.settle(first, "", nil, nil, nil)
	if got := c.heldFault(faultBlocklist, "10.0.0.1:0/1"); got != "second" {
		t.Fatalf("a stale settle replaced or freed the successor: %v", got)
	}
	if c.faults.release(nil) || c.faults.tracks(nil) {
		t.Fatal("a nil token was accepted")
	}
}

func TestFaultRegistryCompletionKindsReturnPreviousHandle(t *testing.T) {
	for _, kind := range []faultKind{faultNetworkInterruption, faultContainerPause} {
		c := &Container{}
		var log []string
		previous := hold(t, c, kind, "net/osd-0", "net", &log)
		c.mu.Lock()
		_, handle, err := c.faults.reserve(t.Context(), kind, "net/osd-0")
		c.mu.Unlock()
		if err == nil || handle != previous {
			t.Fatalf("kind %v: an incomplete fault was not returned with the refusal: %v %v", kind, handle, err)
		}
		// A failing completion check returns its error with the handle.
		probeErr := errors.New("probe canceled")
		c.faults.entries[faultID{kind, "net/osd-0"}].complete = func(context.Context) (bool, error) { return false, probeErr }
		if _, handle, err := c.faults.reserve(t.Context(), kind, "net/osd-0"); !errors.Is(err, probeErr) || handle != previous {
			t.Fatalf("kind %v: probe error lost the handle: %v %v", kind, handle, err)
		}
		c.faults.entries[faultID{kind, "net/osd-0"}].complete = previous.complete
		previous.done = true
		// A complete fault is replaced; a failed replacement puts it back,
		// so the key keeps a handle as it did before the attempt.
		token, handle, err := c.faults.reserve(t.Context(), kind, "net/osd-0")
		if err != nil || handle != nil {
			t.Fatalf("kind %v: a complete fault was not replaced: %v", kind, err)
		}
		c.faults.settle(token, "", nil, nil, nil)
		if got := c.heldFault(kind, "net/osd-0"); got != previous {
			t.Fatalf("kind %v: a failed replacement dropped the complete fault: %v", kind, got)
		}
		token, _, _ = c.faults.reserve(t.Context(), kind, "net/osd-0")
		next := &modelFault{c: c, log: &log}
		c.faults.settle(token, "net", next, next.undo, next.complete)
		if got := c.heldFault(kind, "net/osd-0"); got != next || token.replaced != nil {
			t.Fatalf("kind %v: the replacement was not recorded: %v", kind, got)
		}
	}
}

func TestFaultRegistryTrackedKindsRefuseWithNilHandle(t *testing.T) {
	for kind, want := range map[faultKind]string{faultBlocklist: "client blocklist", faultFullRatios: "full ratios"} {
		c := &Container{}
		var log []string
		hold(t, c, kind, "", "", &log)
		if _, handle, err := c.faults.reserve(t.Context(), kind, ""); err == nil || handle != nil || !strings.Contains(err.Error(), want) || errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("kind %v: overlap reply %v %v", kind, handle, err)
		}
	}
}

func TestTerminateUndoesFaultPhasesInOrderAndLockMode(t *testing.T) {
	c := &Container{}
	var log []string
	hold(t, c, faultContainerPause, "pause-a", "", &log)
	hold(t, c, faultBlocklist, "blocklist", "", &log)
	hold(t, c, faultNetworkInterruption, "link-a", "net", &log)
	hold(t, c, faultNetworkDelay, "delay-a", "", &log)
	hold(t, c, faultContainerPause, "pause-b", "", &log)
	hold(t, c, faultFullRatios, "", "", &log)
	hold(t, c, faultNetworkInterruption, "link-b", "net", &log)
	hold(t, c, faultNetworkDelay, "delay-b", "", &log)
	// A reservation is not undone: its caller settles after Terminate.
	c.mu.Lock()
	reserved, _, _ := c.faults.reserve(t.Context(), faultNetworkDelay, "delay-reserved")
	c.mu.Unlock()
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	phase := func(entry string) int {
		switch {
		case strings.HasPrefix(entry, "delay-"):
			return 1
		case strings.HasPrefix(entry, "link-"):
			return 2
		}
		return 3
	}
	if len(log) != 6 || !slices.IsSortedFunc(log, func(a, b string) int { return phase(a) - phase(b) }) {
		t.Fatalf("undo order %v; want delays, interruptions, pauses and nothing else", log)
	}
	for _, entry := range log {
		if want := map[bool]string{true: ":unlocked", false: ":locked"}[phase(entry) == 1]; !strings.HasSuffix(entry, want) {
			t.Fatalf("undo %s ran with the wrong lock mode", entry)
		}
	}
	if len(c.faultsOf(faultBlocklist)) != 1 || len(c.faultsOf(faultFullRatios)) != 1 {
		t.Fatal("tracked overrides were dropped; they end with the cluster")
	}
	if c.faults.entries[faultID{faultNetworkDelay, "delay-reserved"}] != reserved {
		t.Fatal("Terminate touched an unsettled reservation")
	}
}

func TestTerminateFailedUndoStaysTrackedAndRetryable(t *testing.T) {
	c := &Container{}
	var log []string
	delay := hold(t, c, faultNetworkDelay, "delay", "", &log)
	link := hold(t, c, faultNetworkInterruption, "link", "net", &log)
	pause := hold(t, c, faultContainerPause, "pause", "", &log)
	delay.failures, link.failures, pause.failures = 1, 1, 1
	err := c.Terminate(t.Context())
	for _, want := range []string{"restore network delay: delay undo failed", "restore interrupted network: link undo failed", "resume paused container: pause undo failed"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("Terminate error %v lacks %q", err, want)
		}
	}
	if c.heldFault(faultNetworkDelay, "delay") != delay || c.heldFault(faultNetworkInterruption, "link") != link || c.heldFault(faultContainerPause, "pause") != pause {
		t.Fatal("a failed undo left the registry")
	}
	if !c.closed {
		t.Fatal("Terminate did not close the cluster")
	}
	log = nil
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !delay.done || !link.done || !pause.done || len(log) != 3 {
		t.Fatalf("retry did not undo every failed fault: %v", log)
	}
	if len(c.faultsOf(faultNetworkDelay)) != 0 {
		t.Fatal("a restored delay was not released")
	}
	// Complete interruptions and pauses stay registered; undoing them again
	// is the handle's no-op.
	if c.heldFault(faultNetworkInterruption, "link") != link || c.heldFault(faultContainerPause, "pause") != pause {
		t.Fatal("complete endpoint or freeze faults were dropped")
	}
}

func TestTerminateBlocksOnlyNetworksOfFailedInterruptions(t *testing.T) {
	c := &Container{}
	var log []string
	hold(t, c, faultNetworkInterruption, "public/a", "public", &log).failures = 1
	hold(t, c, faultNetworkInterruption, "cluster/a", "cluster", &log)
	hold(t, c, faultNetworkDelay, "public/b", "public", &log).failures = 1
	hold(t, c, faultContainerPause, "c", "", &log).failures = 1
	c.mu.Lock()
	errs, blocked := c.faults.undo(t.Context(), &c.mu)
	c.mu.Unlock()
	if len(errs) != 3 || len(blocked) != 1 || !blocked["public"] {
		t.Fatalf("errors %v, blocked %v; want only the failed interruption's network", errs, blocked)
	}
	// Through Terminate: the blocked bridge stays, the other is removed.
	c = &Container{nets: networkState{public: &testcontainers.DockerNetwork{Name: "public"}}}
	hold(t, c, faultNetworkInterruption, "public/a", "public", &log).failures = 1
	if err := c.Terminate(t.Context()); err == nil || c.nets.publicRemoved {
		t.Fatal("Terminate removed a network that a failed interruption needs")
	}
}

func TestSettleAfterTerminateReportsClosed(t *testing.T) {
	c := &Container{}
	var log []string
	c.mu.Lock()
	token, _, err := c.faults.reserve(t.Context(), faultNetworkDelay, "net/osd-0")
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	f := &modelFault{name: "late", c: c, token: token, releases: true, log: &log}
	c.mu.Lock()
	c.faults.settle(token, "", f, f.undo, nil)
	closed := c.closed
	c.mu.Unlock()
	if !closed || len(log) != 0 {
		t.Fatalf("settle after Terminate reported closed=%v, undo log %v", closed, log)
	}
	// The caller self-restores, which releases the entry.
	if err := f.undo(t.Context()); err != nil || len(c.faultsOf(faultNetworkDelay)) != 0 {
		t.Fatal("self-restore after a late settle left the entry")
	}
}

// TestTerminateConcurrentWithDelayRestore models a lease whose Restore takes
// its own mutex and then c.mu to release: Terminate must not hold c.mu
// while it calls that Restore, or the two deadlock.
func TestTerminateConcurrentWithDelayRestore(t *testing.T) {
	for range 20 {
		c := &Container{}
		var leaseMu sync.Mutex
		restores := 0
		c.mu.Lock()
		token, _, _ := c.faults.reserve(t.Context(), faultNetworkDelay, "net/osd-0")
		restore := func(context.Context) error {
			leaseMu.Lock()
			defer leaseMu.Unlock()
			restores++
			c.mu.Lock()
			defer c.mu.Unlock()
			c.faults.release(token)
			return nil
		}
		c.faults.settle(token, "", &restores, restore, nil)
		c.mu.Unlock()
		done := make(chan error, 2)
		go func() { done <- c.Terminate(t.Context()) }()
		go func() { done <- restore(t.Context()) }()
		for range 2 {
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Terminate and a concurrent Restore deadlocked")
			}
		}
		if len(c.faultsOf(faultNetworkDelay)) != 0 || restores == 0 {
			t.Fatal("the lease was not released")
		}
	}
}
