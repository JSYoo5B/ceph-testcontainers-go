package override

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

func TestSetFullRatiosOrderingNormalizationAndSharedRestore(t *testing.T) {
	for _, requested := range []ceph.FullRatios{{NearFull: .11, BackfillFull: .22, Full: .33}, {NearFull: .86, BackfillFull: .91, Full: .96}, {NearFull: .86, BackfillFull: .89, Full: .96}, {NearFull: .84, BackfillFull: .91, Full: .94}} {
		t.Run(strconv.FormatFloat(requested.NearFull, 'g', -1, 64)+"/"+strconv.FormatFloat(requested.BackfillFull, 'g', -1, 64), func(t *testing.T) {
			c, ctr := newFullRatiosFixture(t)
			previous := ctr.ratios
			normalized, _ := normalizeFullRatios(requested)
			change, err := SetFullRatios(t.Context(), c, requested)
			if err != nil || change == nil || change.state.refused != nil || ctr.ratios != normalized || !ctr.observedOrdered {
				t.Fatal("requested native tuple not applied with ascending intermediate states", err, ctr.ratios)
			}
			if ctr.mutations != 3 {
				t.Fatal("threshold update was duplicated or omitted", ctr.mutations)
			}
			before := len(ctr.calls)
			if overlap, err := SetFullRatios(t.Context(), c, previous); err == nil || overlap == nil || overlap.state.refused == nil || len(ctr.calls) != before {
				t.Fatal("overlapping lease reached native reads or returned a real handle")
			}
			copy := *change
			if err := copy.Restore(t.Context()); err != nil || ctr.ratios != previous || !ctr.observedOrdered || len(heldFullRatios(c)) != 0 {
				t.Fatal("restore did not recover original native tuple", err)
			}
			before = len(ctr.calls)
			if err := change.Restore(t.Context()); err != nil || len(ctr.calls) != before {
				t.Fatal("copied handles did not share lifecycle state", err)
			}
		})
	}
}

func TestSetFullRatiosPartialApplyAndRestoreLostReplies(t *testing.T) {
	for _, phase := range []string{"apply", "restore"} {
		for step := 1; step <= 3; step++ {
			for _, committed := range []bool{false, true} {
				t.Run(phase+"/"+strconv.Itoa(step)+"/"+strconv.FormatBool(committed), func(t *testing.T) {
					c, ctr := newFullRatiosFixture(t)
					previous := ctr.ratios
					at := step
					if phase == "restore" {
						at += 3
					}
					if committed {
						ctr.failAfter = at
					} else {
						ctr.failBefore = at
					}
					change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
					if change == nil || change.state.refused != nil || (phase == "apply" && err == nil) || (phase == "restore" && err != nil) {
						t.Fatal("partial apply lost tracked handle or original failure", err)
					}
					if len(heldFullRatios(c)) != 1 {
						t.Fatal("the uncertain override was not tracked")
					}
					if phase == "restore" {
						if err := change.Restore(t.Context()); err == nil {
							t.Fatal("lost restore reply was hidden")
						}
					}
					ctr.failBefore, ctr.failAfter = 0, 0
					if err := change.Restore(t.Context()); err != nil || ctr.ratios != previous || len(heldFullRatios(c)) != 0 || !ctr.observedOrdered {
						t.Fatal("partial owned tuple did not safely restore", err, ctr.ratios)
					}
				})
			}
		}
	}
}

func TestSetFullRatiosLostReadbackKeepsOnlyOnePendingTransition(t *testing.T) {
	c, ctr := newFullRatiosFixture(t)
	previous := ctr.ratios
	ctr.afterMutation = func() { ctr.failDump = true }
	change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
	if err == nil || change == nil || change.state.refused != nil || ctr.mutations != 1 || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("uncertain readback was adopted or lost handle/output safety", err)
	}
	ctr.failDump, ctr.afterMutation = false, nil
	if err := change.Restore(t.Context()); err != nil || ctr.ratios != previous {
		t.Fatal("uncertain readback could not reconcile owned before/after states", err)
	}

	c, ctr = newFullRatiosFixture(t)
	change, err = SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
	if err != nil {
		t.Fatal(err)
	}
	// The first apply prefix is historical after all three commits succeeded.
	// It must not become an authorized restore state if an outside writer reuses it.
	ctr.ratios = previous
	ctr.ratios.NearFull = float64(float32(.1))
	before := ctr.mutations
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != before {
		t.Fatal("historical apply prefix authorized outside writes")
	}
}

func TestSetFullRatiosNoopAndOutsideTupleRefuseMutation(t *testing.T) {
	for _, noop := range []bool{false, true} {
		c, ctr := newFullRatiosFixture(t)
		requested := ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3}
		if noop {
			requested = ctr.ratios
		}
		change, err := SetFullRatios(t.Context(), c, requested)
		if err != nil {
			t.Fatal(err)
		}
		ctr.ratios.BackfillFull = float64(float32(.7))
		before := ctr.mutations
		if err := change.Restore(t.Context()); err == nil || ctr.mutations != before || len(heldFullRatios(c)) != 1 {
			t.Fatal("outside tuple overwritten or active lease lost", err)
		}
	}
}

func TestSetFullRatiosRestoreRetainsCapturedIdentityAndMapEpoch(t *testing.T) {
	for _, drift := range []string{"bootstrap", "rollback", "unrelated-epoch"} {
		t.Run(drift, func(t *testing.T) {
			c, ctr := newFullRatiosFixture(t)
			change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
			if err != nil {
				t.Fatal(err)
			}
			switch drift {
			case "bootstrap":
				ctr.fsid = "8797c2db-bf63-40a1-990e-eb6658b50cf7"
				testhelper.TestSetConfig(c, []byte("[global]\nfsid = "+ctr.fsid+"\nmon_host = mon\n"))
			case "rollback":
				ctr.epoch = 1
			case "unrelated-epoch":
				ctr.epoch += 7
			}
			before := ctr.mutations
			err = change.Restore(t.Context())
			if drift == "unrelated-epoch" {
				if err != nil || ctr.mutations != before+3 {
					t.Fatal("unrelated native epoch change blocked owned restoration", err)
				}
			} else if err == nil || ctr.mutations != before || len(heldFullRatios(c)) != 1 {
				t.Fatal("replacement bootstrap or map rollback authorized mutation", err)
			}
		})
	}
}

func TestSetFullRatiosFailureBeforeRegistrationFreesTheReservation(t *testing.T) {
	for _, unavailable := range []string{"closed", "control", "config", "keyring", "native", "unordered"} {
		t.Run(unavailable, func(t *testing.T) {
			c, ctr := newFullRatiosFixture(t)
			ordered := ctr.ratios
			switch unavailable {
			case "closed":
				testhelper.TestSetClosed(c, true)
			case "control":
				// Not the fixture: a cluster without a control container.
				c = testhelper.TestCluster(nil, testhelper.TestConfig{StartupTimeout: time.Minute})
				testhelper.TestSetConfig(c, []byte("[global]\nfsid = "+fullRatiosFSID+"\nmon_host = mon\n"))
				testhelper.TestSetKeyring(c, []byte("confirmed"))
			case "config":
				testhelper.TestSetConfig(c, nil)
			case "keyring":
				testhelper.TestSetKeyring(c, nil)
			case "native":
				ctr.failDump = true
			case "unordered":
				ctr.ratios = ceph.FullRatios{NearFull: ordered.Full, BackfillFull: ordered.BackfillFull, Full: ordered.NearFull}
			}
			change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .86, BackfillFull: .91, Full: .96})
			if err == nil || change == nil || change.state.refused == nil || len(heldFullRatios(c)) != 0 || ctr.mutations != 0 || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("failed initial observation published a lease or native output", unavailable, err)
			}
			if err := change.Restore(t.Context()); err != nil {
				t.Fatal("the not-applied handle failed to restore", err)
			}
			// The freed key is available again once the cluster is healthy.
			if unavailable == "control" {
				return
			}
			testhelper.TestSetClosed(c, false)
			testhelper.TestSetConfig(c, []byte("[global]\nfsid = "+fullRatiosFSID+"\nmon_host = mon\n"))
			testhelper.TestSetKeyring(c, []byte("confirmed"))
			ctr.failDump, ctr.ratios = false, ordered
			if again, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .86, BackfillFull: .91, Full: .96}); err != nil || again.state.refused != nil {
				t.Fatal("a freed key stayed refused", err)
			}
		})
	}
}

func TestSetFullRatiosNilClusterAndUnavailableHandles(t *testing.T) {
	change, err := SetFullRatios(t.Context(), nil, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrInvalidRequest) || !errors.As(err, &op) || op.Op != "override.SetFullRatios" || change == nil || change.state.refused == nil {
		t.Fatal("nil cluster did not return a not-applied handle and a classified error", err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	var absent *FullRatiosHandle
	if absent.Restore(t.Context()) == nil || (&FullRatiosHandle{}).Restore(t.Context()) == nil {
		t.Fatal("unavailable restoration handle accepted")
	}
}

// TestSetFullRatiosNotAppliedHandleTakesNoLock restores every refused call
// while the cluster mutex and the control gates are held by the test, so a
// restore that took any of them would block.
func TestSetFullRatiosNotAppliedHandleTakesNoLock(t *testing.T) {
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	for name, call := range map[string]func(*testing.T) (*ceph.Container, *fullRatiosFixture, *FullRatiosHandle, error){
		"shared": func(t *testing.T) (*ceph.Container, *fullRatiosFixture, *FullRatiosHandle, error) {
			ctr := &fullRatiosFixture{}
			c := testhelper.TestCluster(ctr, testhelper.TestConfig{Shared: true, StartupTimeout: time.Minute})
			h, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
			return c, ctr, h, err
		},
		"context ended": func(t *testing.T) (*ceph.Container, *fullRatiosFixture, *FullRatiosHandle, error) {
			c, ctr := newFullRatiosFixture(t)
			h, err := SetFullRatios(ended, c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
			return c, ctr, h, err
		},
		"invalid ratios": func(t *testing.T) (*ceph.Container, *fullRatiosFixture, *FullRatiosHandle, error) {
			c, ctr := newFullRatiosFixture(t)
			h, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .3, BackfillFull: .2, Full: .1})
			return c, ctr, h, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, ctr, handle, err := call(t)
			if err == nil || handle == nil || handle.state.refused == nil || len(ctr.calls) != 0 {
				t.Fatal("the refusal returned a real handle or reached the native CLI", err)
			}
			switch name {
			case "shared":
				if !errors.Is(err, ceph.ErrSharedCluster) {
					t.Fatal("not a shared-cluster refusal", err)
				}
			case "context ended":
				if !errors.Is(err, context.Canceled) {
					t.Fatal("the context error was lost", err)
				}
			case "invalid ratios":
				if !errors.Is(err, ceph.ErrInvalidRequest) {
					t.Fatal("not classified", err)
				}
			}
			state := testhelper.TestState(c)
			state.Mutex.Lock()
			state.ControlMutex.Lock()
			state.ConfigMutex.Lock()
			defer state.Mutex.Unlock()
			defer state.ControlMutex.Unlock()
			defer state.ConfigMutex.Unlock()
			done := make(chan error, 1)
			go func() { done <- handle.Restore(ended) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal("restoring a not-applied handle failed", err)
				}
			case <-time.After(time.Second):
				t.Fatal("restoring a not-applied handle waited for a lock")
			}
		})
	}
}

func TestSetFullRatiosCapturesTheControlOnce(t *testing.T) {
	c, ctr := newFullRatiosFixture(t)
	replacement := &fullRatiosFixture{fsid: "changed"}
	ctr.hook = func(_ context.Context, args []string) error {
		if args[0] == "fsid" {
			testhelper.TestSetControlPlane(c, replacement)
		}
		return nil
	}
	change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
	if err != nil || change.state.refused != nil || len(replacement.calls) != 0 || ctr.mutations != 3 {
		t.Fatal("a replacement control container received an apply step", err, replacement.calls)
	}
}

func TestSetFullRatiosStrictNativeInputs(t *testing.T) {
	c, ctr := newFullRatiosFixture(t)
	for _, values := range []ceph.FullRatios{{NearFull: 0, BackfillFull: .5, Full: 1}, {NearFull: .9, BackfillFull: .9, Full: .9}, {NearFull: .9, BackfillFull: .7, Full: .5}} {
		ctr.raw = ""
		ctr.ratios = values
		before := ctr.mutations
		if change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3}); err == nil || change.state.refused == nil || ctr.mutations != before || len(heldFullRatios(c)) != 0 {
			t.Fatal("misordered original entered ordered override lifecycle", err)
		}
	}
	// A native map that does not parse strictly is not a base to restore to.
	valid, _ := json.Marshal(map[string]any{"fsid": fullRatiosFSID, "epoch": 7, "nearfull_ratio": .8, "backfillfull_ratio": .9, "full_ratio": 1})
	for _, raw := range []string{"null", "{}", string(valid) + " {}", strings.Replace(string(valid), `"epoch":7`, `"epoch":null`, 1), strings.Replace(string(valid), `"full_ratio":1`, `"full_ratio":"1"`, 1)} {
		ctr.raw = raw
		if change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3}); err == nil || change.state.refused == nil || ctr.mutations != 0 {
			t.Fatal("invalid native map accepted", raw, err)
		}
	}
	ctr.raw, ctr.ratios = "", ceph.FullRatios{NearFull: .85, BackfillFull: .9, Full: .95}
	for _, values := range []ceph.FullRatios{{NearFull: 0, BackfillFull: .2, Full: .3}, {NearFull: .1, BackfillFull: .2, Full: 1.01}, {NearFull: math.NaN(), BackfillFull: .2, Full: .3}, {NearFull: .1, BackfillFull: math.Inf(1), Full: .3}, {NearFull: .2, BackfillFull: .1, Full: .3}, {NearFull: .1, BackfillFull: .1 + 1e-12, Full: .3}, {NearFull: math.SmallestNonzeroFloat64, BackfillFull: .2, Full: .3}} {
		before := len(ctr.calls)
		change, err := SetFullRatios(t.Context(), c, values)
		if err == nil || !errors.Is(err, ceph.ErrInvalidRequest) || change.state.refused == nil || len(ctr.calls) != before {
			t.Fatal("invalid/float32-collapsed fixture reached native CLI", values, err)
		}
	}
}

func TestSetFullRatiosCancellationAndBoundedAdmission(t *testing.T) {
	for _, gate := range []string{"owner", "control", "config"} {
		c, ctr := newFullRatiosFixture(t)
		state := testhelper.TestState(c)
		switch gate {
		case "owner":
			state.Mutex.Lock()
			defer state.Mutex.Unlock()
		case "control":
			state.ControlMutex.Lock()
			defer state.ControlMutex.Unlock()
		case "config":
			state.ConfigMutex.Lock()
			defer state.ConfigMutex.Unlock()
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		done := make(chan error, 1)
		var handle *FullRatiosHandle
		go func() {
			var err error
			handle, err = SetFullRatios(ctx, c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
			done <- err
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) || len(ctr.calls) != 0 || handle.state.refused == nil {
				t.Fatal("expired admission changed fixture or lost deadline", gate, err)
			}
		case <-time.After(time.Second):
			t.Fatal("full ratios admission blocked beyond caller deadline", gate)
		}
		cancel()
	}

	c, ctr := newFullRatiosFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	ctr.afterMutation = cancel
	change, err := SetFullRatios(ctx, c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
	if change == nil || change.state.refused != nil || !errors.Is(err, context.Canceled) || ctr.mutations != 1 {
		t.Fatal("canceled native attempt lost tracked partial fixture", err)
	}
	ctr.afterMutation = nil
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal("new context did not restore cancelled fixture", err)
	}
}

// TestSetFullRatiosRefusesUntrackedHandles forges a handle that has the right
// state but no registry entry, as a stale or copied-from-nothing handle would.
func TestSetFullRatiosRefusesUntrackedHandles(t *testing.T) {
	c, ctr := newFullRatiosFixture(t)
	ordered := ctr.ratios
	change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .86, BackfillFull: .91, Full: .96})
	if err != nil {
		t.Fatal(err)
	}
	forged := &FullRatiosHandle{owner: c, state: &fullRatiosState{previous: ordered, expected: change.state.expected, fsid: change.state.fsid}}
	before := ctr.mutations
	if err := forged.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "not tracked") || ctr.mutations != before {
		t.Fatalf("an untracked override restored: %v", err)
	}
}

// TestSetFullRatiosRestoreAfterTerminateIsRefused keeps the override
// registered when the cluster is terminated: Terminate does not undo it and a
// later Restore is refused.
func TestSetFullRatiosRestoreAfterTerminateIsRefused(t *testing.T) {
	c, ctr := newFullRatiosFixture(t)
	change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
	if err != nil {
		t.Fatal(err)
	}
	mutations := ctr.mutations
	testhelper.TestSetClosed(c, true)
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != mutations || len(heldFullRatios(c)) != 1 {
		t.Fatal("a terminated cluster restored or lost the override", err)
	}
}

// TestSetFullRatiosIsTrackedBeforeTheFirstNativeStep reads the registry from
// inside the first native mutation, where the cluster mutex is held by the
// apply: the handle must already be the held override, so that Terminate and an
// overlapping call see it even if the step never returns.
func TestSetFullRatiosIsTrackedBeforeTheFirstNativeStep(t *testing.T) {
	c, ctr := newFullRatiosFixture(t)
	var seen any
	ctr.hook = func(_ context.Context, args []string) error {
		if strings.HasPrefix(args[0], "osd") && len(args) == 3 && seen == nil {
			seen = testhelper.TestHeld(c, overridehook.FullRatios)[""]
		}
		return nil
	}
	change, err := SetFullRatios(t.Context(), c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
	if err != nil {
		t.Fatal(err)
	}
	if held, ok := seen.(*FullRatiosHandle); !ok || held != change {
		t.Fatalf("the first native step ran while the registry held %v, not the handle", seen)
	}
}
