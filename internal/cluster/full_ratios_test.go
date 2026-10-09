package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const fullRatiosFSID = "35e00c14-bc10-4357-aed3-107912b9bead"

type fullRatiosFixture struct {
	testcontainers.Container
	ratios          FullRatios
	epoch           uint32
	fsid            string
	calls           [][]string
	mutations       int
	failBefore      int
	failAfter       int
	failDump        bool
	raw             string
	hook            func(context.Context, []string) error
	afterMutation   func()
	observedOrdered bool
}

func newFullRatiosFixture() (*Container, *fullRatiosFixture) {
	ratios, _ := normalizeFullRatios(FullRatios{NearFull: .85, BackfillFull: .9, Full: .95})
	ctr := &fullRatiosFixture{ratios: ratios, epoch: 7, fsid: fullRatiosFSID, observedOrdered: true}
	c := &Container{Container: ctr, settings: options{startupTimeout: time.Minute},
		config: []byte("[global]\nfsid = " + fullRatiosFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}
	return c, ctr
}

func (ctr *fullRatiosFixture) Exec(ctx context.Context, command []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	args := slices.Clone(command)
	ctr.calls = append(ctr.calls, args)
	if !slices.Equal(args[:min(3, len(args))], []string{"ceph", "--connect-timeout", "5"}) {
		return 0, nil, errors.New("unexpected fixture CLI prefix")
	}
	args = args[3:]
	if ctr.hook != nil {
		if err := ctr.hook(ctx, args); err != nil {
			return 0, nil, err
		}
	}
	output := ""
	switch {
	case slices.Equal(args, []string{"fsid"}):
		output = ctr.fsid + "\n"
	case slices.Equal(args, []string{"osd", "dump", "--format", "json"}):
		if ctr.failDump {
			return 2, healthDetailsFixtureStream("PRIVATE-NATIVE-OUTPUT"), nil
		}
		if ctr.raw != "" {
			output = ctr.raw
		} else {
			data, _ := json.Marshal(map[string]any{"fsid": ctr.fsid, "epoch": ctr.epoch,
				"nearfull_ratio": ctr.ratios.NearFull, "backfillfull_ratio": ctr.ratios.BackfillFull, "full_ratio": ctr.ratios.Full,
				"flags": "noout,sortbitwise", "pools": []any{}})
			output = string(data)
		}
	case len(args) == 3 && args[0] == "osd" && strings.HasPrefix(args[1], "set-"):
		ctr.mutations++
		if ctr.failBefore == ctr.mutations {
			return 0, nil, errors.New("PRIVATE-TRANSPORT-ERROR")
		}
		value, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return 0, nil, err
		}
		value = float64(float32(value))
		switch args[1] {
		case "set-nearfull-ratio":
			ctr.ratios.NearFull = value
		case "set-backfillfull-ratio":
			ctr.ratios.BackfillFull = value
		case "set-full-ratio":
			ctr.ratios.Full = value
		default:
			return 0, nil, errors.New("unexpected native mutation")
		}
		ctr.epoch++
		ctr.observedOrdered = ctr.observedOrdered && orderedFullRatios(ctr.ratios)
		if ctr.afterMutation != nil {
			ctr.afterMutation()
		}
		if ctr.failAfter == ctr.mutations {
			return 0, nil, errors.New("PRIVATE-LOST-REPLY")
		}
	default:
		return 0, nil, errors.New("unexpected full ratios fixture command")
	}
	return 0, healthDetailsFixtureStream(output), nil
}

func TestFullRatiosObservesNativeMapAndCapturedControl(t *testing.T) {
	c, ctr := newFullRatiosFixture()
	original := ctr.ratios
	ctr.hook = func(_ context.Context, args []string) error {
		if args[0] == "fsid" {
			c.controlPlane = &fullRatiosFixture{fsid: "changed"}
		}
		return nil
	}
	got, err := c.FullRatios(t.Context())
	if err != nil || got != (FullRatioSnapshot{FSID: fullRatiosFSID, Epoch: 7, Ratios: original}) {
		t.Fatalf("native map snapshot changed: %+v %v", got, err)
	}
	want := [][]string{{"ceph", "--connect-timeout", "5", "fsid"},
		{"ceph", "--connect-timeout", "5", "osd", "dump", "--format", "json"},
		{"ceph", "--connect-timeout", "5", "fsid"}}
	if !reflect.DeepEqual(ctr.calls, want) || ctr.mutations != 0 {
		t.Fatal("observation changed control, omitted identity guards or mutated", ctr.calls)
	}
}

func TestFullRatiosTemporaryOrderingNormalizationAndSharedRestore(t *testing.T) {
	for _, requested := range []FullRatios{{.11, .22, .33}, {.86, .91, .96}, {.86, .89, .96}, {.84, .91, .94}} {
		t.Run(strconv.FormatFloat(requested.NearFull, 'g', -1, 64)+"/"+strconv.FormatFloat(requested.BackfillFull, 'g', -1, 64), func(t *testing.T) {
			c, ctr := newFullRatiosFixture()
			previous := ctr.ratios
			normalized, _ := normalizeFullRatios(requested)
			change, err := c.TemporaryFullRatios(t.Context(), requested)
			if err != nil || change == nil || ctr.ratios != normalized || !ctr.observedOrdered {
				t.Fatal("requested native tuple not applied with ascending intermediate states", err, ctr.ratios)
			}
			if ctr.mutations != 3 {
				t.Fatal("threshold update was duplicated or omitted", ctr.mutations)
			}
			before := len(ctr.calls)
			if overlap, err := c.TemporaryFullRatios(t.Context(), previous); err == nil || overlap != nil || len(ctr.calls) != before {
				t.Fatal("overlapping lease reached native reads")
			}
			copy := *change
			if err := copy.Restore(t.Context()); err != nil || ctr.ratios != previous || !ctr.observedOrdered || c.fullRatiosOverride != nil {
				t.Fatal("restore did not recover original native tuple", err)
			}
			before = len(ctr.calls)
			if err := change.Restore(t.Context()); err != nil || len(ctr.calls) != before {
				t.Fatal("copied handles did not share lifecycle state", err)
			}
		})
	}
}

func TestFullRatiosPartialApplyAndRestoreLostReplies(t *testing.T) {
	for _, phase := range []string{"apply", "restore"} {
		for step := 1; step <= 3; step++ {
			for _, committed := range []bool{false, true} {
				t.Run(phase+"/"+strconv.Itoa(step)+"/"+strconv.FormatBool(committed), func(t *testing.T) {
					c, ctr := newFullRatiosFixture()
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
					change, err := c.TemporaryFullRatios(t.Context(), FullRatios{.1, .2, .3})
					if change == nil || (phase == "apply" && err == nil) || (phase == "restore" && err != nil) {
						t.Fatal("partial apply lost tracked handle or original failure", err)
					}
					if phase == "restore" {
						if err := change.Restore(t.Context()); err == nil {
							t.Fatal("lost restore reply was hidden")
						}
					}
					ctr.failBefore, ctr.failAfter = 0, 0
					if err := change.Restore(t.Context()); err != nil || ctr.ratios != previous || c.fullRatiosOverride != nil || !ctr.observedOrdered {
						t.Fatal("partial owned tuple did not safely restore", err, ctr.ratios)
					}
				})
			}
		}
	}
}

func TestFullRatiosLostReadbackKeepsOnlyOnePendingTransition(t *testing.T) {
	c, ctr := newFullRatiosFixture()
	previous := ctr.ratios
	ctr.afterMutation = func() { ctr.failDump = true }
	change, err := c.TemporaryFullRatios(t.Context(), FullRatios{.1, .2, .3})
	if err == nil || change == nil || ctr.mutations != 1 || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("uncertain readback was adopted or lost handle/output safety", err)
	}
	ctr.failDump, ctr.afterMutation = false, nil
	if err := change.Restore(t.Context()); err != nil || ctr.ratios != previous {
		t.Fatal("uncertain readback could not reconcile owned before/after states", err)
	}

	c, ctr = newFullRatiosFixture()
	change, err = c.TemporaryFullRatios(t.Context(), FullRatios{.1, .2, .3})
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

func TestFullRatiosNoopAndOutsideTupleRefuseMutation(t *testing.T) {
	for _, noop := range []bool{false, true} {
		c, ctr := newFullRatiosFixture()
		requested := FullRatios{.1, .2, .3}
		if noop {
			requested = ctr.ratios
		}
		change, err := c.TemporaryFullRatios(t.Context(), requested)
		if err != nil {
			t.Fatal(err)
		}
		ctr.ratios.BackfillFull = float64(float32(.7))
		before := ctr.mutations
		if err := change.Restore(t.Context()); err == nil || ctr.mutations != before || c.fullRatiosOverride == nil {
			t.Fatal("outside tuple overwritten or active lease lost", err)
		}
	}
}

func TestFullRatiosRestoreRetainsCapturedIdentityAndMapEpoch(t *testing.T) {
	for _, drift := range []string{"bootstrap", "rollback", "unrelated-epoch"} {
		t.Run(drift, func(t *testing.T) {
			c, ctr := newFullRatiosFixture()
			change, err := c.TemporaryFullRatios(t.Context(), FullRatios{.1, .2, .3})
			if err != nil {
				t.Fatal(err)
			}
			switch drift {
			case "bootstrap":
				ctr.fsid = "8797c2db-bf63-40a1-990e-eb6658b50cf7"
				c.config = []byte("[global]\nfsid = " + ctr.fsid + "\nmon_host = mon\n")
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
			} else if err == nil || ctr.mutations != before || c.fullRatiosOverride == nil {
				t.Fatal("replacement bootstrap or map rollback authorized mutation", err)
			}
		})
	}
}

func TestFullRatiosFailureBeforeRegistrationAndNativeErrorSafety(t *testing.T) {
	for _, unavailable := range []string{"closed", "control", "config", "keyring", "native"} {
		c, ctr := newFullRatiosFixture()
		switch unavailable {
		case "closed":
			c.closed = true
		case "control":
			c.Container = nil
		case "config":
			c.config = nil
		case "keyring":
			c.keyring = nil
		case "native":
			ctr.failDump = true
		}
		change, err := c.TemporaryFullRatios(t.Context(), FullRatios{.1, .2, .3})
		if err == nil || change != nil || c.fullRatiosOverride != nil || ctr.mutations != 0 || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("failed initial observation published a lease or native output", unavailable, err)
		}
	}
	var unavailable *Container
	if got, err := unavailable.FullRatios(t.Context()); err == nil || got != (FullRatioSnapshot{}) {
		t.Fatal("nil cluster provided a native observation")
	}
	if change, err := unavailable.TemporaryFullRatios(t.Context(), FullRatios{.1, .2, .3}); err == nil || change != nil {
		t.Fatal("nil cluster provided an override")
	}
	var absent *FullRatiosOverride
	if absent.Restore(t.Context()) == nil || (&FullRatiosOverride{}).Restore(t.Context()) == nil {
		t.Fatal("unavailable restoration handle accepted")
	}
}

func TestFullRatiosStrictNativeJSONAndFixtureValidation(t *testing.T) {
	c, ctr := newFullRatiosFixture()
	valid, _ := json.Marshal(map[string]any{"fsid": fullRatiosFSID, "epoch": 7,
		"nearfull_ratio": .8, "backfillfull_ratio": .9, "full_ratio": 1})
	for _, raw := range []string{"null", "{}", string(valid) + " {}",
		strings.Replace(string(valid), `"epoch":7`, `"epoch":null`, 1),
		strings.Replace(string(valid), `"epoch":7`, `"epoch":4294967296`, 1),
		strings.Replace(string(valid), `"epoch":7`, `"epoch":7,"epoch":8`, 1),
		strings.Replace(string(valid), `"full_ratio":1`, `"Full_Ratio":1`, 1),
		strings.Replace(string(valid), `"full_ratio":1`, `"full_ratio":"1"`, 1),
		strings.Replace(string(valid), `"full_ratio":1`, `"full_ratio":1.01`, 1),
		strings.Replace(string(valid), `"full_ratio":1`, `"full_ratio":-0.01`, 1),
		strings.Replace(string(valid), fullRatiosFSID, "8797c2db-bf63-40a1-990e-eb6658b50cf7", 1),
		strings.Repeat(" ", (4<<20)+1)} {
		ctr.raw = raw
		got, err := c.FullRatios(t.Context())
		if err == nil || got != (FullRatioSnapshot{}) || ctr.mutations != 0 {
			t.Fatal("invalid native map accepted", err)
		}
	}
	for _, values := range []FullRatios{{0, .5, 1}, {.9, .9, .9}, {.9, .7, .5}} {
		ctr.raw = ""
		ctr.ratios = values
		if got, err := c.FullRatios(t.Context()); err != nil || got.Ratios != values {
			t.Fatal("diagnostic check rejected native zero/equal/misordered values", err)
		}
		before := ctr.mutations
		if change, err := c.TemporaryFullRatios(t.Context(), FullRatios{.1, .2, .3}); err == nil || change != nil || ctr.mutations != before {
			t.Fatal("misordered original entered ordered override lifecycle", err)
		}
	}
	for _, values := range []FullRatios{{0, .2, .3}, {.1, .2, 1.01}, {math.NaN(), .2, .3}, {.1, math.Inf(1), .3}, {.2, .1, .3}, {.1, .1 + 1e-12, .3}, {math.SmallestNonzeroFloat64, .2, .3}} {
		before := len(ctr.calls)
		if change, err := c.TemporaryFullRatios(t.Context(), values); err == nil || change != nil || len(ctr.calls) != before {
			t.Fatal("invalid/float32-collapsed fixture reached native CLI", values, err)
		}
	}
}

func TestFullRatiosIdentityCancellationAndBoundedAdmission(t *testing.T) {
	for _, phase := range []int{1, 2, 3} {
		c, ctr := newFullRatiosFixture()
		ctx, cancel := context.WithCancel(t.Context())
		ctr.hook = func(_ context.Context, _ []string) error {
			if len(ctr.calls) == phase {
				cancel()
			}
			return nil
		}
		got, err := c.FullRatios(ctx)
		if !errors.Is(err, context.Canceled) || got != (FullRatioSnapshot{}) || len(ctr.calls) != phase {
			t.Fatal("query cancellation published a snapshot or sent later command", phase, err, ctr.calls)
		}
	}
	for _, phase := range []string{"before", "after"} {
		c, ctr := newFullRatiosFixture()
		ctr.hook = func(_ context.Context, _ []string) error {
			if (phase == "before" && len(ctr.calls) == 1) || (phase == "after" && len(ctr.calls) == 3) {
				ctr.fsid = "8797c2db-bf63-40a1-990e-eb6658b50cf7"
			}
			return nil
		}
		if got, err := c.FullRatios(t.Context()); err == nil || got != (FullRatioSnapshot{}) {
			t.Fatal("native FSID drift accepted", phase)
		}
	}
	for _, gate := range []string{"owner", "control", "config"} {
		c, ctr := newFullRatiosFixture()
		switch gate {
		case "owner":
			c.mu.Lock()
			defer c.mu.Unlock()
		case "control":
			c.controlMu.Lock()
			defer c.controlMu.Unlock()
		case "config":
			c.configMu.Lock()
			defer c.configMu.Unlock()
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		done := make(chan error, 1)
		go func() { _, err := c.TemporaryFullRatios(ctx, FullRatios{.1, .2, .3}); done <- err }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) || len(ctr.calls) != 0 || c.fullRatiosOverride != nil {
				t.Fatal("expired admission changed fixture or lost deadline", gate, err)
			}
		case <-time.After(time.Second):
			t.Fatal("full ratios admission blocked beyond caller deadline", gate)
		}
		cancel()
	}

	c, ctr := newFullRatiosFixture()
	ctx, cancel := context.WithCancel(t.Context())
	ctr.afterMutation = cancel
	change, err := c.TemporaryFullRatios(ctx, FullRatios{.1, .2, .3})
	if change == nil || !errors.Is(err, context.Canceled) || ctr.mutations != 1 {
		t.Fatal("canceled native attempt lost tracked partial fixture", err)
	}
	ctr.afterMutation = nil
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal("new context did not restore cancelled fixture", err)
	}
}
