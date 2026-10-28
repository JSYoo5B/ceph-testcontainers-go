package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const fullRatiosFSID = "35e00c14-bc10-4357-aed3-107912b9bead"

type fullRatiosFixture struct {
	testcontainers.Container
	ratios    FullRatios
	epoch     uint32
	fsid      string
	calls     [][]string
	mutations int
	failDump  bool
	raw       string
	hook      func(context.Context, []string) error
}

// float32Ratios rounds each threshold the way the native OSDMap stores it.
func float32Ratios(ratios FullRatios) FullRatios {
	return FullRatios{NearFull: float64(float32(ratios.NearFull)), BackfillFull: float64(float32(ratios.BackfillFull)), Full: float64(float32(ratios.Full))}
}

func newFullRatiosFixture() (*Container, *fullRatiosFixture) {
	ratios := float32Ratios(FullRatios{NearFull: .85, BackfillFull: .9, Full: .95})
	ctr := &fullRatiosFixture{ratios: ratios, epoch: 7, fsid: fullRatiosFSID}
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
		// An observation never mutates: count the attempt and refuse it.
		ctr.mutations++
		return 0, nil, errors.New("unexpected native mutation")
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
			c.controlPlane.handle = &fullRatiosFixture{fsid: "changed"}
		}
		return nil
	}
	got, err := c.fullRatios(t.Context())
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
		got, err := c.fullRatios(t.Context())
		if err == nil || got != (FullRatioSnapshot{}) || ctr.mutations != 0 {
			t.Fatal("invalid native map accepted", err)
		}
	}
	for _, values := range []FullRatios{{0, .5, 1}, {.9, .9, .9}, {.9, .7, .5}} {
		ctr.raw = ""
		ctr.ratios = values
		if got, err := c.fullRatios(t.Context()); err != nil || got.Ratios != values {
			t.Fatal("diagnostic check rejected native zero/equal/misordered values", err)
		}
	}
	if ctr.mutations != 0 {
		t.Fatal("an observation mutated the native map")
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
		got, err := c.fullRatios(ctx)
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
		if got, err := c.fullRatios(t.Context()); err == nil || got != (FullRatioSnapshot{}) {
			t.Fatal("native FSID drift accepted", phase)
		}
	}
}

func TestFullRatiosObservationFailsWithoutACapturedSession(t *testing.T) {
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
		got, err := c.fullRatios(t.Context())
		if err == nil || got != (FullRatioSnapshot{}) || ctr.mutations != 0 || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("failed observation published a snapshot or native output", unavailable, err)
		}
	}
	var unavailable *Container
	if got, err := unavailable.fullRatios(t.Context()); err == nil || got != (FullRatioSnapshot{}) {
		t.Fatal("nil cluster provided a native observation")
	}
}
