package blockio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
)

// controlReceipts substitutes only finite table states and lost completion
// receipts for Container.nativeCommand. It executes no command and models no
// actual kernel timing, I/O, device ownership or native compatibility.
type controlReceipts struct {
	t                                           *testing.T
	name, instance, uuid, live, inactive, state string
	steps, ids, reads                           []string
	finished                                    map[string]control.View
	lose, refuse                                string
	cancel                                      context.CancelFunc
	// never names a step whose dispatch the exec cushion pre-empted, so it
	// never claimed its started marker; unconfirmed leaves that stop
	// unconfirmed instead. unstarted holds the ids of those dispatches.
	never       string
	unconfirmed bool
	unstarted   map[string]bool
}

// unstartedDispatch is the supervised failure of a dispatch that never ran:
// a backend error alone when the cushion confirmed the stop, or joined with
// NotStoppedError when the cancel itself failed.
func unstartedDispatch(unconfirmed bool) error {
	err := error(&supervised.BackendError{Op: "exec block I/O keeper supervisor", Err: context.DeadlineExceeded})
	if unconfirmed {
		err = errors.Join(err, &supervised.NotStoppedError{Marker: supervised.MarkerDir + "/pending", Err: errors.New("canceler unavailable")})
	}
	return err
}

func newControlFixture(t *testing.T) (*Container, *controlReceipts) {
	t.Helper()
	m := &controlReceipts{t: t, name: "owned", instance: "keeper-generation", uuid: "TC-BLK-owned", live: "0 2097152 linear 7:1 0", state: "ACTIVE", finished: map[string]control.View{}, unstarted: map[string]bool{}}
	b := &Container{name: m.name, instance: m.instance, size: DefaultSize, ready: true, linear: m.live,
		loop:   control.LoopIdentity{Loop: "/dev/loop1", File: "/tc-blk/data-owned", Inode: 42, Dev: "7:1"},
		record: control.Record{Name: m.name, UUID: m.uuid, Major: 253, Minor: 1, Live: m.live}}
	b.nativeCommand = m.command
	return b, m
}

func (m *controlReceipts) command(ctx context.Context, args ...string) ([]byte, error) {
	m.t.Helper()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	encode := func(value any) ([]byte, error) { return json.Marshal(value) }
	if len(args) < 7 || args[2] != ownerProgram {
		m.t.Fatalf("unexpected command shape: %q", args)
	}
	if args[3] != "exec" {
		if args[3] != "probe" {
			m.t.Fatalf("unexpected owner action %q", args[3])
		}
		m.reads = append(m.reads, "probe")
		return encode(ownerReply{Lease: m.name, Instance: m.instance, Targets: []string{"linear", "delay", "error"}})
	}
	if args[6] != m.instance {
		return nil, errors.New("keeper generation changed")
	}
	index := -1
	for i, arg := range args {
		if arg == control.Script {
			index = i
			break
		}
	}
	if index < 0 || len(args) < index+3 {
		m.t.Fatal("missing shared control script/action")
	}
	action, tail := args[index+1], args[index+3:]
	if args[index+2] != m.name {
		m.t.Fatal("wrong mapping name")
	}
	switch action {
	case "observe":
		m.reads = append(m.reads, "observe")
		live := m.live
		var inactive *string
		if m.inactive != "" {
			value := m.inactive
			inactive = &value
		}
		return encode(control.View{Action: action, Name: m.name, Exists: true, State: m.state, UUID: m.uuid, Major: 253, Minor: 1, Live: &live, Inactive: inactive, InFlight: []int64{0, 0}})
	case "settled":
		m.reads = append(m.reads, "settled")
		if len(tail) != 1 {
			m.t.Fatal("invalid settle query")
		}
		if m.unstarted[tail[0]] {
			return encode(control.View{Action: "settled", Name: m.name, ID: tail[0]})
		}
		value, ok := m.finished[tail[0]]
		if !ok {
			m.t.Fatalf("unknown completion id %q", tail[0])
		}
		value.Action = "settled"
		return encode(value)
	case "step":
		if len(tail) < 3 {
			m.t.Fatal("invalid step")
		}
		op, id := tail[0], tail[2]
		for _, prior := range m.ids {
			if prior == id {
				m.t.Fatalf("operation id %s was dispatched twice", id)
			}
		}
		m.ids = append(m.ids, id)
		if m.never == op {
			m.never = ""
			m.unstarted[id] = true
			return nil, unstartedDispatch(m.unconfirmed)
		}
		m.steps = append(m.steps, op)
		value := control.View{Action: "step", Name: m.name, Op: op, ID: id, Done: true}
		if m.refuse == op {
			value.RC = 1
			value.Error = "kernel refused this operation"
		} else {
			switch op {
			case "load":
				if len(tail) != 4 {
					m.t.Fatal("missing table")
				}
				m.inactive = tail[3]
			case "suspend":
				m.state = "SUSPENDED"
			case "resume":
				m.state = "ACTIVE"
				if m.inactive != "" {
					m.live, m.inactive = m.inactive, ""
				}
			case "clear":
				m.inactive = ""
			default:
				m.t.Fatalf("unexpected mutation %q", op)
			}
		}
		m.finished[id] = value
		if m.lose == op {
			m.lose = ""
			if m.cancel != nil {
				m.cancel()
				return nil, context.Canceled
			}
			return nil, errors.New("completion reply lost")
		}
		return encode(value)
	default:
		return nil, fmt.Errorf("unexpected control action %q", action)
	}
}

func TestInvalidConfigurationAndProfilesNeedNoDocker(t *testing.T) {
	for _, opts := range [][]testcontainers.ContainerCustomizer{
		{WithConfig(Config{Size: MinSize - 512})}, {WithConfig(Config{Size: MaxSize + 512})},
		{WithConfig(Config{Size: MinSize + 1})}, {WithConfig(Config{}), WithConfig(Config{})},
	} {
		b, err := Run(context.Background(), "example.invalid/helper:1", opts...)
		if b != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("invalid config allocated backend: %v %v", b, err)
		}
	}
	if b, err := Run(context.Background(), ""); b != nil || !errors.Is(err, ceph.ErrImageCapability) {
		t.Fatalf("empty image: %v %v", b, err)
	}
	for _, profile := range []Profile{{}, {Delay: -time.Millisecond}, {Delay: time.Microsecond}, {Delay: MaxDelay + time.Millisecond}, {EIO: true, Delay: time.Millisecond}} {
		b, m := newControlFixture(t)
		if err := b.Apply(context.Background(), profile); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("invalid profile %+v: %v", profile, err)
		}
		if len(m.reads)+len(m.steps) != 0 {
			t.Fatal("invalid profile reached the native boundary")
		}
	}
}

func TestApplyRestoreUsesObservedOwnedStates(t *testing.T) {
	for _, profile := range []Profile{{EIO: true}, {Delay: 25 * time.Millisecond}} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			b, m := newControlFixture(t)
			want, err := profile.table(b.size/512, b.loop.Dev)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Apply(context.Background(), profile); err != nil {
				t.Fatal(err)
			}
			if m.live != want || m.state != "ACTIVE" || b.record.Live != want || b.requested != profile {
				t.Fatalf("apply not confirmed: %+v %+v", b.record, m)
			}
			if err := b.Restore(context.Background()); err != nil {
				t.Fatal(err)
			}
			if m.live != b.linear || m.inactive != "" || m.state != "ACTIVE" || b.requested != (Profile{}) {
				t.Fatalf("restore not confirmed: %+v", m)
			}
			if want := []string{"load", "suspend", "resume", "load", "suspend", "resume"}; !reflect.DeepEqual(m.steps, want) {
				t.Fatalf("steps %v", m.steps)
			}
			before := len(m.steps)
			if err := b.Restore(context.Background()); err != nil || len(m.steps) != before {
				t.Fatalf("repeat restore changed state: %v", err)
			}
		})
	}
}

func TestForeignTablesOrIdentityNeverDispatchMutation(t *testing.T) {
	for _, kind := range []string{"uuid", "live", "inactive"} {
		t.Run(kind, func(t *testing.T) {
			b, m := newControlFixture(t)
			switch kind {
			case "uuid":
				m.uuid = "foreign"
			case "live":
				m.live = "0 2097152 linear 7:9 0"
			case "inactive":
				m.inactive = "0 2097152 error"
			}
			if err := b.Apply(context.Background(), Profile{Delay: time.Millisecond}); err == nil {
				t.Fatal("foreign state accepted")
			}
			if err := b.Restore(context.Background()); err == nil {
				t.Fatal("foreign state restored")
			}
			if len(m.steps) != 0 {
				t.Fatalf("foreign state mutated: %v", m.steps)
			}
		})
	}
}

func TestRefusedLoadStopsWithoutAnotherMutation(t *testing.T) {
	b, m := newControlFixture(t)
	m.refuse = "load"
	if err := b.Apply(context.Background(), Profile{EIO: true}); err == nil {
		t.Fatal("refused load accepted")
	}
	if !reflect.DeepEqual(m.steps, []string{"load"}) || b.record.Unsettled != "" || b.record.Loaded != "" || m.live != b.linear {
		t.Fatalf("refusal state: %+v %+v", b.record, m)
	}
	if err := b.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.steps) != 1 {
		t.Fatal("refusal caused a follow-up mutation")
	}
}

func TestLostStepReplyIsPolledBeforeOwnedInactiveTableIsCleared(t *testing.T) {
	b, m := newControlFixture(t)
	m.lose = "load"
	if err := b.Apply(context.Background(), Profile{EIO: true}); err == nil {
		t.Fatal("lost reply accepted")
	}
	id := b.record.UnsettledID
	if b.record.Unsettled != "load" || id == "" || b.record.Loaded != "" || m.inactive == "" {
		t.Fatalf("lost reply not retained: %+v", b.record)
	}
	before := len(m.reads)
	if err := b.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.reads) <= before || m.reads[before] != "settled" {
		t.Fatalf("did not poll old step first: %v", m.reads)
	}
	if !reflect.DeepEqual(m.steps, []string{"load", "clear"}) || m.ids[0] != id || b.record.Unsettled != "" || m.inactive != "" || m.live != b.linear {
		t.Fatalf("lost reply recovery: %+v %+v", b.record, m)
	}
}

func TestCancellationAfterDispatchRetainsRecoverableRecord(t *testing.T) {
	b, m := newControlFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.lose, m.cancel = "load", cancel
	if err := b.Apply(ctx, Profile{EIO: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if b.record.Unsettled != "load" || b.record.UnsettledID == "" {
		t.Fatalf("cancellation erased record: %+v", b.record)
	}
	if err := b.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.steps, []string{"load", "clear"}) {
		t.Fatalf("cancel recovery replayed: %v", m.steps)
	}
}

func TestStepDispatchThatNeverRanIsWithdrawnAndRetried(t *testing.T) {
	b, m := newControlFixture(t)
	m.never = "load"
	if err := b.Apply(context.Background(), Profile{EIO: true}); err == nil {
		t.Fatal("pre-empted dispatch accepted")
	}
	first := b.record.UnsettledID
	if b.record.Unsettled != "load" || first == "" || len(m.steps) != 0 {
		t.Fatalf("pre-empted dispatch record: %+v %v", b.record, m.steps)
	}
	if err := b.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.record.Unsettled != "" || len(m.steps) != 0 || m.live != b.linear {
		t.Fatalf("unstarted step was not withdrawn: %+v %v", b.record, m.steps)
	}
	if err := b.Apply(context.Background(), Profile{EIO: true}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.steps, []string{"load", "suspend", "resume"}) || m.ids[1] == first {
		t.Fatalf("retry did not dispatch a fresh step: %v %v", m.steps, m.ids)
	}
}

func TestUnconfirmedStopOfUnstartedStepStaysPending(t *testing.T) {
	b, m := newControlFixture(t)
	m.never, m.unconfirmed = "load", true
	if err := b.Apply(context.Background(), Profile{EIO: true}); err == nil {
		t.Fatal("unconfirmed dispatch accepted")
	}
	id := b.record.UnsettledID
	if err := b.Restore(context.Background()); !errors.Is(err, errPending) {
		t.Fatalf("unconfirmed stop settled: %v", err)
	}
	if b.record.Unsettled != "load" || b.record.UnsettledID != id || len(m.ids) != 1 {
		t.Fatalf("unconfirmed dispatch record changed: %+v %v", b.record, m.ids)
	}
}

func TestKeeperGenerationErrorPreventsAnyControlMutation(t *testing.T) {
	b, m := newControlFixture(t)
	m.instance = "replacement-keeper"
	if err := b.Apply(context.Background(), Profile{EIO: true}); err == nil {
		t.Fatal("replacement keeper accepted")
	}
	if !reflect.DeepEqual(m.reads, []string{"probe"}) || len(m.steps) != 0 {
		t.Fatalf("generation error dispatched control: %v %v", m.reads, m.steps)
	}
}

func TestCommandCapabilityClassification(t *testing.T) {
	for _, code := range []int{126, 127, 1, 137} {
		b := &Container{nativeCommand: func(context.Context, ...string) ([]byte, error) {
			return nil, &supervised.ExitError{Code: code}
		}}
		_, err := b.command(t.Context(), "python3", "-I")
		if errors.Is(err, ceph.ErrImageCapability) != (code == 126 || code == 127) {
			t.Fatalf("supervisor exit %d misclassified: %v", code, err)
		}
	}
	b := &Container{nativeCommand: func(context.Context, ...string) ([]byte, error) {
		return nil, context.Canceled
	}}
	if _, err := b.command(t.Context()); !errors.Is(err, context.Canceled) || errors.Is(err, ceph.ErrImageCapability) {
		t.Fatalf("cancellation misclassified: %v", err)
	}
}
