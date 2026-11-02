package blockio

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/testcontainers/testcontainers-go"
)

// cleanupKeeper substitutes only the helper's Terminate outcome. Embedding the
// interface leaves every other container operation unavailable to these tests.
type cleanupKeeper struct {
	testcontainers.Container
	calls, failures int
}

func (k *cleanupKeeper) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	k.calls++
	if k.failures > 0 {
		k.failures--
		return errors.New("helper termination failed")
	}
	return nil
}

// lifetimeReceipts adds finite removal/detachment/unlink receipts around the
// control fixture. Lost replies model completed actions with unknown results;
// no command executes and no receipt proves real kernel/resource cleanup.
type lifetimeReceipts struct {
	b                                                    *Container
	core                                                 *controlReceipts
	keeper                                               *cleanupKeeper
	events                                               []string
	removed, detached, fileRemoved                       bool
	loseRemove, loseAbsent, loseUnlink                   bool
	opens                                                int
	inflight                                             []int64
	changedInode                                         bool
	detachFailures                                       int
	detachError                                          string
	detachAttempts                                       []string
	loseDetach, foreignDetach                            bool
	removeCalls, detachCalls, unlinkCalls, actualUnlinks int
	// neverStart, neverDetach and neverRemove make that dispatch fail as
	// pre-empted by the exec cushion: it claims no started marker and its
	// completion never appears. loseStart runs the build without a reply.
	neverStart, loseStart, neverDetach, neverRemove bool
	building, buildStarted                          bool
	unranDetach                                     map[string]bool
}

func newLifetimeFixture(t *testing.T) (*Container, *lifetimeReceipts) {
	t.Helper()
	b, core := newControlFixture(t)
	k := &cleanupKeeper{}
	m := &lifetimeReceipts{b: b, core: core, keeper: k, detached: true, inflight: []int64{0, 0}, unranDetach: map[string]bool{}}
	b.Container = k
	b.creationSent = true
	b.nativeCommand = m.command
	return b, m
}

func (m *lifetimeReceipts) command(ctx context.Context, args ...string) ([]byte, error) {
	m.core.t.Helper()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	encode := func(value any) ([]byte, error) { return json.Marshal(value) }
	if len(args) < 7 {
		m.core.t.Fatal("invalid owner command")
	}
	if args[3] != "exec" {
		action := args[3]
		m.events = append(m.events, "owner:"+action)
		done := true
		reply := ownerReply{Lease: m.b.name, Instance: m.b.instance, Done: &done}
		switch action {
		case "inspect":
			done = !m.building
		case "start":
			m.building = true
			if m.neverStart {
				return nil, unstartedDispatch(false)
			}
			m.buildStarted = true
			if m.loseStart {
				return nil, errors.New("start dispatch reply lost")
			}
			done = false
		case "build-started":
			reply.Started = m.buildStarted
		case "detach-started":
			if len(args) != 8 || len(m.detachAttempts) == 0 || args[7] != m.detachAttempts[len(m.detachAttempts)-1] {
				m.core.t.Fatal("detach marker query changed invocation identity")
			}
			reply.Attempt, reply.Started = args[7], !m.unranDetach[args[7]]
		case "start-detach":
			if len(args) != 8 || args[7] == "" {
				m.core.t.Fatal("detach dispatch lacks an invocation identity")
			}
			reply.Attempt = args[7]
			m.detachAttempts = append(m.detachAttempts, reply.Attempt)
			m.detachCalls++
			if m.neverDetach {
				m.neverDetach = false
				m.unranDetach[reply.Attempt] = true
				return nil, unstartedDispatch(false)
			}
			m.detachError = ""
			if m.detachFailures > 0 {
				m.detachFailures--
				m.detachError = "loop still has a transient holder"
			}
			if m.loseDetach {
				m.loseDetach = false
				return nil, errors.New("detach dispatch reply lost")
			}
		case "detached":
			if len(args) != 8 || len(m.detachAttempts) == 0 || args[7] != m.detachAttempts[len(m.detachAttempts)-1] {
				m.core.t.Fatal("detach poll changed invocation identity")
			}
			reply.Attempt = args[7]
			if m.foreignDetach {
				reply.Attempt = "another-invocation"
			}
			reply.Detached, reply.Error = m.detached, m.detachError
			if m.unranDetach[args[7]] {
				reply.Done = new(bool)
			}
		case "unlink":
			m.unlinkCalls++
			if !m.fileRemoved {
				m.actualUnlinks++
				m.fileRemoved = true
			}
			if m.loseUnlink {
				m.loseUnlink = false
				return nil, errors.New("unlink completion reply lost")
			}
			reply.FileRemoved = m.fileRemoved
		default:
			m.core.t.Fatalf("unexpected cleanup owner action %q", action)
		}
		return encode(reply)
	}
	index := -1
	for i, arg := range args {
		if arg == control.Script {
			index = i
			break
		}
	}
	if index < 0 || len(args) < index+3 {
		m.core.t.Fatal("missing control action")
	}
	action, tail := args[index+1], args[index+3:]
	m.events = append(m.events, "control:"+action)
	switch action {
	case "step":
		if len(tail) >= 3 && tail[0] == "remove" {
			id := tail[2]
			for _, prior := range m.core.ids {
				if prior == id {
					m.core.t.Fatal("remove step id replayed")
				}
			}
			m.core.ids = append(m.core.ids, id)
			if m.neverRemove {
				m.neverRemove = false
				m.core.unstarted[id] = true
				return nil, unstartedDispatch(false)
			}
			m.core.steps = append(m.core.steps, "remove")
			m.removeCalls++
			m.removed = true
			reply := control.View{Action: "step", Name: m.b.name, Op: "remove", ID: id, Done: true}
			m.core.finished[id] = reply
			if m.loseRemove {
				m.loseRemove = false
				return nil, errors.New("remove completion reply lost")
			}
			return encode(reply)
		}
	case "observe":
		if m.removed {
			if m.loseAbsent {
				m.loseAbsent = false
				return nil, errors.New("absence observation lost")
			}
			return encode(control.View{Action: action, Name: m.b.name, Exists: false})
		}
		raw, err := m.core.command(ctx, args...)
		if err != nil {
			return nil, err
		}
		var view control.View
		if err := json.Unmarshal(raw, &view); err != nil {
			return nil, err
		}
		view.Open, view.InFlight = m.opens, m.inflight
		return encode(view)
	case "loopid":
		inode := m.b.loop.Inode
		if m.changedInode {
			inode++
		}
		return encode(control.View{Action: action, Name: m.b.name, File: m.b.loop.File, Inode: inode, Holders: []string{}})
	}
	return m.core.command(ctx, args...)
}

func TestTerminateRecoversUnknownRemoveWithoutRedispatch(t *testing.T) {
	for _, kind := range []string{"remove-reply", "absence-observation"} {
		t.Run(kind, func(t *testing.T) {
			b, m := newLifetimeFixture(t)
			m.loseRemove, m.loseAbsent = kind == "remove-reply", kind == "absence-observation"
			if err := b.Terminate(context.Background()); err == nil {
				t.Fatal("lost cleanup evidence was accepted")
			}
			if !b.removeSent || b.mappingRemoved || m.removeCalls != 1 || m.detachCalls != 0 || m.unlinkCalls != 0 || m.keeper.calls != 0 {
				t.Fatalf("resources released after unknown remove: %+v", m)
			}
			pending := b.record.UnsettledID
			if kind == "remove-reply" && (b.record.Unsettled != "remove" || pending == "") {
				t.Fatalf("remove record erased: %+v", b.record)
			}
			before := len(m.events)
			if err := b.Terminate(context.Background()); err != nil {
				t.Fatal(err)
			}
			if m.removeCalls != 1 || !b.released || m.keeper.calls != 1 {
				t.Fatalf("remove replay or incomplete cleanup: %+v", m)
			}
			if kind == "remove-reply" {
				later := m.events[before:]
				if len(later) < 3 || !reflect.DeepEqual(later[:3], []string{"owner:inspect", "control:settled", "control:observe"}) {
					t.Fatalf("old remove was not polled before absence: %v", later)
				}
				if m.core.ids[0] != pending {
					t.Fatal("recorded remove id changed")
				}
			}
		})
	}
}

func TestTerminateRetainsLazyLoopAndResumesWithoutAnotherDetach(t *testing.T) {
	b, m := newLifetimeFixture(t)
	m.detached = false
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("lazy detach accepted as release")
	}
	if !b.mappingRemoved || !b.detachSent || b.loopReleased || m.detachCalls != 1 || m.fileRemoved || m.keeper.calls != 0 {
		t.Fatalf("lazy chain was not retained: %+v", m)
	}
	m.detached = true
	if err := b.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.detachCalls != 1 || m.unlinkCalls != 1 || m.keeper.calls != 1 || !b.released {
		t.Fatalf("detach replay or incomplete cleanup: %+v", m)
	}
}

func TestTerminateRetriesLostUnlinkWithoutRepeatingLoopDetach(t *testing.T) {
	b, m := newLifetimeFixture(t)
	m.loseUnlink = true
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("lost unlink reply accepted")
	}
	if !b.loopReleased || b.backingReleased || !m.fileRemoved || m.keeper.calls != 0 {
		t.Fatalf("unlink uncertainty not retained: %+v", m)
	}
	if err := b.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.detachCalls != 1 || m.unlinkCalls != 2 || m.actualUnlinks != 1 || m.keeper.calls != 1 || !b.released {
		t.Fatalf("unlink retry repeated backing mutation: %+v", m)
	}
}

func TestTerminateRetriesOnlyHelperAfterBackingWasReleased(t *testing.T) {
	b, m := newLifetimeFixture(t)
	m.keeper.failures = 1
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("helper failure ignored")
	}
	if !b.backingReleased || b.released || m.keeper.calls != 1 {
		t.Fatalf("backing progress lost: %+v", b)
	}
	before := len(m.events)
	if err := b.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.events) != before || m.keeper.calls != 2 || !b.released {
		t.Fatalf("helper retry reexecuted owner commands: %v", m.events[before:])
	}
	if err := b.Terminate(context.Background()); err != nil || m.keeper.calls != 2 {
		t.Fatalf("successful cleanup not idempotent: %v", err)
	}
}

func TestTerminateRetainsOpenOrInflightMapping(t *testing.T) {
	for _, kind := range []string{"opener", "read", "write"} {
		t.Run(kind, func(t *testing.T) {
			b, m := newLifetimeFixture(t)
			switch kind {
			case "opener":
				m.opens = 1
			case "read":
				m.inflight = []int64{1, 0}
			case "write":
				m.inflight = []int64{0, 1}
			}
			if err := b.Terminate(context.Background()); err == nil {
				t.Fatal("busy mapping released")
			}
			if m.removeCalls != 0 || m.detachCalls != 0 || m.unlinkCalls != 0 || m.keeper.calls != 0 {
				t.Fatalf("busy chain changed: %+v", m)
			}
		})
	}
}

func TestTerminateRetainsReplacedLoopBackingFile(t *testing.T) {
	b, m := newLifetimeFixture(t)
	m.changedInode = true
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("foreign loop backing accepted")
	}
	if !b.mappingRemoved || m.detachCalls != 0 || m.unlinkCalls != 0 || m.keeper.calls != 0 {
		t.Fatalf("foreign loop/file was changed: %+v", m)
	}
}

func TestTerminateRetriesConfirmedDetachFailureWithFreshIdentityChecks(t *testing.T) {
	b, m := newLifetimeFixture(t)
	m.detachFailures = 1
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("confirmed refusal was accepted")
	}
	first := b.detachID
	if first == "" || b.detachSent || b.loopReleased || m.detachCalls != 1 || m.fileRemoved || m.keeper.calls != 0 {
		t.Fatalf("confirmed refusal progress: %+v", m)
	}
	m.changedInode = true
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("replacement backing was accepted on retry")
	}
	if m.detachCalls != 1 || b.detachID != first || m.unlinkCalls != 0 || m.keeper.calls != 0 {
		t.Fatal("identity refusal dispatched another detach")
	}
	m.changedInode = false
	before := len(m.events)
	if err := b.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(m.detachAttempts) != 2 || m.detachAttempts[0] == m.detachAttempts[1] || !b.released {
		t.Fatalf("confirmed failure did not get a fresh attempt: %v", m.detachAttempts)
	}
	later := m.events[before:]
	if len(later) < 3 || !reflect.DeepEqual(later[:3], []string{"owner:inspect", "control:loopid", "owner:start-detach"}) {
		t.Fatalf("retry skipped identity/holders check: %v", later)
	}
}

func TestTerminateNeverRedispatchesUnknownDetachAttempt(t *testing.T) {
	for _, kind := range []string{"dispatch-reply-lost", "foreign-completion"} {
		t.Run(kind, func(t *testing.T) {
			b, m := newLifetimeFixture(t)
			m.loseDetach, m.foreignDetach = kind == "dispatch-reply-lost", kind == "foreign-completion"
			if err := b.Terminate(context.Background()); err == nil {
				t.Fatal("unknown detach was accepted")
			}
			first := b.detachID
			if !b.detachSent || b.loopReleased || m.detachCalls != 1 || m.unlinkCalls != 0 || m.keeper.calls != 0 {
				t.Fatal("unknown detach was not retained")
			}
			m.foreignDetach = false
			if err := b.Terminate(context.Background()); err != nil {
				t.Fatal(err)
			}
			if m.detachCalls != 1 || b.detachID != first || !b.released {
				t.Fatalf("unknown detach replayed: %v", m.detachAttempts)
			}
		})
	}
}

func TestTerminateReleasesKeeperAfterCreationDispatchNeverRan(t *testing.T) {
	b, m := newLifetimeFixture(t)
	b.ready, b.creationSent, m.neverStart = false, false, true
	if err := b.startCreation(context.Background()); err == nil {
		t.Fatal("pre-empted creation dispatch accepted")
	}
	if !b.creationSent {
		t.Fatal("creation dispatch was not recorded before evidence")
	}
	status, err := b.Status(context.Background())
	if err != nil || status.PendingStep != "" {
		t.Fatalf("unstarted creation reported pending: %+v %v", status, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if !b.released || m.keeper.calls != 1 || m.removeCalls+m.detachCalls+m.unlinkCalls != 0 {
		t.Fatalf("keeper not released alone: %+v", m)
	}
}

func TestTerminateRetainsKeeperAfterLostCreationReply(t *testing.T) {
	b, m := newLifetimeFixture(t)
	b.ready, b.creationSent, m.loseStart = false, false, true
	if err := b.startCreation(context.Background()); err == nil {
		t.Fatal("lost creation reply accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := b.Terminate(ctx); err == nil {
		t.Fatal("started creation released before completion")
	}
	if !b.creationSent || b.released || m.keeper.calls != 0 {
		t.Fatalf("started creation was not retained: %+v", m)
	}
}

func TestTerminateRedispatchesDetachThatNeverRan(t *testing.T) {
	b, m := newLifetimeFixture(t)
	m.neverDetach = true
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("pre-empted detach accepted")
	}
	first := b.detachID
	if !b.detachSent || b.loopReleased || m.unlinkCalls != 0 || m.keeper.calls != 0 {
		t.Fatalf("pre-empted detach progress: %+v", m)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if len(m.detachAttempts) != 2 || m.detachAttempts[1] == first || !b.released || m.keeper.calls != 1 {
		t.Fatalf("unstarted detach was not redispatched: %v %+v", m.detachAttempts, m)
	}
}

func TestTerminateRedispatchesRemoveThatNeverRan(t *testing.T) {
	b, m := newLifetimeFixture(t)
	m.neverRemove = true
	if err := b.Terminate(context.Background()); err == nil {
		t.Fatal("pre-empted remove accepted")
	}
	if !b.removeSent || b.record.Unsettled != "remove" || m.removed || m.keeper.calls != 0 {
		t.Fatalf("pre-empted remove progress: %+v %+v", b.record, m)
	}
	if err := b.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.removeCalls != 1 || len(m.core.ids) != 2 || m.core.ids[0] == m.core.ids[1] || !b.released || m.keeper.calls != 1 {
		t.Fatalf("unstarted remove was not redispatched: %v %+v", m.core.ids, m)
	}
}
