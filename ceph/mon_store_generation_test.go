package ceph

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
)

// monStoreGenerationsFake is a small, explicit stand-in for the container side
// of a MON generation. It records every call in order, and models the Docker
// fake's view of which containers exist. Its answers are synthetic: it says
// nothing about how a daemon creates, starts, stops or removes a container.
type monStoreGenerationsFake struct {
	docker      *monStoreDockerFake
	events      []string
	launches    []monStoreLaunch
	images      []string
	next        int
	createErr   error
	partialID   string // returned with createErr when set
	readyErr    error
	removeErr   error
	removeLies  bool // Remove reports success and leaves the container
	removeCalls int
	// Address side. bound is what each created container binds; stolen holds
	// the ports another process owns; held the ports under an exact hold.
	bound      map[string]monStoreEndpoint
	stolen     map[int]bool
	held       map[int]bool
	startErr   error
	observeErr error
	holdErr    error
	releaseErr error
	holdCalls  int
	nilHold    bool
	// partialHold makes Hold return a releasable hold together with holdErr.
	partialHold bool
	// onEvent runs after every recorded event: a third party acting in a gap.
	onEvent func(event string)
}

var monStoreFakeAddress = monStoreEndpoint{ip: "192.0.2.10", ports: []int{3300, 6789}}

func (g *monStoreGenerationsFake) record(event string) {
	g.events = append(g.events, event)
	if g.onEvent != nil {
		g.onEvent(event)
	}
}

// steal is a third party taking a port. It fails while the port is held or owned.
func (g *monStoreGenerationsFake) steal(port int) bool {
	if g.stolen[port] || g.held[port] {
		return false
	}
	if g.stolen == nil {
		g.stolen = map[int]bool{}
	}
	g.stolen[port] = true
	return true
}

type monStoreHoldFake struct {
	g     *monStoreGenerationsFake
	ports []int
	done  bool
}

func (h *monStoreHoldFake) Release(context.Context) error {
	if h.g.releaseErr != nil {
		h.g.record("release")
		return h.g.releaseErr
	}
	for _, p := range h.ports {
		delete(h.g.held, p)
	}
	h.done = true
	// Recorded after the ports are free: a third party acts in the window.
	h.g.record("release")
	return nil
}

func (g *monStoreGenerationsFake) Hold(_ context.Context, a monStoreEndpoint) (monStoreEndpointHold, error) {
	g.holdCalls++
	g.record(fmt.Sprintf("hold %v", a.ports))
	if g.holdErr != nil && g.partialHold {
		if g.held == nil {
			g.held = map[int]bool{}
		}
		for _, p := range a.ports {
			g.held[p] = true
		}
		return &monStoreHoldFake{g: g, ports: a.ports}, g.holdErr
	}
	if g.holdErr != nil {
		return nil, g.holdErr
	}
	if g.nilHold {
		return (*monStoreHoldFake)(nil), nil
	}
	for _, p := range a.ports {
		if g.stolen[p] {
			return nil, fmt.Errorf("port %d is taken", p)
		}
	}
	if g.held == nil {
		g.held = map[int]bool{}
	}
	for _, p := range a.ports {
		g.held[p] = true
	}
	return &monStoreHoldFake{g: g, ports: a.ports}, nil
}

func (g *monStoreGenerationsFake) Start(_ context.Context, id string) error {
	g.record("start " + id)
	if g.startErr != nil {
		return g.startErr
	}
	for _, p := range g.bound[id].ports {
		if g.stolen[p] {
			return fmt.Errorf("port %d is already in use", p)
		}
	}
	return nil
}

func (g *monStoreGenerationsFake) Observe(_ context.Context, id string) (monStoreEndpoint, error) {
	g.record("observe " + id)
	if g.observeErr != nil {
		return monStoreEndpoint{}, g.observeErr
	}
	return g.bound[id].clone(), nil
}

func (g *monStoreGenerationsFake) Create(_ context.Context, launch monStoreLaunch, image string) (string, error) {
	g.launches = append(g.launches, launch)
	defer func() {
		g.record(fmt.Sprintf("create gen%d %s %s", launch.generation, image, map[monStoreMode]string{monStoreFormat: "format", monStoreRequire: "require"}[launch.mode]))
	}()
	g.images = append(g.images, image)
	if g.createErr != nil {
		if g.partialID != "" {
			g.docker.existing[g.partialID] = true
		}
		return g.partialID, g.createErr
	}
	g.next++
	id := fmt.Sprintf("ctr-%d", g.next)
	g.docker.existing[id] = true
	if g.bound == nil {
		g.bound = map[string]monStoreEndpoint{}
	}
	g.bound[id] = monStoreFakeAddress.clone()
	if launch.endpoint != nil {
		g.bound[id] = launch.endpoint.clone()
	}
	return id, nil
}

func (g *monStoreGenerationsFake) WaitReady(_ context.Context, id string) error {
	g.record("ready " + id)
	return g.readyErr
}

func (g *monStoreGenerationsFake) Remove(_ context.Context, id string) error {
	g.removeCalls++
	g.record("remove " + id)
	if g.removeErr != nil {
		return g.removeErr
	}
	if !g.removeLies {
		delete(g.docker.existing, id)
	}
	return nil
}

func monStoreGenerationFixture(t *testing.T) (*monStoreVolume, *monStoreDockerFake, *monStoreGenerationsFake) {
	t.Helper()
	_, r, d := newMonStoreFixture()
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	return r, d, &monStoreGenerationsFake{docker: d}
}

// monStoreRunningFixture has a first generation, from image "old", that
// reached its ready check.
func monStoreRunningFixture(t *testing.T) (*monStoreVolume, *monStoreDockerFake, *monStoreGenerationsFake) {
	t.Helper()
	r, d, g := monStoreGenerationFixture(t)
	if _, err := r.startGeneration(t.Context(), g, "old"); err != nil {
		t.Fatal(err)
	}
	return r, d, g
}

func TestMonStoreFirstGenerationFormatsAndIsReadyWithoutAnIdentity(t *testing.T) {
	r, d, g := monStoreGenerationFixture(t)
	id, err := r.startGeneration(t.Context(), g, "old")
	if err != nil || id != "ctr-1" {
		t.Fatalf("%q %v", id, err)
	}
	if want := []string{"create gen1 old format", "start ctr-1", "ready ctr-1", "observe ctr-1"}; !slices.Equal(g.events, want) {
		t.Fatalf("events %v, want %v", g.events, want)
	}
	if g.launches[0].volume != r.name || r.phase != monStoreBound || r.identity != nil || r.attachment == nil || r.attachment.pending || r.attachment.id != "ctr-1" {
		t.Fatalf("state after the first generation: phase %s identity %v attachment %+v", r.phase, r.identity, r.attachment)
	}
	if d.creates != 1 {
		t.Fatalf("volume creates %d", d.creates)
	}
}

func TestMonStoreReplaceRemovesTheOldContainerThenStartsTheNewImageOnTheSameVolume(t *testing.T) {
	r, d, g := monStoreRunningFixture(t)
	creates, volume := d.creates, r.name
	id, err := r.replaceGeneration(t.Context(), g, "new")
	if err != nil || id != "ctr-2" {
		t.Fatalf("%q %v", id, err)
	}
	want := []string{"create gen1 old format", "start ctr-1", "ready ctr-1", "observe ctr-1", "remove ctr-1", "hold [3300 6789]", "create gen2 new require", "release", "start ctr-2", "ready ctr-2", "observe ctr-2"}
	if !slices.Equal(g.events, want) {
		t.Fatalf("events %v, want %v", g.events, want)
	}
	if g.launches[0].volume != g.launches[1].volume || g.launches[1].volume != volume || r.name != volume {
		t.Fatalf("the generations did not mount one volume: %+v", g.launches)
	}
	if d.creates != creates || d.removals != 0 {
		t.Fatalf("the replacement created %d or removed %d volumes", d.creates-creates, d.removals)
	}
	if r.phase != monStoreBound || r.generation != 2 || r.attachment == nil || r.attachment.id != "ctr-2" {
		t.Fatalf("state after the replacement: phase %s generation %d attachment %+v", r.phase, r.generation, r.attachment)
	}
	// And again, with the image going back: nothing depends on the direction.
	if _, err := r.replaceGeneration(t.Context(), g, "old"); err != nil || r.generation != 3 || g.launches[2].mode != monStoreRequire {
		t.Fatalf("third generation: %v %+v", err, g.launches)
	}
}

func TestMonStoreReplaceDoesNotStartWhileTheOldContainerMayStillUseTheVolume(t *testing.T) {
	cases := map[string]func(d *monStoreDockerFake, g *monStoreGenerationsFake){
		"remove fails": func(_ *monStoreDockerFake, g *monStoreGenerationsFake) {
			g.removeErr = errors.New("daemon unreachable")
		},
		"remove reports success":   func(_ *monStoreDockerFake, g *monStoreGenerationsFake) { g.removeLies = true },
		"another container mounts": func(d *monStoreDockerFake, _ *monStoreGenerationsFake) { d.users = []string{"stray"} },
		"list fails":               func(d *monStoreDockerFake, _ *monStoreGenerationsFake) { d.listErr = errors.New("daemon unreachable") },
	}
	for name, apply := range cases {
		t.Run(name, func(t *testing.T) {
			r, d, g := monStoreRunningFixture(t)
			apply(d, g)
			if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil {
				t.Fatal("the replacement went on")
			}
			if len(g.launches) != 1 || r.generation != 1 || r.attachment == nil {
				t.Fatalf("a second container was dispatched: launches %d generation %d attachment %+v", len(g.launches), r.generation, r.attachment)
			}
		})
	}
	// Control: with none of those the same call succeeds.
	r, _, g := monStoreRunningFixture(t)
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err != nil {
		t.Fatal(err)
	}
}

func TestMonStoreReplaceChecksTheNextGenerationBeforeItTouchesTheOldOne(t *testing.T) {
	cases := map[string]func(r *monStoreVolume, d *monStoreDockerFake){
		"volume missing":        func(_ *monStoreVolume, d *monStoreDockerFake) { d.inspectErr = errdefs.ErrNotFound },
		"volume foreign":        func(_ *monStoreVolume, d *monStoreDockerFake) { d.stored.Labels[monStoreOwnerLabel] = "foreign" },
		"volume changed":        func(_ *monStoreVolume, d *monStoreDockerFake) { d.stored.CreatedAt = "replaced-volume" },
		"identity not verified": func(r *monStoreVolume, _ *monStoreDockerFake) { r.identity = &monStoreIdentity{} },
	}
	for name, apply := range cases {
		t.Run(name, func(t *testing.T) {
			r, d, g := monStoreRunningFixture(t)
			apply(r, d)
			if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil {
				t.Fatal("the replacement went on")
			}
			if g.removeCalls != 0 || len(g.launches) != 1 || r.attachment == nil || d.creates != 1 {
				t.Fatalf("the old generation was touched: removes %d launches %d creates %d", g.removeCalls, len(g.launches), d.creates)
			}
		})
	}
}

func TestMonStoreReplaceIsRefusedWithoutAReadyGeneration(t *testing.T) {
	r, _, g := monStoreGenerationFixture(t) // created, no generation yet
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.events) != 0 {
		t.Fatalf("a store without a generation was replaced: %v %v", err, g.events)
	}
	r, _, g = monStoreGenerationFixture(t)
	g.readyErr = errors.New("never ready")
	if _, err := r.startGeneration(t.Context(), g, "old"); err == nil || r.phase != monStoreFresh {
		t.Fatalf("a first generation that never got ready: %v, phase %s", err, r.phase)
	}
	g.events = nil
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.events) != 0 {
		t.Fatalf("a fresh store was replaced: %v %v", err, g.events)
	}
}

func TestMonStoreFailedNewGenerationKeepsTheVolumeAndTheRecordAndNeverFormats(t *testing.T) {
	r, d, g := monStoreRunningFixture(t)
	g.readyErr = errors.New("new image never got ready")
	id, err := r.replaceGeneration(t.Context(), g, "new")
	if err == nil || id != "ctr-2" {
		t.Fatalf("%q %v", id, err)
	}
	if r.phase != monStoreBound || r.attachment == nil || r.attachment.id != "ctr-2" || d.removals != 0 {
		t.Fatalf("state after the failed start: phase %s attachment %+v removals %d", r.phase, r.attachment, d.removals)
	}
	// The failed container is removed and the next generation, from the old
	// image again, requires the same store.
	g.readyErr = nil
	if err := r.stopGeneration(t.Context(), g); err != nil || r.attachment != nil {
		t.Fatalf("stop the failed generation: %v", err)
	}
	if _, err := r.startGeneration(t.Context(), g, "old"); err != nil {
		t.Fatal(err)
	}
	for i, l := range g.launches[1:] {
		if l.mode != monStoreRequire || l.volume != r.name {
			t.Fatalf("launch %d: %+v", i+1, l)
		}
	}
}

func TestMonStoreUnknownCreateOutcomeIsNotReplayedAndAPartialContainerIsRecorded(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	g.createErr = errors.New("reply lost")
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || r.attachment == nil || !r.attachment.pending {
		t.Fatalf("the lost reply: %v, attachment %+v", err, r.attachment)
	}
	creates := len(g.launches)
	g.createErr = nil
	if _, err := r.startGeneration(t.Context(), g, "new"); err == nil || len(g.launches) != creates {
		t.Fatalf("an unknown outcome was replayed: %v", err)
	}
	if err := r.stopGeneration(t.Context(), g); err == nil {
		t.Fatal("a pending attachment was stopped without an ID")
	}
	// A partial container: the ID is recorded, so it can be removed.
	r, _, g = monStoreRunningFixture(t)
	g.createErr, g.partialID = errors.New("start failed"), "ctr-partial"
	if id, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || id != "ctr-partial" || r.attachment == nil || r.attachment.pending || r.attachment.id != "ctr-partial" {
		t.Fatalf("the partial container: %q %v %+v", id, err, r.attachment)
	}
	if err := r.stopGeneration(t.Context(), g); err != nil || r.attachment != nil {
		t.Fatalf("remove the partial container: %v", err)
	}
}

func TestMonStoreGenerationsRefuseABadImageAndANilContainerSide(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	if _, err := r.replaceGeneration(t.Context(), g, ""); err == nil || g.removeCalls != 0 {
		t.Fatalf("an empty image: %v", err)
	}
	var typedNil *monStoreGenerationsFake
	if _, err := r.replaceGeneration(t.Context(), typedNil, "new"); err == nil {
		t.Fatal("a typed nil container side was accepted")
	}
	if _, err := r.replaceGeneration(t.Context(), nil, "new"); err == nil {
		t.Fatal("a nil container side was accepted")
	}
	if err := r.stopGeneration(t.Context(), nil); err == nil {
		t.Fatal("stop accepted a nil container side")
	}
}

// A store bound with an identity (the verified path of the inspector slices)
// keeps its refusal: a generation needs the verification.
func TestMonStoreIdentityBoundStoreStillNeedsItsVerification(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	g := &monStoreGenerationsFake{docker: d}
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || g.removeCalls != 0 {
		t.Fatalf("an unverified identity-bound store was replaced: %v", err)
	}
	if err := r.verify(monStoreIdentityFixture()); err == nil {
		// verify needs no attachment; the live attachment keeps it unverified.
		t.Fatal("verify accepted a store with a live attachment")
	}
	// With the old container gone the store is still bound with its identity,
	// and a dispatch without a verification is refused; with one it is admitted.
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err == nil || r.attachment != nil {
		t.Fatalf("an unverified identity-bound store was dispatched: %v", err)
	}
	if err := r.verify(monStoreIdentityFixture()); err != nil {
		t.Fatal(err)
	}
	if launch, err := r.dispatch(t.Context()); err != nil || launch.mode != monStoreRequire {
		t.Fatalf("a verified store: %+v %v", launch, err)
	}
}

func TestMonStoreReadyNeedsAFreshStoreWithAKnownAttachment(t *testing.T) {
	_, r, _ := newMonStoreFixture()
	if err := r.ready(); err == nil {
		t.Fatal("a planned store was marked ready")
	}
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.ready(); err == nil {
		t.Fatal("a created store was marked ready")
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.ready(); err == nil || r.phase != monStoreFresh {
		t.Fatalf("a store with a pending attachment was marked ready: %v", err)
	}
	if err := r.recordContainer("ctr-1"); err != nil {
		t.Fatal(err)
	}
	if err := r.ready(); err != nil || r.phase != monStoreBound || r.identity != nil {
		t.Fatalf("ready: %v phase %s", err, r.phase)
	}
}

// ---- address handoff ----

func TestMonStoreFirstGenerationRecordsItsAddressAndLaterOnesAreCreatedForIt(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	if r.endpoint == nil || !r.endpoint.equal(monStoreFakeAddress) || g.launches[0].endpoint != nil {
		t.Fatalf("recorded %+v, first launch %+v", r.endpoint, g.launches[0].endpoint)
	}
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err != nil {
		t.Fatal(err)
	}
	if l := g.launches[1]; l.endpoint == nil || !l.endpoint.equal(monStoreFakeAddress) || l.endpoint == r.endpoint {
		t.Fatalf("the replacement was not created for a copy of the recorded address: %+v", l.endpoint)
	}
}

func TestMonStoreHoldKeepsAThirdPartyOutWhileTheReplacementIsCreated(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	stolen := map[string]bool{}
	g.onEvent = func(event string) {
		if strings.HasPrefix(event, "create gen2") {
			stolen[event] = g.steal(3300)
		}
	}
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err != nil {
		t.Fatal(err)
	}
	for event, ok := range stolen {
		if ok {
			t.Fatalf("a port was taken during %q", event)
		}
	}
	if len(stolen) != 1 || len(g.held) != 0 || r.hold != nil {
		t.Fatalf("attempts %v, held %v, hold %v", stolen, g.held, r.hold)
	}
}

func TestMonStoreAPortTakenBeforeTheHoldFailsClosedWithoutADispatch(t *testing.T) {
	r, d, g := monStoreRunningFixture(t)
	g.onEvent = func(event string) {
		if event == "remove ctr-1" {
			g.steal(6789)
		}
	}
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil {
		t.Fatal("the replacement went on without its address")
	}
	if len(g.launches) != 1 || r.generation != 1 || r.attachment != nil || r.hold != nil || d.creates != 1 || d.removals != 0 || r.phase != monStoreBound {
		t.Fatalf("launches %d generation %d attachment %+v hold %v creates %d removals %d phase %s", len(g.launches), r.generation, r.attachment, r.hold, d.creates, d.removals, r.phase)
	}
}

func TestMonStoreAPortTakenInTheWindowBeforeStartFailsClosed(t *testing.T) {
	r, d, g := monStoreRunningFixture(t)
	g.onEvent = func(event string) {
		if event == "release" {
			g.steal(3300)
		}
	}
	id, err := r.replaceGeneration(t.Context(), g, "new")
	if err == nil || id != "ctr-2" {
		t.Fatalf("%q %v", id, err)
	}
	if r.attachment == nil || r.attachment.id != "ctr-2" || r.attachment.pending || r.phase != monStoreBound || d.removals != 0 || r.hold != nil || slices.Contains(g.events, "ready ctr-2") {
		t.Fatalf("state after the failed start: attachment %+v phase %s removals %d", r.attachment, r.phase, d.removals)
	}
	if want := monStoreRequire; g.launches[1].mode != want {
		t.Fatalf("the replacement formatted: %+v", g.launches[1])
	}
}

func TestMonStoreReplacementThatBoundAnotherAddressIsReportedAndNotRecorded(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	g.onEvent = func(event string) {
		if strings.HasPrefix(event, "create gen2") {
			other := g.bound["ctr-2"].clone()
			other.ports = []int{3301, 6789}
			g.bound["ctr-2"] = other
		}
	}
	if id, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || id != "ctr-2" {
		t.Fatalf("%q %v", id, err)
	}
	if !r.endpoint.equal(monStoreFakeAddress) {
		t.Fatalf("the recorded address was replaced: %+v", r.endpoint)
	}
}

func TestMonStoreHoldReleaseFailureStopsBeforeTheStartAndIsRetried(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	g.releaseErr = errors.New("allocator removal timed out")
	id, err := r.replaceGeneration(t.Context(), g, "new")
	if err == nil || id != "ctr-2" || slices.Contains(g.events, "start ctr-2") || r.hold == nil || r.attachment == nil || r.attachment.id != "ctr-2" {
		t.Fatalf("%q %v hold %v attachment %+v", id, err, r.hold, r.attachment)
	}
	// The next generation first releases the leftover hold, so there is never
	// more than one; that needs the created container gone first.
	g.releaseErr = nil
	if err := r.stopGeneration(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	before := g.holdCalls
	if _, err := r.startGeneration(t.Context(), g, "new"); err != nil || r.hold != nil || len(g.held) != 0 || g.holdCalls != before+1 {
		t.Fatalf("%v hold %v held %v", err, r.hold, g.held)
	}
	if err := r.releaseAddressHold(t.Context()); err != nil {
		t.Fatalf("releasing with no hold: %v", err)
	}
}

func TestMonStoreFailedCreationReleasesTheHold(t *testing.T) {
	for name, apply := range map[string]func(g *monStoreGenerationsFake){
		"create fails":   func(g *monStoreGenerationsFake) { g.createErr = errors.New("daemon refused") },
		"partial create": func(g *monStoreGenerationsFake) { g.createErr, g.partialID = errors.New("half"), "ctr-partial" },
	} {
		t.Run(name, func(t *testing.T) {
			r, _, g := monStoreRunningFixture(t)
			apply(g)
			if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || r.hold != nil || len(g.held) != 0 || slices.Contains(g.events, "start ctr-partial") {
				t.Fatalf("%v hold %v held %v", err, r.hold, g.held)
			}
		})
	}
	// A dispatch refusal after the hold releases it as well.
	_, r, d := monStoreBoundFixture(t)
	g := &monStoreGenerationsFake{docker: d}
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	r.endpoint = &monStoreEndpoint{ip: "192.0.2.10", ports: []int{3300}}
	if _, err := r.startGeneration(t.Context(), g, "new"); err == nil || r.hold != nil || len(g.held) != 0 || g.holdCalls != 1 {
		t.Fatalf("%v hold %v held %v holds %d", err, r.hold, g.held, g.holdCalls)
	}
}

func TestMonStorePartialHoldReturnedWithAnErrorStaysOwnedAndIsReleased(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	g.holdErr, g.partialHold = errors.New("allocator half started"), true
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.launches) != 1 {
		t.Fatalf("a partial hold went on: %v", err)
	}
	if r.hold != nil || len(g.held) != 0 || !slices.Contains(g.events, "release") {
		t.Fatalf("the partial hold was dropped: hold %v held %v events %v", r.hold, g.held, g.events)
	}
}

func TestMonStorePartialHoldWhoseReleaseFailsIsRetainedForRetry(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	g.holdErr, g.partialHold, g.releaseErr = errors.New("allocator half started"), true, errors.New("removal timed out")
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.launches) != 1 {
		t.Fatalf("%v", err)
	}
	if r.hold == nil || len(g.held) == 0 {
		t.Fatalf("a hold whose release failed was forgotten: hold %v held %v", r.hold, g.held)
	}
	// The next attempt releases the leftover first, so the ports are free again
	// before a new hold is taken.
	g.holdErr, g.partialHold, g.releaseErr = nil, false, nil
	if _, err := r.startGeneration(t.Context(), g, "new"); err != nil || r.hold != nil || len(g.held) != 0 {
		t.Fatalf("%v hold %v held %v", err, r.hold, g.held)
	}
}

func TestMonStoreReplaceNeedsARecordedAddressBeforeTouchingTheOldGeneration(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	r.endpoint = nil
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || g.removeCalls != 0 || g.holdCalls != 0 {
		t.Fatalf("%v removes %d", err, g.removeCalls)
	}
	// A first generation whose address cannot be observed is ready but cannot
	// be replaced.
	r, _, g = monStoreGenerationFixture(t)
	g.observeErr = errors.New("inspect failed")
	if _, err := r.startGeneration(t.Context(), g, "old"); err == nil || r.phase != monStoreBound || r.endpoint != nil {
		t.Fatalf("%v phase %s endpoint %+v", err, r.phase, r.endpoint)
	}
	g.observeErr = nil
	g.events = nil
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.events) != 0 {
		t.Fatalf("%v %v", err, g.events)
	}
	// And a later generation is refused without an address before any hold.
	if err := r.stopGeneration(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	if _, err := r.startGeneration(t.Context(), g, "new"); err == nil || g.holdCalls != 0 {
		t.Fatalf("%v holds %d", err, g.holdCalls)
	}
}

func TestMonStoreLiveAttachmentIsRefusedBeforeAnythingIsHeld(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	if _, err := r.startGeneration(t.Context(), g, "new"); err == nil || g.holdCalls != 0 {
		t.Fatalf("%v holds %d", err, g.holdCalls)
	}
}

func TestMonStoreEndpointValidation(t *testing.T) {
	good := []monStoreEndpoint{{ip: "192.0.2.10", ports: []int{3300, 6789}}, {ip: "2001:db8::1", ports: []int{3300}}, {ip: "172.20.0.5", network: "public"}}
	for _, e := range good {
		if err := e.validate(); err != nil {
			t.Errorf("%+v: %v", e, err)
		}
	}
	bad := map[string]monStoreEndpoint{
		"empty":       {},
		"no ip":       {ports: []int{3300}},
		"unspecified": {ip: "0.0.0.0", ports: []int{3300}},
		"not an ip":   {ip: "mon-a", ports: []int{3300}},
		"neither":     {ip: "192.0.2.10"},
		"both":        {ip: "192.0.2.10", network: "public", ports: []int{3300}},
		"port zero":   {ip: "192.0.2.10", ports: []int{0}},
		"port big":    {ip: "192.0.2.10", ports: []int{65536}},
		"repeated":    {ip: "192.0.2.10", ports: []int{3300, 3300}},
	}
	for name, e := range bad {
		if e.validate() == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	a := monStoreEndpoint{ip: "192.0.2.10", ports: []int{3300, 6789}}
	b := a.clone()
	b.ports[0] = 1
	if a.ports[0] != 3300 || a.equal(b) || a.equal(monStoreEndpoint{ip: "192.0.2.10", ports: []int{6789, 3300}}) {
		t.Fatal("clone or equal ignore the ports")
	}
}

func TestMonStoreHoldRefusesATypedNilAndAFailingHold(t *testing.T) {
	r, _, g := monStoreRunningFixture(t)
	g.holdErr = errors.New("no allocator")
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || len(g.launches) != 1 {
		t.Fatalf("a failing hold: %v", err)
	}
	g.holdErr, g.nilHold = nil, true
	if _, err := r.replaceGeneration(t.Context(), g, "new"); err == nil || r.hold != nil {
		t.Fatalf("a typed nil hold was accepted: %v", err)
	}
}

// ---- guards ----

var monStoreGenerationFile = "mon_store_generation.go"

// monStoreGenerationFiles are the two files of the replacement path: the
// generations and the address they keep.
var monStoreGenerationFiles = []string{monStoreGenerationFile, "mon_store_address.go"}

// monStoreGenerationSymbols are the names the file adds. Production code
// outside it must not mention them until a slice adds a caller and updates this
// table in its own diff.
var monStoreGenerationSymbols = []string{"monStoreGenerations", "checkMonStoreGenerations", "startGeneration", "stopGeneration", "replaceGeneration",
	"observeAddress", "holdAddress", "releaseAddressHold"}

func monStoreGenerationProblems(files map[string]*ast.File) (problems []string) {
	for file, parsed := range files {
		if slices.Contains(monStoreGenerationFiles, file) {
			continue
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && slices.Contains(monStoreGenerationSymbols, id.Name) {
				problems = append(problems, file+" mentions "+id.Name)
			}
			return true
		})
	}
	return problems
}

func TestMonStoreGenerationsHaveNoProductionCaller(t *testing.T) {
	files := parseProductionFiles(t)
	if files[monStoreGenerationFile] == nil {
		t.Fatalf("%s not found: the guard is vacuous", monStoreGenerationFile)
	}
	if problems := monStoreGenerationProblems(files); len(problems) != 0 {
		t.Fatalf("the generation path is used in production: %v", problems)
	}
	declared := map[string]bool{}
	for _, name := range monStoreGenerationFiles {
		if files[name] == nil {
			t.Fatalf("%s not found: the guard is vacuous", name)
		}
		ast.Inspect(files[name], func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.FuncDecl:
				declared[d.Name.Name] = true
			case *ast.TypeSpec:
				declared[d.Name.Name] = true
			}
			return true
		})
	}
	for _, symbol := range monStoreGenerationSymbols {
		if !declared[symbol] {
			t.Errorf("%s is in the table but the file does not declare it", symbol)
		}
	}
}

func TestMonStoreGenerationsGuardDetectsACaller(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", "package ceph\nfunc use(r *monStoreVolume) { _, _ = r.replaceGeneration(nil, nil, \"\") }\n", 0)
	if err != nil {
		t.Fatal(err)
	}
	if problems := monStoreGenerationProblems(map[string]*ast.File{"synthetic.go": parsed}); len(problems) != 1 {
		t.Fatalf("problems %v", problems)
	}
}

// TestMonStoreGenerationsAddNoLockAndKeepTheirOrder pins that the files import
// no sync package, that the container side and the hold are called only from
// their pinned methods, that replaceGeneration checks, then stops, then starts,
// and that startGeneration holds, dispatches, creates, releases the hold, starts,
// waits and observes, in that order.
func TestMonStoreGenerationsAddNoLockAndKeepTheirOrder(t *testing.T) {
	files := parseProductionFiles(t)
	portCalls := map[string]string{"Create": "startGeneration", "Start": "startGeneration", "WaitReady": "startGeneration",
		"Remove": "stopGeneration", "Observe": "observeAddress", "Hold": "holdAddress"}
	replace := map[string]token.Pos{}
	start := map[string][]token.Pos{}
	for _, file := range monStoreGenerationFiles {
		parsed := files[file]
		for _, imp := range parsed.Imports {
			if strings.HasPrefix(imp.Path.Value, `"sync`) {
				t.Errorf("%s imports %s", file, imp.Path.Value)
			}
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if want, isPort := portCalls[sel.Sel.Name]; isPort {
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == "g" && fn.Name.Name != want {
						t.Errorf("%s calls the container side's %s", fn.Name.Name, sel.Sel.Name)
					}
				}
				if sel.Sel.Name == "Release" && fn.Name.Name != "releaseAddressHold" && fn.Name.Name != "Release" {
					t.Errorf("%s releases a hold outside releaseAddressHold", fn.Name.Name)
				}
				switch fn.Name.Name {
				case "replaceGeneration":
					switch sel.Sel.Name {
					case "inspect", "stopGeneration", "startGeneration":
						replace[sel.Sel.Name] = call.Pos()
					}
				case "startGeneration":
					start[sel.Sel.Name] = append(start[sel.Sel.Name], call.Pos())
				}
				return true
			})
		}
	}
	if !(replace["inspect"].IsValid() && replace["inspect"] < replace["stopGeneration"] && replace["stopGeneration"] < replace["startGeneration"]) {
		t.Fatalf("replaceGeneration must check, stop, then start: %v", replace)
	}
	first := func(name string) token.Pos {
		if len(start[name]) == 0 {
			t.Fatalf("startGeneration does not call %s", name)
		}
		return start[name][0]
	}
	releaseAfterCreate := token.NoPos
	for _, pos := range start["releaseAddressHold"] {
		if pos > first("Create") && (releaseAfterCreate == token.NoPos || pos < releaseAfterCreate) {
			releaseAfterCreate = pos
		}
	}
	order := []token.Pos{first("holdAddress"), first("dispatch"), first("Create"), releaseAfterCreate, first("Start"), first("WaitReady"), first("observeAddress")}
	for i := 1; i < len(order); i++ {
		if !order[i-1].IsValid() || order[i-1] >= order[i] {
			t.Fatalf("startGeneration must hold, dispatch, create, release, start, wait, observe: %v", order)
		}
	}
}
