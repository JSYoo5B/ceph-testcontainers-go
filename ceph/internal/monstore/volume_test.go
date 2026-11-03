package monstore

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
)

// monStoreDockerFake is a small Docker model for the MON store records: the
// volume calls of volumeDockerFake, plus the set of existing containers and
// the containers that use the volume. Its answers are synthetic; it says nothing
// about Docker's behavior.
type monStoreDockerFake struct {
	volumeDockerFake
	existing         map[string]bool
	users            []string
	inspectContainer error
	listErr          error
	onCreate         func()
	listed           int
}

func (d *monStoreDockerFake) VolumeCreate(ctx context.Context, opts mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error) {
	if d.onCreate != nil {
		d.onCreate()
	}
	return d.volumeDockerFake.VolumeCreate(ctx, opts)
}

func (d *monStoreDockerFake) ContainerInspect(_ context.Context, id string, _ mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if d.inspectContainer != nil {
		return mobycl.ContainerInspectResult{}, d.inspectContainer
	}
	if d.existing[id] {
		return mobycl.ContainerInspectResult{}, nil
	}
	return mobycl.ContainerInspectResult{}, errdefs.ErrNotFound
}

func (d *monStoreDockerFake) ContainerList(_ context.Context, _ mobycl.ContainerListOptions) (mobycl.ContainerListResult, error) {
	d.listed++
	if d.listErr != nil {
		return mobycl.ContainerListResult{}, d.listErr
	}
	var items []container.Summary
	for _, id := range d.users {
		items = append(items, container.Summary{ID: id})
	}
	return mobycl.ContainerListResult{Items: items}, nil
}

func newMonStoreFixture() (*State, *Volume, *monStoreDockerFake) {
	return newMonStoreFixtureFor("a")
}

func newMonStoreFixtureFor(mon string) (*State, *Volume, *monStoreDockerFake) {
	d := &monStoreDockerFake{existing: map[string]bool{}}
	s := &State{monDocker: func(context.Context) (monStoreDocker, error) { return d, nil }}
	r, err := s.Begin(mon)
	if err != nil {
		panic(err)
	}
	return s, r, d
}

// monStoreBoundFixture drives a record through create, the first dispatch, the
// container record and ready. The first generation container stays "existing".
func monStoreBoundFixture(t *testing.T) (*State, *Volume, *monStoreDockerFake) {
	t.Helper()
	s, r, d := newMonStoreFixture()
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	if err := r.recordContainer("ctr-1"); err != nil {
		t.Fatal(err)
	}
	if err := r.ready(); err != nil {
		t.Fatal(err)
	}
	return s, r, d
}

func TestMonStorePhaseNamesAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for p := Planned; p <= Released; p++ {
		if p.String() == "unset" || seen[p.String()] {
			t.Fatalf("phase %d is named %q", p, p)
		}
		seen[p.String()] = true
	}
	if Phase(0).String() != "unset" {
		t.Fatal("the zero phase must read as unset")
	}
}

func TestMonStoreVolumeMetadataIsValidatedAgainstTheRecord(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	good := d.stored
	if err := r.validate(good); err != nil {
		t.Fatalf("the created volume was refused: %v", err)
	}
	cases := map[string]func(v *volume.Volume){
		"name":           func(v *volume.Volume) { v.Name = "other" },
		"driver":         func(v *volume.Volume) { v.Driver = "nfs" },
		"owner label":    func(v *volume.Volume) { v.Labels[monStoreOwnerLabel] = "foreign" },
		"mon label":      func(v *volume.Volume) { v.Labels[monStoreMonLabel] = "b" },
		"no mon label":   func(v *volume.Volume) { delete(v.Labels, monStoreMonLabel) },
		"driver options": func(v *volume.Volume) { v.Options = map[string]string{"type": "tmpfs"} },
		"empty creation": func(v *volume.Volume) { v.CreatedAt = "" },
		"changed creation": func(v *volume.Volume) {
			v.CreatedAt = "another-creation-timestamp"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			v := good
			v.Labels = maps.Clone(good.Labels)
			mutate(&v)
			if err := r.validate(v); err == nil {
				t.Fatal("a volume that differs was accepted")
			}
		})
	}
}

func TestMonStoreCreateIsPublishedBeforeTheCallAndIsOrdinaryLocal(t *testing.T) {
	s, r, d := newMonStoreFixture()
	published := false
	d.onCreate = func() { published = s.Store("a") == r && r.createRequested }
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("the record or the create request was not published before the Docker call")
	}
	if d.createOptions.Driver != "local" || len(d.createOptions.DriverOpts) != 0 ||
		d.createOptions.Labels[monStoreOwnerLabel] != r.owner || d.createOptions.Labels[monStoreMonLabel] != "a" ||
		d.createOptions.Name != r.name || !strings.HasPrefix(r.name, "tc-ceph-mon-volume-") {
		t.Fatalf("not an owned ordinary local volume: %+v", d.createOptions)
	}
	if r.Phase != Created || r.createdAt == "" || d.closes != 1 {
		t.Fatalf("phase %s, created %q, closes %d", r.Phase, r.createdAt, d.closes)
	}
	if err := r.Create(t.Context()); err == nil || d.creates != 1 {
		t.Fatalf("a created volume was created again: %v", err)
	}
}

func TestMonStoreUnknownCreateOutcomeIsNotRetriedAndIsSettledByRelease(t *testing.T) {
	_, r, d := newMonStoreFixture()
	lost := errors.New("synthetic lost create reply")
	d.createErr = lost
	if err := r.Create(t.Context()); !errors.Is(err, lost) || r.Phase != Planned || !r.createRequested {
		t.Fatalf("lost reply: %v, phase %s", err, r.Phase)
	}
	if err := r.Create(t.Context()); err == nil || d.creates != 1 {
		t.Fatalf("an unknown create outcome was replayed: %v, creates %d", err, d.creates)
	}
	if _, err := r.dispatch(t.Context()); err == nil || r.Attachment != nil {
		t.Fatalf("a planned store was mounted: %v", err)
	}
	// Docker did commit the volume: the release finds it, checks it and removes
	// it without force.
	if err := r.release(t.Context(), false); err != nil || r.Phase != Released || d.removals != 1 || d.removeOptions.Force {
		t.Fatalf("release: %v, phase %s, removals %d, forced %v", err, r.Phase, d.removals, d.removeOptions.Force)
	}
}

func TestMonStoreReleaseOfAnUnrequestedOrMissingVolumeSettles(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if err := r.release(t.Context(), false); err != nil || r.Phase != Released || d.inspections != 0 || d.removals != 0 {
		t.Fatalf("a record that never requested a volume called Docker: %v", err)
	}
	_, r, d = newMonStoreFixture()
	r.createRequested = true
	d.inspectErr = errdefs.ErrNotFound
	if err := r.release(t.Context(), false); err != nil || r.Phase != Released || d.removals != 0 {
		t.Fatalf("a confirmed missing volume did not settle: %v", err)
	}
}

func TestMonStoreFirstDispatchIsFreshAndFormats(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if _, err := r.dispatch(t.Context()); err == nil {
		t.Fatal("a planned store was dispatched")
	}
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	launch, err := r.dispatch(t.Context())
	if err != nil || launch.Mode != Format || launch.Generation != 1 || launch.Volume != r.name || r.Phase != Fresh {
		t.Fatalf("first dispatch: %+v, %v, phase %s", launch, err, r.Phase)
	}
	if r.Attachment == nil || !r.Attachment.pending || r.Attachment.generation != 1 {
		t.Fatalf("the attachment was not published before the container: %+v", r.Attachment)
	}
	// At most one attachment: neither the pending one nor a recorded one admits
	// another dispatch.
	if _, err := r.dispatch(t.Context()); err == nil {
		t.Fatal("a second generation was dispatched over a pending attachment")
	}
	if err := r.recordContainer("ctr-1"); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	if _, err := r.dispatch(t.Context()); err == nil || r.generation != 1 {
		t.Fatalf("a second generation was dispatched over a fresh store: %v", err)
	}
}

func TestMonStoreUnknownContainerOutcomeStaysPendingUntilTheScan(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.recordContainer(""); err == nil || r.Attachment == nil || !r.Attachment.pending {
		t.Fatalf("an empty container ID settled the outcome: %v", err)
	}
	if err := r.ready(); err == nil || r.Phase != Fresh {
		t.Fatalf("a store with an unknown attachment was marked ready: %v", err)
	}
	// Before the labeled-container scan nothing settles it, not even an empty list.
	listed := d.listed
	if err := r.confirmGone(t.Context(), false); err == nil || r.Attachment == nil || d.listed != listed {
		t.Fatalf("the unknown outcome was settled before the scan: %v", err)
	}
	if err := r.discardFresh(t.Context()); err == nil || d.removals != 0 || r.Phase != Fresh {
		t.Fatalf("a fresh store with an unknown attachment was discarded: %v", err)
	}
	if err := r.release(t.Context(), false); err == nil || d.removals != 0 {
		t.Fatalf("a store with an unknown attachment was released before the scan: %v", err)
	}
	// After the scan a user that still shows up keeps it pending; an empty list settles it.
	d.users = []string{"survivor"}
	if err := r.confirmGone(t.Context(), true); err == nil || r.Attachment == nil {
		t.Fatalf("a volume user did not keep the attachment: %v", err)
	}
	d.users = nil
	if err := r.release(t.Context(), true); err != nil || r.Phase != Released || d.removals != 1 {
		t.Fatalf("release after the scan: %v", err)
	}
}

func TestMonStoreConfirmGoneRequiresAbsenceByInspectAndByIndependentList(t *testing.T) {
	cases := map[string]func(d *monStoreDockerFake){
		"container still exists":        func(d *monStoreDockerFake) { d.existing["ctr-1"] = true },
		"container inspect fails":       func(d *monStoreDockerFake) { d.inspectContainer = errors.New("daemon unreachable") },
		"another container uses volume": func(d *monStoreDockerFake) { d.users = []string{"stray"} },
		"list fails":                    func(d *monStoreDockerFake) { d.listErr = errors.New("daemon unreachable") },
	}
	for name, apply := range cases {
		t.Run(name, func(t *testing.T) {
			_, r, d := monStoreBoundFixture(t)
			delete(d.existing, "ctr-1")
			apply(d)
			if err := r.confirmGone(t.Context(), false); err == nil || r.Attachment == nil {
				t.Fatalf("the attachment was settled: %v", err)
			}
		})
	}
	// Control: with neither signal the attachment is gone, and a store without an
	// attachment calls nothing.
	_, r, d := monStoreBoundFixture(t)
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil || r.Attachment != nil {
		t.Fatalf("a gone container was not settled: %v", err)
	}
	listed := d.listed
	if err := r.confirmGone(t.Context(), false); err != nil || d.listed != listed {
		t.Fatalf("a store without an attachment called Docker: %v", err)
	}
}

// A bound store whose last generation is gone reopens on the same volume in
// require-store mode, without creating a volume, once per gone attachment.
func TestMonStoreReopenRequiresTheStoreAndAGoneAttachment(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	creates := d.creates
	if _, err := r.dispatch(t.Context()); err == nil || r.generation != 1 {
		t.Fatalf("a store with a live attachment was dispatched: %v", err)
	}
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	launch, err := r.dispatch(t.Context())
	if err != nil || launch.Mode != Require || launch.Generation != 2 || launch.Volume != r.name || r.Phase != Bound {
		t.Fatalf("reopen dispatch: %+v, %v, phase %s", launch, err, r.Phase)
	}
	if d.creates != creates {
		t.Fatal("reopen created a volume")
	}
	if _, err := r.dispatch(t.Context()); err == nil || r.generation != 2 {
		t.Fatal("one gone attachment admitted two dispatches")
	}
}

func TestMonStoreBoundStoreWithAMissingChangedOrForeignVolumeIsNeverRecreated(t *testing.T) {
	cases := map[string]func(d *monStoreDockerFake){
		"missing":      func(d *monStoreDockerFake) { d.inspectErr = errdefs.ErrNotFound },
		"foreign":      func(d *monStoreDockerFake) { d.stored.Labels[monStoreOwnerLabel] = "foreign" },
		"changed time": func(d *monStoreDockerFake) { d.stored.CreatedAt = "replaced-volume" },
		"inspect fail": func(d *monStoreDockerFake) { d.inspectErr = errors.New("daemon unreachable") },
	}
	for name, apply := range cases {
		t.Run(name, func(t *testing.T) {
			_, r, d := monStoreBoundFixture(t)
			delete(d.existing, "ctr-1")
			if err := r.confirmGone(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			apply(d)
			creates, generation := d.creates, r.generation
			if _, err := r.dispatch(t.Context()); err == nil || d.creates != creates || r.generation != generation || r.Attachment != nil || r.Phase != Bound {
				t.Fatalf("a bound store was dispatched or recreated: %v", err)
			}
			if name != "inspect fail" && name != "missing" {
				if err := r.release(t.Context(), false); err == nil || d.removals != 0 {
					t.Fatalf("a changed or foreign volume was removed: %v", err)
				}
			}
		})
	}
}

func TestMonStoreOnlyAFreshStoreWithAGoneAttachmentIsDiscardedBeforeTermination(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if err := r.discardFresh(t.Context()); err == nil {
		t.Fatal("a planned store was discarded as fresh")
	}
	if err := r.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	_ = r.recordContainer("ctr-1")
	if err := r.discardFresh(t.Context()); err == nil || d.removals != 0 {
		t.Fatalf("a fresh store with a live container was discarded: %v", err)
	}
	delete(d.existing, "ctr-1")
	if err := r.discardFresh(t.Context()); err != nil || r.Phase != Released || d.removals != 1 || d.removeOptions.Force {
		t.Fatalf("discard: %v, phase %s", err, r.Phase)
	}
	// A bound store is not discarded by this path.
	_, r, d = monStoreBoundFixture(t)
	delete(d.existing, "ctr-1")
	if err := r.discardFresh(t.Context()); err == nil || d.removals != 0 {
		t.Fatalf("a bound store was discarded as fresh: %v", err)
	}
}

func TestMonStoreReleaseIsNeverForcedAndKeepsAnUnsettledRecord(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	// A live attachment refuses the release before any volume call.
	if err := r.release(t.Context(), false); err == nil || d.removals != 0 {
		t.Fatalf("a store with a live container was released: %v", err)
	}
	delete(d.existing, "ctr-1")
	busy := errors.New("volume is in use")
	d.removeErr = busy
	if err := r.release(t.Context(), false); !errors.Is(err, busy) || r.Phase != Bound {
		t.Fatalf("a busy removal settled the record: %v", err)
	}
	d.removeErr = nil
	if err := r.release(t.Context(), true); err != nil || r.Phase != Released || d.removeOptions.Force {
		t.Fatalf("retry: %v, forced %v", err, d.removeOptions.Force)
	}
	removals := d.removals
	if err := r.release(t.Context(), true); err != nil || d.removals != removals {
		t.Fatalf("a released store called Docker again: %v", err)
	}
}

func TestMonStoreReleaseClosesTheClientOnEveryPath(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	delete(d.existing, "ctr-1")
	d.closeErr = errors.New("close failed")
	if err := r.release(t.Context(), false); !errors.Is(err, d.closeErr) {
		t.Fatalf("a client close failure was dropped: %v", err)
	}
	if d.closes == 0 {
		t.Fatal("no client was closed")
	}
}
