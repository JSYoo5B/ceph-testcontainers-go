package ceph

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
// volume calls of osdVolumeDockerFake, plus the set of existing containers and
// the containers that use the volume. Its answers are synthetic; it says nothing
// about Docker's behavior.
type monStoreDockerFake struct {
	osdVolumeDockerFake
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
	return d.osdVolumeDockerFake.VolumeCreate(ctx, opts)
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

func newMonStoreFixture() (*monStoreState, *monStoreVolume, *monStoreDockerFake) {
	d := &monStoreDockerFake{existing: map[string]bool{}}
	s := &monStoreState{monDocker: func(context.Context) (monStoreDocker, error) { return d, nil }}
	r, err := s.begin("a")
	if err != nil {
		panic(err)
	}
	return s, r, d
}

const (
	digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	digestC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	fsidOne = "0a1b2c3d-2222-3333-4444-55555555555a"
	fsidTwo = "66666666-7777-8888-9999-000000000000"
)

func monStoreIdentityFixture() monStoreIdentity {
	return monStoreIdentity{keyringDigest: digestA, shape: monStoreShapeRocksDB, fingerprint: digestB, fsid: fsidOne,
		addresses: []string{"v2:192.0.2.10:3300/0", "v1:192.0.2.10:6789/0"}}
}

// monStoreBoundFixture drives a record through create, the first dispatch, the
// container record and bind. The first generation container stays "existing".
func monStoreBoundFixture(t *testing.T) (*monStoreState, *monStoreVolume, *monStoreDockerFake) {
	t.Helper()
	s, r, d := newMonStoreFixture()
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	if err := r.recordContainer("ctr-1"); err != nil {
		t.Fatal(err)
	}
	if err := r.bind(monStoreIdentityFixture()); err != nil {
		t.Fatal(err)
	}
	return s, r, d
}

func TestMonStorePhaseNamesAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for p := monStorePlanned; p <= monStoreReleased; p++ {
		if p.String() == "unset" || seen[p.String()] {
			t.Fatalf("phase %d is named %q", p, p)
		}
		seen[p.String()] = true
	}
	if monStorePhase(0).String() != "unset" {
		t.Fatal("the zero phase must read as unset")
	}
}

func TestMonStoreVolumeMetadataIsValidatedAgainstTheRecord(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if err := r.create(t.Context()); err != nil {
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
	d.onCreate = func() { published = s.store("a") == r && r.createRequested }
	if err := r.create(t.Context()); err != nil {
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
	if r.phase != monStoreCreated || r.createdAt == "" || d.closes != 1 {
		t.Fatalf("phase %s, created %q, closes %d", r.phase, r.createdAt, d.closes)
	}
	if err := r.create(t.Context()); err == nil || d.creates != 1 {
		t.Fatalf("a created volume was created again: %v", err)
	}
}

func TestMonStoreUnknownCreateOutcomeIsNotRetriedAndIsSettledByRelease(t *testing.T) {
	_, r, d := newMonStoreFixture()
	lost := errors.New("synthetic lost create reply")
	d.createErr = lost
	if err := r.create(t.Context()); !errors.Is(err, lost) || r.phase != monStorePlanned || !r.createRequested {
		t.Fatalf("lost reply: %v, phase %s", err, r.phase)
	}
	if err := r.create(t.Context()); err == nil || d.creates != 1 {
		t.Fatalf("an unknown create outcome was replayed: %v, creates %d", err, d.creates)
	}
	if _, err := r.dispatch(t.Context()); err == nil || r.attachment != nil {
		t.Fatalf("a planned store was mounted: %v", err)
	}
	// Docker did commit the volume: the release finds it, checks it and removes
	// it without force.
	if err := r.release(t.Context(), false); err != nil || r.phase != monStoreReleased || d.removals != 1 || d.removeOptions.Force {
		t.Fatalf("release: %v, phase %s, removals %d, forced %v", err, r.phase, d.removals, d.removeOptions.Force)
	}
}

func TestMonStoreReleaseOfAnUnrequestedOrMissingVolumeSettles(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if err := r.release(t.Context(), false); err != nil || r.phase != monStoreReleased || d.inspections != 0 || d.removals != 0 {
		t.Fatalf("a record that never requested a volume called Docker: %v", err)
	}
	_, r, d = newMonStoreFixture()
	r.createRequested = true
	d.inspectErr = errdefs.ErrNotFound
	if err := r.release(t.Context(), false); err != nil || r.phase != monStoreReleased || d.removals != 0 {
		t.Fatalf("a confirmed missing volume did not settle: %v", err)
	}
}

func TestMonStoreFirstDispatchIsFreshAndFormats(t *testing.T) {
	_, r, d := newMonStoreFixture()
	if _, err := r.dispatch(t.Context()); err == nil {
		t.Fatal("a planned store was dispatched")
	}
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	launch, err := r.dispatch(t.Context())
	if err != nil || launch.mode != monStoreFormat || launch.generation != 1 || launch.volume != r.name || r.phase != monStoreFresh {
		t.Fatalf("first dispatch: %+v, %v, phase %s", launch, err, r.phase)
	}
	if r.attachment == nil || !r.attachment.pending || r.attachment.generation != 1 {
		t.Fatalf("the attachment was not published before the container: %+v", r.attachment)
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
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.recordContainer(""); err == nil || r.attachment == nil || !r.attachment.pending {
		t.Fatalf("an empty container ID settled the outcome: %v", err)
	}
	if err := r.bind(monStoreIdentityFixture()); err == nil || r.phase != monStoreFresh {
		t.Fatalf("a store with an unknown attachment was bound: %v", err)
	}
	// Before the labeled-container scan nothing settles it, not even an empty list.
	listed := d.listed
	if err := r.confirmGone(t.Context(), false); err == nil || r.attachment == nil || d.listed != listed {
		t.Fatalf("the unknown outcome was settled before the scan: %v", err)
	}
	if err := r.discardFresh(t.Context()); err == nil || d.removals != 0 || r.phase != monStoreFresh {
		t.Fatalf("a fresh store with an unknown attachment was discarded: %v", err)
	}
	if err := r.release(t.Context(), false); err == nil || d.removals != 0 {
		t.Fatalf("a store with an unknown attachment was released before the scan: %v", err)
	}
	// After the scan a user that still shows up keeps it pending; an empty list settles it.
	d.users = []string{"survivor"}
	if err := r.confirmGone(t.Context(), true); err == nil || r.attachment == nil {
		t.Fatalf("a volume user did not keep the attachment: %v", err)
	}
	d.users = nil
	if err := r.release(t.Context(), true); err != nil || r.phase != monStoreReleased || d.removals != 1 {
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
			if err := r.confirmGone(t.Context(), false); err == nil || r.attachment == nil {
				t.Fatalf("the attachment was settled: %v", err)
			}
		})
	}
	// Control: with neither signal the attachment is gone, and a store without an
	// attachment calls nothing.
	_, r, d := monStoreBoundFixture(t)
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil || r.attachment != nil {
		t.Fatalf("a gone container was not settled: %v", err)
	}
	listed := d.listed
	if err := r.confirmGone(t.Context(), false); err != nil || d.listed != listed {
		t.Fatalf("a store without an attachment called Docker: %v", err)
	}
}

func TestMonStoreBindRejectsIncompleteIdentityInputs(t *testing.T) {
	edits := map[string]func(i *monStoreIdentity){
		"empty keyring digest":     func(i *monStoreIdentity) { i.keyringDigest = "" },
		"short keyring digest":     func(i *monStoreIdentity) { i.keyringDigest = "abcd" },
		"uppercase keyring digest": func(i *monStoreIdentity) { i.keyringDigest = strings.ToUpper(digestA) },
		"non-hex keyring digest":   func(i *monStoreIdentity) { i.keyringDigest = strings.Repeat("z", 64) },
		"empty shape":              func(i *monStoreIdentity) { i.shape = "" },
		"other shape":              func(i *monStoreIdentity) { i.shape = "leveldb" },
		"empty fingerprint":        func(i *monStoreIdentity) { i.fingerprint = "" },
		"non-hex fingerprint":      func(i *monStoreIdentity) { i.fingerprint = strings.Repeat("g", 64) },
		"empty fsid":               func(i *monStoreIdentity) { i.fsid = "" },
		"malformed fsid":           func(i *monStoreIdentity) { i.fsid = "not-a-uuid" },
		"uppercase fsid":           func(i *monStoreIdentity) { i.fsid = strings.ToUpper(fsidOne) },
		"nil address vector":       func(i *monStoreIdentity) { i.addresses = nil },
		"empty address vector":     func(i *monStoreIdentity) { i.addresses = []string{} },
		"empty address":            func(i *monStoreIdentity) { i.addresses = []string{"v2:192.0.2.10:3300/0", ""} },
		"duplicate address":        func(i *monStoreIdentity) { i.addresses = []string{"v2:192.0.2.10:3300/0", "v2:192.0.2.10:3300/0"} },
		"zero value":               func(i *monStoreIdentity) { *i = monStoreIdentity{} },
		"all fields blank strings": func(i *monStoreIdentity) { *i = monStoreIdentity{addresses: []string{""}} },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			_, r, d := newMonStoreFixture()
			if err := r.create(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := r.dispatch(t.Context()); err != nil {
				t.Fatal(err)
			}
			d.existing["ctr-1"] = true
			if err := r.recordContainer("ctr-1"); err != nil {
				t.Fatal(err)
			}
			identity := monStoreIdentityFixture()
			edit(&identity)
			if err := r.bind(identity); err == nil || r.phase != monStoreFresh || r.identity != nil {
				t.Fatalf("an incomplete identity bound the store: %v", err)
			}
		})
	}
	// Control: the complete identity binds.
	_, r, _ := monStoreBoundFixture(t)
	if r.phase != monStoreBound || r.identity == nil {
		t.Fatalf("the complete identity did not bind: %s", r.phase)
	}
}

func TestMonStoreBindWorksOnACopyOfTheAddressVector(t *testing.T) {
	s, r, d := newMonStoreFixture()
	_ = s
	if err := r.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dispatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.existing["ctr-1"] = true
	_ = r.recordContainer("ctr-1")
	identity := monStoreIdentityFixture()
	if err := r.bind(identity); err != nil {
		t.Fatal(err)
	}
	identity.addresses[0] = "v2:198.51.100.1:3300/0"
	if len(r.identity.differences(monStoreIdentityFixture())) != 0 {
		t.Fatal("the record aliases the caller's address slice")
	}
}

func TestMonStoreBindNeedsAFreshStoreWithAKnownAttachment(t *testing.T) {
	_, r, _ := newMonStoreFixture()
	if err := r.bind(monStoreIdentityFixture()); err == nil {
		t.Fatal("a planned store was bound")
	}
	_, r, _ = monStoreBoundFixture(t)
	if err := r.bind(monStoreIdentityFixture()); err == nil {
		t.Fatal("a bound store was bound again")
	}
}

func TestMonStoreErrorsDoNotPrintDigestsOrFingerprints(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	observed := monStoreIdentityFixture()
	observed.keyringDigest, observed.fingerprint = digestC, digestC
	err := r.verify(observed)
	if err == nil {
		t.Fatal("a different store verified")
	}
	for _, secret := range []string{digestA, digestB, digestC} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("the error prints a digest: %v", err)
		}
	}
	if !strings.Contains(err.Error(), "keyring digest") || !strings.Contains(err.Error(), "store fingerprint") {
		t.Fatalf("the error does not name the fields: %v", err)
	}
}

func TestMonStoreDetachRequiresAGoneAttachmentAndTheBoundSecrets(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	if err := r.detach(monStoreIdentityFixture()); err == nil || r.phase != monStoreBound {
		t.Fatalf("a store with a live attachment was detached: %v", err)
	}
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	immutable := map[string]func(i *monStoreIdentity){
		"keyring digest": func(i *monStoreIdentity) { i.keyringDigest = digestC },
		"cluster FSID":   func(i *monStoreIdentity) { i.fsid = fsidTwo },
	}
	for name, edit := range immutable {
		observed := monStoreIdentityFixture()
		edit(&observed)
		if err := r.detach(observed); err == nil || !strings.Contains(err.Error(), name) || r.phase != monStoreBound {
			t.Fatalf("a changed %s was accepted: %v", name, err)
		}
	}
	incomplete := monStoreIdentityFixture()
	incomplete.fingerprint = ""
	if err := r.detach(incomplete); err == nil || r.phase != monStoreBound {
		t.Fatalf("an incomplete observation detached the store: %v", err)
	}
	// The fingerprint legitimately changed while the generation ran.
	observed := monStoreIdentityFixture()
	observed.fingerprint = digestC
	if err := r.detach(observed); err != nil || r.phase != monStoreDetached || r.identity.fingerprint != digestC {
		t.Fatalf("detach: %v, phase %s", err, r.phase)
	}
	if err := r.detach(observed); err == nil {
		t.Fatal("a detached store was detached again")
	}
}

func TestMonStoreReopenNeedsAMatchingVerifiedIdentityAndRequiresTheStore(t *testing.T) {
	_, r, d := monStoreBoundFixture(t)
	delete(d.existing, "ctr-1")
	if err := r.confirmGone(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if err := r.detach(monStoreIdentityFixture()); err != nil {
		t.Fatal(err)
	}
	creates := d.creates
	// No verification: refused, and no volume is created in its place.
	if _, err := r.dispatch(t.Context()); err == nil || r.attachment != nil || d.creates != creates {
		t.Fatalf("an unverified store was mounted: %v", err)
	}
	mismatches := map[string]func(i *monStoreIdentity){
		"keyring digest":    func(i *monStoreIdentity) { i.keyringDigest = digestC },
		"store shape":       func(i *monStoreIdentity) { i.shape = "other" },
		"store fingerprint": func(i *monStoreIdentity) { i.fingerprint = digestC },
		"cluster FSID":      func(i *monStoreIdentity) { i.fsid = fsidTwo },
		"address vector (port)": func(i *monStoreIdentity) {
			i.addresses = []string{"v2:192.0.2.10:3301/0", "v1:192.0.2.10:6789/0"}
		},
		"address vector (family)": func(i *monStoreIdentity) {
			i.addresses = []string{"v2:[2001:db8::10]:3300/0", "v1:192.0.2.10:6789/0"}
		},
		"address vector (missing)": func(i *monStoreIdentity) { i.addresses = i.addresses[:1] },
	}
	for name, edit := range mismatches {
		observed := monStoreIdentityFixture()
		edit(&observed)
		if err := r.verify(observed); err == nil || r.verified {
			t.Fatalf("a different %s verified: %v", name, err)
		}
		if _, err := r.dispatch(t.Context()); err == nil || r.attachment != nil || r.phase != monStoreDetached {
			t.Fatalf("a store with a different %s was mounted: %v", name, err)
		}
	}
	// A missing observation is not a match.
	if err := r.verify(monStoreIdentity{}); err == nil || r.verified {
		t.Fatalf("an empty observation verified: %v", err)
	}
	// The address vector has no order: the same set verifies.
	same := monStoreIdentityFixture()
	same.addresses = []string{same.addresses[1], same.addresses[0]}
	if err := r.verify(same); err != nil {
		t.Fatal(err)
	}
	// A failed verification withdraws an earlier one.
	if err := r.verify(monStoreIdentity{}); err == nil || r.verified {
		t.Fatal("an earlier verification survived a failed one")
	}
	if err := r.verify(monStoreIdentityFixture()); err != nil {
		t.Fatal(err)
	}
	launch, err := r.dispatch(t.Context())
	if err != nil || launch.mode != monStoreRequire || launch.generation != 2 || launch.volume != r.name || r.phase != monStoreBound {
		t.Fatalf("reopen dispatch: %+v, %v, phase %s", launch, err, r.phase)
	}
	if d.creates != creates {
		t.Fatal("reopen created a volume")
	}
	// The verification admits one dispatch only.
	if _, err := r.dispatch(t.Context()); err == nil {
		t.Fatal("one verification admitted two dispatches")
	}
	// A verification does not outlive a live attachment: dispatch checks the
	// attachment itself, not only the verification that preceded it.
	r.verified = true
	if _, err := r.dispatch(t.Context()); err == nil || r.generation != 2 {
		t.Fatalf("a verified store with a live attachment was dispatched: %v", err)
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
			if err := r.verify(monStoreIdentityFixture()); err != nil {
				t.Fatal(err)
			}
			apply(d)
			creates, generation := d.creates, r.generation
			if _, err := r.dispatch(t.Context()); err == nil || d.creates != creates || r.generation != generation || r.attachment != nil || r.phase != monStoreBound {
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
	if err := r.create(t.Context()); err != nil {
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
	if err := r.discardFresh(t.Context()); err != nil || r.phase != monStoreReleased || d.removals != 1 || d.removeOptions.Force {
		t.Fatalf("discard: %v, phase %s", err, r.phase)
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
	if err := r.release(t.Context(), false); !errors.Is(err, busy) || r.phase != monStoreBound {
		t.Fatalf("a busy removal settled the record: %v", err)
	}
	d.removeErr = nil
	if err := r.release(t.Context(), true); err != nil || r.phase != monStoreReleased || d.removeOptions.Force {
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
