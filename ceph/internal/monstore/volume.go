package monstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const (
	monStoreOwnerLabel = "org.testcontainers.ceph.mon-volume-owner"
	monStoreMonLabel   = "org.testcontainers.ceph.mon-volume-mon"
)

// Phase is the state of one MON store volume. The zero value is not a
// phase, so a record that was never published is detectable.
type Phase int

const (
	// Planned: the record is published and the volume create was not
	// requested, or was requested and its outcome is unknown.
	Planned Phase = iota + 1
	// Created: the volume exists and passed the metadata checks.
	Created
	// Fresh: a first generation was requested and mkfs may have run.
	Fresh
	// Bound: the first generation reached its ready check and the
	// identity was recorded.
	Bound
	// Detached: bound, with no attachment, and the identity was
	// re-recorded after the last generation stopped.
	Detached
	// Released: the volume removal is confirmed.
	Released
)

func (p Phase) String() string {
	switch p {
	case Planned:
		return "planned"
	case Created:
		return "created"
	case Fresh:
		return "fresh"
	case Bound:
		return "bound"
	case Detached:
		return "detached"
	case Released:
		return "released"
	}
	return "unset"
}

// Mode is what the MON entrypoint must do with the data directory of a
// generation.
type Mode int

const (
	// Format: the store is new; mkfs may run when store.db is absent.
	Format Mode = iota + 1
	// Require: the store must already exist; the generation refuses to
	// format when store.db is absent.
	Require
)

// Launch is what dispatch hands to the container creation: the volume
// to mount at the MON data directory, the mode, and the generation number.
type Launch struct {
	Volume     string
	Mode       Mode
	Generation int
	// Endpoint is the address the container must bind: the one recorded from
	// the first generation. It is nil for the first generation, whose address
	// the container side chooses and startGeneration records after it is ready.
	Endpoint *Endpoint
}

// Attachment is the one container that may use the volume. pending is
// true until the creation request returned a container ID: a pending attachment
// is an unknown creation outcome, which is never replayed.
type Attachment struct {
	generation int
	ID         string
	pending    bool
}

// monStoreDocker is the Docker surface of the store records: the volume calls
// of the OSD volume, the container list that finds a volume user independently
// of the ledger, and the container inspection of a recorded attachment.
type monStoreDocker interface {
	VolumeCreate(context.Context, mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error)
	VolumeInspect(context.Context, string, mobycl.VolumeInspectOptions) (mobycl.VolumeInspectResult, error)
	VolumeRemove(context.Context, string, mobycl.VolumeRemoveOptions) (mobycl.VolumeRemoveResult, error)
	Close() error
	ContainerList(context.Context, mobycl.ContainerListOptions) (mobycl.ContainerListResult, error)
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
}

// Volume is the record of one MON's store: an ordinary Docker local
// named volume owned by the cluster, one per MON name. Container.mu guards it;
// see State. It models the OSD volume of osd_volume_storage.go with one
// volume per MON and no helper container (design note D1, D6).
type Volume struct {
	mon, name, owner, createdAt string
	Phase                       Phase
	// createRequested is set before the create call. A planned record with it
	// set has an unknown create outcome and is never created again.
	createRequested bool
	generation      int
	Attachment      *Attachment
	identity        *monStoreIdentity
	// Endpoint is the address the first generation bound, recorded after its
	// ready check; hold is the exact hold on it while a replacement is created
	// (address.go).
	Endpoint *Endpoint
	hold     EndpointHold
	// verified is set by verify and consumed by the dispatch of a generation
	// that must find the retained store.
	verified  bool
	newClient func(context.Context) (monStoreDocker, error)
}

func newMonStoreVolume(mon string) *Volume {
	owner := uuid.NewString()
	return &Volume{mon: mon, name: "tc-ceph-mon-volume-" + owner, owner: owner, Phase: Planned}
}

func (s *Volume) client(ctx context.Context) (monStoreDocker, error) {
	if s.newClient != nil {
		return s.newClient(ctx)
	}
	return testcontainers.NewDockerClientWithOpts(ctx)
}

// validate accepts only the volume this record created: exact name, local
// driver, owner and MON labels, no driver options, a creation timestamp that
// has not changed.
func (s *Volume) validate(v volume.Volume) error {
	if v.Name != s.name || v.Driver != "local" || v.Labels[monStoreOwnerLabel] != s.owner || v.Labels[monStoreMonLabel] != s.mon || len(v.Options) != 0 || v.CreatedAt == "" {
		return fmt.Errorf("MON %s store volume identity, ownership or ordinary local driver differs", s.mon)
	}
	if s.createdAt != "" && v.CreatedAt != s.createdAt {
		return fmt.Errorf("MON %s store volume creation identity changed", s.mon)
	}
	return nil
}

// Create requests the volume once. The request is recorded before it is made,
// so a lost reply leaves a record that is not created again; only a release
// settles it.
func (s *Volume) Create(ctx context.Context) (err error) {
	if s.Phase != Planned {
		return fmt.Errorf("MON %s store is %s, not planned", s.mon, s.Phase)
	}
	if s.createRequested {
		return fmt.Errorf("MON %s store volume create outcome is unknown; it is not replayed, terminate this cluster", s.mon)
	}
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	s.createRequested = true
	labels := testcontainers.GenericLabels()
	labels[monStoreOwnerLabel] = s.owner
	labels[monStoreMonLabel] = s.mon
	result, err := docker.VolumeCreate(ctx, mobycl.VolumeCreateOptions{Name: s.name, Driver: "local", Labels: labels})
	if err != nil {
		return fmt.Errorf("create MON %s store volume: %w", s.mon, err)
	}
	if err := s.validate(result.Volume); err != nil {
		return err
	}
	s.createdAt = result.Volume.CreatedAt
	s.Phase = Created
	return nil
}

func (s *Volume) inspect(ctx context.Context) (err error) {
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	result, err := docker.VolumeInspect(ctx, s.name, mobycl.VolumeInspectOptions{})
	if err != nil {
		return fmt.Errorf("inspect MON %s store volume: %w", s.mon, err)
	}
	return s.validate(result.Volume)
}

// dispatch admits the creation of the next generation's container and hands
// over what it mounts. It publishes the pending attachment before the caller
// creates the container, so a lost reply stays owned. It never creates a
// volume: a bound store whose volume is missing, changed or foreign fails here.
func (s *Volume) dispatch(ctx context.Context) (Launch, error) {
	mode := Require
	switch s.Phase {
	case Created:
		mode = Format
	case Bound:
		// A store bound with an identity needs it verified for each generation.
		// A store marked ready without one (ready) is retained by its volume
		// and the require-store mode alone: the volume is validated below and a
		// missing store makes the generation fail instead of formatting.
		if s.identity != nil && !s.verified {
			return Launch{}, fmt.Errorf("MON %s store identity was not verified for this generation", s.mon)
		}
	case Detached:
		if s.identity == nil {
			return Launch{}, fmt.Errorf("MON %s store has no recorded identity", s.mon)
		}
		if !s.verified {
			return Launch{}, fmt.Errorf("MON %s store identity was not verified for this generation", s.mon)
		}
	case Fresh:
		return Launch{}, fmt.Errorf("MON %s store is fresh: discard it after its first generation is gone", s.mon)
	default:
		return Launch{}, fmt.Errorf("MON %s store is %s and cannot be mounted", s.mon, s.Phase)
	}
	if s.Attachment != nil {
		return Launch{}, fmt.Errorf("MON %s store still has an attachment that is not confirmed gone", s.mon)
	}
	if err := s.inspect(ctx); err != nil {
		return Launch{}, err
	}
	s.generation++
	s.Attachment = &Attachment{generation: s.generation, pending: true}
	s.verified = false
	switch s.Phase {
	case Created:
		s.Phase = Fresh
	case Detached:
		s.Phase = Bound
	}
	return Launch{Volume: s.name, Mode: mode, Generation: s.generation}, nil
}

// recordContainer records the container ID that the creation request returned,
// also when it returned an error with a partial container. Without an ID the
// attachment stays pending: an unknown outcome.
func (s *Volume) recordContainer(id string) error {
	if s.Attachment == nil || !s.Attachment.pending {
		return fmt.Errorf("MON %s store has no pending creation to record", s.mon)
	}
	if id == "" {
		return fmt.Errorf("MON %s store creation returned no container ID; the outcome stays unknown", s.mon)
	}
	s.Attachment.ID, s.Attachment.pending = id, false
	return nil
}

// bind records the identity of the store once the first generation passed its
// ready check. It needs a known attachment: a generation whose creation outcome
// is unknown cannot be bound.
func (s *Volume) bind(identity monStoreIdentity) error {
	if s.Phase != Fresh {
		return fmt.Errorf("MON %s store is %s, not fresh", s.mon, s.Phase)
	}
	if s.Attachment == nil || s.Attachment.pending || s.Attachment.ID == "" {
		return fmt.Errorf("MON %s store has no known attachment to bind", s.mon)
	}
	if err := identity.validate(); err != nil {
		return fmt.Errorf("MON %s store identity: %w", s.mon, err)
	}
	cloned := identity.clone()
	s.identity = &cloned
	s.Phase = Bound
	return nil
}

// ready records that the first generation reached its ready check, without an
// identity: the store is retained by its volume alone. It needs a known
// attachment, as bind does.
func (s *Volume) ready() error {
	if s.Phase != Fresh {
		return fmt.Errorf("MON %s store is %s, not fresh", s.mon, s.Phase)
	}
	if s.Attachment == nil || s.Attachment.pending || s.Attachment.ID == "" {
		return fmt.Errorf("MON %s store has no known attachment to mark ready", s.mon)
	}
	s.Phase = Bound
	return nil
}

// detach records that the last generation is gone and re-records the observed
// fingerprint and address vector. The keyring digest, shape and FSID are the
// bound ones and must not change; the store content legitimately changed while
// a generation ran.
func (s *Volume) detach(observed monStoreIdentity) error {
	if s.Phase != Bound || s.identity == nil {
		return fmt.Errorf("MON %s store is %s, not bound", s.mon, s.Phase)
	}
	if s.Attachment != nil {
		return fmt.Errorf("MON %s store still has an attachment that is not confirmed gone", s.mon)
	}
	if err := observed.validate(); err != nil {
		return fmt.Errorf("MON %s store identity: %w", s.mon, err)
	}
	for _, field := range s.identity.differences(observed) {
		if field != "store fingerprint" && field != "address vector" {
			return fmt.Errorf("MON %s store changed its %s", s.mon, field)
		}
	}
	cloned := observed.clone()
	s.identity = &cloned
	s.Phase = Detached
	s.verified = false
	return nil
}

// requireRetained admits an observation of the stopped store: the record holds
// a retained identity and no generation is attached.
func (s *Volume) requireRetained() error {
	if (s.Phase != Bound && s.Phase != Detached) || s.identity == nil {
		return fmt.Errorf("MON %s store is %s and holds no retained identity", s.mon, s.Phase)
	}
	if s.Attachment != nil {
		return fmt.Errorf("MON %s store still has an attachment that is not confirmed gone", s.mon)
	}
	return nil
}

// verify compares an observation of the stopped store with the recorded
// identity, without writing anything else. A mismatch refuses the next
// generation; a match admits exactly one dispatch.
func (s *Volume) verify(observed monStoreIdentity) error {
	s.verified = false
	if err := s.requireRetained(); err != nil {
		return err
	}
	if err := observed.validate(); err != nil {
		return fmt.Errorf("MON %s store observation: %w", s.mon, err)
	}
	if diff := s.identity.differences(observed); len(diff) != 0 {
		return fmt.Errorf("MON %s store differs from the retained identity: %s", s.mon, strings.Join(diff, ", "))
	}
	s.verified = true
	return nil
}

// confirmGone settles the attachment. A recorded container must be absent by
// inspect, and an independent list of the containers that use the volume must
// be empty: the ledger alone is not trusted. A pending attachment has no ID to
// inspect and is settled only by the list, and only afterScan, that is after
// Terminate removed the containers carrying the cluster's label.
func (s *Volume) confirmGone(ctx context.Context, afterScan bool) (err error) {
	if s.Attachment == nil {
		return nil
	}
	if s.Attachment.pending && !afterScan {
		return fmt.Errorf("MON %s store creation outcome is unknown; only the labeled-container scan of Terminate settles it", s.mon)
	}
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	if !s.Attachment.pending {
		_, err := docker.ContainerInspect(ctx, s.Attachment.ID, mobycl.ContainerInspectOptions{})
		if err == nil {
			return fmt.Errorf("MON %s store container %.12s still exists", s.mon, s.Attachment.ID)
		}
		if !errdefs.IsNotFound(err) {
			return fmt.Errorf("inspect MON %s store container: %w", s.mon, err)
		}
	}
	users, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("volume", s.name)})
	if err != nil {
		return fmt.Errorf("list the users of MON %s store volume: %w", s.mon, err)
	}
	if len(users.Items) != 0 {
		return fmt.Errorf("MON %s store volume is used by %d container(s)", s.mon, len(users.Items))
	}
	s.Attachment = nil
	return nil
}

// release removes the volume, never forced. It needs the attachment confirmed
// gone and the volume to still be the one this record created: a changed or
// foreign volume is never removed. A volume that is confirmed missing settles
// the record. A hold on the MON's address is released first (address.go).
func (s *Volume) release(ctx context.Context, afterScan bool) (err error) {
	if s.Phase == Released {
		return nil
	}
	// A retained address hold is released before anything else, and the record
	// stays owned while that fails.
	if err := s.releaseAddressHold(ctx); err != nil {
		return err
	}
	if err := s.confirmGone(ctx, afterScan); err != nil {
		return err
	}
	if s.Phase == Planned && !s.createRequested {
		s.Phase = Released
		return nil
	}
	docker, err := s.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	inspected, err := docker.VolumeInspect(ctx, s.name, mobycl.VolumeInspectOptions{})
	if errdefs.IsNotFound(err) {
		s.Phase = Released
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect owned MON %s store volume: %w", s.mon, err)
	}
	if err := s.validate(inspected.Volume); err != nil {
		return err
	}
	if _, err := docker.VolumeRemove(ctx, s.name, mobycl.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove owned MON %s store volume: %w", s.mon, err)
	}
	s.Phase = Released
	return nil
}

// discardFresh removes the volume of a store whose first generation never
// reached ready. It is the only discard before termination, and only for a
// fresh store whose attachment is confirmed gone.
func (s *Volume) discardFresh(ctx context.Context) error {
	if s.Phase != Fresh {
		return fmt.Errorf("MON %s store is %s; only a fresh store is discarded", s.mon, s.Phase)
	}
	return s.release(ctx, false)
}
