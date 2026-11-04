package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// Feature is one CephFS client feature: its bit in the MDSMap's
// required_client_features and the name the cluster reports for it. A bit
// that the cluster's release does not name keeps the name the MDSMap prints
// for it.
type Feature struct {
	Bit  int
	Name string
}

// RequirementStep is the evidence of one feature that
// OverrideRequiredFeatures was asked to require, in request order.
type RequirementStep struct {
	Feature Feature
	// AlreadyRequired reports that the filesystem required the feature
	// before the override. The handle sent no command for it, owns nothing
	// for it, and its Restore leaves it required.
	AlreadyRequired bool
	// Sent reports that the add command was attempted. The handle owns the
	// bit from then on, even when the reply was lost or the command never ran.
	Sent bool
	// Confirmed reports that the readback after the command showed the
	// feature required.
	Confirmed bool
}

// RequiredFeaturesOverrideHandle removes again the required client features
// that OverrideRequiredFeatures added. Copies share restoration state. Retain
// a non-nil result even on error: a lost reply or a failed later step can
// leave some features required.
type RequiredFeaturesOverrideHandle struct {
	filesystem *fsbase.Core
	state      *featuresState
	steps      []RequirementStep
}

// featuresState is the restoration state of one override: the bits it added,
// in the order it added them. The setup gate guards it.
type featuresState struct {
	fsID     int64
	owned    []*ownedFeature
	restored bool
}

// ownedFeature is one bit that the override sent an add for. confirmed means
// a readback showed it required afterwards; removalSent means Restore sent its
// rm, so that its absence is no outside edit.
type ownedFeature struct {
	feature     Feature
	confirmed   bool
	removalSent bool
}

type nativeRequired struct {
	ID     int64 `json:"id"`
	MDSMap struct {
		Required *map[string]string `json:"required_client_features"`
	} `json:"mdsmap"`
}

type nativeFeature struct {
	Index *int   `json:"index"`
	Name  string `json:"name"`
}

// RequiredFeatures reads the client features that the MDSMap of fs requires
// now, sorted by bit. It includes bits that the fixture did not add and bits
// that the cluster's release does not name. It reads native state only.
//
//purpose:observation
func RequiredFeatures(ctx context.Context, fs *cephfs.Filesystem) ([]Feature, error) {
	return requiredFeatures(ctx, coreOf(fs))
}

func requiredFeatures(ctx context.Context, fs *fsbase.Core) ([]Feature, error) {
	ctx, fsID, done, err := begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	required, err := readRequired(ctx, fs.Volumes(), fsID)
	if err != nil {
		return nil, err
	}
	return sortedFeatures(required), nil
}

// OverrideRequiredFeatures adds the named client features to the MDSMap's
// required_client_features of fs, so that an MDS of the filesystem refuses a
// new session of a client that does not advertise one of them (its mount
// fails with EPERM) and evicts an open session that lacks one; with Ceph's
// default mds_session_blocklist_on_evict the evicted address is blocklisted.
// Clients that advertise every required feature are admitted as before.
//
// The names must be ones that "fs feature ls" of the cluster's own image
// lists, other than its placeholder "reserved"; a name it does not list
// returns ceph.ErrUnsupported, and an empty,
// repeated or blank name ceph.ErrInvalidRequest, before any change. A feature
// that the filesystem already requires is recorded as AlreadyRequired: the
// handle owns nothing for it and its Restore leaves it required. Other bits,
// including ones the release does not name, are never changed. The handle is
// bound to the filesystem's FSMap ID. Only one handle per filesystem may be
// active, even one that owns nothing.
//
// The native command adds one feature at a time, so the override is a
// sequence, not one atomic change: Steps reports each feature's outcome, and
// a failure stops the sequence and returns the handle with the error. The
// requirement is the filesystem's admission policy only. It proves that the
// MDSMap requires the features; whether a particular client advertises them
// is the client's own state, which the fixture neither reads nor changes, so
// observe the client's outcome separately.
//
//purpose:condition
func OverrideRequiredFeatures(ctx context.Context, fs *cephfs.Filesystem, names ...string) (*RequiredFeaturesOverrideHandle, error) {
	return overrideRequiredFeatures(ctx, coreOf(fs), names)
}

func overrideRequiredFeatures(ctx context.Context, fs *fsbase.Core, names []string) (*RequiredFeaturesOverrideHandle, error) {
	if err := validateFeatureNames(names); err != nil {
		return nil, err
	}
	ctx, fsID, done, err := begin(ctx, fs)
	if err != nil {
		return nil, err
	}
	defer done()
	overrides := overridesOf(fs)
	if active := overrides.features[fs]; active != nil && !active.restored {
		return nil, servicehook.Invalid(errors.New("restore the existing CephFS required client features override first"))
	}
	v := fs.Volumes()
	supported, err := readSupported(ctx, v)
	if err != nil {
		return nil, err
	}
	requested := make([]Feature, 0, len(names))
	for _, name := range names {
		bit, ok := supported[name]
		if !ok {
			return nil, unsupported(v.Cluster(), "CephFS client feature "+name, errors.New(`"fs feature ls" does not list the feature`))
		}
		requested = append(requested, Feature{Bit: bit, Name: name})
	}
	previous, err := readRequired(ctx, v, fsID)
	if err != nil {
		return nil, err
	}
	state := &featuresState{fsID: fsID}
	overrides.features[fs] = state
	change := &RequiredFeaturesOverrideHandle{filesystem: fs, state: state}
	steps := make([]RequirementStep, 0, len(requested))
	defer func() { change.steps = steps }()
	for _, feature := range requested {
		step := RequirementStep{Feature: feature}
		if _, ok := previous[feature.Bit]; ok {
			step.AlreadyRequired = true
			steps = append(steps, step)
			continue
		}
		owned := &ownedFeature{feature: feature}
		state.owned = append(state.owned, owned)
		step.Sent = true
		present, err := changeRequired(ctx, v, fsID, feature, "add")
		owned.confirmed = present
		step.Confirmed = present
		steps = append(steps, step)
		if err == nil && !present {
			err = fmt.Errorf("CephFS required client feature %s is not required after it was added", feature.Name)
		}
		if err != nil {
			return change, err
		}
	}
	return change, nil
}

// Steps reports, in request order, what the override found and did for each
// requested feature. It is a copy of the evidence of the apply; Restore does
// not change it.
//
//purpose:observation
func (change *RequiredFeaturesOverrideHandle) Steps() []RequirementStep {
	if change == nil {
		return nil
	}
	return slices.Clone(change.steps)
}

// Restore removes the features that this handle added, in reverse order, and
// leaves every other bit as it is, including features that were required
// before the override. It is idempotent across handle copies. A feature that
// the handle confirmed as added but that is no longer required is an outside
// edit and is refused, with ceph.ErrInvalidRequest, before any change; a
// filesystem recreated under the same name is refused too. A feature whose
// add was sent but never confirmed is removed when it is required and
// otherwise accepted as not applied.
//
// Restore restores the policy only. A session that the requirement evicted
// stays closed and its blocklist entry stays; a client must mount again. The
// native commands address the filesystem by name and the feature by name
// without a precondition, so an outside remove and re-add of an owned feature
// between two reads cannot be told apart from no edit.
//
//purpose:condition,lifetime
func (change *RequiredFeaturesOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.filesystem == nil || change.state == nil {
		return servicehook.Invalid(errors.New("CephFS required client features override is unavailable"))
	}
	fs := change.filesystem
	state := change.state
	if restored, err := confirmedRestore(ctx, fs, func() bool { return state.restored }); err != nil || restored {
		return err
	}
	ctx, fsID, done, err := begin(ctx, fs)
	if err != nil {
		return err
	}
	defer done()
	if state.restored {
		return nil
	}
	overrides := overridesOf(fs)
	if overrides.features[fs] != state {
		return servicehook.Invalid(errors.New("CephFS required client features override is not tracked by this filesystem"))
	}
	if fsID != state.fsID {
		return servicehook.Invalid(errors.New("CephFS filesystem identity changed since the required client features override"))
	}
	v := fs.Volumes()
	current, err := readRequired(ctx, v, fsID)
	if err != nil {
		return err
	}
	for _, owned := range state.owned {
		if _, ok := current[owned.feature.Bit]; !ok && owned.confirmed && !owned.removalSent {
			return servicehook.Invalid(fmt.Errorf("CephFS required client feature %s was removed outside this override", owned.feature.Name))
		}
	}
	for i := len(state.owned) - 1; i >= 0; i-- {
		owned := state.owned[i]
		if _, ok := current[owned.feature.Bit]; !ok {
			continue
		}
		owned.removalSent = true
		present, err := changeRequired(ctx, v, fsID, owned.feature, "rm")
		if err == nil && present {
			err = fmt.Errorf("CephFS required client feature %s is still required after it was removed", owned.feature.Name)
		}
		if err != nil {
			return err
		}
	}
	state.restored = true
	delete(overrides.features, fs)
	return nil
}

func validateFeatureNames(names []string) error {
	if len(names) == 0 {
		return servicehook.Invalid(errors.New("CephFS required client features override needs at least one feature name"))
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) != name || name == "" {
			return servicehook.Invalid(fmt.Errorf("CephFS client feature name %q is blank or padded", name))
		}
		if seen[name] {
			return servicehook.Invalid(fmt.Errorf("CephFS client feature %s is requested twice", name))
		}
		seen[name] = true
	}
	return nil
}

// changeRequired sends one add or rm and reports whether the feature is
// required afterwards, from a readback that survives cancellation of the
// caller's context and runs even when the command failed. A failed readback
// reports the feature as not required.
func changeRequired(ctx context.Context, v fsbase.VolumeCommands, fsID int64, feature Feature, op string) (bool, error) {
	_, sendErr := v.Cluster().Ceph(ctx, "fs", "required_client_features", v.Name(), op, feature.Name)
	if sendErr != nil {
		sendErr = fmt.Errorf("%s CephFS %q required client feature %s: %w", op, v.Name(), feature.Name, sendErr)
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readbackTimeout)
	defer cancel()
	stored, err := readRequired(readCtx, v, fsID)
	if err != nil {
		return false, errors.Join(sendErr, err)
	}
	_, present := stored[feature.Bit]
	return present, sendErr
}

// reservedFeature is the name "fs feature ls" gives the bits that have no
// feature.
const reservedFeature = "reserved"

// readSupported reads the client features that the cluster's image names,
// by name, without the reserved bits.
func readSupported(ctx context.Context, v fsbase.VolumeCommands) (map[string]int, error) {
	data, err := v.Cluster().Ceph(ctx, "fs", "feature", "ls", "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("list CephFS client features: %w", err)
	}
	return parseSupported(data)
}

func parseSupported(data []byte) (map[string]int, error) {
	var native []nativeFeature
	if err := json.Unmarshal(data, &native); err != nil || len(native) == 0 {
		return nil, errors.New("decode CephFS client features")
	}
	supported := make(map[string]int, len(native))
	for _, feature := range native {
		if feature.Index == nil || *feature.Index < 0 || feature.Name == "" {
			return nil, errors.New("decode CephFS client features: an entry lacks its bit or name")
		}
		// Bits without a feature share the placeholder name "reserved",
		// which the native command does not accept.
		if feature.Name != reservedFeature {
			supported[feature.Name] = *feature.Index
		}
	}
	return supported, nil
}

func readRequired(ctx context.Context, v fsbase.VolumeCommands, fsID int64) (map[int]string, error) {
	data, err := v.Cluster().Ceph(ctx, "fs", "get", v.Name(), "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("read CephFS %q required client features: %w", v.Name(), err)
	}
	required, err := parseRequired(data, fsID)
	if errors.Is(err, errNoRequiredFeatures) {
		return nil, unsupported(v.Cluster(), "fs required_client_features", err)
	}
	return required, err
}

var errNoRequiredFeatures = errors.New("the MDSMap reports no required_client_features")

// parseRequired decodes the MDSMap's required_client_features, which prints
// each set bit as "feature_<bit>" with its name, into names by bit.
func parseRequired(data []byte, fsID int64) (map[int]string, error) {
	var native nativeRequired
	if err := json.Unmarshal(data, &native); err != nil {
		return nil, errors.New("decode CephFS required client features")
	}
	if native.ID != fsID {
		return nil, fmt.Errorf("CephFS filesystem ID is %d, want %d", native.ID, fsID)
	}
	if native.MDSMap.Required == nil {
		return nil, errNoRequiredFeatures
	}
	required := make(map[int]string, len(*native.MDSMap.Required))
	for key, name := range *native.MDSMap.Required {
		digits, ok := strings.CutPrefix(key, "feature_")
		bit, err := strconv.Atoi(digits)
		if !ok || err != nil || bit < 0 || strconv.Itoa(bit) != digits {
			return nil, fmt.Errorf("decode CephFS required client features: key %q", key)
		}
		required[bit] = name
	}
	return required, nil
}

func sortedFeatures(byBit map[int]string) []Feature {
	features := make([]Feature, 0, len(byBit))
	for bit, name := range byBit {
		features = append(features, Feature{Bit: bit, Name: name})
	}
	slices.SortFunc(features, func(a, b Feature) int { return a.Bit - b.Bit })
	return features
}
