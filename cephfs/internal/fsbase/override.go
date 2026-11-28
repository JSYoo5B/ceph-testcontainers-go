package fsbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// OverrideRecord is the shared restoration state of one tracked
// per-filesystem MDSMap override: the FSMap ID the override was applied to
// and whether its restore was confirmed. Every copy of the override's handle
// shares it; the setup gate guards it.
type OverrideRecord struct {
	FSID     int64
	Restored bool
}

// Override is one per-filesystem MDSMap override as its Restore drives it.
// Subject names it in the refusals, such as "max_file_size override".
type Override struct {
	Subject string
	// Record is the override's restoration state.
	Record *OverrideRecord
	// Tracked reports whether the filesystem still tracks Record as its
	// active override of this kind. It runs with the setup gate held.
	Tracked func() bool
	// Restore reads the current native value and writes the captured one
	// back. It runs admitted, with the setup gate held, once the record is
	// known tracked and bound to fsID.
	Restore func(ctx context.Context, fsID int64) error
	// Forget drops Record from the filesystem's tracked overrides after a
	// confirmed restore.
	Forget func()
}

// OverrideUnavailable is the refusal of a Restore on a nil or zero handle.
func OverrideUnavailable(subject string) error {
	return servicehook.Invalid(errors.New("CephFS " + subject + " is unavailable"))
}

// OverrideActive is the refusal of an override of a filesystem that already
// has an unrestored override of the same kind.
func OverrideActive(subject string) error {
	return servicehook.Invalid(errors.New("restore the existing CephFS " + subject + " first"))
}

// RestoreOverride runs the restore protocol that every per-filesystem
// override shares. A restore already confirmed stays complete after the
// filesystem or the cluster is gone. Otherwise it admits the operation as
// AdmitOwned does, refuses a record the filesystem no longer tracks and a
// filesystem whose FSMap ID changed since the override, runs o.Restore, and
// marks the record restored and forgotten only when o.Restore succeeds. The
// refusals are ceph.ErrInvalidRequest; o.Restore's errors pass through.
func (c *Core) RestoreOverride(ctx context.Context, o Override) error {
	if c.Cluster() != nil {
		var restored bool
		if err := c.WithSetupGate(ctx, func() { restored = o.Record.Restored }); err != nil {
			return err
		}
		if restored {
			return nil
		}
	}
	ctx, fsID, done, err := c.AdmitOwned(ctx)
	if err != nil {
		return err
	}
	defer done()
	if o.Record.Restored {
		return nil
	}
	if !o.Tracked() {
		return servicehook.Invalid(errors.New("CephFS " + o.Subject + " is not tracked by this filesystem"))
	}
	if fsID != o.Record.FSID {
		return servicehook.Invalid(errors.New("CephFS filesystem identity changed since the " + o.Subject))
	}
	if err := o.Restore(ctx, fsID); err != nil {
		return err
	}
	o.Record.Restored = true
	o.Forget()
	return nil
}

// ReadMDSMap reads the filesystem's own entry with fs get and decodes its
// mdsmap into mdsmap, as DecodeMDSMap does. what names the value the caller
// reads, in the errors.
func (v VolumeCommands) ReadMDSMap(ctx context.Context, fsID int64, what string, mdsmap any) error {
	data, err := v.cluster.Ceph(ctx, "fs", "get", v.name, "--format", "json")
	if err != nil {
		return fmt.Errorf("read CephFS %q %s: %w", v.name, what, err)
	}
	return DecodeMDSMap(data, fsID, what, mdsmap)
}

// DecodeMDSMap decodes one fs get reply: it refuses a reply without an id or
// an mdsmap object and one whose id is not fsID, and decodes the mdsmap
// object into mdsmap. Which fields of mdsmap must be present is the
// caller's rule.
func DecodeMDSMap(data []byte, fsID int64, what string, mdsmap any) error {
	var native struct {
		ID     *int64          `json:"id"`
		MDSMap json.RawMessage `json:"mdsmap"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.ID == nil || len(native.MDSMap) == 0 || string(native.MDSMap) == "null" {
		return errors.New("decode CephFS " + what)
	}
	if *native.ID != fsID {
		return fmt.Errorf("CephFS filesystem ID is %d, want %d", *native.ID, fsID)
	}
	if err := json.Unmarshal(native.MDSMap, mdsmap); err != nil {
		return errors.New("decode CephFS " + what)
	}
	return nil
}
