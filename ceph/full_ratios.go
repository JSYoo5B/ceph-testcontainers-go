package ceph

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// FullRatios are global OSDMap utilization thresholds, expressed as fractions
// of capacity. These are separate from central configuration and pool quotas.
// Native OSDMap storage uses float32; observations retain its normalized value.
type FullRatios struct {
	NearFull, BackfillFull, Full float64
}

// FullRatioSnapshot is one native OSDMap observation. Epoch is not a generation
// for these thresholds: unrelated OSDMap changes can advance it too. The values
// describe map policy, not whether any OSD has reported the corresponding state.
type FullRatioSnapshot struct {
	FSID   string
	Epoch  uint32
	Ratios FullRatios
}

// FullRatios reads native thresholds through one captured control CLI handle,
// checking the original bootstrap FSID before and after the map query. Native
// zero, equal and misordered thresholds remain observable. It makes no writes,
// retries or background workers and returns a zero snapshot on any failure.
// These separate reads are not atomic; external configuration writers must not
// race the observation. Poll HealthDetails for eventual OSD fullness reports.
func (c *Container) FullRatios(ctx context.Context) (FullRatioSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return FullRatioSnapshot{}, err
	}
	if c == nil {
		return FullRatioSnapshot{}, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return FullRatioSnapshot{}, err
	}
	defer c.mu.Unlock()
	session, err := c.fullRatiosSession(ctx)
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	return session.read(ctx)
}

type fullRatiosCLISession struct {
	control testcontainers.Container
	fsid    string
}

// The caller holds the topology gate. Capture both control and original
// bootstrap identity once; queries never switch to a replacement control handle.
func (c *Container) fullRatiosSession(ctx context.Context) (fullRatiosCLISession, error) {
	if c.closed {
		return fullRatiosCLISession{}, clientOperationError(ctx, "ceph cluster is terminated", nil)
	}
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return fullRatiosCLISession{}, err
	}
	if control == nil {
		return fullRatiosCLISession{}, clientOperationError(ctx, "ceph control container is unavailable", nil)
	}
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return fullRatiosCLISession{}, err
	}
	fsid, identityErr := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	if identityErr != nil || !confirmed {
		return fullRatiosCLISession{}, clientOperationError(ctx, "ceph original bootstrap identity is unavailable", identityErr)
	}
	return fullRatiosCLISession{control: control, fsid: fsid}, nil
}

func (session fullRatiosCLISession) command(ctx context.Context, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := command(ctx, session.control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
	if err != nil || ctx.Err() != nil {
		return nil, clientOperationError(ctx, "native full ratios command failed", err)
	}
	return data, nil
}

func (session fullRatiosCLISession) checkFSID(ctx context.Context) error {
	data, err := session.command(ctx, "fsid")
	if err != nil {
		return err
	}
	native := strings.TrimSpace(string(data))
	parsed, err := uuid.Parse(native)
	if err != nil || parsed == uuid.Nil || parsed.String() != native || native != session.fsid {
		return clientOperationError(ctx, "native full ratios FSID differs from the original cluster", err)
	}
	return ctx.Err()
}

func (session fullRatiosCLISession) read(ctx context.Context) (FullRatioSnapshot, error) {
	if err := session.checkFSID(ctx); err != nil {
		return FullRatioSnapshot{}, err
	}
	data, err := session.command(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	snapshot, err := decodeFullRatios(data, session.fsid)
	if err != nil || ctx.Err() != nil {
		return FullRatioSnapshot{}, clientOperationError(ctx, "decode native full ratios failed", err)
	}
	if err := session.checkFSID(ctx); err != nil {
		return FullRatioSnapshot{}, err
	}
	return snapshot, nil
}

func decodeFullRatios(data []byte, fsid string) (FullRatioSnapshot, error) {
	var native struct {
		FSID         *string  `json:"fsid"`
		Epoch        *uint32  `json:"epoch"`
		NearFull     *float64 `json:"nearfull_ratio"`
		BackfillFull *float64 `json:"backfillfull_ratio"`
		Full         *float64 `json:"full_ratio"`
	}
	if err := healthDetailsJSON(data, &native); err != nil || native.FSID == nil || *native.FSID != fsid || native.Epoch == nil || native.NearFull == nil || native.BackfillFull == nil || native.Full == nil {
		return FullRatioSnapshot{}, errors.New("incomplete native full ratios")
	}
	ratios := FullRatios{NearFull: *native.NearFull, BackfillFull: *native.BackfillFull, Full: *native.Full}
	for _, ratio := range []float64{ratios.NearFull, ratios.BackfillFull, ratios.Full} {
		if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
			return FullRatioSnapshot{}, errors.New("invalid native full ratio")
		}
	}
	return FullRatioSnapshot{FSID: fsid, Epoch: *native.Epoch, Ratios: ratios}, nil
}
