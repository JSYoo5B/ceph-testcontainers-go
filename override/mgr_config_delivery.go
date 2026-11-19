package override

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync/atomic"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// This setting is used by MON elections, so it has no runtime effect when
// set in the exact section of an owned MGR. It is present in the same native
// configuration snapshot as the temporarily empty module value.
const mgrConfigDeliveryOption = "mon_lease_ack_timeout_factor"

// Start at a random position and do not reuse a marker in this process.
var mgrDeliveryStart = uint64(rand.IntN(80_000_000))
var mgrDeliverySequence atomic.Uint64

func (change *ConfigHandle) waitMGRDelivery(ctx context.Context, c *ceph.Container) error {
	targets := overridehook.MGRConfigTargets(c, change.setting.Section)
	if len(targets) == 0 {
		return nil
	}
	entries, err := overridehook.ConfigEntries(ctx, c)
	if err != nil {
		return err
	}
	used := make(map[float64]bool)
	for _, entry := range entries {
		if entry.Name == mgrConfigDeliveryOption {
			if value, err := strconv.ParseFloat(entry.Value, 64); err == nil {
				used[value] = true
			}
		}
	}
	for _, name := range targets {
		value, err := nextMGRDeliveryValue(used)
		if err != nil {
			return err
		}
		marker := ceph.ConfigSetting{
			Section: "mgr." + name, Name: mgrConfigDeliveryOption,
			Value: fmt.Sprintf("%.6f", value),
		}
		handle, err := setConfigLocked(ctx, c, marker)
		change.state.mgrDelivery = handle
		if err != nil {
			return err
		}
		if applied, err := strconv.ParseFloat(handle.state.applied, 64); err != nil || applied != value {
			return fmt.Errorf("mgr.%s configuration delivery marker changed during publication", name)
		}
		if err := overridehook.WaitMGRConfigDelivery(ctx, c, name, marker.Name, handle.state.applied); err != nil {
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(err, &unstopped) {
				change.state.mgrProbe = unstopped
			}
			return err
		}
		if err := change.restoreMGRDelivery(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func nextMGRDeliveryValue(used map[float64]bool) (float64, error) {
	const available = 80_000_000
	for {
		seq := mgrDeliverySequence.Add(1)
		if seq > available {
			return 0, fmt.Errorf("MGR configuration delivery marker range exhausted")
		}
		// Six decimal places survive the MON's option normalization.
		value := float64(10_000_000+(mgrDeliveryStart+seq)%available) / 1_000_000
		if !used[value] {
			return value, nil
		}
	}
}

func (change *ConfigHandle) restoreMGRDelivery(ctx context.Context, c *ceph.Container) error {
	if probe := change.state.mgrProbe; probe != nil {
		if err := probe.Stop(ctx); err != nil {
			return errors.Join(probe, err)
		}
		change.state.mgrProbe = nil
	}
	if marker := change.state.mgrDelivery; marker != nil && marker.state.refused == nil {
		if err := marker.restoreLocked(ctx, c); err != nil {
			return err
		}
	}
	change.state.mgrDelivery = nil
	return nil
}
