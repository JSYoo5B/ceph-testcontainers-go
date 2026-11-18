//go:build all || integration

package rbd_mirror_demote_restart_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// rbdMirrorSnapshot binds a receiver copy to a local snapshot and the source
// mirror snapshot it is copying.
type rbdMirrorSnapshot struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	Namespace struct {
		Type              string `json:"type"`
		State             string `json:"state"`
		Complete          *bool  `json:"complete"`
		PrimaryMirrorUUID string `json:"primary_mirror_uuid"`
		PrimarySnapID     uint64 `json:"primary_snap_id"`
		LastCopiedObject  uint64 `json:"last_copied_object_number"`
	} `json:"namespace"`
}

func decodeDemotionListing(data []byte) ([]rbdMirrorSnapshot, error) {
	var snaps []rbdMirrorSnapshot
	if err := json.Unmarshal(data, &snaps); err != nil {
		return nil, fmt.Errorf("decode snapshot listing: %w", err)
	}
	if snaps == nil {
		return nil, errors.New("snapshot listing is null")
	}
	return snaps, nil
}

func demotionComplete(snaps []rbdMirrorSnapshot, original rbdMirrorSnapshot) (bool, error) {
	for _, snap := range snaps {
		if snap.ID != original.ID {
			continue
		}
		ns, want := snap.Namespace, original.Namespace
		if snap.Name != original.Name || ns.Type != "mirror" || ns.State != "demoted" ||
			ns.PrimaryMirrorUUID != want.PrimaryMirrorUUID || ns.PrimarySnapID != want.PrimarySnapID {
			return false, fmt.Errorf("demotion copy %d changed identity: %+v", snap.ID, snap)
		}
		if ns.Complete == nil {
			return false, fmt.Errorf("demotion copy %d has no completion field", snap.ID)
		}
		return *ns.Complete, nil
	}
	return false, fmt.Errorf("demotion copy %d is gone", original.ID)
}

func demotedReplicaAbsent(data []byte, name string) (bool, error) {
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return false, fmt.Errorf("decode image listing: %w", err)
	}
	if names == nil || slices.Contains(names, "") {
		return false, errors.New("image listing has null or empty evidence")
	}
	return !slices.Contains(names, name), nil
}

var errDemotionWindow = errors.New("demotion observation window elapsed")

// observeDemotionState waits for a condition, or confirms its current state
// with a fresh bounded read once the full local window has elapsed. Only a
// successful probe can supply ready=false as evidence of non-progress.
func observeDemotionState[T any](ctx context.Context, bound, probeBound, interval time.Duration, probe func(context.Context) (T, bool, error)) (T, bool, error) {
	read := func(probeCtx context.Context) (T, bool, error) {
		value, ready, err := probe(probeCtx)
		if ctxErr := probeCtx.Err(); ctxErr != nil {
			return value, false, errors.Join(ctxErr, err)
		}
		return value, ready, err
	}
	window, cancel := context.WithTimeoutCause(ctx, bound, errDemotionWindow)
	defer cancel()
	result, err := harness.Observe(window, bound+probeBound, probeBound, interval, read)
	if ctx.Err() != nil {
		return result.Value, false, ctx.Err()
	}
	if err == nil {
		return result.Value, true, nil
	}
	if !errors.Is(context.Cause(window), errDemotionWindow) || !errors.Is(err, context.DeadlineExceeded) {
		return result.Value, false, err
	}

	// The parent's deadline still applies. If the last in-window probe hit
	// the deadline, its response cannot stand in for a fresh observation.
	finalCtx, cancelFinal := context.WithTimeout(ctx, probeBound)
	defer cancelFinal()
	value, ready, err := read(finalCtx)
	if err != nil {
		return value, false, fmt.Errorf("confirm the state after %s: %w", bound, err)
	}
	return value, ready, nil
}
