package ceph

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// pendingDiscardHelper models an Exec connection lost before rm accesses its
// directory. Only successful cancellation removes the pending invocation;
// release is the barrier that would let every surviving invocation delete.
type pendingDiscardHelper struct {
	*osdVolumeHelperFake
	pending    map[string]string
	failCancel int
	loseReply  bool
	discards   int
}

func (h *pendingDiscardHelper) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 3 && args[0] == "python3" && args[2] == supervised.CancelScript {
		if h.failCancel > 0 {
			h.failCancel--
			return 1, bytes.NewReader(nil), nil
		}
		delete(h.pending, args[3])
		return 0, bytes.NewReader(nil), nil
	}
	if len(args) > 6 && args[0] == "python3" && args[2] == supervised.Script && args[6] == "rm" {
		h.discards++
		if h.loseReply {
			h.pending[supervisedMarker(opts)] = args[len(args)-1]
			return 0, nil, errors.New("discard Exec connection lost before directory access")
		}
	}
	return h.osdVolumeHelperFake.Exec(ctx, args, opts...)
}

func (h *pendingDiscardHelper) release() {
	for _, path := range h.pending {
		delete(h.directories, path)
	}
	clear(h.pending)
}

func TestOSDStoreDiscardSettlesOldInvocationBeforeReuse(t *testing.T) {
	for _, backing := range []string{"volume", "memory"} {
		t.Run(backing, func(t *testing.T) {
			volume, _, base := osdVolumeFixture()
			helper := &pendingDiscardHelper{osdVolumeHelperFake: base, pending: map[string]string{}, failCancel: 2, loseReply: true}
			volume.helper = helper
			discard, fresh := volume.discardOSD, volume.requireFreshOSD
			prefix := osdVolumeHelperPath
			if backing == "memory" {
				memory, _, _ := newOSDMemoryUnitFixture()
				memory.keeper, memory.keeperStarted = helper, helper.startedAt
				discard, fresh = memory.discardOSD, memory.requireFreshOSD
				prefix = osdMemoryKeeperPath
			}
			path, sibling := prefix+"/ceph-4", prefix+"/ceph-5"
			helper.directories[path], helper.directories[sibling] = true, true
			if err := discard(t.Context(), 4); err == nil || len(helper.pending) != 1 {
				t.Fatalf("lost reply did not retain pending work: %v", err)
			}
			// The directory could already be absent while the old invocation
			// remains blocked before its access. Absence alone permits no reuse.
			delete(helper.directories, path)
			if err := fresh(t.Context(), 4); err == nil {
				t.Fatal("unconfirmed discard admitted a fresh store")
			}
			if err := discard(t.Context(), 4); err == nil || helper.discards != 1 || len(helper.pending) != 1 {
				t.Fatalf("failed settlement dispatched another discard: %v", err)
			}
			helper.loseReply = false
			if err := discard(t.Context(), 4); err != nil || helper.discards != 2 || len(helper.pending) != 0 {
				t.Fatalf("settled retry failed: %v", err)
			}
			if err := fresh(t.Context(), 4); err != nil {
				t.Fatal("settled discard prevented numeric ID reuse", err)
			}
			helper.directories[path] = true
			helper.release()
			if !helper.directories[path] || !helper.directories[sibling] {
				t.Fatal("old discard removed the replacement store or a sibling")
			}
		})
	}
}

func TestOSDRemovalKeepsOwnershipWhileStoreDiscardIsPending(t *testing.T) {
	for _, backing := range []string{"volume", "memory"} {
		t.Run(backing, func(t *testing.T) {
			cluster, _, _ := newOSDLifecycleFixture()
			storage, _, base := osdVolumeFixture()
			helper := &pendingDiscardHelper{osdVolumeHelperFake: base, pending: map[string]string{}, failCancel: 2, loseReply: true}
			storage.helper = helper
			cluster.fleet.stores.volume = storage
			if backing == "memory" {
				memory, _, _ := newOSDMemoryUnitFixture()
				memory.keeper, memory.keeperStarted = helper, helper.startedAt
				cluster.fleet.stores.memory, cluster.fleet.stores.volume = memory, nil
			}
			osd := osdHandle(t, t.Context(), cluster, 3)
			if err := osd.Remove(t.Context()); err == nil || cluster.fleet.osdHandles.get(3) != osd || !osd.purged {
				t.Fatalf("pending discard lost its purged descriptor: %v", err)
			}
			if _, err := cluster.AddOSD(t.Context()); err == nil || !strings.Contains(err.Error(), "finish removing osd.3") {
				t.Fatalf("pending discard admitted another registration: %v", err)
			}
			if err := osd.Remove(t.Context()); err == nil || cluster.fleet.osdHandles.get(3) != osd || helper.discards != 1 {
				t.Fatalf("failed settlement released ownership or replayed discard: %v", err)
			}
			helper.loseReply = false
			if err := osd.Remove(t.Context()); err != nil || cluster.fleet.osdHandles.get(3) != nil || len(helper.pending) != 0 {
				t.Fatalf("settled retry did not release ownership: %v", err)
			}
		})
	}
}
