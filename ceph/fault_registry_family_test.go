package ceph

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

func TestOverridesRefuseUntrackedHandlesAndFreeFailedReservations(t *testing.T) {
	t.Run("full ratios", func(t *testing.T) {
		c, ctr := newFullRatiosFixture()
		ordered := ctr.ratios
		// Failures before registration leave no reservation behind.
		keyring := c.keyring
		c.keyring = nil
		if _, err := c.TemporaryFullRatios(t.Context(), FullRatios{.86, .91, .96}); err == nil || len(c.faultsOf(faultFullRatios)) != 0 {
			t.Fatalf("a failed session kept a reservation: %v", err)
		}
		c.keyring = keyring
		ctr.ratios = FullRatios{NearFull: ordered.Full, BackfillFull: ordered.BackfillFull, Full: ordered.NearFull}
		if _, err := c.TemporaryFullRatios(t.Context(), FullRatios{.86, .91, .96}); err == nil || len(c.faultsOf(faultFullRatios)) != 0 {
			t.Fatalf("an unordered original kept a reservation: %v", err)
		}
		ctr.ratios = ordered
		change, err := c.TemporaryFullRatios(t.Context(), FullRatios{.86, .91, .96})
		if err != nil {
			t.Fatalf("a freed key stayed refused: %v", err)
		}
		forged := &FullRatiosOverride{owner: c, state: &fullRatiosOverrideState{previous: ordered, expected: change.state.expected, fsid: change.state.fsid}}
		before := ctr.mutations
		if err := forged.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "not tracked") || ctr.mutations != before {
			t.Fatalf("an untracked override restored: %v", err)
		}
	})
	t.Run("blocklist", func(t *testing.T) {
		c, native := newBlocklistFixture()
		change, err := c.TemporaryBlocklist(t.Context(), blocklistTestAddress, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		forged := &BlocklistOverride{owner: c, address: change.address, state: &blocklistOverrideState{until: change.state.until, known: true}}
		if err := forged.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "not tracked") || native.removals != 0 {
			t.Fatalf("an untracked override removed the entry: %v", err)
		}
	})
}

// closingTarget is a caller-owned target whose network inspection overlaps
// a Terminate: the cluster closes between the admission and the reservation.
type closingTarget struct {
	contextLockClient
	c *Container
}

func (target closingTarget) Networks(context.Context) ([]string, error) {
	target.c.mu.Lock()
	target.c.closed = true
	target.c.mu.Unlock()
	return []string{target.c.NetworkName()}, nil
}

func TestNetworkDelayReservationRefusesClusterClosedDuringAdmission(t *testing.T) {
	c := &Container{network: &testcontainers.DockerNetwork{Name: "public"}}
	if _, token, err := c.reserveNetworkDelay(t.Context(), closingTarget{c: c}, PublicNetworkPlane); !errors.Is(err, errClusterTerminated) || token != nil || len(c.faultsOf(faultNetworkDelay)) != 0 {
		t.Fatalf("a delay was reserved on a cluster closed meanwhile: %v", err)
	}
}
