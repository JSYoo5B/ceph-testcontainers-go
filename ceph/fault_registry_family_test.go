package ceph

import (
	"strings"
	"testing"
)

func TestOverridesRefuseUntrackedHandlesAndFreeFailedReservations(t *testing.T) {
	t.Run("full ratios", func(t *testing.T) {
		c, ctr := newFullRatiosFixture()
		ordered := ctr.ratios
		// Failures before registration leave no reservation behind.
		keyring := c.keyring
		c.keyring = nil
		if _, err := c.OverrideFullRatios(t.Context(), FullRatios{.86, .91, .96}); err == nil || len(c.faultsOf(faultFullRatios)) != 0 {
			t.Fatalf("a failed session kept a reservation: %v", err)
		}
		c.keyring = keyring
		ctr.ratios = FullRatios{NearFull: ordered.Full, BackfillFull: ordered.BackfillFull, Full: ordered.NearFull}
		if _, err := c.OverrideFullRatios(t.Context(), FullRatios{.86, .91, .96}); err == nil || len(c.faultsOf(faultFullRatios)) != 0 {
			t.Fatalf("an unordered original kept a reservation: %v", err)
		}
		ctr.ratios = ordered
		change, err := c.OverrideFullRatios(t.Context(), FullRatios{.86, .91, .96})
		if err != nil {
			t.Fatalf("a freed key stayed refused: %v", err)
		}
		forged := &FullRatiosOverride{owner: c, state: &fullRatiosOverrideState{previous: ordered, expected: change.state.expected, fsid: change.state.fsid}}
		before := ctr.mutations
		if err := forged.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "not tracked") || ctr.mutations != before {
			t.Fatalf("an untracked override restored: %v", err)
		}
	})
}
