//go:build all || integration

package rbd_group_snap_late_complete_test

import (
	"fmt"
	"slices"
	"strings"
)

// The two snapshots of each group: the sibling that stays, and the one that
// is removed and then gets the late update.
const (
	groupSnapHealthy = "healthy"
	groupSnapVictim  = "victim"
)

// groupSnapInvalid is the errno with which a server that refuses an update of
// a missing group snapshot record answers.
const groupSnapInvalid = 22

// groupSnapOutcome is what the group that got the late update shows after it.
type groupSnapOutcome struct {
	// Return is the result of the late group_snap_set: 0 when the server
	// accepted it, the negative errno when it refused.
	Return int
	// Keys are the omap keys of the group header object.
	Keys []string
	// ListExit and ListOutput are the exit code and output of the listing
	// command, and Listed the snapshot names it printed, in its order.
	ListExit   int
	ListOutput string
	Listed     []string
}

// judgeLateComplete decides what a group shows after a COMPLETE update for
// its removed snapshot victimID.
//
// It returns the symptoms of the defect when the listing shows the removed
// snapshot again, or fails, while the header object holds the snapshot's
// record without its order entry after an accepted update. It returns
// neither symptoms nor an error when the listing succeeds with exactly the
// healthy sibling, whether the server accepted the update or refused it as
// invalid. Everything else is an error: an update that ended in another way
// was not delivered, and a listing that is wrong without the orphan record
// is another fault.
func judgeLateComplete(victimID string, got groupSnapOutcome) ([]string, error) {
	if got.Return != 0 && got.Return != -groupSnapInvalid {
		return nil, fmt.Errorf("trigger not delivered: the late update returned %d, want 0 (accepted) or %d (refused as invalid)", got.Return, -groupSnapInvalid)
	}
	orphan := got.Return == 0 && slices.Contains(got.Keys, "snapshot_"+victimID) && !slices.Contains(got.Keys, "snap_order_"+victimID)
	evidence := fmt.Sprintf("the late update returned %d and the header object has the keys %v", got.Return, got.Keys)
	if got.ListExit != 0 {
		if !orphan {
			return nil, fmt.Errorf("another fault: the listing exited %d (%s) and the removed snapshot has no record without an order entry: %s", got.ListExit, strings.TrimSpace(got.ListOutput), evidence)
		}
		return []string{
			fmt.Sprintf("the listing exited %d: %s", got.ListExit, strings.TrimSpace(got.ListOutput)),
			"the record of the removed snapshot is back without its order entry: " + evidence,
		}, nil
	}
	if !slices.Contains(got.Listed, groupSnapHealthy) {
		return nil, fmt.Errorf("another fault: the listing %v lacks the sibling %s: %s", got.Listed, groupSnapHealthy, evidence)
	}
	if slices.Contains(got.Listed, groupSnapVictim) {
		if !orphan || len(got.Listed) != 2 {
			return nil, fmt.Errorf("another fault: the listing %v names the removed snapshot, but not as the one record that an accepted update left without an order entry: %s", got.Listed, evidence)
		}
		return []string{
			fmt.Sprintf("the listing names the removed snapshot again: %v", got.Listed),
			"its record is back without its order entry: " + evidence,
		}, nil
	}
	if len(got.Listed) != 1 {
		return nil, fmt.Errorf("another fault: the listing is %v, want only %s: %s", got.Listed, groupSnapHealthy, evidence)
	}
	return nil, nil
}
