//go:build all || integration

package rbd_group_snap_late_complete_test

import (
	"encoding/hex"
	"encoding/json"
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

// groupSnapInvalid is EINVAL: the errno with which a server that refuses an
// update of a missing group snapshot record answers, and the errno with
// which a listing refuses a record without an order entry. The listing
// command prints nothing for that refusal and exits with the errno.
const groupSnapInvalid = 22

// groupSnapExists is EEXIST: the errno with which the server refuses an
// INCOMPLETE update of a snapshot whose record exists.
const groupSnapExists = 17

// The stored state bytes of a snapshot record.
const (
	groupSnapIncomplete = 0
	groupSnapComplete   = 1
)

// groupSnapRecord is one snapshot record of a group header object. State is
// the stored state byte and Value the stored encoded record in hex.
type groupSnapRecord struct {
	ID    string
	Name  string
	State int
	Value string
}

// groupSnapHeader is one report of the header reading script: the header
// object's name, its omap keys and its snapshot records, and in the "set"
// mode the result of the update (0, or a negative errno) with the client's
// error text.
type groupSnapHeader struct {
	Object    string
	Return    int
	Error     string
	Keys      []string
	Snapshots []groupSnapRecord
}

// withState returns the stored value of record, in hex, with its state byte
// replaced by state. The value starts with a header of six bytes; the ID and
// the name follow, each as a 32-bit length and its text, and then the state
// byte. It refuses a value in which that byte is not the record's state.
func (record groupSnapRecord) withState(state byte) (string, error) {
	value, err := hex.DecodeString(record.Value)
	if err != nil {
		return "", fmt.Errorf("value of record %s: %w", record.ID, err)
	}
	at := 6 + 4 + len(record.ID) + 4 + len(record.Name)
	if record.ID == "" || record.Name == "" || at >= len(value) || record.State < 0 || record.State > 255 || value[at] != byte(record.State) {
		return "", fmt.Errorf("value of record %s (%s) has no state byte %d at offset %d: %s", record.ID, record.Name, record.State, at, record.Value)
	}
	value[at] = state
	return hex.EncodeToString(value), nil
}

// record returns the record named name.
func (header groupSnapHeader) record(name string) (groupSnapRecord, bool) {
	for _, record := range header.Snapshots {
		if record.Name == name {
			return record, true
		}
	}
	return groupSnapRecord{}, false
}

// parseGroupSnapHeader reads one report of the header reading script. Every
// field a decision rests on must be in the report: the object's name, the
// key list and the record list (either may be empty, neither absent or
// null), each record's ID, name, state and value, and, when update is set,
// the result of the update. A report of a read must carry no result. A
// state of 0 and a result of 0 are observations; a missing or null one is
// not.
func parseGroupSnapHeader(report []byte, update bool) (groupSnapHeader, error) {
	var raw struct {
		Object    *string   `json:"object"`
		Return    *int      `json:"return"`
		Error     string    `json:"error"`
		Keys      *[]string `json:"keys"`
		Snapshots *[]struct {
			ID    *string `json:"id"`
			Name  *string `json:"name"`
			State *int    `json:"state"`
			Value *string `json:"value"`
		} `json:"snapshots"`
	}
	fail := func(format string, args ...any) (groupSnapHeader, error) {
		return groupSnapHeader{}, fmt.Errorf("header report %s: %s", report, fmt.Sprintf(format, args...))
	}
	if err := json.Unmarshal(report, &raw); err != nil {
		return fail("%v", err)
	}
	if raw.Object == nil || *raw.Object == "" || raw.Keys == nil || raw.Snapshots == nil {
		return fail("the object name, the key list or the record list is missing")
	}
	if update != (raw.Return != nil) {
		return fail("result of an update present: %t, want %t", raw.Return != nil, update)
	}
	header := groupSnapHeader{Object: *raw.Object, Error: raw.Error, Keys: *raw.Keys}
	if raw.Return != nil {
		header.Return = *raw.Return
	}
	for _, record := range *raw.Snapshots {
		if record.ID == nil || *record.ID == "" || record.Name == nil || *record.Name == "" || record.State == nil || record.Value == nil {
			return fail("a record lacks its ID, name, state or value")
		}
		if value, err := hex.DecodeString(*record.Value); err != nil || len(value) == 0 {
			return fail("the value of record %s is not a hex string of at least one byte", *record.ID)
		}
		if !slices.Contains(header.Keys, "snapshot_"+*record.ID) {
			return fail("record %s has no key snapshot_%s", *record.ID, *record.ID)
		}
		header.Snapshots = append(header.Snapshots, groupSnapRecord{ID: *record.ID, Name: *record.Name, State: *record.State, Value: *record.Value})
	}
	return header, nil
}

// parseGroupSnapListing reads the JSON listing of a group's snapshots and
// returns their names in its order. The listing must be an array, which may
// be empty, and every entry must have a name.
func parseGroupSnapListing(listing []byte) ([]string, error) {
	var entries *[]struct {
		Snapshot *string `json:"snapshot"`
	}
	if err := json.Unmarshal(listing, &entries); err != nil {
		return nil, fmt.Errorf("listing %s: %w", listing, err)
	}
	if entries == nil {
		return nil, fmt.Errorf("listing %s: not an array", listing)
	}
	names := []string{}
	for _, entry := range *entries {
		if entry.Snapshot == nil || *entry.Snapshot == "" {
			return nil, fmt.Errorf("listing %s: an entry has no name", listing)
		}
		names = append(names, *entry.Snapshot)
	}
	return names, nil
}

// groupSnapOutcome is what the group that got the late update shows after it.
type groupSnapOutcome struct {
	// Return is the result of the late group_snap_set: 0 when the server
	// accepted it, the negative errno when it refused.
	Return int
	// Keys are the omap keys of the group header object, read after the
	// update.
	Keys []string
	// ListExit and ListOutput are the exit code and output of the listing
	// command. Listed holds the snapshot names it printed, in its order,
	// when it exited 0 and its output was read as a listing.
	ListExit   int
	ListOutput string
	Listed     []string
}

// judgeLateComplete decides what a group shows after a COMPLETE update for
// its removed snapshot victimID.
//
// It returns the symptoms of the defect only for one of two positively
// identified observations, each together with the snapshot's record back in
// the header object without its order entry after an accepted update: the
// listing succeeds and names the removed snapshot again beside its sibling,
// or the listing command exits with EINVAL, the status of a listing that
// refuses a record without an order. The command reports that refusal by
// its status alone, so its output decides nothing: an empty output is
// accepted, and a text that names EINVAL beside another status is not. It
// returns neither symptoms nor an error when the listing
// succeeds with exactly the healthy sibling, whether the server accepted the
// update or refused it as invalid. Everything else is an error. An update
// that ended in another way was not delivered. A listing that failed in any
// other way, such as a timeout or a permission error, observed nothing about
// the group, whatever the header object holds. A listing that is wrong
// without the orphan record is another fault.
func judgeLateComplete(victimID string, got groupSnapOutcome) ([]string, error) {
	if victimID == "" {
		return nil, fmt.Errorf("outcome not observed: the removed snapshot has no ID")
	}
	if got.Return != 0 && got.Return != -groupSnapInvalid {
		return nil, fmt.Errorf("trigger not delivered: the late update returned %d, want 0 (accepted) or %d (refused as invalid)", got.Return, -groupSnapInvalid)
	}
	orphan := got.Return == 0 && slices.Contains(got.Keys, "snapshot_"+victimID) && !slices.Contains(got.Keys, "snap_order_"+victimID)
	evidence := fmt.Sprintf("the late update returned %d and the header object has the keys %v", got.Return, got.Keys)
	if got.ListExit != 0 {
		failure := fmt.Sprintf("the listing exited %d: %s", got.ListExit, strings.TrimSpace(got.ListOutput))
		if got.ListExit != groupSnapInvalid {
			return nil, fmt.Errorf("outcome not observed: %s; only a listing that exits %d is the refusal of a record without an order: %s", failure, groupSnapInvalid, evidence)
		}
		if !orphan {
			return nil, fmt.Errorf("another fault: %s, and the removed snapshot has no record without an order entry after an accepted update: %s", failure, evidence)
		}
		return []string{failure, "the record of the removed snapshot is back without its order entry: " + evidence}, nil
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
