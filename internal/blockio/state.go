package blockio

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// View is one reply of blockControlScript.
type View struct {
	Action  string `json:"action"`
	Name    string `json:"name"`
	Created bool   `json:"created"`
	Error   string `json:"error"`
	// observe
	Exists   bool    `json:"exists"`
	State    string  `json:"state"`
	UUID     string  `json:"uuid"`
	Open     int     `json:"open"`
	Event    int     `json:"event"`
	Major    uint32  `json:"major"`
	Minor    uint32  `json:"minor"`
	Live     *string `json:"live"`
	Inactive *string `json:"inactive"`
	InFlight []int64 `json:"in_flight"`
	// step, settled, written
	Op   string `json:"op"`
	ID   string `json:"id"`
	Done bool   `json:"done"`
	RC   int    `json:"rc"`
	// write, read
	SHA256  string `json:"sha256"`
	Started bool   `json:"started"`
	// loop, loopid, unloop
	Loop        string   `json:"loop"`
	File        string   `json:"file"`
	Inode       uint64   `json:"inode"`
	Detached    bool     `json:"detached"`
	FileRemoved bool     `json:"file_removed"`
	Holders     []string `json:"holders"`
	// Lazy is a detach the kernel accepted for a busy loop and has not done.
	Lazy bool `json:"lazy"`
}

func Parse(raw []byte, action string) (View, error) {
	var r View
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("control %s reply: %w: %s", action, err, raw)
	}
	if r.Action != action {
		return r, fmt.Errorf("control reply is for %q, want %q", r.Action, action)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return r, err
	}
	required := []string{"action", "name"}
	switch action {
	case "observe":
		required = append(required, "exists")
		if r.Exists {
			required = append(required, "state", "uuid", "open", "event", "major", "minor", "in_flight")
			for _, name := range []string{"live", "inactive"} {
				if _, present := fields[name]; !present {
					return r, fmt.Errorf("control observe reply lacks %s", name)
				}
			}
		} else if r.Error != "" {
			return r, fmt.Errorf("control observe did not establish absence: %s", r.Error)
		}
	case "create":
		required = append(required, "created", "error")
	case "step", "settled":
		required = append(required, "id", "done")
		if action == "step" {
			required = append(required, "op")
		}
		if r.Done {
			required = append(required, "rc", "error")
		}
	case "loop":
		required = append(required, "loop", "file", "inode", "error")
		if r.Loop != "" {
			required = append(required, "major", "minor")
		}
	case "loopid":
		if r.Error == "" {
			required = append(required, "file", "inode", "holders")
		}
	case "unloop":
		required = append(required, "holders", "detached", "error")
	case "rmfile":
		required = append(required, "file_removed")
	case "write":
		required = append(required, "sha256", "started")
	case "written":
		required = append(required, "done")
		if r.Done {
			required = append(required, "rc", "error")
		}
	case "read":
		required = append(required, "rc", "sha256")
	case "hold":
		required = append(required, "started")
	default:
		return r, fmt.Errorf("unknown control reply action %q", action)
	}
	for _, name := range required {
		value, present := fields[name]
		if !present || string(value) == "null" {
			return r, fmt.Errorf("control %s reply lacks non-null %s", action, name)
		}
	}
	if action == "observe" && r.Exists {
		var counters []*int64
		if err := json.Unmarshal(fields["in_flight"], &counters); err != nil || len(counters) != 2 || counters[0] == nil || counters[1] == nil {
			return r, fmt.Errorf("control observe reply lacks two non-null in-flight counters")
		}
	}
	return r, nil
}

// Record is what the control knows about the node it owns: its identity,
// the live table it last confirmed, and a step it started without seeing it
// finish.
type Record struct {
	Name, UUID   string
	Major, Minor uint32
	// Live is the live table the control last observed after a step it
	// confirmed, or after creating the node.
	Live string
	// Loaded is the table the control loaded into the inactive slot and has
	// not seen swapped in, or "".
	Loaded string
	// Unsettled is a step the control may have started and has not seen
	// finish, or "". It is recorded before the step is dispatched, so a lost
	// reply or an ended context leaves it here, with the table of a load
	// (UnsettledTable) and the id of its status file (UnsettledID).
	Unsettled, UnsettledTable, UnsettledID string
}

// Begin records a step before it is dispatched.
func (rec *Record) Begin(op, table, id string) {
	rec.Unsettled, rec.UnsettledTable, rec.UnsettledID = op, table, id
}

// Settle records the reply of the recorded step: nothing while it is not
// done, and once it is, what a successful step changed.
func (rec *Record) Settle(r View) {
	if !r.Done {
		return
	}
	op, table := rec.Unsettled, rec.UnsettledTable
	rec.Begin("", "", "")
	if r.RC != 0 {
		return
	}
	switch op {
	case "load":
		rec.Loaded = table
	case "clear":
		rec.Loaded = ""
	case "resume":
		if rec.Loaded != "" {
			rec.Live, rec.Loaded = rec.Loaded, ""
		}
	}
}

// The states of an owned node, as classified against its record and the table
// the control wants live.
const (
	Gone            = "gone"             // no node of that name
	IdentityChanged = "identity-changed" // the name now has another UUID or device number
	Foreign         = "foreign"          // a table neither the record nor the wanted one, live or inactive
	StepUnsettled   = "step-unsettled"   // a step the control started is still running in the kernel
	Applied         = "applied"          // the wanted table is live, active, nothing inactive
	Unchanged       = "unchanged"        // the recorded table is live, active, nothing inactive
	Loaded          = "loaded"           // recorded table live, wanted table inactive, active
	Suspended       = "suspended"        // recorded table live, nothing inactive, suspended
	SuspendedLoaded = "suspended-loaded" // recorded table live, wanted table inactive, suspended
	SwappedOnHold   = "swapped-on-hold"  // wanted table live, suspended
	// LoadRefused is what apply returns when the kernel refused the wanted
	// table; the node keeps the recorded one.
	LoadRefused = "load-refused"
)

// Normalize makes table strings comparable: dmsetup prints a trailing space
// after a target without arguments.
func Normalize(s string) string { return strings.Join(strings.Fields(s), " ") }

// Classify classifies an observation of the node of rec against rec and the
// wanted live table. Identity comes first: a node that is not the recorded one
// is never acted on, whatever its tables. A step the control has not seen
// finish comes next: until it ends, any other step could block behind it or
// race it.
func Classify(rec Record, want string, v View) string {
	if !v.Exists {
		return Gone
	}
	if v.UUID != rec.UUID || v.Major != rec.Major || v.Minor != rec.Minor {
		return IdentityChanged
	}
	if rec.Unsettled != "" {
		return StepUnsettled
	}
	live, inactive := "", ""
	if v.Live != nil {
		live = Normalize(*v.Live)
	}
	if v.Inactive != nil {
		inactive = Normalize(*v.Inactive)
	}
	recorded, wanted := Normalize(rec.Live), Normalize(want)
	suspended := v.State == "SUSPENDED"
	switch {
	case v.State != "ACTIVE" && !suspended:
		return Foreign
	case inactive != "" && inactive != wanted:
		return Foreign
	case inactive != "" && inactive != Normalize(rec.Loaded):
		// The wanted table is inactive, but it is not the table the control
		// loaded: the control loaded none, or another actor replaced it.
		return Foreign
	case live == wanted && inactive == "" && !suspended:
		return Applied
	case live == wanted && inactive == "" && suspended:
		return SwappedOnHold
	case live != recorded:
		return Foreign
	case inactive == "" && !suspended:
		return Unchanged
	case inactive == "" && suspended:
		return Suspended
	case !suspended:
		return Loaded
	}
	return SuspendedLoaded
}

// Forward is the next step that moves a node in state toward the wanted
// table, or "" when it is there or when nothing may be done.
func Forward(state string) string {
	switch state {
	case Unchanged, Suspended:
		return "load"
	case Loaded:
		return "suspend"
	case SuspendedLoaded, SwappedOnHold:
		return "resume"
	}
	return ""
}

// Refuses reports the states in which the control does nothing at all to
// the node: it is not the owned node, or another actor or an unsettled step
// holds it.
func Refuses(state string) bool {
	return slices.Contains([]string{Gone, IdentityChanged, Foreign, StepUnsettled}, state)
}

// Release verdicts of an owned node.
const (
	Releasable  = "releasable"
	ReleaseHeld = "held"
)

// JudgeRelease decides whether the owned node may be removed: it must be the
// recorded node, with the recorded table live and active, no inactive table,
// no unsettled step, no opener and no I/O in flight. Anything else keeps the
// node, its loop and its file, with the reason.
func JudgeRelease(rec Record, v View) (string, string) {
	state := Classify(rec, rec.Live, v)
	switch {
	case state != Applied:
		return ReleaseHeld, "state " + state
	case v.Open != 0:
		return ReleaseHeld, fmt.Sprintf("%d openers", v.Open)
	case len(v.InFlight) != 2:
		return ReleaseHeld, "in-flight counters not read"
	case v.InFlight[0] != 0 || v.InFlight[1] != 0:
		return ReleaseHeld, fmt.Sprintf("%d reads and %d writes in flight", v.InFlight[0], v.InFlight[1])
	}
	return Releasable, "recorded table live, idle"
}

// LoopIdentity is a loop device as the control attached it: the loop node,
// its device number and the inode of its backing file.
type LoopIdentity struct {
	Loop  string
	File  string
	Inode uint64
	// Dev is the loop's major:minor, the lower device of mappings over it.
	Dev string
}

// JudgeLoopRelease allows detaching loop only while it still backs the file
// the control attached, nothing holds it, and no mapping the fixture built on
// it was kept or is in an unknown state (kept): a loop number the kernel gave
// to another file after a release is someone else's, and a loop under a
// remaining mapping is still in use.
func JudgeLoopRelease(own LoopIdentity, seen View, kept []string) (bool, string) {
	switch {
	case seen.Error != "":
		return false, "loop not readable: " + seen.Error
	case seen.File != own.File || seen.Inode != own.Inode:
		return false, fmt.Sprintf("%s now backs %s (inode %d), not %s (inode %d)", own.Loop, seen.File, seen.Inode, own.File, own.Inode)
	case len(kept) != 0:
		return false, fmt.Sprintf("%s is under kept mappings %v", own.Loop, kept)
	case len(seen.Holders) != 0:
		return false, fmt.Sprintf("%s is held by %v", own.Loop, seen.Holders)
	}
	return true, "still backs the attached file, unheld"
}
