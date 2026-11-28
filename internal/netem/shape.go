package netem

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Shape is the hierarchy that one lease owns on an interface: its root
// handle, and with peers the netem child in PeerBand and one filter per
// peer, read as Match says.
type Shape struct {
	Handle, Child string
	Peers         []Peer
	Match         Match
}

// Step is one tc command of a profile's change.
type Step struct {
	Label string
	Argv  []string
}

// rootKind is the kind of the shape's root qdisc.
func (s Shape) rootKind() string {
	if len(s.Peers) > 0 {
		return "prio"
	}
	return "netem"
}

// Steps are the tc commands that install netem (Params.Arguments) on device
// under the shape's root: netem itself as the root, or, with peers, a prio
// root whose every priority is in band 1, netem on PeerBand and the peer
// filters added last, so nothing is shaped until the filters select the
// peers' packets.
func (s Shape) Steps(device string, netem []string) []Step {
	if len(s.Peers) == 0 {
		return []Step{{"add the netem profile", append([]string{"tc", "qdisc", "add", "dev", device, "root", "handle", s.Handle}, netem...)}}
	}
	priomap := slices.Repeat([]string{"0"}, 16)
	steps := []Step{
		{"add the profile's prio root", append([]string{"tc", "qdisc", "add", "dev", device, "root", "handle", s.Handle, "prio", "bands", "3", "priomap"}, priomap...)},
		{"add the netem profile", append([]string{"tc", "qdisc", "add", "dev", device, "parent", s.Handle + PeerBand, "handle", s.Child}, netem...)},
	}
	leaves := PlanLeaves(s.Peers)
	for i, leaf := range leaves {
		label := "add the peer filter"
		if len(leaves) > 1 {
			label = fmt.Sprintf("add peer filter %d of %d", i+1, len(leaves))
		}
		steps = append(steps, Step{label, FilterArgs(device, s.Handle, leaf, s.Match)})
	}
	return steps
}

// State is what the kernel reports for a shape's interface.
type State struct {
	// RootKind and RootHandle describe the interface's current root qdisc.
	RootKind, RootHandle string
	// Applied means that the shape's own hierarchy is the interface's root,
	// exactly, with nothing foreign under it; Mismatch says why not, for a
	// root that is the shape's.
	Applied  bool
	Mismatch error
	// The rest is the netem qdisc's configuration and counters while
	// Applied. Rate is in bit/s.
	Delay, Jitter                              time.Duration
	Loss                                       float64
	Rate                                       uint64
	Limit                                      int
	Reorder, Duplicate, Corrupt                Fault
	Gap                                        int
	Packets, Bytes, Drops                      uint64
	Overlimits, Requeues, Backlog, QueueLength uint64
}

// Inspect judges the qdiscs and filters of the inspected interface against
// the shape.
func (s Shape) Inspect(report Report) (State, error) {
	root, err := report.Root()
	if err != nil {
		return State{}, err
	}
	state := State{RootKind: root.Kind, RootHandle: root.Handle}
	netem := root
	switch {
	case len(s.Peers) == 0:
		if root.Kind == "netem" && root.Handle == s.Handle {
			state.Mismatch = s.OwnedPrefix(report.Qdiscs, report.Filters)
			state.Applied = state.Mismatch == nil
		}
	case root.Kind == "prio" && root.Handle == s.Handle:
		// The profile is applied only as the whole hierarchy: the root, its
		// netem child in the selected band, and the filters to it.
		var qdiscs []Qdisc
		if err := json.Unmarshal(report.Qdiscs, &qdiscs); err != nil {
			return State{}, fmt.Errorf("parse tc qdisc output: %w", err)
		}
		children := 0
		for _, qdisc := range qdiscs {
			if qdisc.Kind == "netem" && qdisc.Handle == s.Child && qdisc.Parent == s.Handle+PeerBand {
				netem, children = qdisc, children+1
			}
		}
		state.Mismatch = errors.Join(CheckPriorityMap(root), CheckFilters(report.Filters, s.Handle, s.Peers, s.Match, true))
		if children != 1 {
			state.Mismatch = errors.Join(state.Mismatch, fmt.Errorf("the prio root has no netem child %s in band %s", s.Child, s.Handle+PeerBand))
		} else if err := s.OwnedPrefix(report.Qdiscs, report.Filters); state.Mismatch == nil && err != nil {
			state.Mismatch = err
		}
		state.Applied = state.Mismatch == nil
	}
	if state.Applied {
		options := netem.Options
		state.Delay, state.Jitter = Seconds(options.Delay.Delay), Seconds(options.Delay.Jitter)
		state.Loss = Percent(options.Loss.Loss)
		state.Packets, state.Bytes, state.Drops = netem.Packets, netem.Bytes, netem.Drops
		state.Overlimits, state.Requeues, state.Backlog, state.QueueLength = netem.Overlimits, netem.Requeues, netem.Backlog, netem.Qlen
		state.Rate, state.Limit = options.Rate.Rate*8, options.Limit
		state.Reorder, state.Duplicate = options.Reorder.Fault("reorder"), options.Duplicate.Fault("duplicate")
		state.Corrupt, state.Gap = options.Corrupt.Fault("corrupt"), int(options.Gap)
	}
	return state, nil
}

// OwnedPrefix checks that the qdiscs and filters under the shape's root are
// a prefix of what applying it adds: the root, and for a selective profile
// its netem child in the selected band and then the filters. Removing the
// root removes everything under it, so anything else there is foreign;
// qdiscs outside the root, such as an ingress qdisc, are not.
func (s Shape) OwnedPrefix(qdiscData, filterData json.RawMessage) error {
	var qdiscs []Qdisc
	if err := json.Unmarshal(qdiscData, &qdiscs); err != nil {
		return fmt.Errorf("parse tc qdisc output: %w", err)
	}
	rootKind := s.rootKind()
	// A qdisc is under the root when its parent is the root or a qdisc under
	// it; tc does not promise to list a parent first.
	under := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, qdisc := range qdiscs {
			major, _, _ := strings.Cut(qdisc.Handle, ":")
			parent, _, _ := strings.Cut(qdisc.Parent, ":")
			if !under[major] && (qdisc.Root || under[parent]) {
				under[major], changed = true, true
			}
		}
	}
	for _, qdisc := range qdiscs {
		if major, _, _ := strings.Cut(qdisc.Handle, ":"); !under[major] {
			continue
		}
		switch {
		case qdisc.Root && qdisc.Kind == rootKind && qdisc.Handle == s.Handle:
		case len(s.Peers) > 0 && !qdisc.Root && qdisc.Kind == "netem" && qdisc.Handle == s.Child && qdisc.Parent == s.Handle+PeerBand:
		case qdisc.Root:
			return fmt.Errorf("the root is %s %s, not the profile's %s", qdisc.Kind, qdisc.Handle, rootKind)
		default:
			return fmt.Errorf("a %s qdisc %s under %s", qdisc.Kind, qdisc.Handle, qdisc.Parent)
		}
	}
	if len(s.Peers) > 0 {
		return CheckFilters(filterData, s.Handle, s.Peers, s.Match, false)
	}
	var filters []Filter
	if err := json.Unmarshal(filterData, &filters); err != nil {
		return fmt.Errorf("parse tc filter output: %w", err)
	}
	if len(filters) > 0 {
		return fmt.Errorf("%d filters under the root", len(filters))
	}
	return nil
}
