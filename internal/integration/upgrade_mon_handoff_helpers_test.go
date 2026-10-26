//go:build all || integration

package integration_test

import (
	"fmt"
	"net/netip"
	"sort"
	"strconv"
)

// The types below model, without Ceph or Docker, the address handoff of a
// retained MON store to a destination-image writer in a private mixed-version
// upgrade harness. mon-join.sh updates the bootstrap map and runs mkfs only
// when store.db is absent. With a retained store it runs ceph-mon directly, so
// the endpoint the destination writer binds has to be the one the retained
// monmap already records. The handoff therefore keeps the complete expected
// address vector with the store identity and refuses a launch before any
// writer starts when the reserved endpoint differs. Ordinary AddMonitor,
// RemoveMonitor and release admission are not involved.

// upgradeMonAddr is one entry of a MON address vector: a host with its msgr2
// port and its msgr1 port (0 when the MON has no v1 listener).
type upgradeMonAddr struct {
	IP netip.Addr
	V2 uint16
	V1 uint16
}

func (a upgradeMonAddr) String() string {
	return a.IP.String() + "/v2=" + strconv.Itoa(int(a.V2)) + "/v1=" + strconv.Itoa(int(a.V1))
}

// upgradeMonVector is a MON's complete address vector.
type upgradeMonVector []upgradeMonAddr

// validate rejects a vector that cannot be compared: empty, an invalid
// address, a missing v2 port, or a duplicated entry. None of these is read as
// an empty-but-matching vector.
func (v upgradeMonVector) validate() error {
	if len(v) == 0 {
		return fmt.Errorf("address vector is empty")
	}
	seen := map[netip.Addr]bool{}
	for _, a := range v {
		if !a.IP.IsValid() {
			return fmt.Errorf("address vector has an invalid address")
		}
		if a.V2 == 0 {
			return fmt.Errorf("address %s has no msgr2 port", a.IP)
		}
		if a.V1 != 0 && a.V1 == a.V2 {
			return fmt.Errorf("address %s uses one port for msgr1 and msgr2", a.IP)
		}
		if seen[a.IP] {
			return fmt.Errorf("address vector repeats %s", a.IP)
		}
		seen[a.IP] = true
	}
	return nil
}

// sameSet reports whether both vectors hold exactly the same entries, in any
// order. Families, addresses and both ports must all match.
func (v upgradeMonVector) sameSet(other upgradeMonVector) bool {
	if len(v) != len(other) {
		return false
	}
	have := map[upgradeMonAddr]int{}
	for _, a := range v {
		have[a]++
	}
	for _, a := range other {
		if have[a] == 0 {
			return false
		}
		have[a]--
	}
	return true
}

func (v upgradeMonVector) describe() string {
	parts := make([]string, 0, len(v))
	for _, a := range v {
		parts = append(parts, a.String())
	}
	sort.Strings(parts)
	return fmt.Sprint(parts)
}

// upgradeMonRetained is what a retained MON store owns across a transition.
type upgradeMonRetained struct {
	Name    string
	FSID    string
	StoreID string
	Addrs   upgradeMonVector
}

// upgradeMonLaunch is a destination launch request for a retained MON store.
type upgradeMonLaunch struct {
	Transition string
	Name       string
	FSID       string
	StoreID    string
	HasStore   bool
	Reserved   upgradeMonVector
}

// upgradeMonPlan is the first-start configuration an accepted request
// receives. Mkfs, MonmapUpdate and MembershipChange stay false: a retained
// store is never re-created or re-addressed by the handoff.
type upgradeMonPlan struct {
	Env              map[string]string
	Mkfs             bool
	MonmapUpdate     bool
	MembershipChange bool
}

// upgradeMonHandoff admits at most one destination launch per transition.
type upgradeMonHandoff struct {
	retained upgradeMonRetained
	admitted map[string]bool
}

// newUpgradeMonHandoff validates the retained record itself. The bounded
// experiment starts with one address-preserving IPv4 bridge endpoint per MON,
// so a vector of any other shape is refused here; address migration and
// host-port handoff need a separate plan.
func newUpgradeMonHandoff(r upgradeMonRetained) (*upgradeMonHandoff, error) {
	switch {
	case r.Name == "":
		return nil, fmt.Errorf("retained MON has no name")
	case r.FSID == "":
		return nil, fmt.Errorf("retained MON %s has no FSID", r.Name)
	case r.StoreID == "":
		return nil, fmt.Errorf("retained MON %s has no store identity", r.Name)
	}
	if err := r.Addrs.validate(); err != nil {
		return nil, fmt.Errorf("retained MON %s: %w", r.Name, err)
	}
	if len(r.Addrs) != 1 || !r.Addrs[0].IP.Is4() {
		return nil, fmt.Errorf("retained MON %s: the bounded experiment supports one IPv4 endpoint, got %s", r.Name, r.Addrs.describe())
	}
	return &upgradeMonHandoff{retained: r, admitted: map[string]bool{}}, nil
}

// admit decides a launch before any writer exists. A rejected request changes
// no state, so it can be corrected and presented again.
func (h *upgradeMonHandoff) admit(req upgradeMonLaunch) (upgradeMonPlan, error) {
	r := h.retained
	switch {
	case req.Transition == "":
		return upgradeMonPlan{}, fmt.Errorf("launch request has no transition identity")
	case h.admitted[req.Transition]:
		return upgradeMonPlan{}, fmt.Errorf("transition %q already admitted a writer for MON %s", req.Transition, r.Name)
	case req.Name != r.Name:
		return upgradeMonPlan{}, fmt.Errorf("launch names MON %q, retained store belongs to %q", req.Name, r.Name)
	case req.FSID != r.FSID:
		return upgradeMonPlan{}, fmt.Errorf("launch FSID %q differs from the retained FSID", req.FSID)
	case req.StoreID != r.StoreID:
		return upgradeMonPlan{}, fmt.Errorf("launch store %q differs from the retained store", req.StoreID)
	case !req.HasStore:
		// Without store.db the script would run mkfs and a monmap update.
		return upgradeMonPlan{}, fmt.Errorf("retained MON %s has no store.db at launch; the handoff never creates a store", r.Name)
	}
	if err := req.Reserved.validate(); err != nil {
		return upgradeMonPlan{}, fmt.Errorf("reserved endpoint: %w", err)
	}
	if !req.Reserved.sameSet(r.Addrs) {
		return upgradeMonPlan{}, fmt.Errorf("reserved endpoint %s differs from the retained address vector %s", req.Reserved.describe(), r.Addrs.describe())
	}
	h.admitted[req.Transition] = true
	a := r.Addrs[0]
	env := map[string]string{
		"CEPH_MON_ID":         r.Name,
		"CEPH_ADDRESS_FAMILY": "ipv4",
		"CEPH_PUBLIC_ADDRESS": a.IP.String(),
		"CEPH_MON_PORT_V2":    strconv.Itoa(int(a.V2)),
	}
	if a.V1 != 0 {
		env["CEPH_MON_PORT_V1"] = strconv.Itoa(int(a.V1))
	} else {
		env["CEPH_MSGR2_ONLY"] = "true"
	}
	return upgradeMonPlan{Env: env}, nil
}
