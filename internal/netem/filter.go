package netem

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
)

// PeerBand is the prio band of a selective profile's root that leads to its
// netem child; the filters send the selected packets there.
const PeerBand = "3"

// Peer is one selected peer, with the fields of fault.NetworkPeer, which
// converts to it.
type Peer struct {
	Addr netip.Addr
	Port uint16
}

// Match is how a lease's filters read their peers: whether each port is a
// TCP source port rather than a destination port, and whether only segments
// with SYN set are selected.
type Match struct {
	Source, Setup bool
}

// Leaf is one u32 filter of a profile with peers: the filter that selects
// one peer's packets.
type Leaf struct {
	Peer Peer
	// Protocol is tc's protocol of the filter, ip or ipv6.
	Protocol string
	// Pref is the filter's preference, Table the u32 table that the kernel
	// creates for that preference, and Handle the filter's own key node.
	Pref, Table, Handle string
}

// PlanLeaves lays out the filters of a profile with peers: one preference
// per family, IPv4 first, each with its own u32 table, and one key node per
// peer in the caller's order. A u32 filter has one protocol, so the families
// cannot share a preference. The kernel numbers the u32 tables of one qdisc
// from 800: in the order that their preferences are created; the filters are
// added in this order, and each names its table, so a table that the kernel
// numbered otherwise refuses the filter or shows it under another preference,
// and neither is accepted as applied. One peer of one family gives the
// single filter pref 10, handle 800::1.
func PlanLeaves(peers []Peer) []Leaf {
	var leaves []Leaf
	group := 0
	for _, family := range []struct {
		protocol string
		match    func(netip.Addr) bool
	}{{"ip", netip.Addr.Is4}, {"ipv6", netip.Addr.Is6}} {
		node := 0
		for _, peer := range peers {
			if !family.match(peer.Addr) {
				continue
			}
			node++
			table := fmt.Sprintf("%x:", 0x800+group)
			leaves = append(leaves, Leaf{Peer: peer, Protocol: family.protocol, Pref: strconv.Itoa(10 + group),
				Table: table, Handle: fmt.Sprintf("%s:%x", table, node)})
		}
		if node > 0 {
			group++
		}
	}
	return leaves
}

// Key is one 32-bit match of a u32 filter, at a byte offset of the IP
// header, as tc prints it.
type Key struct {
	Off   int
	Value uint32
	Mask  uint32
}

// tcpFlags is the offset of the TCP flags byte in a TCP header, and tcpSYN
// its SYN bit.
const (
	tcpFlags = 13
	tcpSYN   = 0x02
)

// FilterKeys are the u32 keys that select packets to peer and, when port is
// not zero, TCP packets to that destination port, or from that source port
// with match.Source, and with match.Setup only TCP segments with SYN set. tc
// aligns every key to a 32-bit word: the IPv4 protocol is at offset 8, the
// IPv6 next header at offset 4, the destination port is the low half and the
// source port the high half of the word at offset 20 (IPv4) or 40 (IPv6), and
// the SYN bit is in the word at offset 32 (52), right after a header without
// options or extension headers.
func FilterKeys(peer netip.Addr, port uint16, match Match) []Key {
	var keys []Key
	address := peer.AsSlice()
	tcp := 20
	protocol := Key{Off: 8, Value: 6 << 16, Mask: 0xff << 16}
	if peer.Is4() {
		keys = append(keys, Key{Off: 16, Value: binary.BigEndian.Uint32(address), Mask: 0xffffffff})
	} else {
		for word := range 4 {
			keys = append(keys, Key{Off: 24 + 4*word, Value: binary.BigEndian.Uint32(address[4*word:]), Mask: 0xffffffff})
		}
		tcp, protocol = 40, Key{Off: 4, Value: 6 << 8, Mask: 0xff << 8}
	}
	if port != 0 || match.Setup {
		keys = append(keys, protocol)
	}
	switch {
	case port != 0 && match.Source:
		keys = append(keys, Key{Off: tcp, Value: uint32(port) << 16, Mask: 0xffff << 16})
	case port != 0:
		keys = append(keys, Key{Off: tcp, Value: uint32(port), Mask: 0xffff})
	}
	if match.Setup {
		flags := tcp + tcpFlags
		shift := 8 * (3 - flags%4)
		keys = append(keys, Key{Off: flags - flags%4, Value: tcpSYN << shift, Mask: tcpSYN << shift})
	}
	return keys
}

// FilterArgs is the tc command that adds one filter of a profile with peers.
func FilterArgs(device, root string, leaf Leaf, match Match) []string {
	family, bits, tcp := "ip", "/32", 20
	if leaf.Peer.Addr.Is6() {
		family, bits, tcp = "ip6", "/128", 40
	}
	argv := []string{"tc", "filter", "add", "dev", device, "parent", root, "protocol", leaf.Protocol, "pref", leaf.Pref,
		"handle", leaf.Handle, "u32", "match", family, "dst", leaf.Peer.Addr.String() + bits}
	if leaf.Peer.Port != 0 || match.Setup {
		argv = append(argv, "match", family, "protocol", "6", "0xff")
	}
	if leaf.Peer.Port != 0 {
		field := "dport"
		if match.Source {
			field = "sport"
		}
		argv = append(argv, "match", family, field, strconv.Itoa(int(leaf.Peer.Port)), "0xffff")
	}
	if match.Setup {
		argv = append(argv, "match", "u8", fmt.Sprintf("0x%02x", tcpSYN), fmt.Sprintf("0x%02x", tcpSYN), "at", strconv.Itoa(tcp+tcpFlags))
	}
	return append(argv, "flowid", root+PeerBand)
}

// Filter is one entry of tc's filter dump, with the u32 keys that Script
// collected under "matches".
type Filter struct {
	Parent   string `json:"parent"`
	Protocol string `json:"protocol"`
	Pref     int    `json:"pref"`
	Kind     string `json:"kind"`
	Options  struct {
		Handle  string     `json:"fh"`
		FlowID  string     `json:"flowid"`
		Matches []KeyMatch `json:"matches"`
	} `json:"options"`
}

// KeyMatch is one u32 key as tc prints it.
type KeyMatch struct {
	Value   string `json:"value"`
	Mask    string `json:"mask"`
	OffMask string `json:"offmask"`
	Off     int    `json:"off"`
}

// Keys decodes the filter's u32 keys; a key that is not a plain 32-bit
// match is an error.
func (f Filter) Keys() ([]Key, error) {
	var keys []Key
	for _, match := range f.Options.Matches {
		value, err1 := strconv.ParseUint(match.Value, 16, 32)
		mask, err2 := strconv.ParseUint(match.Mask, 16, 32)
		if err1 != nil || err2 != nil || match.OffMask != "" {
			return nil, fmt.Errorf("tc printed the u32 key %+v", match)
		}
		keys = append(keys, Key{Off: match.Off, Value: uint32(value), Mask: uint32(mask)})
	}
	return keys, nil
}

// CheckFilters reports why the egress filters of an interface are not
// exactly the filters of a profile with these peers under root, or nil.
// Header entries, which tc prints for each preference's chain and its u32
// table, may accompany the filters; any other filter is foreign state.
// Unless required, only some of the filters, or none, are also accepted:
// the state of an apply that stopped before its last filter.
func CheckFilters(data json.RawMessage, root string, peers []Peer, match Match, required bool) error {
	var filters []Filter
	if err := json.Unmarshal(data, &filters); err != nil {
		return fmt.Errorf("parse tc filter output: %w", err)
	}
	leaves := PlanLeaves(peers)
	seen := map[string]bool{}
	for _, filter := range filters {
		pref := strconv.Itoa(filter.Pref)
		group := slices.IndexFunc(leaves, func(leaf Leaf) bool { return leaf.Protocol == filter.Protocol && leaf.Pref == pref })
		if filter.Parent != root || group < 0 || filter.Kind != "u32" {
			return fmt.Errorf("a filter with parent %s, protocol %s, preference %d and kind %s is not the profile's", filter.Parent, filter.Protocol, filter.Pref, filter.Kind)
		}
		if len(filter.Options.Matches) == 0 && filter.Options.FlowID == "" {
			// The preference's chain, or its u32 table.
			if filter.Options.Handle != "" && filter.Options.Handle != leaves[group].Table {
				return fmt.Errorf("preference %d has the u32 table %s, not the profile's %s", filter.Pref, filter.Options.Handle, leaves[group].Table)
			}
			continue
		}
		i := slices.IndexFunc(leaves, func(leaf Leaf) bool {
			return leaf.Protocol == filter.Protocol && leaf.Pref == pref && leaf.Handle == filter.Options.Handle
		})
		if i < 0 {
			return fmt.Errorf("the filter %s of preference %d is not one of the profile's", filter.Options.Handle, filter.Pref)
		}
		leaf := leaves[i]
		if seen[leaf.Handle] {
			return fmt.Errorf("the interface has the filter %s twice", leaf.Handle)
		}
		seen[leaf.Handle] = true
		if filter.Options.FlowID != root+PeerBand {
			return fmt.Errorf("the filter %s leads to %s, not %s", leaf.Handle, filter.Options.FlowID, root+PeerBand)
		}
		got, err := filter.Keys()
		if err != nil {
			return fmt.Errorf("the filter %s: %w", leaf.Handle, err)
		}
		if want := FilterKeys(leaf.Peer.Addr, leaf.Peer.Port, match); !slices.Equal(got, want) {
			return fmt.Errorf("the filter %s has the keys %v, not %v", leaf.Handle, got, want)
		}
	}
	if required {
		for _, leaf := range leaves {
			if !seen[leaf.Handle] {
				return fmt.Errorf("the filter %s for %v port %d is missing", leaf.Handle, leaf.Peer.Addr, leaf.Peer.Port)
			}
		}
	}
	return nil
}

// CheckPriorityMap validates selection semantics independently of
// ownership. An in-place edit can divert unselected traffic into netem
// without replacing the root, child or filters that Restore owns.
func CheckPriorityMap(root Qdisc) error {
	var bands int
	var priorities []*int
	if json.Unmarshal(root.Options.Bands, &bands) != nil || bands != 3 ||
		json.Unmarshal(root.Options.Priomap, &priorities) != nil || len(priorities) != 16 {
		return fmt.Errorf("unexpected prio routing: bands %s, priomap %s", root.Options.Bands, root.Options.Priomap)
	}
	for priority, band := range priorities {
		if band == nil || *band != 0 {
			return fmt.Errorf("prio priority %d does not select the unshaped first band: %s", priority, root.Options.Priomap)
		}
	}
	return nil
}
