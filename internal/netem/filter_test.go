package netem

import (
	"encoding/json"
	"net/netip"
	"slices"
	"strings"
	"testing"
)

// Filter dumps that tc 6.1 printed for these keys, with
// the repeated "match" members collected under "matches" as Script does.
const (
	nativeIPv4PeerFilter = `[{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0},{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0,"options":{"fh":"800:","ht_divisor":1}},{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0,"options":{"fh":"800::1","order":1,"key_ht":"800","bkt":"0","flowid":"7e00:3","not_in_hw":true,"matches":[{"value":"ac130003","mask":"ffffffff","offmask":"","off":16},{"value":"60000","mask":"ff0000","offmask":"","off":8},{"value":"1b58","mask":"ffff","offmask":"","off":20}]}}]`
	nativeIPv6PeerFilter = `[{"parent":"7e00:","protocol":"ipv6","pref":10,"kind":"u32","chain":0,"options":{"fh":"800::1","order":1,"key_ht":"800","bkt":"0","flowid":"7e00:3","not_in_hw":true,"matches":[{"value":"fd6293fe","mask":"ffffffff","offmask":"","off":24},{"value":"2c9e0000","mask":"ffffffff","offmask":"","off":28},{"value":"0","mask":"ffffffff","offmask":"","off":32},{"value":"3","mask":"ffffffff","offmask":"","off":36},{"value":"600","mask":"ff00","offmask":"","off":4},{"value":"1b58","mask":"ffff","offmask":"","off":40}]}}]`
)

func TestCheckPeerFilterAgainstNativeDumps(t *testing.T) {
	v4, v6 := netip.MustParseAddr("172.19.0.3"), netip.MustParseAddr("fd62:93fe:2c9e::3")
	if err := CheckFilters(json.RawMessage(nativeIPv4PeerFilter), "7e00:", []Peer{{v4, 7000}}, Match{}, true); err != nil {
		t.Fatalf("IPv4: %v", err)
	}
	if err := CheckFilters(json.RawMessage(nativeIPv6PeerFilter), "7e00:", []Peer{{v6, 7000}}, Match{}, true); err != nil {
		t.Fatalf("IPv6: %v", err)
	}
	for name, test := range map[string]struct {
		dump string
		root string
		peer netip.Addr
		port uint16
	}{
		"another peer":           {nativeIPv4PeerFilter, "7e00:", netip.MustParseAddr("172.19.0.4"), 7000},
		"another port":           {nativeIPv4PeerFilter, "7e00:", v4, 7001},
		"no port selected":       {nativeIPv4PeerFilter, "7e00:", v4, 0},
		"another root":           {nativeIPv4PeerFilter, "7e01:", v4, 7000},
		"another family":         {nativeIPv4PeerFilter, "7e00:", v6, 7000},
		"a wider protocol mask":  {strings.Replace(nativeIPv4PeerFilter, `"mask":"ff0000"`, `"mask":"ffff0000"`, 1), "7e00:", v4, 7000},
		"an offmask":             {strings.Replace(nativeIPv4PeerFilter, `"offmask":"","off":20`, `"offmask":"ff","off":20`, 1), "7e00:", v4, 7000},
		"another flowid":         {strings.Replace(nativeIPv4PeerFilter, `"flowid":"7e00:3"`, `"flowid":"7e00:1"`, 1), "7e00:", v4, 7000},
		"a missing address word": {strings.Replace(nativeIPv6PeerFilter, `{"value":"0","mask":"ffffffff","offmask":"","off":32},`, ``, 1), "7e00:", v6, 7000},
		"two leaves":             {strings.TrimSuffix(nativeIPv4PeerFilter, "]") + "," + strings.TrimPrefix(nativeIPv4PeerFilter, "[{\"parent\":\"7e00:\",\"protocol\":\"ip\",\"pref\":10,\"kind\":\"u32\",\"chain\":0},{\"parent\":\"7e00:\",\"protocol\":\"ip\",\"pref\":10,\"kind\":\"u32\",\"chain\":0,\"options\":{\"fh\":\"800:\",\"ht_divisor\":1}},"), "7e00:", v4, 7000},
		"a foreign filter":       {strings.Replace(nativeIPv4PeerFilter, `[{"parent":"7e00:","protocol":"ip","pref":10,`, `[{"parent":"7e00:","protocol":"ip","pref":20,`, 1), "7e00:", v4, 7000},
		"no filter":              {`[]`, "7e00:", v4, 7000},
	} {
		if err := CheckFilters(json.RawMessage(test.dump), test.root, []Peer{{test.peer, test.port}}, Match{}, true); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// nativeIPv4PeerFilters is nativeIPv4PeerFilter with a second key node in
// the same table, for 172.19.0.4 on any port.
var nativeIPv4PeerFilters = strings.TrimSuffix(nativeIPv4PeerFilter, "]") +
	`,{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0,"options":{"fh":"800::2","order":2,"key_ht":"800","bkt":"0","flowid":"7e00:3","not_in_hw":true,"matches":[{"value":"ac130004","mask":"ffffffff","offmask":"","off":16}]}}]`

func TestCheckPeerFiltersOfSeveralPeers(t *testing.T) {
	first := Peer{Addr: netip.MustParseAddr("172.19.0.3"), Port: 7000}
	second := Peer{Addr: netip.MustParseAddr("172.19.0.4")}
	peers := []Peer{first, second}
	if err := CheckFilters(json.RawMessage(nativeIPv4PeerFilters), "7e00:", peers, Match{}, true); err != nil {
		t.Fatal(err)
	}
	// Only the first filter, as after an apply that stopped before the
	// second: owned, but not applied.
	if err := CheckFilters(json.RawMessage(nativeIPv4PeerFilter), "7e00:", peers, Match{}, false); err != nil {
		t.Fatalf("a prefix: %v", err)
	}
	if err := CheckFilters(json.RawMessage(nativeIPv4PeerFilter), "7e00:", peers, Match{}, true); err == nil {
		t.Fatal("a missing filter was accepted")
	}
	for name, dump := range map[string]string{
		"swapped peers":        strings.Replace(strings.Replace(nativeIPv4PeerFilters, `"fh":"800::2"`, `"fh":"800::X"`, 1), `"fh":"800::1"`, `"fh":"800::2"`, 1),
		"another table":        strings.Replace(nativeIPv4PeerFilters, `"fh":"800:","ht_divisor":1`, `"fh":"802:","ht_divisor":1`, 1),
		"a third key node":     strings.Replace(nativeIPv4PeerFilters, `"fh":"800::2"`, `"fh":"800::3"`, 1),
		"the same node twice":  strings.Replace(nativeIPv4PeerFilters, `"fh":"800::2","order":2`, `"fh":"800::1","order":2`, 1),
		"second to band 1":     strings.Replace(nativeIPv4PeerFilters, `"order":2,"key_ht":"800","bkt":"0","flowid":"7e00:3"`, `"order":2,"key_ht":"800","bkt":"0","flowid":"7e00:1"`, 1),
		"second in pref 11":    strings.Replace(nativeIPv4PeerFilters, `"pref":10,"kind":"u32","chain":0,"options":{"fh":"800::2"`, `"pref":11,"kind":"u32","chain":0,"options":{"fh":"800::2"`, 1),
		"second with a port":   strings.Replace(nativeIPv4PeerFilters, `"value":"ac130004","mask":"ffffffff","offmask":"","off":16}`, `"value":"ac130004","mask":"ffffffff","offmask":"","off":16},{"value":"1b58","mask":"ffff","offmask":"","off":20}`, 1),
		"second to another IP": strings.Replace(nativeIPv4PeerFilters, `"ac130004"`, `"ac130005"`, 1),
	} {
		if err := CheckFilters(json.RawMessage(dump), "7e00:", peers, Match{}, true); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The IPv6 filter of a mixed selection is in preference 11 and table
	// 801:, not in the single-family 10 and 800:.
	v6 := Peer{Addr: netip.MustParseAddr("fd62:93fe:2c9e::3"), Port: 7000}
	if err := CheckFilters(json.RawMessage(nativeIPv6PeerFilter), "7e00:", []Peer{first, v6}, Match{}, true); err == nil {
		t.Fatal("an IPv6 filter in preference 10 was accepted for a mixed selection")
	}
	mixed := strings.TrimSuffix(nativeIPv4PeerFilter, "]") + "," + strings.TrimPrefix(
		strings.NewReplacer(`"pref":10`, `"pref":11`, `"fh":"800::1"`, `"fh":"801::1"`, `"key_ht":"800"`, `"key_ht":"801"`).Replace(nativeIPv6PeerFilter), "[")
	if err := CheckFilters(json.RawMessage(mixed), "7e00:", []Peer{first, v6}, Match{}, true); err != nil {
		t.Fatalf("mixed: %v", err)
	}
}

func TestPeerFilterKeysSourcePortAndSetup(t *testing.T) {
	v4, v6 := netip.MustParseAddr("10.0.0.2"), netip.MustParseAddr("fd00::2")
	tests := []struct {
		name  string
		peer  netip.Addr
		port  uint16
		match Match
		tail  []Key
	}{
		{"ipv4 source port", v4, 7000, Match{Source: true}, []Key{{8, 6 << 16, 0xff << 16}, {20, 7000 << 16, 0xffff0000}}},
		{"ipv6 source port", v6, 7000, Match{Source: true}, []Key{{4, 6 << 8, 0xff << 8}, {40, 7000 << 16, 0xffff0000}}},
		{"ipv4 setup", v4, 7000, Match{Setup: true}, []Key{{8, 6 << 16, 0xff << 16}, {20, 7000, 0xffff}, {32, 0x00020000, 0x00020000}}},
		{"ipv6 setup", v6, 7000, Match{Setup: true}, []Key{{4, 6 << 8, 0xff << 8}, {40, 7000, 0xffff}, {52, 0x00020000, 0x00020000}}},
		{"ipv4 setup without a port", v4, 0, Match{Setup: true}, []Key{{8, 6 << 16, 0xff << 16}, {32, 0x00020000, 0x00020000}}},
		{"ipv4 source setup", v4, 6800, Match{Source: true, Setup: true}, []Key{{8, 6 << 16, 0xff << 16}, {20, 6800 << 16, 0xffff0000}, {32, 0x00020000, 0x00020000}}},
		{"ipv4 source without a port", v4, 0, Match{Source: true}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keys := FilterKeys(test.peer, test.port, test.match)
			address := 1
			if test.peer.Is6() {
				address = 4
			}
			if got := keys[address:]; !slices.Equal(got, test.tail) {
				t.Fatalf("keys after the address %v, want %v", got, test.tail)
			}
		})
	}
}

func TestFaultMembersOfAForeignQdisc(t *testing.T) {
	var qdisc Qdisc
	if err := json.Unmarshal([]byte(`{"kind":"foreign","options":{"duplicate":3,"corrupt":true,"reorder":"x","gap":"none"}}`), &qdisc); err != nil {
		t.Fatal(err)
	}
	if qdisc.Options.Duplicate.Fault("duplicate") != (Fault{}) || qdisc.Options.Gap != 0 {
		t.Fatalf("decoded %+v", qdisc.Options)
	}
	if err := json.Unmarshal([]byte(`{"kind":"netem","options":{"gap":1.5,"reorder":{"reorder":0.25,"correlation":0.5}}}`), &qdisc); err != nil {
		t.Fatal(err)
	}
	if qdisc.Options.Gap != 0 || qdisc.Options.Reorder.Fault("reorder") != (Fault{Percent: 25, Correlation: 50}) {
		t.Fatalf("decoded %+v", qdisc.Options)
	}
}
