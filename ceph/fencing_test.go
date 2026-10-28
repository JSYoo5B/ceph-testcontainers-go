package ceph

import "testing"

// The tests of the blocklist override moved to the override package, which
// runs them against servicehook.UnitCluster. The tests here keep the session
// normalization and the decoder, which stay in this package.

const blocklistTestAddress = "192.0.2.7:0/1234"

func TestClientBlocklistExactSessionNormalization(t *testing.T) {
	for input, want := range map[string]string{
		blocklistTestAddress:                        blocklistTestAddress,
		"[v2:192.0.2.7:0/1234,v1:192.0.2.7:0/1234]": blocklistTestAddress,
		"v2:[2001:db8::1]:0/1234":                   "[2001:db8::1]:0/1234",
	} {
		got, err := normalizeBlocklistSession(input, true)
		if err != nil || got != want {
			t.Fatalf("%s: %q %v", input, got, err)
		}
	}
	for _, invalid := range []string{"192.0.2.7", "192.0.2.7:0", "192.0.2.7:0/0", "192.0.2.0/24", "0.0.0.0:0/1", "224.0.0.1:0/1", "192.0.2.7:70000/1", "192.0.2.7:0/4294967296", "[v2:192.0.2.7:0/1,v1:192.0.2.7:0/2]", "[v2:192.0.2.7:0/1"} {
		if _, err := normalizeBlocklistSession(invalid, true); err == nil {
			t.Fatal("unsafe fencing address accepted:", invalid)
		}
	}
}

func TestBlocklistDecodePreservesRangesAndRejectsMalformedState(t *testing.T) {
	data := []byte(`[{"addr":"192.0.2.7:0/1234","until":"2026-10-03T08:00:00.123456+0000"}][{"range":"198.51.100.0:0/24","until":"2026-10-03T08:00:00.123456+0000"}]`)
	entries, err := decodeBlocklistEntries(data)
	if err != nil || len(entries) != 2 || !entries[1].Range || entries[0].Until.Nanosecond() != 123456000 {
		t.Fatalf("native arrays: %+v %v", entries, err)
	}
	for _, malformed := range []string{"", "null", "[] [] []", "[] {}", `[{"addr":"192.0.2.7:0/1","until":"invalid"}]`, `[{"addr":"192.0.2.7:0/1","until":"2026-10-03T08:00:00Z"},{"addr":"v2:192.0.2.7:0/1","until":"2026-10-03T08:00:00Z"}]`} {
		if _, err := decodeBlocklistEntries([]byte(malformed)); err == nil {
			t.Fatal("malformed native blocklist accepted:", malformed)
		}
	}
}
