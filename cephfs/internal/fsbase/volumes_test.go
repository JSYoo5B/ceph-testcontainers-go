package fsbase

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestVolumeQuotaDecodeAndNativeNameListing(t *testing.T) {
	for _, test := range []struct {
		raw     string
		want    int64
		invalid bool
	}{
		{`"infinite"`, 0, false}, {`0`, 0, false}, {`9223372036854775807`, 9223372036854775807, false},
		{`null`, 0, true}, {`"64"`, 0, true}, {`-1`, 0, true}, {`9223372036854775808`, 0, true}, {`1.5`, 0, true},
	} {
		quota, err := decodeVolumeQuota(json.RawMessage(test.raw))
		if (err != nil) != test.invalid || (!test.invalid && quota != test.want) {
			t.Fatalf("quota=%s decoded as %d error=%v", test.raw, quota, err)
		}
	}
	names, err := DecodeVolumeNames([]byte(`[{"name":"z"},{"name":"a"}]`))
	if err != nil || !slices.Equal(names, []string{"a", "z"}) {
		t.Fatalf("listing not sorted: %v error=%v", names, err)
	}
	for _, raw := range []string{`null`, `{}`, `[{}]`, `[{"name":"same"},{"name":"same"}]`} {
		if _, err := DecodeVolumeNames([]byte(raw)); err == nil {
			t.Fatalf("malformed listing accepted: %s", raw)
		}
	}
	if _, err := DecodeVolumeNames([]byte(`[]`)); err != nil {
		t.Fatal(err)
	}
}
