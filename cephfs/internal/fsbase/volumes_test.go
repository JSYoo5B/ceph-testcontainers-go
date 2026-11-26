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

func TestVolumeUsageRequiresAMeasuredNumber(t *testing.T) {
	base := `{"path":"/volumes/owned","created_at":"2026-10-03 00:00:00","data_pool":"data"}`
	for _, test := range []struct {
		name, usage string
		valid       bool
	}{
		{"missing", "", false}, {"null", `,"bytes_used":null`, false},
		{"zero", `,"bytes_used":0`, true}, {"positive", `,"bytes_used":8192`, true},
		{"negative", `,"bytes_used":-1`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var raw rawVolumeInfo
			data := base[:len(base)-1] + test.usage + "}"
			if err := json.Unmarshal([]byte(data), &raw); err != nil {
				t.Fatal(err)
			}
			if err := validateVolumeInfo(raw); (err == nil) != test.valid {
				t.Fatalf("usage %s validation error: %v", data, err)
			}
		})
	}
}
