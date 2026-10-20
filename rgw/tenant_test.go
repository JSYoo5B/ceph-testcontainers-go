package rgw

import (
	"slices"
	"testing"
)

func TestZonePoolsListsEveryPoolOfTheZone(t *testing.T) {
	zone := []byte(`{
		"id": "z", "name": "default",
		"domain_root": "default.rgw.meta:root",
		"control_pool": "default.rgw.control",
		"gc_pool": "default.rgw.log:gc",
		"log_pool": "default.rgw.log",
		"otp_pool": "default.rgw.otp",
		"account_pool": "default.rgw.meta:accounts",
		"system_key": {"access_key": "", "secret_key": ""},
		"placement_pools": [{
			"key": "default-placement",
			"val": {
				"index_pool": "default.rgw.buckets.index",
				"storage_classes": {"STANDARD": {"data_pool": "default.rgw.buckets.data"}, "COLD": {"data_pool": "cold.data"}},
				"data_extra_pool": "default.rgw.buckets.non-ec",
				"index_type": 0
			}
		}]
	}`)
	pools, err := zonePools(zone)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"cold.data", "default.rgw.buckets.data", "default.rgw.buckets.index", "default.rgw.buckets.non-ec",
		"default.rgw.control", "default.rgw.log", "default.rgw.meta", "default.rgw.otp"}
	if !slices.Equal(pools, want) {
		t.Fatalf("zone pools %v, want %v", pools, want)
	}
	if _, err := zonePools([]byte("[]")); err == nil {
		t.Fatal("a zone that is not an object was accepted")
	}
}
