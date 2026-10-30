package ceph

import (
	"strings"
	"testing"
)

// The pool usage and MON store readers gained the shared nesting limit, and
// pool usage now refuses null for a known field, which it used to decode as
// absent.
func TestPoolUsageAndMONStoreJSONGainedTheSharedGuards(t *testing.T) {
	deep := strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100)
	var ids []struct {
		ID   *int64 `json:"pool_id"`
		Name string `json:"pool_name"`
	}
	if err := poolUsageJSON([]byte(`[{"pool_id":7,"pool_name":"fixture","future":`+deep+`}]`), &ids); err == nil {
		t.Fatal("pool usage accepted nesting beyond the shared limit")
	}
	if err := poolUsageJSON([]byte(`[{"pool_id":null,"pool_name":"fixture"}]`), &ids); err == nil {
		t.Fatal("pool usage accepted null for a known field")
	}
	if err := poolUsageJSON([]byte(`[{"pool_id":7,"pool_name":"fixture","future":null}]`), &ids); err != nil {
		t.Fatalf("pool usage refused null in an unknown field: %v", err)
	}
	if err := monStoreJSONCheck([]byte(`{"mons":` + deep + `}`)); err == nil {
		t.Fatal("MON store JSON accepted nesting beyond the shared limit")
	}
	if err := monStoreJSONCheck([]byte(`{"a":{"b":1,"b":2}}`)); err == nil || strings.Contains(err.Error(), `"b"`) {
		t.Fatalf("MON store duplicate key: %v", err)
	}
}
