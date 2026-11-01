package ceph

import (
	"strings"
	"testing"
)

// The pool usage reader has the shared nesting limit and refuses null for a
// known field; the MON store reader's guard is tested in its package.
func TestPoolUsageJSONHasTheSharedGuards(t *testing.T) {
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
}
