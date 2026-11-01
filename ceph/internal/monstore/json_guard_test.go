package monstore

import (
	"strings"
	"testing"
)

// The MON store reader has the shared nesting limit and refuses a duplicate
// key without quoting it.
func TestMonStoreJSONHasTheSharedGuards(t *testing.T) {
	deep := strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100)
	if err := monStoreJSONCheck([]byte(`{"mons":` + deep + `}`)); err == nil {
		t.Fatal("MON store JSON accepted nesting beyond the shared limit")
	}
	if err := monStoreJSONCheck([]byte(`{"a":{"b":1,"b":2}}`)); err == nil || strings.Contains(err.Error(), `"b"`) {
		t.Fatalf("MON store duplicate key: %v", err)
	}
}
