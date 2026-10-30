package cephfs

import (
	"strings"
	"testing"
)

// The cold MDS reader gained the alias refusal and the nesting limit: a key
// that matches a known field only when case is ignored no longer reaches
// encoding/json's case-insensitive matching.
func TestColdMDSJSONGainedTheSharedGuards(t *testing.T) {
	var auth struct {
		Entities *[]struct {
			Entity *string `json:"entity"`
		} `json:"auth_dump"`
	}
	if err := coldMDSJSON([]byte(`{"AUTH_DUMP":[{"entity":"mds.a"}]}`), &auth); err == nil {
		t.Fatal("cold MDS JSON adopted a case alias of a known field")
	}
	if err := coldMDSJSON([]byte(`{"auth_dump":[{"Entity":"mds.a"}]}`), &auth); err == nil {
		t.Fatal("cold MDS JSON adopted a nested case alias of a known field")
	}
	deep := strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100)
	if err := coldMDSJSON([]byte(`{"auth_dump":[],"future":`+deep+`}`), &auth); err == nil {
		t.Fatal("cold MDS JSON accepted nesting beyond the shared limit")
	}
	if err := coldMDSJSON([]byte(`{"auth_dump":[{"entity":"mds.a"}],"future":{"Entity":1}}`), &auth); err != nil || auth.Entities == nil {
		t.Fatalf("cold MDS JSON refused an unknown field: %v", err)
	}
}
