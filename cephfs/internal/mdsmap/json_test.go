package mdsmap

import (
	"strings"
	"testing"
)

// DecodeJSON refuses a key that matches a known field only when case is
// ignored, at any depth, and nesting beyond the shared limit; it accepts
// unknown fields.
func TestDecodeJSONKeepsTheSharedGuards(t *testing.T) {
	var auth struct {
		Entities *[]struct {
			Entity *string `json:"entity"`
		} `json:"auth_dump"`
	}
	if err := DecodeJSON([]byte(`{"AUTH_DUMP":[{"entity":"mds.a"}]}`), &auth); err == nil {
		t.Fatal("adopted a case alias of a known field")
	}
	if err := DecodeJSON([]byte(`{"auth_dump":[{"Entity":"mds.a"}]}`), &auth); err == nil {
		t.Fatal("adopted a nested case alias of a known field")
	}
	deep := strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100)
	if err := DecodeJSON([]byte(`{"auth_dump":[],"future":`+deep+`}`), &auth); err == nil {
		t.Fatal("accepted nesting beyond the shared limit")
	}
	if err := DecodeJSON([]byte(`{"auth_dump":[{"entity":"mds.a"}],"future":{"Entity":1}}`), &auth); err != nil || auth.Entities == nil {
		t.Fatalf("refused an unknown field: %v", err)
	}
	if err := DecodeJSON([]byte(`{"auth_dump":[{"entity":"mds.a","key":"PRIVATE-KEY"}]}{}`), &auth); err == nil || strings.Contains(err.Error(), "PRIVATE-KEY") {
		t.Fatalf("a trailing document: %v", err)
	}
}

func TestIdentityPresent(t *testing.T) {
	const listed = `{"auth_dump":[{"entity":"client.admin"},{"entity":"mds.a"}]}`
	if present, err := IdentityPresent([]byte(listed), "mds.a"); err != nil || !present {
		t.Fatalf("listed identity: %v %v", present, err)
	}
	if present, err := IdentityPresent([]byte(listed), "mds.b"); err != nil || present {
		t.Fatalf("absent identity: %v %v", present, err)
	}
	for _, data := range []string{
		`{}`,
		`{"auth_dump":null}`,
		`{"auth_dump":[{}]}`,
		`{"auth_dump":[{"entity":""}]}`,
		`{"auth_dump":[{"entity":" mds.a"}]}`,
		`{"auth_dump":[{"entity":"mds.a"},{"entity":"mds.a"}]}`,
	} {
		if _, err := IdentityPresent([]byte(data), "mds.b"); err == nil {
			t.Errorf("%s: accepted", data)
		}
	}
}
