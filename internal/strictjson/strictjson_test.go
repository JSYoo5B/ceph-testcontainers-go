package strictjson

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type target struct {
	Name   *string            `json:"name"`
	Count  *int64             `json:"count"`
	Nested *target            `json:"nested"`
	Items  []target           `json:"items"`
	Codes  map[string]*target `json:"codes"`
	Loose  *int               // untagged: matched case-insensitively on purpose
}

func nest(depth int) string {
	return strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
}

func TestCheckRefusesEachRule(t *testing.T) {
	for name, tc := range map[string]struct {
		data string
		opts Options
		want error
	}{
		"empty":                {"", Options{}, ErrSize},
		"whitespace":           {" \n\t", Options{}, ErrSize},
		"too large":            {`{"a":1}`, Options{MaxBytes: 3}, ErrSize},
		"syntax":               {`{"a":`, Options{}, ErrMalformed},
		"duplicate at top":     {`{"a":1,"a":2}`, Options{}, ErrDuplicateKey},
		"duplicate at depth":   {`{"a":[{"b":{"c":1,"c":1}}]}`, Options{}, ErrDuplicateKey},
		"duplicate in unknown": {`{"future":{"x":1,"x":1}}`, Options{}, ErrDuplicateKey},
		"nesting":              {nest(MaxDepth + 1), Options{}, ErrDepth},
		"trailing value":       {`{} {}`, Options{}, ErrTrailing},
		"trailing garbage":     {`{}x`, Options{}, ErrTrailing},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Check([]byte(tc.data), tc.opts); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	if err := Check([]byte(nest(MaxDepth)), Options{}); err != nil {
		t.Fatalf("nesting at the limit refused: %v", err)
	}
	if err := Check([]byte(`{"a":null,"b":[1e9999]}`), Options{RejectNull: true}); err != nil {
		t.Fatalf("an untyped check refused null or a large number: %v", err)
	}
}

func TestDecodeRefusesAliasesAndNullsOfKnownFields(t *testing.T) {
	for name, tc := range map[string]struct {
		data string
		opts Options
		want error
	}{
		"alias":              {`{"NAME":"x"}`, Options{}, ErrNoncanonicalKey},
		"alias with canon":   {`{"name":"x","Name":"y"}`, Options{}, ErrNoncanonicalKey},
		"alias in slice":     {`{"items":[{"Count":1}]}`, Options{}, ErrNoncanonicalKey},
		"alias in map value": {`{"codes":{"A":{"nested":{"NAME":"x"}}}}`, Options{}, ErrNoncanonicalKey},
		"null known":         {`{"name":null}`, Options{RejectNull: true}, ErrNull},
		"null map value":     {`{"codes":{"A":null}}`, Options{RejectNull: true}, ErrNull},
		"null root":          {`null`, Options{RejectNull: true}, ErrNull},
		"wrong type":         {`{"count":"1"}`, Options{}, ErrMalformed},
	} {
		t.Run(name, func(t *testing.T) {
			var v target
			if err := Decode([]byte(tc.data), &v, tc.opts); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeAllowsWhatEncodingJSONMayDecide(t *testing.T) {
	var v target
	data := `{"name":"x","count":9007199254740993,"codes":{"a":{},"A":{}},"loose":1,` +
		`"future":{"NAME":null,"name":1e9999},"Future":null}`
	if err := Decode([]byte(data), &v, Options{RejectNull: true}); err != nil {
		t.Fatal(err)
	}
	if *v.Name != "x" || *v.Count != 9007199254740993 || len(v.Codes) != 2 || v.Loose == nil || *v.Loose != 1 {
		t.Fatalf("decoded %+v", v)
	}
	if err := Decode([]byte(`{"name":null}`), &v, Options{}); err != nil {
		t.Fatalf("null refused without RejectNull: %v", err)
	}
}

func TestDecodeUseNumberKeepsExactNumbers(t *testing.T) {
	var v map[string]any
	if err := Decode([]byte(`{"n":18446744073709551615}`), &v, Options{UseNumber: true}); err != nil {
		t.Fatal(err)
	}
	if n, ok := v["n"].(json.Number); !ok || n.String() != "18446744073709551615" {
		t.Fatalf("got %#v", v["n"])
	}
}

func TestErrorsNeverCarryNativeData(t *testing.T) {
	const secret = "SECRET_NATIVE_VALUE"
	for _, data := range []string{`{"` + secret + `":1,"` + secret + `":2}`, `{"a":"` + secret, `{"NAME":"` + secret + `"}`, `{} "` + secret + `"`} {
		var v target
		if err := Decode([]byte(data), &v, Options{}); err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("%q: %v", data, err)
		}
	}
}
