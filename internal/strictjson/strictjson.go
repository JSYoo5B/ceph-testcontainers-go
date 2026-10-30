// Package strictjson reads native Ceph JSON without the silent choices of
// encoding/json. A document is exactly one JSON value: no object repeats a key
// at any depth, nesting stays within MaxDepth and nothing follows the value.
// When the document is decoded into a typed value, a key that differs from a
// known field's name only by case is refused instead of adopted by the
// decoder's case-insensitive matching, and unrelated unknown keys are allowed.
//
// Errors are the sentinels below and never copy native data, which can carry
// credentials, into their text.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
)

// MaxDepth bounds the nesting of objects and arrays. Native Ceph output nests
// far less deeply; the bound keeps a hostile document from growing the stack.
const MaxDepth = 64

var (
	// ErrSize reports an empty document or one larger than Options.MaxBytes.
	ErrSize = errors.New("JSON document is empty or too large")
	// ErrMalformed reports invalid JSON, or a value of the wrong type for the
	// field it is decoded into.
	ErrMalformed = errors.New("malformed JSON document")
	// ErrDuplicateKey reports an object that repeats a key.
	ErrDuplicateKey = errors.New("duplicate JSON object key")
	// ErrNoncanonicalKey reports a key that matches a known field only when
	// case is ignored.
	ErrNoncanonicalKey = errors.New("noncanonical JSON object key")
	// ErrNull reports null for a known field when Options.RejectNull is set.
	ErrNull = errors.New("null JSON value for a known field")
	// ErrDepth reports nesting beyond MaxDepth.
	ErrDepth = errors.New("JSON nesting exceeds limit")
	// ErrTrailing reports data after the first JSON value.
	ErrTrailing = errors.New("trailing data after the JSON value")
)

// Options holds the rules that legitimately differ between callers.
type Options struct {
	// MaxBytes refuses a larger document. Zero sets no size limit; an empty
	// document is refused either way.
	MaxBytes int
	// RejectNull refuses null for any field whose type is known from the
	// decoded value, so a present but null field cannot pass as absent.
	RejectNull bool
	// UseNumber decodes numbers in interface values as json.Number.
	UseNumber bool
}

// Check validates the shape of data without decoding it.
func Check(data []byte, opts Options) error {
	return check(data, nil, opts)
}

// Decode validates data against the shape of value, then decodes it into value.
func Decode(data []byte, value any, opts Options) error {
	if err := check(data, reflect.TypeOf(value), opts); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if opts.UseNumber {
		decoder.UseNumber()
	}
	if decoder.Decode(value) != nil {
		return ErrMalformed
	}
	return nil
}

func check(data []byte, expected reflect.Type, opts Options) error {
	if len(bytes.TrimSpace(data)) == 0 || opts.MaxBytes > 0 && len(data) > opts.MaxBytes {
		return ErrSize
	}
	w := walker{decoder: json.NewDecoder(bytes.NewReader(data)), rejectNull: opts.RejectNull}
	// The walk only checks shape: a number is kept as text, so one beyond the
	// float64 range in an unknown field is not refused here.
	w.decoder.UseNumber()
	if err := w.value(expected, 0); err != nil {
		return err
	}
	if _, err := w.decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrTrailing
	}
	return nil
}

type walker struct {
	decoder    *json.Decoder
	rejectNull bool
}

// value reads one JSON value. expected is the Go type it will be decoded into,
// or nil when that is unknown (an unknown field, or Check).
func (w walker) value(expected reflect.Type, depth int) error {
	if depth > MaxDepth {
		return ErrDepth
	}
	for expected != nil && expected.Kind() == reflect.Pointer {
		expected = expected.Elem()
	}
	token, err := w.decoder.Token()
	if err != nil {
		return ErrMalformed
	}
	if token == nil && expected != nil && w.rejectNull {
		return ErrNull
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		err = w.object(expected, depth)
	case '[':
		err = w.array(expected, depth)
	default:
		return ErrMalformed
	}
	if err != nil {
		return err
	}
	if _, err := w.decoder.Token(); err != nil {
		return ErrMalformed
	}
	return nil
}

func (w walker) object(expected reflect.Type, depth int) error {
	fields := structFields(expected)
	seen := map[string]bool{}
	for w.decoder.More() {
		token, err := w.decoder.Token()
		if err != nil {
			return ErrMalformed
		}
		key, ok := token.(string)
		if !ok {
			return ErrMalformed
		}
		if seen[key] {
			return ErrDuplicateKey
		}
		seen[key] = true
		var next reflect.Type
		if expected != nil && expected.Kind() == reflect.Map {
			next = expected.Elem()
		} else if next = fields[key]; next == nil {
			for name := range fields {
				if strings.EqualFold(key, name) {
					return ErrNoncanonicalKey
				}
			}
		}
		if err := w.value(next, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (w walker) array(expected reflect.Type, depth int) error {
	var element reflect.Type
	if expected != nil && (expected.Kind() == reflect.Slice || expected.Kind() == reflect.Array) {
		element = expected.Elem()
	}
	for w.decoder.More() {
		if err := w.value(element, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// structFields maps the tagged JSON names of a struct type to their field
// types. An untagged field is matched by encoding/json case-insensitively on
// purpose (native keys are lowercase, Go names are not), so it has no canonical
// spelling to enforce and stays out of the map.
func structFields(t reflect.Type) map[string]reflect.Type {
	fields := map[string]reflect.Type{}
	if t == nil || t.Kind() != reflect.Struct {
		return fields
	}
	for i := 0; i < t.NumField(); i++ {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			fields[name] = t.Field(i).Type
		}
	}
	return fields
}
