package ceph

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
	"github.com/google/uuid"
)

// This file holds the strict JSON reading shared by the MON store adapters
// (the monmap decoder and the inspector listing parser). A document is exactly
// one JSON value without a duplicate object key at any depth, and a required
// field that is missing, null or of the wrong type is an error. Nothing is
// decoded into a zero value that could count as evidence.

var monStoreJSONUint = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// monStoreJSONCheck verifies that data is exactly one JSON value without a
// duplicate object key at any depth: encoding/json keeps the last of two equal
// keys silently.
func monStoreJSONCheck(data []byte) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return errors.New("no output")
	}
	return strictjson.Check(data, strictjson.Options{})
}

type monStoreJSONObject map[string]json.RawMessage

func monStoreObjectOf(data []byte, what string) (monStoreJSONObject, error) {
	if err := monStoreJSONCheck(data); err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	var obj monStoreJSONObject
	if err := json.Unmarshal(data, &obj); err != nil || obj == nil {
		return nil, fmt.Errorf("%s is not a JSON object", what)
	}
	return obj, nil
}

// field returns a present, non-null member.
func (o monStoreJSONObject) field(name, what string) (json.RawMessage, error) {
	raw, ok := o[name]
	if !ok || string(bytes.TrimSpace(raw)) == "null" {
		return nil, fmt.Errorf("%s field %q is missing or null", what, name)
	}
	return raw, nil
}

func (o monStoreJSONObject) object(name, what string) (monStoreJSONObject, error) {
	raw, err := o.field(name, what)
	if err != nil {
		return nil, err
	}
	var out monStoreJSONObject
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, fmt.Errorf("%s field %q is not an object", what, name)
	}
	return out, nil
}

func (o monStoreJSONObject) array(name, what string) ([]json.RawMessage, error) {
	raw, err := o.field(name, what)
	if err != nil {
		return nil, err
	}
	var out []json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, fmt.Errorf("%s field %q is not an array", what, name)
	}
	return out, nil
}

func (o monStoreJSONObject) str(name, what string) (string, error) {
	raw, err := o.field(name, what)
	if err != nil {
		return "", err
	}
	var out string
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("%s field %q is not a string", what, name)
	}
	return out, nil
}

func (o monStoreJSONObject) boolean(name, what string) (bool, error) {
	raw, err := o.field(name, what)
	if err != nil {
		return false, err
	}
	var out bool
	if err := json.Unmarshal(raw, &out); err != nil {
		return false, fmt.Errorf("%s field %q is not a boolean", what, name)
	}
	return out, nil
}

// uint64 reads a non-negative integer written without sign, fraction or
// exponent.
func (o monStoreJSONObject) uint64(name, what string) (uint64, error) {
	raw, err := o.field(name, what)
	if err != nil {
		return 0, err
	}
	return monStoreUintOf(raw, fmt.Sprintf("%s field %q", what, name))
}

func (o monStoreJSONObject) int64(name, what string) (int64, error) {
	raw, err := o.field(name, what)
	if err != nil {
		return 0, err
	}
	text := string(bytes.TrimSpace(raw))
	if len(text) > 1 && text[0] == '-' {
		magnitude, err := monStoreUintOf([]byte(text[1:]), fmt.Sprintf("%s field %q", what, name))
		if err != nil || magnitude > math.MaxInt64 {
			return 0, fmt.Errorf("%s field %q is not a 64-bit integer", what, name)
		}
		return -int64(magnitude), nil
	}
	value, err := monStoreUintOf(raw, fmt.Sprintf("%s field %q", what, name))
	if err != nil || value > math.MaxInt64 {
		return 0, fmt.Errorf("%s field %q is not a 64-bit integer", what, name)
	}
	return int64(value), nil
}

func monStoreUintOf(raw json.RawMessage, what string) (uint64, error) {
	text := string(bytes.TrimSpace(raw))
	if !monStoreJSONUint.MatchString(text) {
		return 0, fmt.Errorf("%s is not an unsigned integer", what)
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is out of range", what)
	}
	return value, nil
}

// monStoreExactKeys refuses an object that has any key besides want or lacks
// one of them.
func (o monStoreJSONObject) exactKeys(what string, want ...string) error {
	if len(o) != len(want) {
		return fmt.Errorf("%s has %d fields, want %d", what, len(o), len(want))
	}
	for _, key := range want {
		if _, ok := o[key]; !ok {
			return fmt.Errorf("%s lacks field %q", what, key)
		}
	}
	return nil
}

func monStoreCanonicalUUID(s string) bool {
	parsed, err := uuid.Parse(s)
	return err == nil && parsed.String() == s
}
