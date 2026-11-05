package multisite

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
)

// The functions in this file are the sync policy document component: they
// interpret and transform decoded native radosgw-admin documents (sync
// policies, sync groups, periods) and decide whether a readback matches an
// owned intent. They take documents, native identities and plain values only,
// and never a Fixture, SyncGroup, zone state, context or container, so they
// run no command and change no handle state. Locking, native reads and writes,
// publication, import, gateway reload and pending-mutation bookkeeping stay
// with their callers.
//
// Documents stay map[string]any as syncJSON decodes them: members the fixture
// does not interpret survive every transformation, transformations work on a
// clone and never mutate their input, and comparisons use reflect.DeepEqual on
// the exact documents, so a member that is missing, null or of another type is
// never treated as equal to a present one unless a function says otherwise.

func syncJSON(data []byte) (map[string]any, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil || value == nil {
		return nil, errors.New("decode RGW native sync configuration")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("RGW native sync configuration has trailing output")
	}
	return value, nil
}
func syncJSONClone(value map[string]any) map[string]any {
	data, _ := json.Marshal(value)
	copy, _ := syncJSON(data)
	return copy
}
func syncStrings(values []string) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = v
	}
	return result
}
func syncPolicyFromDocument(doc map[string]any) (map[string]any, error) {
	if doc["sync_policy"] == nil {
		return map[string]any{"groups": []any{}}, nil
	}
	policy, ok := doc["sync_policy"].(map[string]any)
	if !ok {
		return nil, errors.New("RGW native sync policy is malformed")
	}
	groups, ok := policy["groups"].([]any)
	if !ok {
		return nil, errors.New("RGW native sync groups are malformed")
	}
	seen := map[string]bool{}
	for _, v := range groups {
		group, ok := v.(map[string]any)
		id, _ := group["id"].(string)
		if !ok || id == "" || seen[id] {
			return nil, errors.New("RGW native sync group identity is malformed or duplicated")
		}
		seen[id] = true
	}
	return policy, nil
}
func syncNativeGroup(policy map[string]any, id string) map[string]any {
	return nativeDoc(policy).identified("groups", id)
}
func syncPolicyReplace(policy map[string]any, id string, group map[string]any) map[string]any {
	copy := syncJSONClone(policy)
	groups, _ := copy["groups"].([]any)
	groups = slices.DeleteFunc(groups, func(v any) bool { g, _ := v.(map[string]any); return g["id"] == id })
	if group != nil {
		groups = append(groups, syncJSONClone(group))
	}
	slices.SortFunc(groups, func(a, b any) int {
		left, _ := a.(map[string]any)
		right, _ := b.(map[string]any)
		return strings.Compare(fmt.Sprint(left["id"]), fmt.Sprint(right["id"]))
	})
	copy["groups"] = groups
	return copy
}
func syncGroupEmpty(g map[string]any) bool {
	flows, _ := g["data_flow"].(map[string]any)
	pipes, _ := g["pipes"].([]any)
	return flows != nil && len(flows) == 0 && pipes != nil && len(pipes) == 0
}
func syncNativePipe(g map[string]any, id string) map[string]any {
	return nativeDoc(g).identified("pipes", id)
}
func syncGroupWithoutPipe(g map[string]any, id string) map[string]any {
	copy := syncJSONClone(g)
	pipes, _ := copy["pipes"].([]any)
	copy["pipes"] = slices.DeleteFunc(pipes, func(v any) bool { p, _ := v.(map[string]any); return p["id"] == id })
	return copy
}
func syncFlowPresent(g map[string]any, key string, flow map[string]any) bool {
	data, _ := g["data_flow"].(map[string]any)
	flows, _ := data[key].([]any)
	return slices.ContainsFunc(flows, func(v any) bool {
		f, _ := v.(map[string]any)
		if key == "symmetrical" {
			return f["id"] == flow["id"]
		}
		return reflect.DeepEqual(f, flow)
	})
}
func syncFlowMatches(g map[string]any, key string, flow map[string]any) bool {
	data, _ := g["data_flow"].(map[string]any)
	flows, _ := data[key].([]any)
	return slices.ContainsFunc(flows, func(v any) bool { return reflect.DeepEqual(v, flow) })
}
func syncGroupWithoutFlow(g map[string]any, key string, flow map[string]any) map[string]any {
	copy := syncJSONClone(g)
	data, _ := copy["data_flow"].(map[string]any)
	flows, _ := data[key].([]any)
	flows = slices.DeleteFunc(flows, func(v any) bool {
		f, _ := v.(map[string]any)
		if key == "symmetrical" {
			return f["id"] == flow["id"]
		}
		return reflect.DeepEqual(f, flow)
	})
	if len(flows) == 0 {
		delete(data, key)
	} else {
		data[key] = flows
	}
	return copy
}

// syncPeriodGroups returns the period's zonegroup documents. They are
// returned as plain maps because callers compare them with reflect.DeepEqual
// against documents read elsewhere, which a named map type would never equal.
func syncPeriodGroups(period map[string]any) []map[string]any {
	groups := nativeDoc(period).object("period_map").objects("zonegroups")
	result := make([]map[string]any, len(groups))
	for i, group := range groups {
		result[i] = group
	}
	return result
}

func syncPeriodWithGroup(previous map[string]any, groupID, id string, group map[string]any) (map[string]any, error) {
	want := syncJSONClone(previous)
	found := false
	for _, nativeGroup := range syncPeriodGroups(want) {
		if nativeGroup["id"] != groupID {
			continue
		}
		if found {
			return nil, errors.New("RGW period has duplicate zonegroup identity")
		}
		found = true
		policy, err := syncPolicyFromDocument(nativeGroup)
		if err != nil {
			return nil, err
		}
		nativeGroup["sync_policy"] = syncPolicyReplace(policy, id, group)
	}
	if !found {
		return nil, errors.New("RGW owned zonegroup is missing from the captured period")
	}
	return want, nil
}

func syncPeriodPolicy(period map[string]any) map[string]any {
	policy := syncJSONClone(period)
	for _, key := range []string{"id", "epoch", "realm_epoch", "predecessor_uuid"} {
		delete(policy, key)
	}
	if periodMap, ok := policy["period_map"].(map[string]any); ok {
		delete(periodMap, "id")
	}
	return policy
}
func syncPeriodPolicyEqual(a, b map[string]any) bool {
	return reflect.DeepEqual(syncPeriodPolicy(a), syncPeriodPolicy(b))
}
