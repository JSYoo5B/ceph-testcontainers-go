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

// syncFreshGroup returns the group id of after when it is a fresh empty group
// with status and after differs from before only by that group.
func syncFreshGroup(before, after map[string]any, id string, status SyncGroupStatus) (map[string]any, error) {
	created := syncNativeGroup(after, id)
	if created == nil || created["status"] != string(status) || !syncGroupEmpty(created) || !reflect.DeepEqual(before, syncPolicyReplace(after, id, nil)) {
		return nil, errors.New("RGW fresh sync group readback or unrelated policy differs")
	}
	return created, nil
}

// syncGroupWithStatus returns a copy of group with status.
func syncGroupWithStatus(group map[string]any, status SyncGroupStatus) map[string]any {
	want := syncJSONClone(group)
	want["status"] = string(status)
	return want
}

// syncGroupWithPipePrefix returns a copy of group whose pipe id selects prefix;
// an empty prefix removes the member. A pipe absent from group is a refused
// request; a present pipe without a source filter object is a native document
// the fixture cannot edit.
func syncGroupWithPipePrefix(group map[string]any, id, prefix string) (map[string]any, error) {
	want := syncJSONClone(group)
	pipe := syncNativePipe(want, id)
	if pipe == nil {
		return nil, multisiteInvalid(errors.New("RGW sync pipe is absent from owned group"))
	}
	filter := nativeDoc(pipe).object("params").object("source").object("filter")
	if filter == nil {
		return nil, errors.New("RGW native pipe source filter is missing")
	}
	if prefix == "" {
		delete(filter, "prefix")
	} else {
		filter["prefix"] = prefix
	}
	return want, nil
}

// syncFlowAdded reports whether after holds exactly flow under key and is
// otherwise before.
func syncFlowAdded(before, after map[string]any, key string, flow map[string]any) bool {
	return syncFlowMatches(after, key, flow) && reflect.DeepEqual(before, syncGroupWithoutFlow(after, key, flow))
}

// syncPipeAdded reports whether after holds exactly pipe as its pipe id and is
// otherwise before.
func syncPipeAdded(before, after map[string]any, id string, pipe map[string]any) bool {
	return reflect.DeepEqual(syncNativePipe(after, id), pipe) && reflect.DeepEqual(before, syncGroupWithoutPipe(after, id))
}

// syncPendingOutcome classifies the policy actual read after a pending
// mutation of group id that started from before and whose exact intent is
// check. It returns the confirmed group when the mutation took effect, nil
// with no error when actual is still before (the mutation has not been
// applied), and an error when actual changed anything else or changed the
// group differently from the intent.
func syncPendingOutcome(actual, before map[string]any, id string, check func(map[string]any) error) (map[string]any, error) {
	group := syncNativeGroup(actual, id)
	if reflect.DeepEqual(actual, before) {
		// An idempotent intent can already hold in the unchanged policy.
		if group != nil && check(group) == nil {
			return group, nil
		}
		return nil, nil
	}
	if group == nil || !reflect.DeepEqual(syncPolicyReplace(actual, id, nil), syncPolicyReplace(before, id, nil)) {
		return nil, errors.New("RGW pending mutation readback contains unrelated sync policy changes")
	}
	if err := check(group); err != nil {
		return nil, errors.New("RGW pending mutation differs from its exact owned intent")
	}
	return group, nil
}

// syncPeriodPolicyOwned reports whether period has the policy of previous,
// the last owned publication, or of want, the owned change. Any other policy
// is an edit this fixture does not own and must not overwrite.
func syncPeriodPolicyOwned(period, previous, want map[string]any) bool {
	return syncPeriodPolicyEqual(period, previous) || syncPeriodPolicyEqual(period, want)
}
