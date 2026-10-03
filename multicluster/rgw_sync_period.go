package multicluster

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
)

// ApplySyncGroup publishes only this owned zonegroup policy change, pulls the
// resulting period into every owned zone and reloads running gateways. Resolve
// their S3Endpoint again afterwards: a bridge gateway's mapped port can change.
// Bucket policy is dynamic and this method only checks its owned native snapshot.
// Unrelated current, locally pending or staging-period edits are refused before
// period update can overwrite them. Partial publication/reload is not rolled
// back; retry the same handle after resolving an external failure.
func (f *RGWMultisite) ApplySyncGroup(ctx context.Context, g *RGWSyncGroup) error {
	if f == nil {
		return errors.New("RGW multisite fixture is required")
	}
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	if err := f.validateSyncGroup(g, true); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return err
	}
	if err := f.finishPendingSyncGroup(ctx, master, g); err != nil {
		return err
	}
	policy, _, err := f.syncPolicyDocument(ctx, master, g)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(syncNativeGroup(policy, g.ID()), g.state.group) {
		return errors.New("RGW sync group native policy changed before activation")
	}
	if g.scope.Bucket != "" {
		return nil
	}
	want, err := syncPeriodWithGroup(g.state.period, g.groupID, g.ID(), g.state.group)
	if err != nil {
		return err
	}
	if !syncPeriodPolicyEqual(current, g.state.period) && !syncPeriodPolicyEqual(current, want) {
		return errors.New("RGW current period changed outside this owned sync group")
	}
	// period update reflects every stored zonegroup, not only the selected one.
	// Check each native config before asking it to construct the staging map.
	for _, stored := range syncPeriodGroups(want) {
		name, _ := stored["name"].(string)
		data, err := f.syncPolicyAdmin(ctx, master, name, "zonegroup", "get")
		if err != nil {
			return err
		}
		actual, err := syncJSON(data)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, stored) {
			return errors.New("RGW stored zonegroup has unrelated pending policy or topology")
		}
	}
	if !syncPeriodPolicyEqual(current, want) {
		staging, err := f.syncStagingPeriod(ctx, master)
		if err != nil {
			return err
		}
		if staging != nil && !syncPeriodPolicyEqual(staging, current) && !syncPeriodPolicyEqual(staging, want) {
			return errors.New("RGW existing staging period contains unrelated policy; refusing overwrite")
		}
		data, err := f.syncPolicyAdmin(ctx, master, master.Zonegroup, "period", "update")
		if err != nil {
			return err
		}
		staging, err = syncJSON(data)
		if err != nil {
			return err
		}
		if !syncPeriodPolicyEqual(staging, want) {
			return errors.New("RGW generated staging period changed unrelated policy; refusing publication")
		}
		if _, err := f.syncPolicyAdmin(ctx, master, master.Zonegroup, "period", "commit"); err != nil {
			return err
		}
		_, current, err = f.syncPolicyMaster(ctx)
		if err != nil {
			return err
		}
		if !syncPeriodPolicyEqual(current, want) {
			return errors.New("RGW committed sync policy differs from the owned change")
		}
	}
	g.state.period = current
	states := f.zoneStates()
	// Use known bootstrap system credentials even before that user's metadata
	// has arrived at a secondary. These credentials never enter returned errors.
	for _, zone := range states {
		if zone.ID != master.ID {
			if _, err := f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "realm", "pull", "--url", master.PeerEndpoint,
				"--access-key", f.systemAccess, "--secret", f.systemSecret); err != nil {
				return err
			}
		}
		data, err := f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "period", "get")
		if err != nil {
			return err
		}
		actual, err := syncJSON(data)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, current) {
			return errors.New("RGW owned zone has not imported the exact committed policy period")
		}
	}
	return f.reloadZoneGateways(ctx, states, nil)
}

func syncPeriodGroups(period map[string]any) []map[string]any {
	periodMap, _ := period["period_map"].(map[string]any)
	groups, _ := periodMap["zonegroups"].([]any)
	result := make([]map[string]any, 0, len(groups))
	for _, value := range groups {
		if group, ok := value.(map[string]any); ok {
			result = append(result, group)
		}
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

// Get the exact native staging object. errno 2 with empty stdout denotes a
// missing object; malformed output and all other failures remain failures.
func (f *RGWMultisite) syncStagingPeriod(ctx context.Context, master *rgwZoneState) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	id := f.RealmID + ":staging"
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring",
		"--realm-id", f.RealmID, "--zonegroup-id", f.groupIDs[master.Zonegroup], "--zone-id", master.ID,
		"--format", "json", "period", "get", "--period", id, "--epoch", "1"}
	code, reader, err := master.client.Exec(ctx, argv)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("RGW staging read failed; output is redacted")
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, errors.New("RGW staging stream failed; output is redacted")
	}
	if code == 2 && stdout.Len() == 0 {
		return nil, nil
	}
	if code != 0 {
		return nil, errors.New("RGW staging read failed; output is redacted")
	}
	period, err := syncJSON(stdout.Bytes())
	if err != nil {
		return nil, err
	}
	if period["id"] != id || period["realm_id"] != f.RealmID {
		return nil, errors.New("RGW staging native realm identity differs")
	}
	return period, nil
}
