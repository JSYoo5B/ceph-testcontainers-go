package multicluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// This mutation is only admitted inside fresh RunRGWMultisite initialization,
// after both clusters passed ensureFreshRGWCluster and before exposing the
// fixture. It must never turn a later foreign staging period into permission
// to overwrite pending configuration in ApplySyncGroup.
func (f *RGWMultisite) canonicalizeBootstrapStaging(ctx context.Context) error {
	if f == nil || f.closed || !f.bootstrapStagingPending {
		return errors.New("RGW staging canonicalization is restricted to fresh owned bootstrap")
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return err
	}
	id, epoch, err := syncCommittedPeriodIdentity(current)
	if err != nil {
		return err
	}
	if master.Name != f.config.SourceZone || master.ID != f.SourceZoneID {
		return errors.New("RGW fresh bootstrap staging requires its original owned metadata master")
	}
	for _, group := range syncPeriodGroups(current) {
		name, _ := group["name"].(string)
		data, err := f.syncPolicyAdmin(ctx, master, name, "zonegroup", "get")
		if err != nil {
			return err
		}
		stored, err := syncJSON(data)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(stored, group) {
			return errors.New("RGW fresh bootstrap stored zonegroup differs from the complete committed policy")
		}
	}
	data, err := f.syncPolicyAdmin(ctx, master, master.Zonegroup, "period", "update")
	if err != nil {
		return err
	}
	staging, err := syncJSON(data)
	if err != nil {
		return err
	}
	// Native fork preserves the committed epoch in its JSON, even though the
	// staging storage key is read with --epoch 1. Only realm_epoch advances.
	if staging["id"] != f.RealmID+":staging" || staging["realm_id"] != f.RealmID || staging["epoch"] != current["epoch"] || staging["predecessor_uuid"] != id || fmt.Sprint(staging["realm_epoch"]) != fmt.Sprint(epoch+1) || !syncPeriodPolicyEqual(staging, current) {
		return errors.New("RGW fresh bootstrap generated staging differs from its committed topology/policy")
	}
	persisted, err := f.syncStagingPeriod(ctx, master)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(persisted, staging) {
		return errors.New("RGW fresh bootstrap staging was not persisted exactly")
	}
	_, after, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(after, current) {
		return errors.New("RGW fresh bootstrap staging changed the active committed period")
	}
	f.bootstrapStagingPending = false
	return nil
}
