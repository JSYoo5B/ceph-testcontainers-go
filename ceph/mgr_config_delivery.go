package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MGRConfigTargets includes identity prefixes and conservatively includes
// managers when a location mask may apply to their MON session. The caller
// holds c.mu.
func (overrideHooks) MGRConfigTargets(c *Container, section string) []string {
	var names []string
	for _, manager := range c.members.roster.eachManager() {
		entity := "mgr." + manager.DaemonName
		if section == "global" || section == entity || strings.HasPrefix(entity, section+".") {
			names = append(names, manager.DaemonName)
		}
	}
	slices.Sort(names)
	return names
}

// WaitMGRConfigDelivery reads the MON configuration layer on the owned
// daemon. The config lock in Ceph covers both the module-option callback
// and installation of the marker from that same snapshot. The caller
// holds c.mu.
func (overrideHooks) WaitMGRConfigDelivery(ctx context.Context, c *Container, name, key, value string) error {
	want, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return errors.New("invalid MGR delivery marker")
	}
	manager := c.members.roster.manager(name)
	if manager == nil {
		return errors.New("owned MGR configuration target is unavailable")
	}
	for {
		state, err := manager.State(ctx)
		if err != nil {
			return fmt.Errorf("inspect mgr.%s for configuration delivery: %w", name, err)
		}
		if state != nil && !state.Paused {
			if !state.Running && !state.Restarting {
				return nil
			}
			matched, err := c.probeMGRConfigDelivery(ctx, manager, key, want)
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for mgr.%s configuration delivery: %w", name, ctx.Err())
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// probeMGRConfigDelivery runs one registered admin-socket invocation with
// c.mu held. A read failure permits retry only after confirmed cleanup.
func (c *Container) probeMGRConfigDelivery(ctx context.Context, manager *ManagerContainer, key string, want float64) (bool, error) {
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	runCtx, cancelCommand := context.WithCancelCause(probe)
	defer cancelCommand(nil)
	daemon := "mgr." + manager.DaemonName
	inv, err := c.admitAdminSocketLocked(runCtx, manager, daemon, cancelCommand)
	if err != nil {
		return false, err
	}
	data, err := c.runAdminSocketInvocation(runCtx, inv, daemon, "config", "diff")
	var unstopped *NativeCommandUnstoppedError
	if errors.As(err, &unstopped) {
		return false, err
	}
	return err == nil && mgrConfigMONValue(data, key, want), nil
}

func mgrConfigMONValue(data []byte, key string, want float64) bool {
	var result map[string]json.RawMessage
	if json.Unmarshal(data, &result) != nil {
		return false
	}
	for range 2 {
		wrapper, exists := result["diff"]
		if !exists {
			break
		}
		var nested map[string]json.RawMessage
		if json.Unmarshal(wrapper, &nested) != nil {
			return false
		}
		result = nested
	}
	var levels struct {
		MON *float64 `json:"mon"`
	}
	return json.Unmarshal(result[key], &levels) == nil && levels.MON != nil && *levels.MON == want
}
