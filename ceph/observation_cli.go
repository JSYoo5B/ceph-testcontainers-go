package ceph

import (
	"context"
	"encoding/json"
	"fmt"
)

// The observations that only run `ceph` commands through the control container
// (Status, ManagerStatus and DaemonAddresses) are implemented here against the
// narrow cliQuery contract. They take no topology lock and no admission check,
// exactly like the exported methods that delegate to them, and every query goes
// through whatever the caller passes in, so the control container is still
// selected on each call by cephConn.ceph. The contract is deliberately not
// given to observations that need the topology lock, a captured control handle
// with the bootstrap identity, a client container or supervised execution.

// cliQuery runs one `ceph` command and returns its standard output. It carries
// the caller's context for cancellation and returns the original error.
type cliQuery func(ctx context.Context, args ...string) ([]byte, error)

func readStatus(ctx context.Context, run cliQuery) (Status, error) {
	var s Status
	data, err := run(ctx, "status", "--format", "json")
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, fmt.Errorf("decode ceph status: %w", err)
	}
	return s, nil
}

func readManagerStatus(ctx context.Context, run cliQuery) (ManagerStatus, error) {
	var status ManagerStatus
	data, err := run(ctx, "mgr", "dump", "--format", "json")
	if err == nil {
		err = json.Unmarshal(data, &status)
	}
	return status, err
}

func readDaemonAddresses(ctx context.Context, run cliQuery) (DaemonAddresses, error) {
	var result DaemonAddresses
	mon, err := run(ctx, "mon", "dump", "--format", "json")
	if err != nil {
		return DaemonAddresses{}, fmt.Errorf("read monmap: %w", err)
	}
	mgr, err := run(ctx, "mgr", "dump", "--format", "json")
	if err != nil {
		return DaemonAddresses{}, fmt.Errorf("read mgrmap: %w", err)
	}
	osd, err := run(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return DaemonAddresses{}, fmt.Errorf("read osdmap: %w", err)
	}
	if err := parseMonitorAddresses(mon, &result); err != nil {
		return DaemonAddresses{}, err
	}
	if err := parseManagerAddresses(mgr, &result); err != nil {
		return DaemonAddresses{}, err
	}
	if err := parseOSDAddresses(osd, &result); err != nil {
		return DaemonAddresses{}, err
	}
	return result, nil
}
