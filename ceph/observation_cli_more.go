package ceph

import (
	"context"
	"errors"
	"fmt"
)

// The observations below also run only `ceph` commands, but their exported
// methods take the topology lock and the liveness check first; that
// acquisition stays on the methods. This file keeps the command and the
// decoding of each, against the same cliQuery contract as observation_cli.go,
// so a fake query can drive them without a cluster. Internal callers that
// reuse a helper (the wait loops and the override families) go through the
// same functions, so there is one command order for each observation. An
// observation that combines several native reads under its own rule stays
// with its domain: the MGR modules in mgr_modules.go and the owned OSD states
// in observation_osd.go.

func readManagerServices(ctx context.Context, run cliQuery) (map[string]string, error) {
	data, err := run(ctx, "mgr", "services", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeManagerServices(data)
}

func readBlocklistEntries(ctx context.Context, run cliQuery) ([]BlocklistEntry, error) {
	data, err := run(ctx, "osd", "blocklist", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeBlocklistEntries(data)
}

func readConfiguration(ctx context.Context, run cliQuery) ([]ConfigEntry, error) {
	data, err := runConfigCommand(ctx, run, "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeConfiguration(data)
}

// runConfigCommand runs `ceph config ...` and redacts a native failure; the
// native output can contain stored values. A cancelled context is reported as
// the context error.
func runConfigCommand(ctx context.Context, run cliQuery, args ...string) ([]byte, error) {
	data, err := run(ctx, append([]string{"config"}, args...)...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("Ceph configuration command: %w", ctx.Err())
		}
		return nil, quiet(errors.New("Ceph configuration command failed; native output is redacted"), err)
	}
	return data, nil
}

func readOSDPolicyDump(ctx context.Context, run cliQuery) (osdPolicySnapshot, error) {
	data, err := run(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return osdPolicySnapshot{osds: make(map[int]OSDState)}, err
	}
	return decodeOSDPolicyDump(data)
}

func readOSDFlags(ctx context.Context, run cliQuery) ([]OSDFlag, error) {
	dump, err := readOSDPolicyDump(ctx, run)
	if err != nil {
		return nil, err
	}
	flags := make([]OSDFlag, len(dump.flags))
	for i, flag := range dump.flags {
		flags[i] = OSDFlag(flag)
	}
	return flags, nil
}
