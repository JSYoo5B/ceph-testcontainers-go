package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
)

const (
	monitorQuorumProcessTimeout = 5 * time.Second
	monitorQuorumStdoutLimit    = 1 << 20
	monitorQuorumStderrLimit    = 64 << 10
)

func queryMonitorQuorum(ctx context.Context, control testcontainers.Container) (QuorumStatus, error) {
	var status QuorumStatus
	data, err := monitorQuorumCommand(ctx, control, monitorQuorumProcessTimeout)
	if err != nil {
		return status, err
	}
	// Missing/null native arrays are not an observation of an empty membership.
	// Empty arrays remain a valid native observation for quorum wait predicates.
	var shape struct {
		QuorumNames *[]string `json:"quorum_names"`
		MonMap      *struct {
			Mons *[]struct {
				Name *string `json:"name"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err := json.Unmarshal(data, &shape); err != nil || shape.QuorumNames == nil || shape.MonMap == nil || shape.MonMap.Mons == nil {
		return status, errors.New("invalid native monitor quorum schema")
	}
	members := make(map[string]bool)
	for _, member := range *shape.MonMap.Mons {
		if member.Name == nil || *member.Name == "" || members[*member.Name] {
			return status, errors.New("invalid native monitor membership names")
		}
		members[*member.Name] = true
	}
	quorum := make(map[string]bool)
	for _, name := range *shape.QuorumNames {
		if !members[name] || quorum[name] {
			return status, errors.New("invalid native monitor quorum names")
		}
		quorum[name] = true
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return QuorumStatus{}, fmt.Errorf("decode native monitor quorum: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return QuorumStatus{}, err
	}
	return status, nil
}

func monitorQuorumCommand(ctx context.Context, control testcontainers.Container, processLimit time.Duration) ([]byte, error) {
	if processLimit <= 0 || processLimit > monitorQuorumProcessTimeout {
		return nil, errors.New("invalid monitor quorum process limit")
	}
	// The CLI's connection timeout does not bound command discovery, execution
	// or shutdown, so the read runs under the supervisor.
	result, err := supervised.Command(ctx, control, "monitor quorum", processLimit, monitorQuorumStdoutLimit, monitorQuorumStderrLimit,
		"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json")
	if err != nil {
		return nil, err
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf("quorum_status exited %d: %s", result.ExitCode, strings.TrimSpace(string(result.Stdout)+string(result.Stderr)))
	}
	return result.Stdout, nil
}
