//go:build all || integration

package cephfs_mirror_daemon_redistribution_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
)

// redistributionMembership keeps service registrations and filesystem
// watchers in their own identity spaces.
type redistributionMembership struct {
	services, watchers map[string]bool
}

func redistributionWatchers(watchers map[string]string, daemonStatus map[string][]byte, peer string) (map[string]bool, error) {
	byAddress := make(map[string]string, len(watchers))
	for id, address := range watchers {
		if address == "" || id == "" || byAddress[address] != "" {
			return nil, fmt.Errorf("filesystem watcher address is missing or ambiguous")
		}
		byAddress[address] = id
	}
	owned := make(map[string]bool, len(daemonStatus))
	for daemon, raw := range daemonStatus {
		var status struct {
			Address string               `json:"rados_inst"`
			Peers   map[string]*struct{} `json:"peers"`
		}
		if err := strictjson.Decode(raw, &status, strictjson.Options{RejectNull: true}); err != nil {
			return nil, fmt.Errorf("decode filesystem session for %s: %w", daemon, err)
		}
		if status.Peers[peer] == nil {
			return nil, fmt.Errorf("%s has not discovered peer %s", daemon, peer)
		}
		id := byAddress[status.Address]
		if id == "" || owned[id] {
			return nil, fmt.Errorf("%s filesystem session does not identify a distinct watcher", daemon)
		}
		owned[id] = true
	}
	if len(owned) != len(watchers) {
		return nil, fmt.Errorf("filesystem lists %d watchers and %d belong to the sampled daemons", len(watchers), len(owned))
	}
	return owned, nil
}

func redistributionRegisteredServices(serviceDump, daemonStatus []byte, sourceID, filesystem, peer string) (map[string]bool, error) {
	var dump struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := strictjson.Decode(serviceDump, &dump, strictjson.Options{RejectNull: true}); err != nil {
		return nil, fmt.Errorf("decode service dump: %w", err)
	}
	owned := make(map[uint64]bool)
	for id, raw := range dump.Services["cephfs-mirror"].Daemons {
		gid, err := strconv.ParseUint(id, 10, 64)
		if err != nil || gid == 0 {
			continue
		}
		var daemon struct {
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &daemon); err != nil {
			return nil, fmt.Errorf("decode mirror service %s: %w", id, err)
		}
		if daemon.Metadata["id"] == sourceID {
			owned[gid] = true
		}
	}
	var statuses []struct {
		DaemonID    uint64 `json:"daemon_id"`
		Filesystems []struct {
			Name  string `json:"name"`
			Peers []struct {
				UUID string `json:"uuid"`
			} `json:"peers"`
		} `json:"filesystems"`
	}
	if err := strictjson.Decode(daemonStatus, &statuses, strictjson.Options{RejectNull: true}); err != nil {
		return nil, fmt.Errorf("decode daemon status: %w", err)
	}
	registered := make(map[string]bool)
	for _, status := range statuses {
		if !owned[status.DaemonID] {
			continue
		}
		for _, fs := range status.Filesystems {
			if fs.Name != filesystem {
				continue
			}
			for _, p := range fs.Peers {
				if p.UUID == peer {
					registered[strconv.FormatUint(status.DaemonID, 10)] = true
				}
			}
		}
	}
	if len(registered) != len(owned) {
		return nil, fmt.Errorf("peer %s has %d of %d owned service registrations", peer, len(registered), len(owned))
	}
	return registered, nil
}

// redistributionCandidate finds the single new identity in one namespace
// alongside the survivor. The case starts only one new daemon.
func redistributionCandidate(prior, current map[string]bool, survivor string) string {
	if len(current) != 2 || !current[survivor] {
		return ""
	}
	for id := range current {
		if id != "0" && id != "" && !prior[id] && id != survivor {
			return id
		}
	}
	return ""
}

// judgeRedistribution requires matching peer membership on both sides of
// the distribution read. A complete 4/0 map differs from unreadable or stale
// evidence. Only keeping all directories on the survivor is the target
// symptom; moving all directories to the replacement is a different result.
func judgeRedistribution(raw []byte, expected, before, after redistributionMembership, survivor, replacement string, total int) (bool, error) {
	if survivor == replacement || len(expected.watchers) != 2 || len(expected.services) != 2 ||
		!expected.watchers[survivor] || !expected.watchers[replacement] ||
		!maps.Equal(expected.watchers, before.watchers) || !maps.Equal(expected.watchers, after.watchers) ||
		!maps.Equal(expected.services, before.services) || !maps.Equal(expected.services, after.services) {
		return false, fmt.Errorf("observed membership before %+v and after %+v must match admitted services and watchers %+v", before, after, expected)
	}
	var distribution struct {
		Mapping map[string]string `json:"mapping"`
	}
	if err := strictjson.Decode(raw, &distribution, strictjson.Options{RejectNull: true}); err != nil {
		return false, err
	}
	if len(distribution.Mapping) != 2 {
		return false, fmt.Errorf("expected two distribution entries, got %d", len(distribution.Mapping))
	}
	count := func(id string) (int, error) {
		value, ok := strings.CutSuffix(distribution.Mapping[id], " directories")
		n, err := strconv.Atoi(value)
		if !ok || err != nil || n < 0 || n > total {
			return 0, fmt.Errorf("invalid directory count %q for instance %s", distribution.Mapping[id], id)
		}
		return n, nil
	}
	kept, err := count(survivor)
	if err != nil {
		return false, err
	}
	moved, err := count(replacement)
	if err != nil {
		return false, err
	}
	if kept+moved != total {
		return false, fmt.Errorf("distribution accounts for %d directories, want %d", kept+moved, total)
	}
	if kept == 0 {
		return false, fmt.Errorf("all directories moved to the replacement without redistribution")
	}
	return moved != 0, nil
}
