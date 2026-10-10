package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cephStateFake keeps a small model of the OSDMap, CRUSH rules, erasure-code
// profiles, FSMap and the mon_allow_pool_delete entry. Reads are rendered
// from that state and mutations change it, refusing what Ceph refuses, so
// tests assert the resulting state instead of the commands that produced it.
type cephStateFake struct {
	*poolFixtureContainer
	pools       []fakePool
	rules       map[int]string
	profiles    []string
	filesystems []fakeFilesystem
	allowDelete *string
	// failures makes the next N runs of a command fail before any change.
	failures map[string]int
}

type fakePool struct {
	ID      int
	Name    string
	Erasure bool
	Rule    int
	Profile string
}

type fakeFilesystem struct {
	Name, Metadata string
	Data           []string
}

func newCephStateFake() *cephStateFake {
	return &cephStateFake{
		poolFixtureContainer: &poolFixtureContainer{output: map[string]string{}},
		pools:                []fakePool{{ID: 1, Name: ".mgr", Rule: 0}},
		rules:                map[int]string{0: "replicated_rule"},
		profiles:             []string{"default"},
		failures:             map[string]int{},
	}
}

// addFixturePool adds a pool as CreatePool would have created it.
func (f *cephStateFake) addFixturePool(name string, erasure bool) {
	rule := len(f.rules)
	for slices.Contains(sortedKeys(f.rules), rule) {
		rule++
	}
	pool := fakePool{ID: len(f.pools) + 2, Name: name, Erasure: erasure, Rule: rule}
	if erasure {
		f.rules[rule] = "tc-" + name + "-ec"
		pool.Profile = "tc-" + name + "-ec"
		f.profiles = append(f.profiles, pool.Profile)
	} else {
		f.rules[rule] = "tc-" + name + "-replicated"
	}
	f.pools = append(f.pools, pool)
}

func sortedKeys(rules map[int]string) []int {
	keys := make([]int, 0, len(rules))
	for id := range rules {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	return keys
}

func (f *cephStateFake) hasPool(name string) bool {
	return slices.ContainsFunc(f.pools, func(pool fakePool) bool { return pool.Name == name })
}

func (f *cephStateFake) hasRule(name string) bool {
	for _, rule := range f.rules {
		if rule == name {
			return true
		}
	}
	return false
}

// snapshot renders the whole model so a refused call can be shown to leave
// every part of it unchanged.
func (f *cephStateFake) snapshot() string {
	f.render()
	keys := []string{"osd pool ls detail --format json", "osd crush rule dump --format json",
		"osd erasure-code-profile ls --format json", "fs ls --format json", "config dump --format json"}
	var parts []string
	for _, key := range keys {
		parts = append(parts, f.output[key])
	}
	return strings.Join(parts, "\n")
}

func (f *cephStateFake) render() {
	type pool struct {
		ID      int    `json:"pool_id"`
		Name    string `json:"pool_name"`
		Type    int    `json:"type"`
		Size    int    `json:"size"`
		MinSize int    `json:"min_size"`
		PGNum   int    `json:"pg_num"`
		Rule    int    `json:"crush_rule"`
		Profile string `json:"erasure_code_profile,omitempty"`
	}
	pools := []pool{}
	for _, p := range f.pools {
		native := pool{ID: p.ID, Name: p.Name, Type: 1, Size: 2, MinSize: 1, PGNum: 8, Rule: p.Rule, Profile: p.Profile}
		if p.Erasure {
			native.Type, native.Size, native.MinSize = 3, 3, 3
		}
		pools = append(pools, native)
	}
	type rule struct {
		ID   int    `json:"rule_id"`
		Name string `json:"rule_name"`
	}
	rules := []rule{}
	for _, id := range sortedKeys(f.rules) {
		rules = append(rules, rule{id, f.rules[id]})
	}
	type filesystem struct {
		Name     string   `json:"name"`
		Metadata string   `json:"metadata_pool"`
		Data     []string `json:"data_pools"`
	}
	filesystems := []filesystem{}
	for _, fs := range f.filesystems {
		filesystems = append(filesystems, filesystem{fs.Name, fs.Metadata, fs.Data})
	}
	config := []map[string]any{}
	if f.allowDelete != nil {
		config = append(config, map[string]any{"section": "mon", "mask": "", "name": "mon_allow_pool_delete",
			"value": *f.allowDelete, "level": "advanced", "can_update_at_runtime": true})
	}
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		return string(data)
	}
	f.output["osd pool ls detail --format json"] = encode(pools)
	f.output["osd crush rule dump --format json"] = encode(rules)
	f.output["osd erasure-code-profile ls --format json"] = encode(f.profiles)
	f.output["fs ls --format json"] = encode(filesystems)
	f.output["config dump --format json"] = encode(config)
}

func (f *cephStateFake) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.render()
	words := monitorQuorumTestModuleArgs(args)
	command := strings.Join(words, " ")
	f.fail = ""
	if f.failures[command] > 0 {
		f.failures[command]--
		f.fail = command
	} else if refusal := f.refuse(words); refusal != "" {
		// The embedded fake reports every failure with one generic message.
		f.fail = command
	}
	code, reader, err := f.poolFixtureContainer.Exec(ctx, args, opts...)
	if code == 0 && err == nil {
		f.apply(words)
	}
	return code, reader, err
}

// refuse reports why Ceph would reject a mutation in the current state.
func (f *cephStateFake) refuse(words []string) string {
	switch {
	case len(words) == 6 && words[0] == "osd" && words[1] == "pool" && words[2] == "rm":
		if f.allowDelete == nil || *f.allowDelete != "true" {
			return "EPERM: pool deletion is disabled; you must first set the mon_allow_pool_delete config option to true"
		}
		for _, fs := range f.filesystems {
			if fs.Metadata == words[3] || slices.Contains(fs.Data, words[3]) {
				return "EBUSY: pool is in use by CephFS"
			}
		}
	case len(words) == 5 && words[0] == "osd" && words[1] == "crush" && words[3] == "rm":
		for _, pool := range f.pools {
			if f.rules[pool.Rule] == words[4] {
				return "EBUSY: crush rule is in use"
			}
		}
	case len(words) == 4 && words[0] == "osd" && words[1] == "erasure-code-profile" && words[2] == "rm":
		if slices.ContainsFunc(f.pools, func(pool fakePool) bool { return pool.Profile == words[3] }) {
			return "EBUSY: erasure-code profile is in use"
		}
	case len(words) == 3 && words[0] == "fs" && words[1] == "fail":
		if !slices.ContainsFunc(f.filesystems, func(fs fakeFilesystem) bool { return fs.Name == words[2] }) {
			return "ENOENT: filesystem not found"
		}
	}
	return ""
}

func (f *cephStateFake) apply(words []string) {
	switch {
	case len(words) == 6 && words[0] == "osd" && words[1] == "pool" && words[2] == "rm":
		f.pools = slices.DeleteFunc(f.pools, func(pool fakePool) bool { return pool.Name == words[3] })
	case len(words) == 5 && words[0] == "osd" && words[1] == "crush" && words[3] == "rm":
		for id, name := range f.rules {
			if name == words[4] {
				delete(f.rules, id)
			}
		}
	case len(words) == 4 && words[0] == "osd" && words[1] == "erasure-code-profile" && words[2] == "rm":
		f.profiles = slices.DeleteFunc(f.profiles, func(profile string) bool { return profile == words[3] })
	case len(words) == 5 && words[0] == "config" && words[1] == "set" && words[3] == "mon_allow_pool_delete":
		value := words[4]
		f.allowDelete = &value
	case len(words) == 4 && words[0] == "config" && words[1] == "rm" && words[3] == "mon_allow_pool_delete":
		f.allowDelete = nil
	case len(words) >= 3 && words[0] == "fs" && words[1] == "rm":
		f.filesystems = slices.DeleteFunc(f.filesystems, func(fs fakeFilesystem) bool { return fs.Name == words[2] })
	}
}

func (f *cephStateFake) failNext(command string, times int) {
	f.failures[command] = times
}

func (f *cephStateFake) String() string {
	return fmt.Sprintf("pools=%v rules=%v profiles=%v filesystems=%v allow=%v", f.pools, f.rules, f.profiles, f.filesystems, f.allowDelete)
}
