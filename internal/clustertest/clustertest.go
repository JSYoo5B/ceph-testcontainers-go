// Package clustertest holds unit-test fakes and assertions that package ceph,
// the service packages and internal/multicluster share. CannedCommandContainer
// answers each Ceph command from a table, StateFake keeps a small model of the cluster that
// commands read and change, and the lock helpers check that operations wait
// for a held lock without native effects.
package clustertest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Quorum reports a supervised quorum read: a Python supervisor that runs
// "ceph --connect-timeout 5 quorum_status --format json" with bounded output.
func Quorum(args []string) bool {
	return len(args) == 12 && args[0] == "python3" && args[1] == "-c" &&
		slices.Equal(args[6:], []string{"ceph", "--connect-timeout", "5", "quorum_status", "--format", "json"})
}

// ModuleArgs strips the CLI prefix, "ceph --connect-timeout 5" or the quorum
// supervisor, and returns the command's own words.
func ModuleArgs(args []string) []string {
	if Quorum(args) {
		return args[9:]
	}
	return args[3:]
}

// QuorumEnvelope encodes the supervisor's reply for a quorum read.
func QuorumEnvelope(stdout, stderr []byte, code int, timeout, clipped bool) []byte {
	data, _ := json.Marshal(struct {
		SchemaVersion   int    `json:"schema_version"`
		Stdout          string `json:"stdout"`
		Stderr          string `json:"stderr"`
		ExitCode        int    `json:"exit_code"`
		TimedOut        bool   `json:"timed_out"`
		StdoutTruncated bool   `json:"stdout_truncated"`
		StderrTruncated bool   `json:"stderr_truncated"`
	}{1, base64.StdEncoding.EncodeToString(stdout), base64.StdEncoding.EncodeToString(stderr), code, timeout, clipped, false})
	return data
}

// Reply returns output as the Exec result of args: a supervisor envelope for
// a quorum read, or a multiplexed stdout stream otherwise.
func Reply(args []string, code int, output []byte) (int, io.Reader, error) {
	if Quorum(args) {
		return 0, bytes.NewReader(QuorumEnvelope(output, nil, code, false, false)), nil
	}
	var stream bytes.Buffer
	header := make([]byte, 8)
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	stream.Write(header)
	stream.Write(output)
	return code, &stream, nil
}

// CannedCommandContainer is a control container whose Exec answers each
// command from Output, keyed by the command's words joined with
// spaces. Commands with an ls word default to an empty JSON list. The
// command equal to Fail exits FailCode, or 1, and writes FailReason, or a
// generic message, to standard error. Calls records every command's words.
type CannedCommandContainer struct {
	testcontainers.Container
	Calls      [][]string
	Output     map[string]string
	Fail       string
	FailReason string
	FailCode   int
}

// Exec answers args from the table.
func (ctr *CannedCommandContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	commandArgs := args
	args = slices.Clone(ModuleArgs(args))
	ctr.Calls = append(ctr.Calls, args)
	command := strings.Join(args, " ")
	output := ""
	if slices.Contains(args, "ls") {
		output = "[]"
	}
	if value, ok := ctr.Output[command]; ok {
		output = value
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	code := 0
	if command == ctr.Fail {
		header[0], code, output = byte(stdcopy.Stderr), 1, "injected command failure"
		if ctr.FailCode != 0 {
			code = ctr.FailCode
		}
		if ctr.FailReason != "" {
			output = ctr.FailReason
		}
	}
	if Quorum(commandArgs) {
		return Reply(commandArgs, code, []byte(output))
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}

// StateFake keeps a small model of the OSDMap, CRUSH rules, erasure-code
// profiles, FSMap and the mon_allow_pool_delete entry. Reads are rendered
// from that state and mutations change it, refusing what Ceph refuses, so
// tests assert the resulting state instead of the commands that produced it.
type StateFake struct {
	*CannedCommandContainer
	Pools       []FakePool
	Rules       map[int]string
	Profiles    []string
	Filesystems []FakeFilesystem
	AllowDelete *string
	// failures makes the next N runs of a command fail before any change.
	failures map[string]int
}

// FakePool is a pool in the StateFake model.
type FakePool struct {
	ID      int
	Name    string
	Erasure bool
	Rule    int
	Profile string
}

// FakeFilesystem is a filesystem in the StateFake model.
type FakeFilesystem struct {
	Name, Metadata string
	Data           []string
}

// NewStateFake returns a model with only the .mgr pool and the default rule
// and profile.
func NewStateFake() *StateFake {
	return &StateFake{
		CannedCommandContainer: &CannedCommandContainer{Output: map[string]string{}},
		Pools:                  []FakePool{{ID: 1, Name: ".mgr", Rule: 0}},
		Rules:                  map[int]string{0: "replicated_rule"},
		Profiles:               []string{"default"},
		failures:               map[string]int{},
	}
}

// AddFixturePool adds a pool as CreatePool would have created it.
func (f *StateFake) AddFixturePool(name string, erasure bool) {
	rule := len(f.Rules)
	for slices.Contains(sortedKeys(f.Rules), rule) {
		rule++
	}
	pool := FakePool{ID: len(f.Pools) + 2, Name: name, Erasure: erasure, Rule: rule}
	if erasure {
		f.Rules[rule] = "tc-" + name + "-ec"
		pool.Profile = "tc-" + name + "-ec"
		f.Profiles = append(f.Profiles, pool.Profile)
	} else {
		f.Rules[rule] = "tc-" + name + "-replicated"
	}
	f.Pools = append(f.Pools, pool)
}

func sortedKeys(rules map[int]string) []int {
	keys := make([]int, 0, len(rules))
	for id := range rules {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	return keys
}

// HasPool reports a pool in the model.
func (f *StateFake) HasPool(name string) bool {
	return slices.ContainsFunc(f.Pools, func(pool FakePool) bool { return pool.Name == name })
}

// HasRule reports a CRUSH rule in the model.
func (f *StateFake) HasRule(name string) bool {
	for _, rule := range f.Rules {
		if rule == name {
			return true
		}
	}
	return false
}

// Snapshot renders the whole model so a refused call can be shown to leave
// every part of it unchanged.
func (f *StateFake) Snapshot() string {
	f.render()
	keys := []string{"osd pool ls detail --format json", "osd crush rule dump --format json",
		"osd erasure-code-profile ls --format json", "fs ls --format json", "config dump --format json"}
	var parts []string
	for _, key := range keys {
		parts = append(parts, f.Output[key])
	}
	return strings.Join(parts, "\n")
}

func (f *StateFake) render() {
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
	for _, p := range f.Pools {
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
	for _, id := range sortedKeys(f.Rules) {
		rules = append(rules, rule{id, f.Rules[id]})
	}
	type filesystem struct {
		Name     string   `json:"name"`
		Metadata string   `json:"metadata_pool"`
		Data     []string `json:"data_pools"`
	}
	filesystems := []filesystem{}
	for _, fs := range f.Filesystems {
		filesystems = append(filesystems, filesystem(fs))
	}
	config := []map[string]any{}
	if f.AllowDelete != nil {
		config = append(config, map[string]any{"section": "mon", "mask": "", "name": "mon_allow_pool_delete",
			"value": *f.AllowDelete, "level": "advanced", "can_update_at_runtime": true})
	}
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		return string(data)
	}
	f.Output["osd pool ls detail --format json"] = encode(pools)
	f.Output["osd crush rule dump --format json"] = encode(rules)
	f.Output["osd erasure-code-profile ls --format json"] = encode(f.Profiles)
	f.Output["fs ls --format json"] = encode(filesystems)
	f.Output["config dump --format json"] = encode(config)
}

// Exec renders reads from the model and applies the mutations Ceph would
// accept.
func (f *StateFake) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.render()
	words := ModuleArgs(args)
	command := strings.Join(words, " ")
	f.Fail, f.FailReason, f.FailCode = "", "", 0
	if f.failures[command] > 0 {
		f.failures[command]--
		f.Fail = command
	} else if refusal := f.refuse(words); refusal != "" {
		// The ceph CLI prints the MON's errno and reason and exits with the
		// errno.
		name, _, _ := strings.Cut(refusal, ":")
		f.Fail, f.FailReason, f.FailCode = command, "Error "+refusal+"\n", errnos[name]
	}
	code, reader, err := f.CannedCommandContainer.Exec(ctx, args, opts...)
	if code == 0 && err == nil {
		f.apply(words)
	}
	return code, reader, err
}

// errnos are the Linux values of the errnos that refuse returns.
var errnos = map[string]int{"EPERM": 1, "ENOENT": 2, "EBUSY": 16}

// refuse reports why Ceph would reject a mutation in the current state.
func (f *StateFake) refuse(words []string) string {
	switch {
	case len(words) == 6 && words[0] == "osd" && words[1] == "pool" && words[2] == "rm":
		if f.AllowDelete == nil || *f.AllowDelete != "true" {
			return "EPERM: pool deletion is disabled; you must first set the mon_allow_pool_delete config option to true"
		}
		for _, fs := range f.Filesystems {
			if fs.Metadata == words[3] || slices.Contains(fs.Data, words[3]) {
				return "EBUSY: pool is in use by CephFS"
			}
		}
	case len(words) == 5 && words[0] == "osd" && words[1] == "crush" && words[3] == "rm":
		for _, pool := range f.Pools {
			if f.Rules[pool.Rule] == words[4] {
				return "EBUSY: crush rule is in use"
			}
		}
	case len(words) == 4 && words[0] == "osd" && words[1] == "erasure-code-profile" && words[2] == "rm":
		if slices.ContainsFunc(f.Pools, func(pool FakePool) bool { return pool.Profile == words[3] }) {
			return "EBUSY: erasure-code profile is in use"
		}
	case len(words) == 3 && words[0] == "fs" && words[1] == "fail":
		if !slices.ContainsFunc(f.Filesystems, func(fs FakeFilesystem) bool { return fs.Name == words[2] }) {
			return "ENOENT: filesystem not found"
		}
	}
	return ""
}

func (f *StateFake) apply(words []string) {
	switch {
	case len(words) == 6 && words[0] == "osd" && words[1] == "pool" && words[2] == "rm":
		f.Pools = slices.DeleteFunc(f.Pools, func(pool FakePool) bool { return pool.Name == words[3] })
	case len(words) == 5 && words[0] == "osd" && words[1] == "crush" && words[3] == "rm":
		for id, name := range f.Rules {
			if name == words[4] {
				delete(f.Rules, id)
			}
		}
	case len(words) == 4 && words[0] == "osd" && words[1] == "erasure-code-profile" && words[2] == "rm":
		f.Profiles = slices.DeleteFunc(f.Profiles, func(profile string) bool { return profile == words[3] })
	case len(words) == 5 && words[0] == "config" && words[1] == "set" && words[3] == "mon_allow_pool_delete":
		value := words[4]
		f.AllowDelete = &value
	case len(words) == 4 && words[0] == "config" && words[1] == "rm" && words[3] == "mon_allow_pool_delete":
		f.AllowDelete = nil
	case len(words) >= 3 && words[0] == "fs" && words[1] == "rm":
		f.Filesystems = slices.DeleteFunc(f.Filesystems, func(fs FakeFilesystem) bool { return fs.Name == words[2] })
	}
}

// FailNext makes the next times runs of command fail before any change.
func (f *StateFake) FailNext(command string, times int) {
	f.failures[command] = times
}

func (f *StateFake) String() string {
	return fmt.Sprintf("pools=%v rules=%v profiles=%v filesystems=%v allow=%v", f.Pools, f.Rules, f.Profiles, f.Filesystems, f.AllowDelete)
}
