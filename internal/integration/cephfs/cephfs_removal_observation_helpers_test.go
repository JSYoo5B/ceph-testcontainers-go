//go:build all || integration

package cephfs_test

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cephFSMutationCounter is a transparent recorder in front of a cluster's CLI
// handle. Every command reaches the real container unchanged and its reply is
// returned unchanged; the counter only records which commands could change
// cluster state, so an observational API that dispatched one is visible even
// when the command was idempotent and left the final state unchanged.
type cephFSMutationCounter struct {
	testcontainers.Container
	commands  atomic.Int64
	mu        sync.Mutex
	mutations map[string]int
}

func (c *cephFSMutationCounter) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.commands.Add(1)
	if kind := cephFSMutationKind(args); kind != "" {
		c.mu.Lock()
		if c.mutations == nil {
			c.mutations = make(map[string]int)
		}
		c.mutations[kind]++
		c.mu.Unlock()
	}
	return c.Container.Exec(ctx, args, opts...)
}

// counts returns a copy of the recorded mutation counts by kind.
func (c *cephFSMutationCounter) counts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.mutations)
}

// cephFSValuedFlags are the CLI flags whose value is a separate word.
var cephFSValuedFlags = map[string]bool{"--connect-timeout": true, "--format": true, "-f": true, "--pool": true, "-p": true, "--cluster": true, "-c": true, "--conf": true, "--id": true, "-i": true, "--name": true, "-n": true, "--keyring": true, "-k": true}

// cephFSCephMutations are the ceph CLI command prefixes that change cluster
// state and that no read-only observation of a removal receipt may issue.
var cephFSCephMutations = [][]string{
	{"fs", "snapshot", "mirror", "add"}, {"fs", "snapshot", "mirror", "remove"},
	{"fs", "snapshot", "mirror", "enable"}, {"fs", "snapshot", "mirror", "disable"},
	{"fs", "snapshot", "mirror", "peer_add"}, {"fs", "snapshot", "mirror", "peer_remove"},
	{"fs", "snapshot", "mirror", "peer_bootstrap"},
	{"mgr", "module", "enable"}, {"mgr", "module", "disable"},
	{"auth", "get-or-create"}, {"auth", "get-or-create-key"}, {"auth", "add"}, {"auth", "import"},
	{"auth", "caps"}, {"auth", "del"}, {"auth", "rm"},
	{"config", "set"}, {"config", "rm"}, {"config-key", "set"}, {"config-key", "rm"},
	{"fs", "new"}, {"fs", "rm"}, {"fs", "set"}, {"fs", "fail"}, {"fs", "add_data_pool"},
	{"fs", "rm_data_pool"}, {"fs", "authorize"},
	{"osd", "pool", "create"}, {"osd", "pool", "rm"}, {"osd", "pool", "set"}, {"osd", "pool", "application"},
}

// cephFSRadosMutations are the rados verbs that write objects or watches.
var cephFSRadosMutations = []string{"put", "rm", "create", "append", "truncate", "cp", "setxattr", "rmxattr", "setomapval", "rmomapkey", "clearomap", "setomapheader"}

// cephFSMutationKind names the state-changing command args runs, or returns
// "" for a read. A command run under the fixture's process supervisor is
// classified by the command it supervises, and the supervisor's canceler,
// which only stops such a command, changes no cluster state. ceph and rados
// commands are classified by their command words with flags removed; any
// other program (a shell, an interpreter) can change anything, so it counts
// as a mutation of its own kind.
func cephFSMutationKind(args []string) string {
	if len(args) == 0 {
		return "exec <empty>"
	}
	if len(args) >= 3 && args[1] == "-c" {
		switch {
		case args[0] == "python3" && args[2] == supervised.Script && len(args) > 6:
			return cephFSMutationKind(args[6:])
		case args[0] == "sh" && args[2] == supervised.ShellScript && len(args) > 7:
			return cephFSMutationKind(args[7:])
		case args[0] == "python3" && args[2] == supervised.CancelScript,
			args[0] == "sh" && args[2] == supervised.ShellCancelScript:
			return ""
		}
	}
	var words []string
	for i := 1; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-") {
			if !strings.Contains(args[i], "=") && cephFSValuedFlags[args[i]] {
				i++
			}
			continue
		}
		words = append(words, args[i])
	}
	switch args[0] {
	case "ceph":
		for _, prefix := range cephFSCephMutations {
			if len(words) >= len(prefix) && slices.Equal(words[:len(prefix)], prefix) {
				return "ceph " + strings.Join(prefix, " ")
			}
		}
		return ""
	case "rados":
		if len(words) > 0 && slices.Contains(cephFSRadosMutations, words[0]) {
			return "rados " + words[0]
		}
		return ""
	default:
		return "exec " + args[0]
	}
}

// cephFSMutationsChanged reports every mutation kind whose count differs
// between two snapshots. Reads are never counted, so the total number of
// commands is free to change.
func cephFSMutationsChanged(before, after map[string]int) error {
	kinds := append(slices.Collect(maps.Keys(before)), slices.Collect(maps.Keys(after))...)
	slices.Sort(kinds)
	var changed []string
	for _, kind := range slices.Compact(kinds) {
		if before[kind] != after[kind] {
			changed = append(changed, fmt.Sprintf("%s: %d -> %d", kind, before[kind], after[kind]))
		}
	}
	if len(changed) == 0 {
		return nil
	}
	return fmt.Errorf("observation dispatched state-changing commands: %s", strings.Join(changed, ", "))
}

// cephFSInstallMutationCounter puts a counter in front of cluster's embedded
// CLI handle, which is the command container of a default single-MON
// fixture. The returned restore puts the original handle back; a cleanup
// registered here does the same before the fixture cleanups registered
// earlier run.
func cephFSInstallMutationCounter(t *testing.T, ctx context.Context, cluster *ceph.Container) (*cephFSMutationCounter, func()) {
	t.Helper()
	original := cluster.Container
	control, err := cluster.ControlContainer(ctx)
	if err != nil || control != original {
		t.Fatalf("mutation counter needs the original single-MON CLI handle: %v", err)
	}
	counter := &cephFSMutationCounter{Container: original}
	cluster.Container = counter
	restore := func() { cluster.Container = original }
	t.Cleanup(restore)
	return counter, restore
}

// cephFSRequireNoObservationalMutation runs observe with the counters in the
// dispatch path and fails when any counter recorded a state-changing command.
// It returns how many commands reached each counter, so a caller can show
// that a counter was still in the dispatch path of an observation that must
// read through it.
func cephFSRequireNoObservationalMutation(t *testing.T, name string, counters []*cephFSMutationCounter, observe func()) []int64 {
	t.Helper()
	before := make([]map[string]int, len(counters))
	commands := make([]int64, len(counters))
	for i, counter := range counters {
		before[i] = counter.counts()
		commands[i] = -counter.commands.Load()
	}
	observe()
	for i, counter := range counters {
		commands[i] += counter.commands.Load()
		if err := cephFSMutationsChanged(before[i], counter.counts()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	return commands
}
