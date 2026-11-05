//go:build all || integration

package cephfs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
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

// cephFSHandleCounters are the counters in front of one cluster's command
// handles: the CLI handle and, when the cluster runs a separate client image,
// the client container that ClientContainer returns. client is nil when the
// client is the CLI handle itself.
type cephFSHandleCounters struct {
	control, client *cephFSMutationCounter
}

func (h cephFSHandleCounters) all() []*cephFSMutationCounter {
	if h.client == nil {
		return []*cephFSMutationCounter{h.control}
	}
	return []*cephFSMutationCounter{h.control, h.client}
}

// cephFSWrapHandles puts a counter in front of control and, when client is a
// different container, in front of client through swapClient, which publishes
// next only while current is still the client. The returned restore puts the
// originals back in the reverse order, CLI handle first.
func cephFSWrapHandles(ctx context.Context, control, client testcontainers.Container, setControl func(testcontainers.Container), swapClient func(ctx context.Context, current, next testcontainers.Container) (bool, error)) (cephFSHandleCounters, func(context.Context) error, error) {
	if control == nil || client == nil {
		return cephFSHandleCounters{}, nil, errors.New("mutation counters need both command handles")
	}
	counters := cephFSHandleCounters{control: &cephFSMutationCounter{Container: control}}
	if client != control {
		counters.client = &cephFSMutationCounter{Container: client}
		if swapped, err := swapClient(ctx, client, counters.client); err != nil || !swapped {
			return cephFSHandleCounters{}, nil, fmt.Errorf("install the client container counter (swapped=%t): %w", swapped, err)
		}
	}
	setControl(counters.control)
	var once sync.Once
	var restoreErr error
	restore := func(ctx context.Context) error {
		once.Do(func() {
			setControl(control)
			if counters.client != nil {
				if swapped, err := swapClient(ctx, counters.client, client); err != nil || !swapped {
					restoreErr = fmt.Errorf("restore the original client container (swapped=%t): %w", swapped, err)
				}
			}
		})
		return restoreErr
	}
	return counters, restore, nil
}

// cephFSInstallMutationCounters puts counters in front of cluster's command
// handles: the embedded CLI handle of a default single-MON fixture and, for a
// separate client image, the started client container. The returned restore
// puts the originals back; a cleanup registered here does the same before the
// fixture cleanups registered earlier run.
func cephFSInstallMutationCounters(t *testing.T, ctx context.Context, cluster *ceph.Container) (cephFSHandleCounters, func()) {
	t.Helper()
	original := cluster.Container
	control, err := cluster.ControlContainer(ctx)
	if err != nil || control != original {
		t.Fatalf("mutation counter needs the original single-MON CLI handle: %v", err)
	}
	client, err := cluster.ClientContainer(ctx)
	if err != nil {
		t.Fatalf("mutation counter needs the client container: %v", err)
	}
	counters, restoreHandles, err := cephFSWrapHandles(ctx, original, client,
		func(handle testcontainers.Container) { cluster.Container = handle },
		func(ctx context.Context, current, next testcontainers.Container) (bool, error) {
			return testhelper.TestSwapFeatureClient(ctx, cluster, current, next)
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mutation counters installed: CLI handle, separate client container %t", counters.client != nil)
	restore := func() error {
		restoreCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return restoreHandles(restoreCtx)
	}
	t.Cleanup(func() {
		if err := restore(); err != nil {
			t.Errorf("restore original command handles: %v", err)
		}
	})
	return counters, func() {
		t.Helper()
		if err := restore(); err != nil {
			t.Fatalf("restore original command handles: %v", err)
		}
	}
}

// cephFSRequireNoObservationalMutation runs observe with the counters in the
// dispatch path and fails when any counter recorded a state-changing command.
// Each counter in required must have received at least one command, which
// shows that it was still in the dispatch path of an observation that must
// read through it.
func cephFSRequireNoObservationalMutation(t *testing.T, name string, counters, required []*cephFSMutationCounter, observe func()) {
	t.Helper()
	before := make(map[*cephFSMutationCounter]map[string]int, len(counters))
	commands := make(map[*cephFSMutationCounter]int64, len(counters))
	for _, counter := range append(slices.Clone(counters), required...) {
		before[counter] = counter.counts()
		commands[counter] = counter.commands.Load()
	}
	observe()
	for _, counter := range counters {
		if err := cephFSMutationsChanged(before[counter], counter.counts()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for i, counter := range required {
		if counter.commands.Load() == commands[counter] {
			t.Fatalf("%s issued no command through required mutation counter %d", name, i)
		}
	}
}
