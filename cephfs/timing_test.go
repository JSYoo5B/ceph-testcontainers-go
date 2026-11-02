package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// timingControl answers only the profile's native preflight and central config
// commands. failAfterSet persists a chosen write before losing its reply;
// blocked deliberately models a higher-precedence daemon override. These
// cases test real ConfigHandle ownership and restoration, not native timing.
type timingControl struct {
	testcontainers.Container
	output                map[string]string
	entries               []ceph.ConfigEntry
	calls                 []string
	failAfterSet, blocked string
	versions              map[string]string
}

func (c *timingControl) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	words := clustertest.ModuleArgs(args)
	call := strings.Join(words, " ")
	c.calls = append(c.calls, call)
	var output string
	code := 0
	switch {
	case len(words) == 3 && words[0] == "tell" && words[2] == "version":
		data, _ := json.Marshal(map[string]string{"version": c.versions[words[1]]})
		output = string(data)
	case len(words) == 5 && slices.Equal(words[:2], []string{"config", "show"}):
		return 0, nil, errors.New("unexpected config show arguments")
	case len(words) == 4 && slices.Equal(words[:2], []string{"config", "show"}) && words[3] == "mds_tick_interval":
		output = "5"
		if words[2] != c.blocked {
			for _, entry := range c.entries {
				if entry.Section == words[2] && entry.Name == "mds_tick_interval" {
					output = entry.Value
				}
			}
		}
	case call == "config dump --format json":
		entries := c.entries
		if entries == nil {
			entries = []ceph.ConfigEntry{}
		}
		data, _ := json.Marshal(entries)
		output = string(data)
	case len(words) >= 4 && words[0] == "config" && (words[1] == "set" || words[1] == "rm"):
		c.entries = slices.DeleteFunc(c.entries, func(entry ceph.ConfigEntry) bool {
			return entry.Section == words[2] && entry.Name == words[3]
		})
		if words[1] == "set" {
			c.entries = append(c.entries, ceph.ConfigEntry{Section: words[2], Name: words[3], Value: words[4]})
			if words[2] == c.failAfterSet {
				code, output = 1, "injected lost configuration reply"
			}
		}
	default:
		var ok bool
		output, ok = c.output[call]
		if !ok {
			return 0, nil, fmt.Errorf("unexpected timing command: %s", call)
		}
	}
	return clustertest.Reply(args, code, []byte(output))
}

// timingDaemon supplies only immutable container identity; the native map and
// version responses above are explicit inputs to the ownership checks.
type timingDaemon struct {
	testcontainers.Container
	id string
}

func (d *timingDaemon) GetContainerID() string { return d.id }

func timingFixture(t *testing.T) (*Filesystem, *timingControl) {
	t.Helper()
	control := &timingControl{output: fstest.Output(), versions: map[string]string{"mds.fixture-0": "20.2.4", "mds.fixture-1": "20.2.4"}}
	control.output["fs dump --format json"] = `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":2,"metadata_pool":1,"data_pools":[2,3],"info":{"gid_10":{"name":"fixture-0","rank":0,"gid":10,"state":"up:active"},"gid_11":{"name":"fixture-1","rank":1,"gid":11,"state":"up:active"}}}}],"standbys":[]}`
	cluster := poolFixtureCluster(control, 2)
	fs := newFilesystem(cluster, Config{Name: "fixture", ActiveMDS: 2, MetadataPool: ceph.PoolConfig{Name: "metadata"}, DataPool: ceph.PoolConfig{Name: "data"}}, nil)
	fs.base().ReplaceNative(&fsbase.NativeIdentity{ID: 41, MetadataPool: 1, DefaultPool: 2})
	filesystems(cluster).byName = map[string]*Filesystem{"fixture": fs}
	for index := range 2 {
		container := &timingDaemon{id: fmt.Sprint(index)}
		name := fmt.Sprintf("fixture-%d", index)
		daemon := &MDS{Container: container, ID: name, FilesystemName: "fixture"}
		daemon.identity = &cephFSMDSIdentity{fleet: fs.fleet(), native: fs.base().Native(), descriptor: daemon, container: container, name: name, cid: container.id, confirmed: true}
		fs.mds.roster.daemons = append(fs.mds.roster.daemons, daemon)
		servicehook.SetService(cluster, "mds."+name, container)
		if index == 0 {
			fs.Container = container
		}
	}
	return fs, control
}

func TestOverrideTimingUsesExistingConfigHandlesAndRestoresExactEntries(t *testing.T) {
	fs, control := timingFixture(t)
	prior := ceph.ConfigEntry{Section: "mds.fixture-0", Name: "mds_tick_interval", Value: "5"}
	foreign := ceph.ConfigEntry{Section: "mds", Name: "mds_tick_interval", Value: "5"}
	control.entries = []ceph.ConfigEntry{prior, foreign}
	handles, err := fs.OverrideTiming(t.Context(), TimingProfile{DirectoryPins: true})
	if err != nil || len(handles) != 2 {
		t.Fatalf("apply qualified profile: handles=%d error=%v", len(handles), err)
	}
	for index := len(handles) - 1; index >= 0; index-- {
		if err := handles[index].Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := handles[index].Restore(t.Context()); err != nil {
			t.Fatal("idempotent restore", err)
		}
	}
	if len(control.entries) != 2 || !slices.Contains(control.entries, prior) || !slices.Contains(control.entries, foreign) {
		t.Fatalf("restoration changed absent, existing or inherited entries: %+v", control.entries)
	}
}

func TestOverrideTimingPreservesUncertainAndEarlierHandles(t *testing.T) {
	fs, control := timingFixture(t)
	control.failAfterSet = "mds.fixture-1"
	handles, err := fs.OverrideTiming(t.Context(), TimingProfile{DirectoryPins: true})
	if err == nil || len(handles) != 2 || len(control.entries) != 2 {
		t.Fatalf("uncertain second write lost owned changes: handles=%d entries=%d error=%v", len(handles), len(control.entries), err)
	}
	control.failAfterSet = ""
	for index := len(handles) - 1; index >= 0; index-- {
		if err := handles[index].Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if len(control.entries) != 0 {
		t.Fatal("uncertain write or earlier entry was not restored")
	}
}

func TestOverrideTimingRefusesUnqualifiedOrUnownedTargetsBeforeWrites(t *testing.T) {
	for _, reason := range []string{"version", "baseline", "copied", "descriptor", "native", "foreign", "partial", "closed"} {
		t.Run(reason, func(t *testing.T) {
			fs, control := timingFixture(t)
			switch reason {
			case "version":
				control.versions["mds.fixture-1"] = "20.2.3"
			case "baseline":
				control.entries = []ceph.ConfigEntry{{Section: "mds.fixture-1", Name: "mds_tick_interval", Value: "2"}}
			case "copied":
				copy := *fs
				fs = &copy
			case "descriptor":
				fs.FilesystemName = "foreign"
			case "native":
				fs.base().Native().ID = 42
			case "foreign":
				control.output["fs dump --format json"] = strings.ReplaceAll(control.output["fs dump --format json"], "fixture-1", "external")
			case "partial":
				fs.mds.roster.daemons[1].identity.confirmed = false
			case "closed":
				testhelper.TestSetClosed(fs.base().Cluster(), true)
			}
			handles, err := fs.OverrideTiming(t.Context(), TimingProfile{DirectoryPins: true})
			if err == nil || len(handles) != 0 {
				t.Fatalf("%s admitted: handles=%d error=%v", reason, len(handles), err)
			}
			for _, call := range control.calls {
				if strings.HasPrefix(call, "config set ") || strings.HasPrefix(call, "config rm ") {
					t.Fatal("preflight refusal sent a mutation", call)
				}
			}
		})
	}
}

func TestOverrideTimingEffectiveReadbackFailureRetainsHandleAndReleasesGate(t *testing.T) {
	fs, control := timingFixture(t)
	control.blocked = "mds.fixture-0"
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	handles, err := fs.OverrideTiming(ctx, TimingProfile{DirectoryPins: true})
	if !errors.Is(err, context.DeadlineExceeded) || len(handles) != 1 || len(control.entries) != 1 {
		t.Fatalf("unconsumed change was not retained: handles=%d error=%v", len(handles), err)
	}
	if err := handles[0].Restore(t.Context()); err != nil {
		t.Fatal("real handle cannot restore after admission deadline", err)
	}
	control.blocked = ""
	handles, err = fs.OverrideTiming(t.Context(), TimingProfile{DirectoryPins: true})
	if err != nil || len(handles) != 2 {
		t.Fatal("deadline retained the setup gate", err)
	}
	for _, handle := range slices.Backward(handles) {
		if err := handle.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOverrideTimingZeroCancellationAndAdmissionLocks(t *testing.T) {
	fs, control := timingFixture(t)
	if handles, err := fs.OverrideTiming(t.Context(), TimingProfile{}); err != nil || len(handles) != 0 || len(control.calls) != 0 {
		t.Fatal("empty profile had an effect", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := fs.OverrideTiming(ctx, TimingProfile{}); !errors.Is(err, context.Canceled) {
		t.Fatal("empty profile lost cancellation", err)
	}
	for _, mutex := range []*sync.Mutex{setupGate(fs.base().Cluster()), testhelper.TestState(fs.base().Cluster()).Mutex} {
		assertContextLockQueued(t, mutex, func(ctx context.Context) error {
			_, err := fs.OverrideTiming(ctx, TimingProfile{DirectoryPins: true})
			return err
		})
	}
	if len(control.calls) != 0 {
		t.Fatal("queued admission issued native commands", control.calls)
	}
	var missing *Filesystem
	if _, err := missing.OverrideTiming(t.Context(), TimingProfile{DirectoryPins: true}); err == nil {
		t.Fatal("nil filesystem accepted")
	}
}
