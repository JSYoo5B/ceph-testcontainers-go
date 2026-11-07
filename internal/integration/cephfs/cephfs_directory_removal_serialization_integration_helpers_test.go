//go:build all || (integration && multicluster)

package cephfs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// testCephFSDirectoryRemovalSerialization maps four directories to one mirror
// daemon and delays every packet that the source's MGR sends to its one OSD
// by a second. The mirroring module's remove holds its policy lock while it
// waits for its RADOS write, and a daemon's acknowledgement of another
// directory's acquire or release, which runs on the same librados callback
// strand and takes that lock, then blocks that write's completion: without
// the fixture's admission a second remove sent right after the first stops
// the module, and its ls does not answer. Here a fifth directory is added and
// all five are removed at once, each with one RemoveDirectory and no retry;
// each must return, and afterwards the native policy, read straight from the
// source's CLI, answers and tracks none of them.
func testCephFSDirectoryRemovalSerialization(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	netem := harness.NetemImage(t)
	source, destination, _, _ := harness.NewMultiClusterPair(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	initial := []string{"/serial-a", "/serial-b", "/serial-c", "/serial-d"}
	const added = "/serial-e"
	mirror, err := cephfsmirror.Run(ctx, harness.NativeMirrorImage(t, "cephfs", harness.StaticFacts(source).Images.CephFS), cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		DestinationSite: "removal-serialization", Directories: initial, DaemonCount: 1,
	}))
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate removal serialization fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	policy := func(ctx context.Context) ([]string, error) {
		data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", sourceFS.FilesystemName)
		if err != nil {
			return nil, err
		}
		var directories []string
		if err := json.Unmarshal(data, &directories); err != nil || directories == nil {
			return nil, fmt.Errorf("native mirror ls: %v: %.256s", err, data)
		}
		return directories, nil
	}
	state := func(ctx context.Context, directory string) (string, error) {
		data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", sourceFS.FilesystemName, directory)
		if err != nil {
			return "", err
		}
		var status struct {
			State *string `json:"state"`
		}
		if err := json.Unmarshal(data, &status); err != nil {
			return "", fmt.Errorf("native dirmap of %s: %v: %.256s", directory, err, data)
		}
		if status.State == nil {
			return "null", nil
		}
		return *status.State, nil
	}
	mapped, err := harness.Observe(ctx, 3*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) ([]string, bool, error) {
		var states []string
		for _, directory := range initial {
			value, err := state(probeCtx, directory)
			if err != nil {
				return states, false, err
			}
			states = append(states, directory+"="+value)
		}
		return states, !slices.ContainsFunc(states, func(s string) bool { return !strings.HasSuffix(s, "=mapped") }), nil
	})
	if err != nil {
		t.Fatalf("the directories are not mapped: %v, %v", mapped.Value, err)
	}

	daemons, err := harness.ObserveCluster(source).DaemonAddresses(ctx)
	if err != nil || len(daemons.OSDs) != 1 || len(daemons.OSDs[0].Public) == 0 {
		t.Fatalf("the source's one OSD: %+v, %v", daemons.OSDs, err)
	}
	osd := harness.AddressIP(t, daemons.OSDs[0].Public[0].Addr)
	lease, err := fault.ApplyNetworkProfile(ctx, source, clusteraccess.InitialManager(t, source), fault.NetworkProfile{
		HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: osd, Delay: time.Second,
	})
	if lease != nil {
		t.Cleanup(func() {
			restoreCtx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := lease.Restore(restoreCtx); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}

	addCtx, stop := context.WithTimeout(ctx, 3*time.Minute)
	err = mirror.AddDirectory(addCtx, added)
	stop()
	if err != nil {
		t.Fatal("add a directory with the slow MGR path:", err)
	}
	original := source.Container
	dispatch := &cephFSRemovalDispatchProbe{Container: original, filesystem: sourceFS.FilesystemName, logf: t.Logf}
	source.Container = dispatch
	defer func() { source.Container = original }()
	paths := append(slices.Clone(initial), added)
	var callers sync.WaitGroup
	callers.Add(len(paths))
	type outcome struct {
		directory string
		elapsed   time.Duration
		err       error
	}
	results := make(chan outcome, len(paths))
	removeCtx, cancelRemoves := context.WithTimeout(ctx, 12*time.Minute)
	defer cancelRemoves()
	for _, directory := range paths {
		go func() {
			callers.Done()
			callers.Wait()
			started := time.Now()
			err := mirror.RemoveDirectory(removeCtx, directory)
			results <- outcome{directory, time.Since(started), err}
		}()
	}
	var timeline []string
	var errs []error
	for range paths {
		result := <-results
		timeline = append(timeline, fmt.Sprintf("%s %s", result.directory, result.elapsed.Round(time.Millisecond)))
		if result.err != nil {
			cancelRemoves()
			errs = append(errs, fmt.Errorf("remove %s: %w", result.directory, result.err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	if len(dispatch.sent) != len(paths) {
		t.Fatalf("observed %d removal dispatches for %d paths", len(dispatch.sent), len(paths))
	}

	// The module answers, and its policy drains.
	drained, err := harness.Observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) ([]string, bool, error) {
		directories, err := policy(probeCtx)
		return directories, err == nil && len(directories) == 0, err
	})
	if err != nil {
		t.Fatalf("the native mirroring policy did not drain: %v, %v", drained.Value, err)
	}
	for _, directory := range append(slices.Clone(initial), added) {
		if value, err := state(ctx, directory); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("native dirmap of removed %s: %q, %v", directory, value, err)
		}
	}
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied {
		t.Fatalf("the MGR's delay: %+v, %v", status, err)
	}
	t.Logf("CEPHFS_MIRROR_REMOVAL_SERIALIZATION %s: MGR to osd %s delayed %s; removals %s; policy drained after %s (%d probes)",
		harness.StaticFacts(source).Images.Essential, osd, time.Second, strings.Join(timeline, ", "), drained.Elapsed.Round(time.Millisecond), drained.Attempts)
}

// cephFSRemovalDispatchProbe inspects the actual admission reads. It adds no
// queries or sleeps that could let a missing production wait pass by chance.
type cephFSRemovalDispatchProbe struct {
	testcontainers.Container
	logf       func(string, ...any)
	filesystem string
	removing   atomic.Bool
	mu         sync.Mutex
	states     map[string]string
	sent       []string
}

func (p *cephFSRemovalDispatchProbe) Exec(ctx context.Context, argv []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	args := argv
	if len(args) >= 3 && args[0] == "ceph" && args[1] == "--connect-timeout" {
		args = args[3:]
	}
	if len(args) < 5 || !slices.Equal(args[:3], []string{"fs", "snapshot", "mirror"}) || args[4] != p.filesystem {
		return p.Container.Exec(ctx, argv, opts...)
	}
	if args[3] == "remove" && len(args) == 6 {
		if !p.removing.CompareAndSwap(false, true) {
			return 0, nil, errors.New("overlapping native directory remove dispatches")
		}
		defer p.removing.Store(false)
		p.mu.Lock()
		err := checkCephFSRemovalDispatch(p.states, p.sent, args[5])
		if err == nil {
			p.logf("remove %s on %s: observed native states %v, previously dispatched %v", args[5], p.filesystem, p.states, p.sent)
			p.sent = append(p.sent, args[5])
			p.states = nil
		}
		p.mu.Unlock()
		if err != nil {
			return 0, nil, err
		}
		return p.Container.Exec(ctx, argv, opts...)
	}
	if !((args[3] == "ls" && len(args) == 5) || (args[3] == "dirmap" && len(args) == 6)) {
		return p.Container.Exec(ctx, argv, opts...)
	}
	code, reader, err := p.Container.Exec(ctx, argv, opts...)
	if err != nil {
		return code, reader, err
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return code, nil, err
	}
	var out, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &stderr, bytes.NewReader(raw)); err != nil {
		return code, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if args[3] == "ls" {
		p.states = nil
		if code == 0 {
			var names []string
			if err := json.Unmarshal(out.Bytes(), &names); err != nil || names == nil {
				return 0, nil, fmt.Errorf("invalid policy ls: %s", out.Bytes())
			}
			p.states = make(map[string]string, len(names))
			for _, name := range names {
				p.states[name] = ""
			}
		}
	} else if _, listed := p.states[args[5]]; listed {
		switch {
		case code == 0:
			var m struct {
				State string `json:"state"`
			}
			if err := json.Unmarshal(out.Bytes(), &m); err != nil {
				return 0, nil, err
			}
			p.states[args[5]] = m.State
		case code == 2 && strings.Contains(stderr.String(), "ENOENT"):
			delete(p.states, args[5])
		default:
			p.states[args[5]] = ""
		}
	}
	return code, bytes.NewReader(raw), nil
}
