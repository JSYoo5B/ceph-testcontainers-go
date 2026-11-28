package mirror

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// publicMirrorCalls calls every public Container method that returns an
// error, and the Daemon methods that act through the mirror.
func publicMirrorCalls(m *Container, daemon *Daemon) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"AddDaemon":              func(ctx context.Context) error { _, err := m.AddDaemon(ctx, "x"); return err },
		"AttachManagers":         m.AttachManagers,
		"AddDirectory":           func(ctx context.Context) error { return m.AddDirectory(ctx, "/added") },
		"RemoveDirectory":        func(ctx context.Context) error { return m.RemoveDirectory(ctx, "/owned") },
		"RebalanceDirectories":   m.RebalanceDirectories,
		"PeerIDs":                func(ctx context.Context) error { _, err := m.PeerIDs(ctx); return err },
		"RemovePeer":             func(ctx context.Context) error { return m.RemovePeer(ctx, cephFSObserverDraftPeer) },
		"RebootstrapPeer":        func(ctx context.Context) error { _, err := m.RebootstrapPeer(ctx); return err },
		"BeginDirectoryAddition": func(ctx context.Context) error { _, err := m.BeginDirectoryAddition(ctx, "/added"); return err },
		"BeginDirectoryRemoval":  func(ctx context.Context) error { _, err := m.BeginDirectoryRemoval(ctx, "/owned"); return err },
		"BeginPeerRemoval": func(ctx context.Context) error {
			_, err := m.BeginPeerRemoval(ctx, cephFSObserverDraftPeer)
			return err
		},
		"DirectoryStatus":    func(ctx context.Context) error { _, err := m.DirectoryStatus(ctx, "/owned"); return err },
		"WaitDirectoryReady": func(ctx context.Context) error { _, err := m.WaitDirectoryReady(ctx, "/owned"); return err },
		"WaitSnapshotSynced": func(ctx context.Context) error {
			_, err := m.WaitSnapshotSynced(ctx, "/owned", Snapshot{ID: 1, Name: "s"})
			return err
		},
		"WatchDirectory": func(ctx context.Context) error {
			_, err := m.WatchDirectory(ctx, ceph.WatchConfig{}, "/owned")
			return err
		},
		"RefreshMonitorConfig":     m.RefreshMonitorConfig,
		"RefreshPeerMonitorConfig": m.RefreshPeerMonitorConfig,
		"Daemon.Remove":            func(ctx context.Context) error { return daemon.Remove(ctx) },
		"Daemon.InterruptPeerLink": func(ctx context.Context) error { _, err := daemon.InterruptPeerLink(ctx); return err },
	}
}

// TestPublicMethodsRefuseNilAndTerminatedFixtures calls every public method
// on a nil and on a terminated fixture: none panics, each refuses at once
// with an invalid request.
func TestPublicMethodsRefuseNilAndTerminatedFixtures(t *testing.T) {
	terminated, _, _, _, _ := newPeerRemovalFixture()
	daemon := terminated.roster.daemons[0]
	if err := terminated.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	for fixture, calls := range map[string]map[string]func(context.Context) error{
		"nil":        publicMirrorCalls(nil, &Daemon{}),
		"terminated": publicMirrorCalls(terminated, daemon),
	} {
		for name, call := range calls {
			t.Run(fixture+"/"+name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				started := time.Now()
				err := call(ctx)
				if !errors.Is(err, ceph.ErrInvalidRequest) || errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
					t.Fatalf("%s on a %s fixture after %s: %v", name, fixture, time.Since(started), err)
				}
			})
		}
	}
	var nilMirror *Container
	if nilMirror.Daemons() != nil || nilMirror.RegisteredDirectories() != nil || nilMirror.Terminate(t.Context()) != nil {
		t.Fatal("nil fixture observations are not empty")
	}
}

// TestDirectoryWaitsEndOnATerminatedCluster requires both directory waits
// to end with a guard once a cluster is terminated, as a watch does,
// instead of retrying until their deadline.
func TestDirectoryWaitsEndOnATerminatedCluster(t *testing.T) {
	for _, site := range []string{"source", "destination"} {
		for _, wait := range []string{"ready", "snapshot"} {
			t.Run(site+"/"+wait, func(t *testing.T) {
				f := newDirectoryWatchFixture()
				cluster := f.mirror.pair.source
				if site == "destination" {
					cluster = f.mirror.pair.destination
				}
				if err := cluster.Terminate(t.Context()); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				var err error
				if wait == "ready" {
					_, err = f.mirror.WaitDirectoryReady(ctx, "/owned")
				} else {
					_, err = f.mirror.WaitSnapshotSynced(ctx, "/owned", Snapshot{ID: 1, Name: "s"})
				}
				if !errors.Is(err, errCephFSObservationGuard) || !errors.Is(err, ceph.ErrInvalidRequest) || errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wait on a terminated %s cluster: %v", site, err)
				}
			})
		}
	}
}

// TestRegisteredDirectoriesIsACopyAndDirectoriesFollowsIt checks the
// observation of the registered paths and that the deprecated field is
// still filled with the same paths.
func TestRegisteredDirectoriesIsACopyAndDirectoriesFollowsIt(t *testing.T) {
	m, _, _, _, _ := newPeerRemovalFixture()
	m.mu.Lock()
	m.registerDirectory("/added", true)
	m.mu.Unlock()
	listed := m.RegisteredDirectories()
	if !slices.Equal(listed, []string{"/owned", "/added"}) || !slices.Equal(m.Directories, listed) { //nolint:staticcheck // the deprecated field is still filled
		t.Fatalf("registered %v, Directories %v", listed, m.Directories) //nolint:staticcheck // the deprecated field is still filled
	}
	listed[0] = "/edited"
	m.Directories[1] = "/edited" //nolint:staticcheck // an edited deprecated field is not read back
	if got := m.RegisteredDirectories(); !slices.Equal(got, []string{"/owned", "/added"}) {
		t.Fatalf("an edited copy changed the registration list: %v", got)
	}
	m.mu.Lock()
	m.forgetDirectory("/owned")
	m.mu.Unlock()
	if got := m.RegisteredDirectories(); !slices.Equal(got, []string{"/added"}) || !slices.Equal(m.Directories, got) { //nolint:staticcheck // the deprecated field is still filled
		t.Fatalf("after removal: registered %v, Directories %v", got, m.Directories) //nolint:staticcheck // the deprecated field is still filled
	}
}
