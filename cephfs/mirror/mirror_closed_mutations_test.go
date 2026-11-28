package mirror

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestCephFSMirrorPolicyMutationsRefuseTerminatedOwner(t *testing.T) {
	for _, cleanupFailure := range []bool{false, true} {
		for _, receipt := range []string{"none", "directory", "peer"} {
			t.Run(receipt+map[bool]string{false: "/clean", true: "/retry"}[cleanupFailure], func(t *testing.T) {
				m, s, _, _, _ := newDirectoryRemovalFixture()
				switch receipt {
				case "directory":
					if _, err := m.beginDirectoryRemoval(t.Context(), "/owned", peerRemovalClusterReader); err != nil {
						t.Fatal(err)
					}
				case "peer":
					if _, err := m.beginPeerRemoval(t.Context(), m.peer.id, peerRemovalClusterReader); err != nil {
						t.Fatal(err)
					}
				}
				failed := cleanupFailure
				m.owned.AddCleanup("cleanup failure control", func(context.Context) error {
					if failed {
						return errors.New("cleanup unavailable")
					}
					return nil
				})
				if err := m.Terminate(t.Context()); (err != nil) != cleanupFailure {
					t.Fatalf("termination: %v", err)
				}
				beforePaths := slices.Clone(m.Directories)
				beforeNative := slices.Clone(s.directories)
				peer := m.peer.id
				calls, adds, removes, peers := len(s.calls), s.adds, s.removals, s.peerRemovalControlFake.removals
				operations := map[string]func() error{
					"add":            func() error { return m.AddDirectory(t.Context(), "/new") },
					"remove":         func() error { return m.RemoveDirectory(t.Context(), "/owned") },
					"peer":           func() error { return m.RemovePeer(t.Context(), cephFSObserverDraftPeer) },
					"tracked-add":    func() error { _, err := m.BeginDirectoryAddition(t.Context(), "/new"); return err },
					"tracked-remove": func() error { _, err := m.BeginDirectoryRemoval(t.Context(), "/owned"); return err },
					"tracked-peer":   func() error { _, err := m.BeginPeerRemoval(t.Context(), cephFSObserverDraftPeer); return err },
				}
				for name, operation := range operations {
					err := operation()
					if err == nil {
						t.Errorf("closed owner admitted %s", name)
					}
					if strings.HasPrefix(name, "tracked-") && !errors.Is(err, errCephFSObservationGuard) {
						t.Errorf("tracked %s did not return its permanent ownership guard: %v", name, err)
					}
				}
				if len(s.calls) != calls || s.adds != adds || s.removals != removes || s.peerRemovalControlFake.removals != peers {
					t.Error("closed owner dispatched native queries or mutations")
				}
				if !slices.Equal(m.Directories, beforePaths) || !slices.Equal(s.directories, beforeNative) || m.peer.id != peer {
					t.Error("closed owner changed policy or ownership")
				}
				failed = false
				if err := m.Terminate(t.Context()); err != nil {
					t.Fatalf("cleanup retry: %v", err)
				}
			})
		}
	}
}

func TestCephFSMirrorLiveZeroDaemonPolicyMutations(t *testing.T) {
	m, s, _, _, _ := newDirectoryRemovalFixture()
	m.roster.daemons = nil
	m.Container = nil
	if err := m.AddDirectory(t.Context(), "/new"); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveDirectory(t.Context(), "/new"); err != nil {
		t.Fatal(err)
	}
	if err := m.RemovePeer(t.Context(), m.peer.id); err != nil {
		t.Fatal(err)
	}
	if s.adds != 1 || s.removals != 1 || s.peerRemovalControlFake.removals != 1 {
		t.Fatal("live zero-daemon fixture lost policy mutations")
	}
}
