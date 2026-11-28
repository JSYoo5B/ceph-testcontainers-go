package ceph

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestClusterWatchesEndAlikeOnAForeignFSID starts each cluster watch through
// its public entry point, then lets the native FSID turn into that of another
// cluster: every watch ends by itself with a stop cause that matches
// ErrInvalidRequest and names no FSID.
func TestClusterWatchesEndAlikeOnAForeignFSID(t *testing.T) {
	cfg := WatchConfig{Interval: 100 * time.Millisecond, Buffer: 8}
	for _, tc := range []struct {
		name  string
		start func(t *testing.T) (done <-chan struct{}, stop func() error, foreign func())
	}{
		{"WatchHealth", func(t *testing.T) (<-chan struct{}, func() error, func()) {
			cluster, ctr := newHealthWatchCluster()
			observer, _ := Observe(cluster)
			w, err := observer.WatchHealth(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			return w.stream.Done(), w.Stop, func() { ctr.set("fsid", daemonWatchOtherFSID+"\n") }
		}},
		{"WatchPGs", func(t *testing.T) (<-chan struct{}, func() error, func()) {
			_, ctr, pool := newPGWatchCluster(t)
			w, err := pool.WatchPGs(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			return w.stream.Done(), w.Stop, func() { ctr.set(pgWatchFSIDKey, daemonWatchOtherFSID+"\n") }
		}},
		{"WatchMonitors", func(t *testing.T) (<-chan struct{}, func() error, func()) {
			cluster, ctr := newMonWatchCluster()
			observer, _ := Observe(cluster)
			w, err := observer.WatchMonitors(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			return w.stream.Done(), w.Stop, func() { ctr.setQuorum(monWatchQuorumJSON(daemonWatchOtherFSID, 1, "a", "c", "a", "b")) }
		}},
		{"WatchManagers", func(t *testing.T) (<-chan struct{}, func() error, func()) {
			cluster, ctr := newMgrWatchCluster()
			observer, _ := Observe(cluster)
			w, err := observer.WatchManagers(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			return w.stream.Done(), w.Stop, func() { ctr.set("fsid", daemonWatchOtherFSID+"\n") }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done, stop, foreign := tc.start(t)
			foreign()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = stop()
				t.Fatal("the watch did not end on a foreign FSID")
			}
			err := stop()
			if !errors.Is(err, ErrInvalidRequest) || !errors.Is(err, errForeignFSID) || strings.Contains(err.Error(), daemonWatchOtherFSID) {
				t.Fatalf("stop cause = %v, want one matching ErrInvalidRequest that names no FSID", err)
			}
		})
	}
}
