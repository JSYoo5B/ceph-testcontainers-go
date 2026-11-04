//go:build all || integration

package cephfs_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfssession "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
)

// sessionWatchLog runs a session.Watch and keeps every event it sends, so
// that a burst of gaps during a takeover cannot overflow the watch's buffer.
type sessionWatchLog struct {
	watch  *cephfssession.Watcher
	mu     sync.Mutex
	events []cephfssession.WatchEvent
	notify chan struct{}
	closed chan struct{}
	cause  error
}

// startSessionWatchLog starts the watch of clients on fs and returns once its
// baseline is logged. The watch stops in t's cleanup at the latest.
func startSessionWatchLog(t *testing.T, ctx context.Context, fs *cephfs.Filesystem, interval time.Duration, clients ...uint64) *sessionWatchLog {
	t.Helper()
	w, err := cephfssession.Watch(ctx, fs, ceph.WatchConfig{Interval: interval, QueryTimeout: 15 * time.Second, Buffer: 64}, clients...)
	if err != nil {
		t.Fatal("watch sessions:", err)
	}
	l := &sessionWatchLog{watch: w, notify: make(chan struct{}, 1), closed: make(chan struct{})}
	go func() {
		defer close(l.closed)
		for event := range w.Events() {
			l.mu.Lock()
			l.events = append(l.events, event)
			l.mu.Unlock()
			select {
			case l.notify <- struct{}{}:
			default:
			}
		}
	}()
	t.Cleanup(func() { l.stop(t) })
	if base := l.await(t, ctx, time.Minute, func(e cephfssession.WatchEvent) bool { return e.Kind == ceph.WatchBaseline }); base.Sequence != 1 {
		t.Fatalf("session watch baseline = %+v", base)
	}
	return l
}

// await returns the first logged event that match accepts, waiting up to
// timeout for it.
func (l *sessionWatchLog) await(t *testing.T, ctx context.Context, timeout time.Duration, match func(cephfssession.WatchEvent) bool) cephfssession.WatchEvent {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		for _, event := range l.snapshot() {
			if match(event) {
				return event
			}
		}
		select {
		case <-l.notify:
		case <-l.closed:
			for _, event := range l.snapshot() {
				if match(event) {
					return event
				}
			}
			t.Fatalf("the session watch ended (%v) without the awaited event: %+v", l.stop(t), l.snapshot())
		case <-deadline.C:
			t.Fatalf("no awaited session watch event within %s: %+v", timeout, l.snapshot())
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func (l *sessionWatchLog) snapshot() []cephfssession.WatchEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

// stop ends the watch, waits for the log and returns the watch's cause.
func (l *sessionWatchLog) stop(t *testing.T) error {
	t.Helper()
	l.cause = l.watch.Stop()
	<-l.closed
	return l.cause
}

// checkNoRemoval requires consecutive sequence numbers, gaps without
// sessions and no removal of a client in ids, and counts the gaps.
func (l *sessionWatchLog) checkNoRemoval(t *testing.T, ids ...uint64) (gaps int) {
	t.Helper()
	for i, event := range l.snapshot() {
		if event.Sequence != uint64(i+1) {
			t.Fatalf("session watch event %d has sequence %d", i+1, event.Sequence)
		}
		if event.Kind == ceph.WatchGap {
			gaps++
			if event.Sessions != nil || event.Removed != nil {
				t.Fatalf("a session watch gap reported sessions: %+v", event)
			}
		}
		for _, removed := range event.Removed {
			if slices.Contains(ids, removed.ID) {
				t.Fatalf("session watch removed surviving client.%d: %+v", removed.ID, event)
			}
		}
	}
	return gaps
}

func sessionIDs(sessions []cephfssession.Session) []uint64 {
	var ids []uint64
	for _, s := range sessions {
		ids = append(ids, s.ID)
	}
	return ids
}
