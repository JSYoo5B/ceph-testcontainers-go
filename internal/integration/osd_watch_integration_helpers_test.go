//go:build all || (integration && features)

package integration_test

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const osdWatchPool = "tc-osd-watch"

// osdWatchWorkload plays the SUT: it writes and reads back objects through
// the rados CLI of the client container until stopped, independently of the
// watch. Each operation records when it started, so the test can count the
// ones that ran entirely inside a fault window.
type osdWatchWorkload struct {
	mu     sync.Mutex
	starts []time.Time
	ends   []time.Time
	failed []string
	done   chan struct{}
}

func startOSDWatchWorkload(ctx context.Context, client testcontainers.Container) *osdWatchWorkload {
	w := &osdWatchWorkload{done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for n := 0; ctx.Err() == nil; n++ {
			started := time.Now()
			name := "watch-" + strconv.Itoa(n)
			err := osdWatchRados(ctx, client, "put", name, "/etc/hosts")
			if err == nil {
				err = osdWatchRados(ctx, client, "get", name, "/tmp/"+name)
			}
			w.mu.Lock()
			if err == nil {
				w.starts, w.ends = append(w.starts, started), append(w.ends, time.Now())
			} else if ctx.Err() == nil {
				w.failed = append(w.failed, fmt.Sprintf("%s: %v", name, err))
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func osdWatchRados(ctx context.Context, client testcontainers.Container, args ...string) error {
	code, r, err := client.Exec(ctx, append([]string{"timeout", "60", "rados", "-p", osdWatchPool}, args...), tcexec.Multiplexed())
	if err != nil {
		return err
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		return fmt.Errorf("rados %s exited %d: %s", args[0], code, out)
	}
	return nil
}

// completedWithin counts the operations that started at or after from and
// finished before to.
func (w *osdWatchWorkload) completedWithin(from, to time.Time) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for i, s := range w.starts {
		if !s.Before(from) && w.ends[i].Before(to) {
			n++
		}
	}
	return n
}

func (w *osdWatchWorkload) failures() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.failed...)
}

// awaitOSDWatchEvent reads events until match accepts one, and fails on a
// closed watch or after limit. Gaps are counted, not fatal: a read can fail
// while an OSD restarts.
func awaitOSDWatchEvent(t *testing.T, w *ceph.OSDWatch, last *uint64, gaps *int, limit time.Duration, what string, match func(ceph.OSDWatchEvent) bool) ceph.OSDWatchEvent {
	t.Helper()
	timeout := time.After(limit)
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				t.Fatalf("%s: watch ended: %v", what, w.Stop())
			}
			if event.Sequence != *last+1 {
				t.Fatalf("%s: event %d follows %d", what, event.Sequence, *last)
			}
			*last = event.Sequence
			if event.Kind == ceph.WatchGap {
				*gaps++
				t.Logf("%s: gap at event %d after %v: %v", what, event.Sequence, event.Finished.Sub(event.Started), event.Err)
				continue
			}
			if match(event) {
				return event
			}
			t.Logf("%s: event %d %s epoch %d: %+v", what, event.Sequence, event.Kind, event.Epoch, event.States)
		case <-timeout:
			t.Fatalf("%s: no matching event within %v", what, limit)
		}
	}
}

func osdWatchState(event ceph.OSDWatchEvent, id int) (ceph.OSDState, bool) {
	for _, s := range event.States {
		if s.ID == id {
			return s, true
		}
	}
	return ceph.OSDState{}, false
}

// testOSDWatch observes one owned OSD go down and come back through a watch
// while the client keeps writing. The down event proves only that a read of
// the MON's OSDMap reported the same owned OSD (same ID and UUID) down at
// that epoch; the workload's own reads prove the data path.
func testOSDWatch(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3))}, opts...)
	cluster, client := harness.NewServiceCluster(t, opts...)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: osdWatchPool, Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// The cost of one read, the same osd dump the watch makes.
	const samples = 10
	began := time.Now()
	for range samples {
		if _, err := observer.OSDStates(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("query cost: %d OSDStates reads took %v (%v each)", samples, time.Since(began), time.Since(began)/samples)

	const interval = time.Second
	watchStarted := time.Now()
	w, err := observer.WatchOSDs(ctx, ceph.WatchConfig{Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the cluster cleanup, so it runs before it.
	t.Cleanup(func() { _ = w.Stop() })
	var last uint64
	gaps := 0
	baseline := awaitOSDWatchEvent(t, w, &last, &gaps, time.Second, "baseline", func(e ceph.OSDWatchEvent) bool { return e.Kind == ceph.WatchBaseline })
	if len(baseline.States) != 3 {
		t.Fatalf("baseline = %+v", baseline)
	}
	for _, s := range baseline.States {
		if !s.Up || !s.In {
			t.Fatalf("baseline OSD not up and in: %+v", s)
		}
	}
	t.Logf("baseline: epoch %d, read %v", baseline.Epoch, baseline.Finished.Sub(baseline.Started))

	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	work := startOSDWatchWorkload(workCtx, client)

	target := clusteraccess.OSDs(t, cluster)[0]
	pinned, _ := osdWatchState(baseline, target.ID)
	grace := 10 * time.Second
	if err := target.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	stopped := time.Now()
	down := awaitOSDWatchEvent(t, w, &last, &gaps, 2*time.Minute, "down", func(e ceph.OSDWatchEvent) bool {
		s, ok := osdWatchState(e, target.ID)
		return ok && !s.Up
	})
	if s, _ := osdWatchState(down, target.ID); s.UUID != pinned.UUID || down.Epoch <= baseline.Epoch {
		t.Fatalf("down event = %+v", down)
	}
	t.Logf("down: event %d %s epoch %d, observed %v after the stop returned", down.Sequence, down.Kind, down.Epoch, down.Finished.Sub(stopped))

	// The down event advances the test: the workload must keep completing
	// operations while the MON reports the owned OSD down.
	for deadline := time.Now().Add(2 * time.Minute); work.completedWithin(down.Finished, time.Now()) < 3; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the workload made no progress while osd.%d was down: %q", target.ID, work.failures())
		}
	}
	if err := target.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := time.Now()
	completedDown := work.completedWithin(down.Finished, restarted)
	up := awaitOSDWatchEvent(t, w, &last, &gaps, 3*time.Minute, "up", func(e ceph.OSDWatchEvent) bool {
		s, ok := osdWatchState(e, target.ID)
		return ok && s.Up && s.In
	})
	if s, _ := osdWatchState(up, target.ID); s.UUID != pinned.UUID || up.Epoch <= down.Epoch {
		t.Fatalf("up event = %+v", up)
	}
	t.Logf("up: event %d %s epoch %d, observed %v after the start returned", up.Sequence, up.Kind, up.Epoch, up.Finished.Sub(restarted))

	stopWork()
	<-work.done
	if failed := work.failures(); len(failed) != 0 {
		t.Fatalf("workload failures: %q", failed)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	for event := range w.Events() {
		if event.Sequence != last+1 {
			t.Fatalf("event %d follows %d", event.Sequence, last)
		}
		last = event.Sequence
	}
	elapsed := time.Since(watchStarted)
	t.Logf("native OSD watch: baseline -> osd.%d stopped -> reported down -> restarted -> reported up; %d events, %d gaps, %d workload operations completed while down, watch ran %v at a %v interval (about %d reads)",
		target.ID, last, gaps, completedDown, elapsed.Round(time.Second), interval, int(elapsed/interval)+1)
}
