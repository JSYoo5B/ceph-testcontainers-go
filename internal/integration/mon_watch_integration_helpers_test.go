//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const monWatchPool = "tc-mon-watch"

// monWatchWorkload plays the SUT: it writes and reads back objects through
// the rados CLI of the client container until stopped, independently of the
// watch, and records when each completed operation started and ended.
type monWatchWorkload struct {
	mu     sync.Mutex
	starts []time.Time
	ends   []time.Time
	failed []string
	done   chan struct{}
}

func startMonWatchWorkload(ctx context.Context, client testcontainers.Container) *monWatchWorkload {
	w := &monWatchWorkload{done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for n := 0; ctx.Err() == nil; n++ {
			started := time.Now()
			name := "mon-watch-" + strconv.Itoa(n)
			err := monWatchRados(ctx, client, "put", name, "/etc/hosts")
			if err == nil {
				err = monWatchRados(ctx, client, "get", name, "/tmp/"+name)
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

func monWatchRados(ctx context.Context, client testcontainers.Container, args ...string) error {
	code, r, err := client.Exec(ctx, append([]string{"timeout", "60", "rados", "-p", monWatchPool}, args...), tcexec.Multiplexed())
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
func (w *monWatchWorkload) completedWithin(from, to time.Time) int {
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

func (w *monWatchWorkload) failures() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.failed)
}

// awaitMonitorWatchEvent reads events until match accepts one, and fails on
// a closed watch or after limit. Gaps are counted, not fatal: a read fails
// while the MONs elect a new quorum.
func awaitMonitorWatchEvent(t *testing.T, w *ceph.MonitorWatch, last *uint64, gaps *int, limit time.Duration, what string, match func(ceph.MonitorWatchEvent) bool) ceph.MonitorWatchEvent {
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
			t.Logf("%s: event %d %s monmap epoch %d election epoch %d leader %q quorum %v", what, event.Sequence, event.Kind, event.MonMapEpoch, event.ElectionEpoch, event.Leader, event.Quorum)
		case <-timeout:
			t.Fatalf("%s: no matching event within %v", what, limit)
		}
	}
}

func monitorWatchNames(event ceph.MonitorWatchEvent) []string {
	names := make([]string, 0, len(event.Members))
	for _, member := range event.Members {
		names = append(names, member.Name)
	}
	return names
}

// monitorDaemonState reads the state that one MON reports about itself
// through its own admin socket, independently of the quorum_status reads of
// the watch.
func monitorDaemonState(t *testing.T, ctx context.Context, cluster *ceph.Container, name string) string {
	t.Helper()
	out, err := harness.MonitorHandle(t, ctx, cluster, name).AdminSocket(ctx, "mon_status")
	if err != nil {
		t.Fatalf("mon.%s mon_status: %v", name, err)
	}
	var status struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(out, &status); err != nil {
		t.Fatalf("mon.%s mon_status: %v", name, err)
	}
	return status.State
}

// testMonitorWatch stops one of three MONs while the client keeps writing,
// observes it leave the quorum while it stays in the monmap, and observes it
// rejoin after it starts again. The quorum event proves only that a
// quorum_status read of the original cluster reported that quorum; the
// workload's own operations prove that the majority kept serving, and the
// MON's own admin socket proves that it rejoined as leader or peon.
func testMonitorWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(1)), ceph.WithStartupTimeout(3*time.Minute))
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: monWatchPool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// The cost of one read, the same quorum_status the watch runs.
	const samples = 10
	began := time.Now()
	for range samples {
		if _, err := observer.QuorumStatus(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("query cost: %d QuorumStatus reads took %v (%v each)", samples, time.Since(began), time.Since(began)/samples)

	watchStarted := time.Now()
	w, err := observer.WatchMonitors(ctx, ceph.WatchConfig{}) // the 1-second default
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the cluster cleanup, so it runs before it.
	t.Cleanup(func() { _ = w.Stop() })
	var last uint64
	gaps := 0
	baseline := awaitMonitorWatchEvent(t, w, &last, &gaps, time.Second, "baseline", func(e ceph.MonitorWatchEvent) bool { return e.Kind == ceph.WatchBaseline })
	if names := monitorWatchNames(baseline); !slices.Equal(names, []string{"a", "b", "c"}) || !slices.Equal(baseline.Quorum, names) || baseline.Leader == "" || baseline.MonMapEpoch == 0 {
		t.Fatalf("baseline = %+v", baseline)
	}
	for _, member := range baseline.Members {
		if len(member.Addrs) == 0 {
			t.Fatalf("baseline member without an advertised address: %+v", member)
		}
	}
	t.Logf("baseline: monmap epoch %d, election epoch %d, leader %s, read %v", baseline.MonMapEpoch, baseline.ElectionEpoch, baseline.Leader, baseline.Finished.Sub(baseline.Started))

	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	work := startMonWatchWorkload(workCtx, client)

	stopped := harness.MonitorHandle(t, ctx, cluster, "c")
	grace := 10 * time.Second
	if err := stopped.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	restored := false
	t.Cleanup(func() {
		if !restored {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			_ = stopped.Start(cleanup)
		}
	})
	stoppedAt := time.Now()
	lost := awaitMonitorWatchEvent(t, w, &last, &gaps, 3*time.Minute, "quorum without c", func(e ceph.MonitorWatchEvent) bool {
		return slices.Equal(monitorWatchNames(e), []string{"a", "b", "c"}) && slices.Equal(e.Quorum, []string{"a", "b"})
	})
	// A stopped MON leaves the quorum, not the monmap; the new quorum comes
	// from a later election.
	if lost.ElectionEpoch <= baseline.ElectionEpoch {
		t.Fatalf("quorum loss event = %+v", lost)
	}
	t.Logf("quorum without c: event %d %s monmap epoch %d election epoch %d leader %s, observed %v after the stop returned", lost.Sequence, lost.Kind, lost.MonMapEpoch, lost.ElectionEpoch, lost.Leader, lost.Finished.Sub(stoppedAt))

	// The event advances the test: the workload must keep completing
	// operations while the remaining majority serves the cluster.
	for deadline := time.Now().Add(2 * time.Minute); work.completedWithin(lost.Finished, time.Now()) < 3; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the workload made no progress with mon.c stopped: %q", work.failures())
		}
	}
	completedLost := work.completedWithin(lost.Finished, time.Now())
	if err := stopped.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restored = true
	startedAt := time.Now()
	rejoined := awaitMonitorWatchEvent(t, w, &last, &gaps, 3*time.Minute, "quorum with c", func(e ceph.MonitorWatchEvent) bool {
		return slices.Equal(e.Quorum, []string{"a", "b", "c"})
	})
	if rejoined.ElectionEpoch <= lost.ElectionEpoch || !slices.Equal(monitorWatchNames(rejoined), []string{"a", "b", "c"}) {
		t.Fatalf("rejoin event = %+v", rejoined)
	}
	if state := monitorDaemonState(t, ctx, cluster, "c"); state != "leader" && state != "peon" {
		t.Fatalf("mon.c reports state %q after the watch saw it rejoin", state)
	}
	t.Logf("quorum with c: event %d %s election epoch %d leader %s, observed %v after the start returned", rejoined.Sequence, rejoined.Kind, rejoined.ElectionEpoch, rejoined.Leader, rejoined.Finished.Sub(startedAt))

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
	t.Logf("native MON watch: baseline -> mon.c stopped -> quorum a,b with c still a member -> restarted -> quorum a,b,c; %d events, %d gaps, %d workload operations completed without c, watch ran %v at the 1s default interval",
		last, gaps, completedLost, elapsed.Round(time.Second))
}
