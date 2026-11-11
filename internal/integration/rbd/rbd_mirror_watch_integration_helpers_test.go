//go:build all || (integration && multicluster)

package rbd_test

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdexpect "github.com/JSYoo5B/ceph-testcontainers-go/rbd/expect"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

const rbdMirrorWatchPool = "tc-rbd-watch"

// rbdMirrorWatchStream reads one watch's events in order: it fails on a
// skipped sequence number or a closed watch, counts gaps (a read can fail
// while a receiver stops), and forwards successful observations.
type rbdMirrorWatchStream[E any] struct {
	name   string
	events <-chan E
	stop   func() error
	meta   func(E) (uint64, ceph.WatchEventKind)
	last   uint64
	gaps   int
}

func (s *rbdMirrorWatchStream[E]) await(t *testing.T, limit time.Duration, what string, match func(E) bool) E {
	t.Helper()
	timeout := time.After(limit)
	for {
		select {
		case event, ok := <-s.events:
			if !ok {
				t.Fatalf("%s %s: watch ended: %v", s.name, what, s.stop())
			}
			if s.record(t, what, event) && match(event) {
				return event
			}
		case <-timeout:
			t.Fatalf("%s %s: no matching event within %v", s.name, what, limit)
		}
	}
}

// record checks the sequence of one event and reports whether it is a
// successful read.
func (s *rbdMirrorWatchStream[E]) record(t *testing.T, what string, event E) bool {
	t.Helper()
	sequence, kind := s.meta(event)
	if sequence != s.last+1 {
		t.Fatalf("%s %s: event %d follows %d", s.name, what, sequence, s.last)
	}
	s.last = sequence
	if kind == ceph.WatchGap {
		s.gaps++
		t.Logf("%s %s: gap at event %d: %+v", s.name, what, sequence, event)
		return false
	}
	t.Logf("%s %s: event %d %s: %+v", s.name, what, sequence, kind, event)
	return true
}

// finish stops the watch and checks that the events left after Stop keep
// the sequence.
func (s *rbdMirrorWatchStream[E]) finish(t *testing.T) {
	t.Helper()
	if err := s.stop(); err != nil {
		t.Fatalf("%s Stop = %v", s.name, err)
	}
	for event := range s.events {
		s.record(t, "after Stop", event)
	}
}

// testRBDMirrorWatch stops the observed image replayer of a two-daemon mirror while
// both watches run. The receiver event that names the survivor as leader,
// with the stopped member still in the pinned cohort, advances the test: a
// new payload and a mirror snapshot checkpoint are written on the source.
// The image event that reports the survivor replaying is followed by an
// independent read of the destination bytes; neither event is taken as
// proof that the payload arrived. The restart of the same container is then
// observed as a new native instance of the same cohort member.
func testRBDMirrorWatch(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPairWithContext(t, ctx, opts...)
	for _, cluster := range []*ceph.Container{source, destination} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: rbdMirrorWatchPool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
			t.Fatal(err)
		}
		if err := rbd.InitPool(ctx, cluster, rbdMirrorWatchPool); err != nil {
			t.Fatal(err)
		}
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sourceStatus, err := harness.ObserveCluster(source).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationStatus, err := harness.ObserveCluster(destination).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	link, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", harness.StaticFacts(source).Images.RBDAux), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: rbdMirrorWatchPool, DaemonCount: 2}))
	if link != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
			defer done()
			if err := link.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := link.WaitReceiverReady(ctx); err != nil {
		t.Fatal(err)
	}
	first := harness.RBDMultiClusterPayload(2<<20, 71)
	harness.RBDScopeCreate(t, ctx, sourceClient, rbdMirrorWatchPool, "", "volume", false)
	if err := link.EnableImage(ctx, "volume"); err != nil {
		t.Fatal(err)
	}
	harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, rbdMirrorWatchPool, "", "volume", 0, first)
	harness.RBDReceiverNativeSnapshot(t, ctx, sourceClient, rbdMirrorWatchPool, "", "volume")
	if _, err := link.WaitReplayReady(ctx, "volume"); err != nil {
		t.Fatal(err)
	}
	harness.RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, rbdMirrorWatchPool, "", "volume", first)

	// The cost of one read of each watch, the same observations they make.
	const samples = 3
	began := time.Now()
	for range samples {
		if _, err := link.ReceiverStatus(ctx); err != nil {
			t.Fatal(err)
		}
	}
	receiverCost := time.Since(began) / samples
	began = time.Now()
	for range samples {
		if _, err := link.ImageStatus(ctx, "volume"); err != nil {
			t.Fatal(err)
		}
	}
	imageCost := time.Since(began) / samples
	t.Logf("query cost: ReceiverStatus %v, ImageStatus %v per read", receiverCost, imageCost)

	const interval = 2 * time.Second
	watchStarted := time.Now()
	receiverWatch, err := link.WatchReceivers(ctx, ceph.WatchConfig{Interval: interval, Buffer: 64})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the mirror's cleanup, so it runs before it.
	t.Cleanup(func() { _ = receiverWatch.Stop() })
	imageWatch, err := link.WatchImage(ctx, ceph.WatchConfig{Interval: interval, Buffer: 64}, "volume")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = imageWatch.Stop() })
	receivers := &rbdMirrorWatchStream[rbdmirror.ReceiverWatchEvent]{name: "receivers", events: receiverWatch.Events(), stop: receiverWatch.Stop,
		meta: func(e rbdmirror.ReceiverWatchEvent) (uint64, ceph.WatchEventKind) { return e.Sequence, e.Kind }}
	images := &rbdMirrorWatchStream[rbdmirror.ImageWatchEvent]{name: "image", events: imageWatch.Events(), stop: imageWatch.Stop,
		meta: func(e rbdmirror.ImageWatchEvent) (uint64, ceph.WatchEventKind) { return e.Sequence, e.Kind }}
	isBaseline := func(kind ceph.WatchEventKind) bool { return kind == ceph.WatchBaseline }
	receiverBaseline := receivers.await(t, time.Second, "baseline", func(e rbdmirror.ReceiverWatchEvent) bool { return isBaseline(e.Kind) })
	imageBaseline := images.await(t, time.Second, "baseline", func(e rbdmirror.ImageWatchEvent) bool { return isBaseline(e.Kind) })
	if s := receiverBaseline.Status; !s.Ready || s.LeaderDaemonName == "" || len(s.ExpectedDaemons) != 2 {
		t.Fatalf("receiver baseline = %+v", receiverBaseline)
	}
	if s := imageBaseline.Status; !s.ReplayReady || s.DaemonName == "" || s.GlobalID == "" {
		t.Fatalf("image baseline = %+v", imageBaseline)
	}
	victim := imageBaseline.Status.DaemonName
	var stopped *rbdmirror.Daemon
	survivor := ""
	for _, daemon := range link.Daemons() {
		if daemon.DaemonName == victim {
			stopped = daemon
		} else {
			survivor = daemon.DaemonName
		}
	}
	victimBefore := receiverBaseline.Status.Daemons[victim]
	if stopped == nil || survivor == "" || victimBefore.InstanceID == "" || victimBefore.InstanceID != imageBaseline.Status.InstanceID {
		t.Fatalf("baseline replayer %s/%s does not match receiver cohort %+v", victim, imageBaseline.Status.InstanceID, receiverBaseline.Status)
	}
	t.Logf("baseline: leader %s, image replayed by %s (instance %s), survivor %s", receiverBaseline.Status.LeaderDaemonName, imageBaseline.Status.DaemonName, imageBaseline.Status.InstanceID, survivor)

	grace := 2 * time.Second
	stopBegan := time.Now()
	if err := stopped.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	stoppedAt := time.Now()
	election := rbdexpect.Receivers{Select: []rbdexpect.ReceiverDaemon{{Name: survivor, Running: true}, {Name: victim}}, Leader: survivor}
	elected := receivers.await(t, 4*time.Minute, "survivor election", func(e rbdmirror.ReceiverWatchEvent) bool {
		result, err := election.Evaluate(e.Status)
		if err != nil {
			t.Fatalf("survivor election: %v", err)
		}
		return result.Matched
	})
	if !slices.Equal(elected.Status.ExpectedDaemons, receiverBaseline.Status.ExpectedDaemons) || elected.Status.Ready {
		t.Fatalf("the stopped member left the pinned cohort or was reported ready: %+v", elected)
	}
	t.Logf("survivor election: event %d %s, observed %v after the stop returned", elected.Sequence, elected.Kind, elected.Finished.Sub(stoppedAt))
	// The existing wait with the explicit survivor cohort confirms the
	// election independently of the stream.
	status, err := link.WaitReceiverReady(ctx, survivor)
	if err != nil {
		t.Fatalf("survivor readiness = %+v, %v", status, err)
	}
	if result, err := (rbdexpect.Receivers{Select: []rbdexpect.ReceiverDaemon{{Name: survivor, Running: true}}, Leader: survivor}).Evaluate(status); err != nil || !result.Matched {
		t.Fatalf("survivor readiness: %v %v", result, err)
	}

	// The election advances the test: the SUT writes a new payload and a
	// checkpoint while only the survivor runs.
	second := harness.RBDMultiClusterPayload(2<<20, 73)
	harness.RBDScopeIO(t, ctx, sourceClient, "write", sourceStatus.FSID, rbdMirrorWatchPool, "", "volume", 0, second)
	checkpoint := harness.RBDReceiverNativeSnapshot(t, ctx, sourceClient, rbdMirrorWatchPool, "", "volume")
	replay := rbdexpect.MirrorImage{Select: imageBaseline.Status, Replaying: true, Daemon: survivor, NewInstance: true}
	replaying := images.await(t, 4*time.Minute, "replayer moved", func(e rbdmirror.ImageWatchEvent) bool {
		result, err := replay.Evaluate(e.Status)
		if err != nil {
			t.Fatal(err)
		}
		return e.Sequence > imageBaseline.Sequence && !e.Finished.Before(stopBegan) && result.Matched
	})
	t.Logf("survivor replay: event %d %s by %s (instance %s; baseline %s)", replaying.Sequence, replaying.Kind, replaying.Status.DaemonName, replaying.Status.InstanceID, imageBaseline.Status.DaemonName)
	harness.RBDScopeWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, rbdMirrorWatchPool, "", "volume", second)
	t.Logf("checkpoint %d delivered through %s", checkpoint, survivor)

	if err := stopped.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restartedAt := time.Now()
	restart := rbdexpect.Receivers{Select: []rbdexpect.ReceiverDaemon{{Name: victim, Baseline: victimBefore, Running: true, NewInstance: true}}, Ready: true}
	restarted := receivers.await(t, 4*time.Minute, "same-container restart", func(e rbdmirror.ReceiverWatchEvent) bool {
		result, err := restart.Evaluate(e.Status)
		if err != nil {
			t.Fatalf("same-container restart: %v", err)
		}
		return result.Matched
	})
	if d := restarted.Status.Daemons[victim]; d.ContainerID != victimBefore.ContainerID || stopped.GetContainerID() != victimBefore.ContainerID {
		t.Fatalf("the restarted member is another container: %+v", restarted)
	}
	t.Logf("restart: event %d %s, %s instance %s -> %s, observed %v after the start returned", restarted.Sequence, restarted.Kind, victim, victimBefore.InstanceID, restarted.Status.Daemons[victim].InstanceID, restarted.Finished.Sub(restartedAt))

	// A leader can assign the image to its peer. In that topology the first
	// stop proved a replay move; stop the actual leader separately to also
	// exercise a change in election.
	if receiverBaseline.Status.LeaderDaemonName != victim {
		leaderName := restarted.Status.LeaderDaemonName
		var leader *rbdmirror.Daemon
		next := ""
		for _, daemon := range link.Daemons() {
			if daemon.DaemonName == leaderName {
				leader = daemon
			} else {
				next = daemon.DaemonName
			}
		}
		if leader == nil || next == "" {
			t.Fatalf("invalid restarted cohort: %+v", restarted.Status)
		}
		if err := leader.Stop(ctx, &grace); err != nil {
			t.Fatal(err)
		}
		receivers.await(t, 4*time.Minute, "change of elected leader", func(e rbdmirror.ReceiverWatchEvent) bool {
			return e.Status.LeaderDaemonName == next && !e.Status.Ready && !e.Status.Daemons[leaderName].Running && e.Status.Daemons[next].Running && slices.Equal(e.Status.ExpectedDaemons, receiverBaseline.Status.ExpectedDaemons)
		})
		if err := leader.Start(ctx); err != nil {
			t.Fatal(err)
		}
		receivers.await(t, 4*time.Minute, "leader restart", func(e rbdmirror.ReceiverWatchEvent) bool {
			member := e.Status.Daemons[leaderName]
			return e.Status.Ready && member.Running && member.ContainerID == leader.GetContainerID() && member.InstanceID != "" && member.InstanceID != restarted.Status.Daemons[leaderName].InstanceID
		})
	}
	receivers.finish(t)
	images.finish(t)
	elapsed := time.Since(watchStarted)
	t.Logf("native RBD mirror watch: replayer %s stopped -> leader %s -> %s replays -> checkpoint delivered -> %s restarted; receivers %d events %d gaps, image %d events %d gaps, watches ran %v at a %v interval",
		victim, survivor, survivor, victim, receivers.last, receivers.gaps, images.last, images.gaps, elapsed.Round(time.Second), interval)
}
