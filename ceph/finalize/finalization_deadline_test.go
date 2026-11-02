package finalize

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
)

// deadlineFixture is a finalization fixture whose test binary has deadline
// and whose before-deadline archive fires as soon as it is armed. waits
// records each arming delay.
func deadlineFixture(t *testing.T, config Config, deadline time.Time) (*finalizationFixture, *[]time.Duration) {
	t.Helper()
	fx := newFinalizationFixture(t, config)
	waits := &[]time.Duration{}
	fx.deadline = func() (time.Time, bool) { return deadline, true }
	fx.afterFunc = func(wait time.Duration, fire func()) *time.Timer {
		*waits = append(*waits, wait)
		return time.AfterFunc(0, fire)
	}
	return fx, waits
}

func TestBeforeDeadlineArchiveRecordsTheStillRunningTest(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	fx, waits := deadlineFixture(t, Config{Archive: true, ArchiveTimeout: time.Minute}, deadline)
	fx.armDeadline()
	<-fx.deadlineDone
	if len(*waits) != 1 {
		t.Fatalf("armed %d times", len(*waits))
	}
	// The archive's bound and the grace end before the deadline.
	if want := time.Until(deadline) - time.Minute - deadlineArchiveGrace; (*waits)[0] > want+time.Second || (*waits)[0] < want-time.Minute {
		t.Fatalf("armed after %s, want about %s", (*waits)[0], want)
	}
	archives := fx.archives(t)
	if len(archives) != 1 {
		t.Fatalf("archives = %v", archives)
	}
	m := readManifest(t, archives[0])
	if m.Phase != "before-deadline" || m.Final || m.Termination != nil || len(m.Cleanups) != 0 || m.Trigger.TestFailed ||
		m.Trigger.TestDeadline == nil || !m.Trigger.TestDeadline.Equal(deadline) || !m.Archive.Complete || len(m.Files) != 2 {
		t.Fatalf("before-deadline manifest = %+v", m)
	}
	if !slices.Contains(fx.events, "diagnostics") || slices.Contains(fx.events, "terminate") {
		t.Fatalf("events = %v", fx.events)
	}

	// The test then ends in time and passes: its finalization adds no
	// archive and leaves the before-deadline one as it was written.
	fx.run()
	if fx.tb.failed || len(fx.archives(t)) != 1 || !slices.Contains(fx.events, "terminate") {
		t.Fatalf("failed=%t %v archives=%v events=%v", fx.tb.failed, fx.tb.errors, fx.archives(t), fx.events)
	}
	if again := readManifest(t, archives[0]); again.Final || again.Termination != nil {
		t.Fatalf("the finalization rewrote the before-deadline manifest: %+v", again)
	}
}

func TestBeforeDeadlineArchiveIsSeparateFromTheFailureArchive(t *testing.T) {
	fx, _ := deadlineFixture(t, Config{Archive: true, ArchiveTimeout: time.Minute}, time.Now().Add(time.Hour))
	fx.tb.failed = true
	fx.armDeadline()
	<-fx.deadlineDone
	fx.run()
	archives := fx.archives(t)
	if len(archives) != 2 {
		t.Fatalf("archives = %v", archives)
	}
	phases := map[string]bool{}
	for _, archive := range archives {
		m := readManifest(t, archive)
		phases[m.Phase] = m.Final
		if m.Phase == "before-deadline" && !m.Trigger.TestFailed {
			t.Fatalf("the before-deadline archive lost the test's failure: %+v", m.Trigger)
		}
	}
	if final, ok := phases["before-deadline"]; !ok || final {
		t.Fatalf("phases = %v", phases)
	}
	if final, ok := phases["before-cleanup"]; !ok || !final {
		t.Fatalf("phases = %v", phases)
	}
}

func TestFinalizationWaitsForARunningBeforeDeadlineArchive(t *testing.T) {
	fx, _ := deadlineFixture(t, Config{Archive: true, ArchiveTimeout: time.Minute}, time.Now().Add(time.Hour))
	entered, release := make(chan struct{}), make(chan struct{})
	collect := fx.collectDiagnostics
	fx.collectDiagnostics = func(ctx context.Context, config diagnostics.Config) (*diagnostics.Report, error) {
		close(entered)
		<-release
		return collect(ctx, config)
	}
	fx.armDeadline()
	<-entered
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		fx.run()
	}()
	select {
	case <-finished:
		t.Fatal("the finalization did not wait for the before-deadline archive")
	case <-time.After(100 * time.Millisecond):
	}
	fx.mu.Lock()
	terminated := slices.Contains(fx.events, "terminate")
	fx.mu.Unlock()
	if terminated {
		t.Fatal("the cluster was terminated while the before-deadline archive collected")
	}
	close(release)
	<-finished
	if len(fx.archives(t)) != 1 || !slices.Contains(fx.events, "terminate") {
		t.Fatalf("archives=%v events=%v", fx.archives(t), fx.events)
	}
}

func TestBeforeDeadlineArchiveIsNotArmedWithoutRoomOrArchive(t *testing.T) {
	for name, c := range map[string]struct {
		config   Config
		deadline time.Time
		ok       bool
	}{
		"no archive":  {Config{ArchiveTimeout: time.Minute}, time.Now().Add(time.Hour), true},
		"no deadline": {Config{Archive: true, ArchiveTimeout: time.Minute}, time.Time{}, false},
		// The archive's bound and the grace no longer fit before it.
		"near deadline": {Config{Archive: true, ArchiveTimeout: time.Minute}, time.Now().Add(time.Minute + deadlineArchiveGrace/2), true},
	} {
		t.Run(name, func(t *testing.T) {
			fx, waits := deadlineFixture(t, c.config, c.deadline)
			fx.deadline = func() (time.Time, bool) { return c.deadline, c.ok }
			fx.armDeadline()
			if len(*waits) != 0 || fx.deadlineTimer != nil {
				t.Fatalf("armed: %v", *waits)
			}
			fx.run()
			if len(fx.archives(t)) != 0 {
				t.Fatalf("archives = %v", fx.archives(t))
			}
		})
	}
}

func TestFinalizationDisarmsAPendingBeforeDeadlineArchive(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true, ArchiveTimeout: time.Minute})
	fx.deadline = func() (time.Time, bool) { return time.Now().Add(time.Hour), true }
	fx.armDeadline()
	fx.run()
	if fx.deadlineTimer.Stop() {
		t.Fatal("the before-deadline archive is still armed after the finalization")
	}
	if len(fx.archives(t)) != 0 || slices.Contains(fx.events, "diagnostics") {
		t.Fatalf("archives=%v events=%v", fx.archives(t), fx.events)
	}
}
