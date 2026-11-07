package placement

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/moby/moby/api/types/container"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// interruptedGateway fails one start after stopping the same container and
// refuses to execute admin commands until Start has actually restored it.
type interruptedGateway struct {
	*gwtest.PlacementContainer
	stopped, failStart bool
	loseStopReply      bool
	loseCompletedStop  bool
	started            string
}

func (g *interruptedGateway) Stop(ctx context.Context, grace *time.Duration) error {
	if g.loseStopReply {
		g.loseStopReply = false
		return errors.New("Docker accepted stop but its reply was lost")
	}
	g.stopped = true
	err := g.PlacementContainer.Stop(ctx, grace)
	if err == nil && g.loseCompletedStop {
		return errors.New("stop completed but Docker lost the reply")
	}
	return err
}

func (g *interruptedGateway) Start(ctx context.Context) error {
	if g.failStart {
		g.failStart = false
		return errors.New("engine unavailable during start")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.stopped = false
	return g.PlacementContainer.Start(ctx)
}

func (g *interruptedGateway) State(context.Context) (*container.State, error) {
	return &container.State{Running: !g.stopped, StartedAt: g.started}, nil
}

func (g *interruptedGateway) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if g.stopped {
		return 0, nil, errors.New("exec on a stopped gateway")
	}
	return g.PlacementContainer.Exec(ctx, args, opts...)
}

// placementControl models a separate CLI container sharing the cluster's
// native defaults. A split essential image can lack radosgw-admin.
type placementControl struct {
	*gwtest.PlacementContainer
	missingAdmin bool
}

func (c *placementControl) GetContainerID() string { return "control" }

func (c *placementControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if c.missingAdmin && gwtest.Argv(args)[0] == "radosgw-admin" {
		return gwtest.Reply(args, 127, gwtest.Stream(true, []byte("radosgw-admin unavailable\n")))
	}
	return c.PlacementContainer.Exec(ctx, args, opts...)
}

func TestPlacementRechecksStartupDefaultsBeforeResuming(t *testing.T) {
	f := gwtest.NewPlacementContainer()
	ctr := &interruptedGateway{PlacementContainer: f, failStart: true, started: "original"}
	control := &placementControl{PlacementContainer: f}
	g := gwtest.NewGateway(ctr, control)
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), g.Core, p); err == nil || !ctr.stopped {
		t.Fatalf("initial restart: %v, stopped=%t", err, ctr.stopped)
	}
	control.missingAdmin = true
	if err := apply(t.Context(), g.Core, p); !errors.Is(err, ceph.ErrImageCapability) || !ctr.stopped || f.StartCount != 0 {
		t.Fatalf("resume without admin control: %v, starts=%d", err, f.StartCount)
	}
	control.missingAdmin = false
	f.ForeignDefault = true
	if err := apply(t.Context(), g.Core, p); !errors.Is(err, ceph.ErrInvalidRequest) || !ctr.stopped || f.StartCount != 0 {
		t.Fatalf("resume with changed default: %v, starts=%d", err, f.StartCount)
	}
	f.ForeignDefault = false
	if err := apply(t.Context(), g.Core, p); err != nil || ctr.stopped || f.StartCount != 1 || f.StopCount != 1 {
		t.Fatalf("after restoring defaults: %v, starts=%d, stops=%d", err, f.StartCount, f.StopCount)
	}
}

func TestPlacementRecognizesCompletedStopDespiteLostReply(t *testing.T) {
	g, f := newRGWPlacementFixture()
	ctr := &interruptedGateway{PlacementContainer: f, loseCompletedStop: true, started: "original"}
	g.Replace(ctr)
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), g.Core, p); err != nil || ctr.stopped || f.StartCount != 1 || f.StopCount != 1 {
		t.Fatalf("Apply after lost reply: %v, stopped=%t, starts=%d stops=%d", err, ctr.stopped, f.StartCount, f.StopCount)
	}
}

func TestPlacementRetryRestartsOnlyItsOwnStoppedGeneration(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		g, f := newRGWPlacementFixture()
		ctr := &interruptedGateway{PlacementContainer: f, failStart: true, started: "original"}
		g.Replace(ctr)
		p, err := create(t.Context(), g.Core, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		copyHandle := *p
		if err := apply(t.Context(), g.Core, p); err == nil || !ctr.stopped {
			t.Fatalf("Apply after failed start: %v, stopped=%t", err, ctr.stopped)
		}
		if replaced {
			// Someone else restarted and fenced this container since Apply.
			ctr.started = "external restart"
		}
		err = reload(t.Context(), g.Core, &copyHandle)
		if replaced {
			if err == nil || !ctr.stopped || f.StartCount != 0 {
				t.Fatalf("fenced generation resumed: %v, starts=%d", err, f.StartCount)
			}
		} else if err != nil || ctr.stopped || f.StopCount != 1 || f.StartCount != 1 {
			t.Fatalf("retry: %v, stopped=%t, stops=%d starts=%d", err, ctr.stopped, f.StopCount, f.StartCount)
		}
	}
}

func TestPlacementRecoversWhenAmbiguousStopCompletesAfterReturn(t *testing.T) {
	g, f := newRGWPlacementFixture()
	ctr := &interruptedGateway{PlacementContainer: f, loseStopReply: true, started: "original"}
	g.Replace(ctr)
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), g.Core, p); err == nil || ctr.stopped {
		t.Fatalf("lost stop reply: %v, stopped=%t", err, ctr.stopped)
	}
	// A retry before Docker completes the stop must not issue another stop.
	if err := apply(t.Context(), g.Core, p); err == nil || f.StopCount != 0 || f.StartCount != 0 {
		t.Fatalf("redispatched while the original generation was running: %v", err)
	}
	ctr.stopped = true
	if err := apply(t.Context(), g.Core, p); err != nil || ctr.stopped || f.StartCount != 1 {
		t.Fatalf("retry: %v, stopped=%t starts=%d", err, ctr.stopped, f.StartCount)
	}
}

func TestPlacementLostStopCanRecognizeAnExternalRestart(t *testing.T) {
	g, f := newRGWPlacementFixture()
	ctr := &interruptedGateway{PlacementContainer: f, loseStopReply: true, started: "original"}
	g.Replace(ctr)
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), g.Core, p); err == nil {
		t.Fatal("missing stop reply was accepted")
	}
	// An operator reconciles a Stop that never took effect. Its old generation
	// no longer runs; the operation still checks the loaded resources.
	ctr.started = "external restart"
	if err := apply(t.Context(), g.Core, p); err != nil || f.StopCount != 0 || f.StartCount != 0 {
		t.Fatalf("restarted reconciled gateway: %v", err)
	}
}

// commitFault changes the model before disrupting the reply or the first
// readback. A delayed dispatch can also fail to confirm its stop.
type commitFault struct {
	*gwtest.PlacementContainer
	cancel                         context.CancelFunc
	loseReply, failRead, committed bool
	exitAfterCommit                bool
	deferOperation                 string
	delayed                        []string
	stopFailures                   int
	commits                        int
}

func (f *commitFault) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if gwtest.IsCancel(args) {
		if f.stopFailures > 0 {
			f.stopFailures--
			return 0, nil, errors.New("command stop could not be confirmed")
		}
		f.delayed = nil
		return 0, nil, nil
	}
	if f.committed && slices.Contains(args, "period") && slices.Contains(args, "get") {
		f.cancel()
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if f.committed && f.failRead && slices.Contains(args, "period") && slices.Contains(args, "get") {
		f.failRead = false
		return 0, nil, errors.New("readback unavailable")
	}
	if f.deferOperation != "" && slices.Contains(args, f.deferOperation) {
		f.deferOperation = ""
		f.delayed = slices.Clone(args)
		return 0, nil, errors.New("exec accepted but not yet launched")
	}
	code, out, err := f.PlacementContainer.Exec(ctx, args, opts...)
	if err == nil && code == 0 && slices.Contains(args, "commit") {
		f.commits++
		f.committed = true
		if f.exitAfterCommit {
			return gwtest.Reply(args, 5, gwtest.Stream(true, []byte("commit reported EIO")))
		}
		if f.loseReply {
			return 0, nil, errors.New("commit reply lost")
		}
	}
	return code, out, err
}

func realmPlacementFixture() (*gwtest.Gateway, *gwtest.PlacementContainer) {
	g, f := newRGWPlacementFixture()
	f.Group["realm_id"], f.Zone["realm_id"] = "realm-id", "realm-id"
	f.CurrentPeriod = map[string]any{"id": "period-id", "epoch": 1, "realm_epoch": 1, "predecessor_uuid": "", "realm_id": "realm-id", "master_zone": "zone-id",
		"period_map": map[string]any{"id": "period-id", "zonegroups": []any{gwtest.Clone(f.Group)}}}
	return g, f
}

func TestPlacementReconcilesOnlyItsDispatchedPeriodCommit(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		lostReply, lostRead, foreign, nonzero bool
	}{
		{name: "success then cancellation"},
		{name: "nonzero exit after commit", nonzero: true},
		{name: "lost commit reply", lostReply: true},
		{name: "retry after failed readback", lostReply: true, lostRead: true},
		{name: "foreign edit before retry", lostReply: true, lostRead: true, foreign: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, f := realmPlacementFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctr := &commitFault{PlacementContainer: f, cancel: cancel, loseReply: tc.lostReply, failRead: tc.lostRead, exitAfterCommit: tc.nonzero}
			g.Replace(ctr)
			p, err := create(t.Context(), g.Core, testConfig())
			if err != nil {
				t.Fatal(err)
			}
			copyHandle := *p
			err = apply(ctx, g.Core, p)
			if (err != nil) != tc.lostRead || ctx.Err() == nil {
				t.Fatalf("Apply = %v, cancellation = %v", err, ctx.Err())
			}
			if tc.foreign {
				f.CurrentPeriod["outside"] = "preserve"
			}
			before := gwtest.Clone(f.CurrentPeriod)
			err = apply(t.Context(), g.Core, &copyHandle)
			if tc.foreign && !errors.Is(err, ceph.ErrInvalidRequest) || !tc.foreign && err != nil {
				t.Fatalf("retry = %v", err)
			}
			if ctr.commits != 1 || !reflect.DeepEqual(before, gwtest.Clone(f.CurrentPeriod)) {
				t.Fatal("retry recommitted the owned policy or changed a foreign policy")
			}
		})
	}
}

func TestPlacementUnstoppedCommitPreventsRestaging(t *testing.T) {
	g, f := realmPlacementFixture()
	ctr := &commitFault{PlacementContainer: f, cancel: func() {}, deferOperation: "commit", stopFailures: 2}
	g.Replace(ctr)
	p, err := create(t.Context(), g.Core, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	var pending *ceph.NativeCommandUnstoppedError
	if err := apply(t.Context(), g.Core, p); !errors.As(err, &pending) {
		t.Fatalf("unsettled commit: %v", err)
	}
	staged := gwtest.Clone(f.StagingPeriod)
	f.Calls = nil
	if err := apply(t.Context(), g.Core, p); err == nil {
		t.Fatal("retry succeeded while the earlier command could still launch")
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "update") || slices.Contains(args, "commit") {
			t.Fatalf("retry issued a mutation while the earlier commit was unfenced: %v", args)
		}
	}
	if !reflect.DeepEqual(staged, gwtest.Clone(f.StagingPeriod)) {
		t.Fatal("staging changed before stop was confirmed")
	}
	if err := apply(t.Context(), g.Core, p); err != nil || ctr.commits != 1 {
		t.Fatalf("retry after stop confirmation = %v, commits=%d", err, ctr.commits)
	}
}

func TestPlacementUnstoppedUpdateCannotOverwriteSubsequentStaging(t *testing.T) {
	for _, anotherHandle := range []bool{false, true} {
		g, f := realmPlacementFixture()
		ctr := &commitFault{PlacementContainer: f, cancel: func() {}, deferOperation: "update", stopFailures: 2}
		g.Replace(ctr)
		p, err := create(t.Context(), g.Core, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		var pending *ceph.NativeCommandUnstoppedError
		if err := apply(t.Context(), g.Core, p); !errors.As(err, &pending) {
			t.Fatalf("lost staging update: %v", err)
		}
		if anotherHandle {
			config := testConfig()
			config.Name = "other"
			p, err = create(t.Context(), g.Core, config)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Calls = nil
		if err := apply(t.Context(), g.Core, p); err == nil {
			t.Fatal("ignored the unconfirmed staging writer")
		}
		for _, argv := range f.Calls {
			if slices.Contains(argv, "period") {
				t.Fatalf("read or changed period while the earlier writer could still run: %v", argv)
			}
		}
		if anotherHandle {
			continue
		}
		if err := apply(t.Context(), g.Core, p); err != nil || ctr.commits != 1 {
			t.Fatalf("publish after stopping writer: %v, commits=%d", err, ctr.commits)
		}
		f.StagingPeriod["independent"] = "keep"
		if ctr.delayed != nil {
			_, _, _ = f.Exec(t.Context(), ctr.delayed)
		}
		if f.StagingPeriod["independent"] != "keep" {
			t.Fatal("late update overwrote an independent staging edit")
		}
	}
}

func TestPlacementRejectsUnrelatedPeriodLineageAfterReadFailure(t *testing.T) {
	for _, mutate := range []func(map[string]any){
		func(p map[string]any) { p["id"] = "another-period" },
		func(p map[string]any) { p["epoch"] = 1 },
		func(p map[string]any) { p["epoch"] = 3 },
		func(p map[string]any) { p["realm_epoch"] = 2 },
		func(p map[string]any) { p["predecessor_uuid"] = "other-predecessor" },
	} {
		g, f := realmPlacementFixture()
		ctr := &commitFault{PlacementContainer: f, cancel: func() {}, failRead: true}
		g.Replace(ctr)
		p, err := create(t.Context(), g.Core, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := apply(t.Context(), g.Core, p); err == nil {
			t.Fatal("missing readback was accepted")
		}
		mutate(f.CurrentPeriod)
		before := gwtest.Clone(f.CurrentPeriod)
		if err := apply(t.Context(), g.Core, p); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("foreign lineage accepted: %v", err)
		}
		if ctr.commits != 1 || !reflect.DeepEqual(before, gwtest.Clone(f.CurrentPeriod)) {
			t.Fatal("foreign period was overwritten")
		}
	}
}
