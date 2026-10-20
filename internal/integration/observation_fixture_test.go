//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestObserveReturnsOnceTheConditionHolds(t *testing.T) {
	probes := 0
	seen, err := observe(t.Context(), time.Second, 100*time.Millisecond, time.Millisecond, func(context.Context) (int, bool, error) {
		probes++
		return probes, probes == 3, nil
	})
	if err != nil || seen.Value != 3 || seen.Attempts != 3 {
		t.Fatalf("observed %+v, %v", seen, err)
	}
}

func TestObserveRejectsASampleThatReturnsAfterThePhase(t *testing.T) {
	// The read finishes 75 ms into a 60 ms phase, as a slow exec whose
	// output arrives late, and reports the condition reached.
	seen, err := observe(t.Context(), 60*time.Millisecond, time.Second, time.Millisecond, func(context.Context) (string, bool, error) {
		time.Sleep(75 * time.Millisecond)
		return "late", true, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || seen.Value != "" || seen.Attempts != 1 {
		t.Fatalf("a late sample: %+v, %v", seen, err)
	}
}

func TestObserveBoundsEachProbeWithinThePhase(t *testing.T) {
	var deadlines []time.Duration
	seen, err := observe(t.Context(), 200*time.Millisecond, 20*time.Millisecond, time.Millisecond, func(ctx context.Context) (int, bool, error) {
		deadline, _ := ctx.Deadline()
		deadlines = append(deadlines, time.Until(deadline))
		<-ctx.Done()
		return 0, false, ctx.Err()
	})
	// The probe's own deadline is a failure, not a reason to wait longer.
	if !errors.Is(err, context.DeadlineExceeded) || seen.Attempts != 1 || seen.Elapsed > 150*time.Millisecond {
		t.Fatalf("a probe past its own bound: %+v, %v", seen, err)
	}
	if len(deadlines) != 1 || deadlines[0] > 20*time.Millisecond {
		t.Fatalf("probe deadlines %v", deadlines)
	}
}

func TestObserveStopsAtATerminalError(t *testing.T) {
	terminal := errors.New("the object was deleted")
	seen, err := observe(t.Context(), time.Second, 100*time.Millisecond, time.Millisecond, func(context.Context) (int, bool, error) {
		return 7, false, terminal
	})
	if !errors.Is(err, terminal) || seen.Attempts != 1 || seen.Value != 7 {
		t.Fatalf("a terminal error: %+v, %v", seen, err)
	}
}

func TestObserveEndsWithItsCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	probes := 0
	_, err := observe(ctx, time.Minute, time.Second, 10*time.Millisecond, func(context.Context) (int, bool, error) {
		probes++
		if probes == 2 {
			cancel()
		}
		return 0, false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled caller: %v", err)
	}
}

// eventLogContainer answers the notification log read after delay, with
// events, whether or not the read's context ended meanwhile.
type eventLogContainer struct {
	testcontainers.Container
	delay  time.Duration
	events string
	reads  int
}

func (ctr *eventLogContainer) Exec(_ context.Context, _ []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.reads++
	time.Sleep(ctr.delay)
	// The caller asks for tcexec.Multiplexed, so the output is plain.
	return 0, strings.NewReader(ctr.events), nil
}

func TestNotificationEventsAreReadWithinTheirPhase(t *testing.T) {
	want := []string{"ObjectCreated:Put in/a"}
	ready := &eventLogContainer{events: "ObjectCreated:Put in/a\nObjectRemoved:Delete in/a\n"}
	seen, err := observeNotificationEvents(t.Context(), ready, want, time.Second)
	if err != nil || len(seen.Value) != 2 || ready.reads != 1 {
		t.Fatalf("events present: %+v, %v", seen, err)
	}
	// A read that returns after the phase is not evidence, even when it
	// holds the wanted event.
	late := &eventLogContainer{delay: 80 * time.Millisecond, events: "ObjectCreated:Put in/a\n"}
	seen, err = observeNotificationEvents(t.Context(), late, want, 50*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || seen.Value != nil {
		t.Fatalf("a late read: %+v, %v", seen, err)
	}
	if missing := missingNotificationEvents(want, []string{"ObjectRemoved:Delete in/a"}); len(missing) != 1 {
		t.Fatalf("missing %v", missing)
	}
}
