package ceph

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cleanupControl answers the stale admin-socket removal that ReopenOSD runs
// through the control container, and records every attempt.
type cleanupControl struct {
	*retainedControl
	mu      sync.Mutex
	removes []string
	err     error
	onClear func()
}

func (c *cleanupControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) == 3 && args[0] == "rm" && args[1] == "-f" {
		c.mu.Lock()
		c.removes = append(c.removes, args[2])
		err, hook := c.err, c.onClear
		c.mu.Unlock()
		if hook != nil {
			hook()
		}
		if err != nil {
			return 0, nil, err
		}
		return 0, noInitialOSDStream(""), nil
	}
	return c.retainedControl.Exec(ctx, args, opts...)
}

func (c *cleanupControl) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

func (c *cleanupControl) removeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.removes)
}

// newCleanupFixture is the retained fixture with a run volume, so that the
// stale socket cleanup actually reaches the control container.
func newCleanupFixture(t *testing.T) (*retainedFixture, *cleanupControl) {
	t.Helper()
	f := newRetainedFixture(t)
	control := &cleanupControl{retainedControl: f.control}
	f.cluster.Container = control
	f.cluster.run = &runVolume{name: "synthetic-run"}
	return f, control
}

// runnerOutcome replaces the fixture runner by one that counts dispatches and
// either returns no container with an error or the fixture candidate with one.
func runnerOutcome(f *retainedFixture, withCandidate bool) *int {
	dispatches := new(int)
	inner := f.cluster.osdRunner
	var mu sync.Mutex
	f.cluster.osdRunner = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		mu.Lock()
		*dispatches++
		mu.Unlock()
		if withCandidate {
			f.runErr = errors.New("synthetic readiness failure")
			return inner(ctx, image, opts...)
		}
		return nil, errors.New("synthetic creation reply lost")
	}
	return dispatches
}

func requireNoRegistrationOrStoreWrites(t *testing.T, f *retainedFixture) {
	t.Helper()
	if len(f.helperWrites()) != 0 || len(f.control.actions) != 0 {
		t.Fatalf("registration or store written: helper=%v ceph=%v", f.helperWrites(), f.control.actions)
	}
}

// A failed stale-socket cleanup is a known zero-dispatch failure: the detached
// handle stays reusable and a retry reopens it once.
func TestReopenRetriesAfterAFailedStaleSocketCleanup(t *testing.T) {
	f, control := newCleanupFixture(t)
	detached, err := f.cluster.DetachOSD(t.Context(), 3)
	if err != nil {
		t.Fatal(err)
	}
	control.setErr(errors.New("synthetic cleanup failure"))
	osd, err := f.cluster.ReopenOSD(t.Context(), detached)
	if err == nil || osd != nil || !strings.Contains(err.Error(), "synthetic cleanup failure") {
		t.Fatalf("failed cleanup: %v %v", err, osd)
	}
	if f.runs != 0 || control.removeCount() != 1 || detached.reopened || f.cluster.detachedOSDs[3] != detached || f.cluster.osds[3] != nil {
		t.Fatalf("failed cleanup consumed the handle: runs=%d removes=%d reopened=%v", f.runs, control.removeCount(), detached.reopened)
	}
	requireNoRegistrationOrStoreWrites(t, f)

	control.setErr(nil)
	reopened, err := f.cluster.ReopenOSD(t.Context(), detached)
	if err != nil {
		t.Fatal("retry after the known zero-dispatch failure:", err)
	}
	if f.runs != 1 || control.removeCount() != 2 || f.cluster.osds[3] != reopened || reopened.GetContainerID() != "osd-reopened-3" || !detached.reopened {
		t.Fatalf("retry state runs=%d removes=%d", f.runs, control.removeCount())
	}
	if _, err := f.cluster.ReopenOSD(t.Context(), detached); err == nil || !strings.Contains(err.Error(), "already reopened") || f.runs != 1 {
		t.Fatalf("a consumed handle was replayed: %v runs=%d", err, f.runs)
	}
}

// A cancellation that arrives during the cleanup is rechecked before the
// irreversible transition.
func TestReopenRechecksCancellationAfterTheStaleSocketCleanup(t *testing.T) {
	f, control := newCleanupFixture(t)
	detached, err := f.cluster.DetachOSD(t.Context(), 3)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	control.onClear = cancel
	osd, err := f.cluster.ReopenOSD(ctx, detached)
	if !errors.Is(err, context.Canceled) || osd != nil {
		t.Fatalf("canceled reopen: %v %v", err, osd)
	}
	if f.runs != 0 || detached.reopened || f.cluster.detachedOSDs[3] != detached || f.cluster.osds[3] != nil {
		t.Fatalf("cancellation consumed the handle: runs=%d", f.runs)
	}
	requireNoRegistrationOrStoreWrites(t, f)
	control.onClear = nil
	if _, err := f.cluster.ReopenOSD(t.Context(), detached); err != nil || f.runs != 1 {
		t.Fatalf("retry after cancellation: %v runs=%d", err, f.runs)
	}
}

// A dispatched creation that returns no container may have lost its reply, so
// it stays consumed and is never replayed.
func TestReopenKeepsADispatchedCreationErrorConsumed(t *testing.T) {
	f, _ := newCleanupFixture(t)
	dispatches := runnerOutcome(f, false)
	detached, err := f.cluster.DetachOSD(t.Context(), 3)
	if err != nil {
		t.Fatal(err)
	}
	osd, err := f.cluster.ReopenOSD(t.Context(), detached)
	if err == nil || osd == nil || osd.Container != nil || f.cluster.osds[3] != osd || !detached.reopened || f.cluster.detachedOSDs[3] != nil {
		t.Fatalf("dispatched nil,error: %v %+v", err, osd)
	}
	if _, err := f.cluster.ReopenOSD(t.Context(), detached); err == nil || !strings.Contains(err.Error(), "already reopened") {
		t.Fatal("retry of the dispatched creation:", err)
	}
	if _, err := f.cluster.DetachOSD(t.Context(), 3); err == nil || !strings.Contains(err.Error(), "no container") {
		t.Fatal("detach of the container-less candidate:", err)
	}
	if *dispatches != 1 {
		t.Fatalf("dispatches %d", *dispatches)
	}
	requireNoRegistrationOrStoreWrites(t, f)
}

// Eight competing calls on one detached pointer dispatch once, return one
// owned error result and refuse the other seven as consumed.
func TestCompetingReopensDispatchOnce(t *testing.T) {
	for _, tc := range []struct {
		name          string
		withCandidate bool
	}{{"creation returns no container", false}, {"creation returns a candidate with an error", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := newCleanupFixture(t)
			dispatches := runnerOutcome(f, tc.withCandidate)
			detached, err := f.cluster.DetachOSD(t.Context(), 3)
			if err != nil {
				t.Fatal(err)
			}
			const callers = 8
			type result struct {
				osd *OSDContainer
				err error
			}
			results := make(chan result, callers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for range callers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					osd, err := f.cluster.ReopenOSD(t.Context(), detached)
					results <- result{osd, err}
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			var owned, refused int
			var winner *OSDContainer
			for r := range results {
				switch {
				case r.err != nil && r.osd != nil:
					owned++
					winner = r.osd
				case r.err != nil && r.osd == nil && strings.Contains(r.err.Error(), "already reopened"):
					refused++
				default:
					t.Fatalf("unexpected result %v %v", r.osd, r.err)
				}
			}
			if owned != 1 || refused != callers-1 || *dispatches != 1 || f.cluster.osds[3] != winner || (winner.Container != nil) != tc.withCandidate {
				t.Fatalf("owned=%d refused=%d dispatches=%d", owned, refused, *dispatches)
			}
			requireNoRegistrationOrStoreWrites(t, f)
		})
	}
}

// reopenOrderOK reports whether the ReopenOSD source cleans the stale socket
// and rechecks the context before it consumes the detached handle.
func reopenOrderOK(src string) bool {
	start := strings.Index(src, "func (c *Container) ReopenOSD(")
	if start < 0 {
		return false
	}
	body := src[start:]
	if end := strings.Index(body, "\n}\n"); end >= 0 {
		body = body[:end]
	}
	clean := strings.Index(body, "c.clearAdminSocket(")
	recheck := strings.Index(body[max(clean, 0):], "ctx.Err()")
	consume := strings.Index(body, "detached.reopened = true")
	return clean >= 0 && consume >= 0 && recheck >= 0 && clean+recheck < consume &&
		strings.Count(body, "c.clearAdminSocket(") == 1 && !strings.Contains(body[consume:], "clearAdminSocket")
}

// The exact production source keeps the order; the control sources do not.
func TestReopenSourceCleansBeforeConsuming(t *testing.T) {
	src, err := os.ReadFile("osd_retained.go")
	if err != nil {
		t.Fatal(err)
	}
	if !reopenOrderOK(string(src)) {
		t.Fatal("ReopenOSD no longer cleans the stale socket and rechecks the context before consuming the handle")
	}
	old := `func (c *Container) ReopenOSD(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	detached.reopened = true
	c.osds[id] = osd
	if err := c.clearAdminSocket(ctx, "osd"); err != nil {
		return
	}
}
`
	noRecheck := strings.Replace(string(src), "// Last check before the irreversible transition.\n\tif err := ctx.Err(); err != nil {\n\t\treturn nil, err\n\t}\n", "", 1)
	for name, control := range map[string]string{"old order": old, "no recheck": noRecheck, "missing": "package ceph\n"} {
		if name == "no recheck" && noRecheck == string(src) {
			t.Fatal("the no-recheck control did not change the source")
		}
		if reopenOrderOK(control) {
			t.Fatalf("control %q was accepted", name)
		}
	}
}
