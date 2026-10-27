package ceph

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// cleanupControl answers the stale admin-socket removal that ReopenOSD runs
// through the control container, under the supervisor or directly, and
// records every attempt. A removal whose Exec stream broke stays pending in
// the control container until its cancellation succeeds, and unlinks the
// socket path when released, like a late command of a real container.
type cleanupControl struct {
	*retainedControl
	mu      sync.Mutex
	removes []string
	// mode is "" (succeeds), "exit" (rm exits nonzero) or "stream" (the
	// Exec stream breaks while the removal stays pending).
	mode        string
	cancelFails int
	cancels     int
	pending     map[string]bool
	socket      bool
	onClear     func()
}

func (c *cleanupControl) State(context.Context) (*container.State, error) {
	return &container.State{Running: true, StartedAt: "control-start"}, nil
}

func (c *cleanupControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case len(args) > 3 && args[0] == "python3" && args[2] == supervised.CancelScript:
		c.cancels++
		if c.cancelFails > 0 {
			c.cancelFails--
			return 3, bytes.NewReader(nil), nil
		}
		delete(c.pending, args[3])
		return 0, bytes.NewReader(nil), nil
	case len(args) > 6 && args[0] == "python3" && args[2] == supervised.Script && args[6] == "rm":
		return c.remove(ctx, args[6:], supervisedMarker(opts), true)
	case len(args) == 3 && args[0] == "rm" && args[1] == "-f":
		return c.remove(ctx, args, "", false)
	}
	c.mu.Unlock()
	defer c.mu.Lock()
	return c.retainedControl.Exec(ctx, args, opts...)
}

// remove runs one rm -f of the socket. The caller holds c.mu.
func (c *cleanupControl) remove(ctx context.Context, argv []string, marker string, supervisedRun bool) (int, io.Reader, error) {
	c.removes = append(c.removes, strings.Join(argv, " "))
	if c.onClear != nil {
		c.mu.Unlock()
		c.onClear()
		c.mu.Lock()
	}
	switch c.mode {
	case "stream":
		if c.pending == nil {
			c.pending = map[string]bool{}
		}
		c.pending[marker] = true
		return 0, nil, errors.New("the exec stream broke")
	case "exit":
		return envelope(supervisedRun, "", "rm failed", 1)
	}
	c.socket = false
	return envelope(supervisedRun, "", "", 0)
}

func envelope(supervisedRun bool, stdout, stderr string, code int) (int, io.Reader, error) {
	if supervisedRun {
		return 0, bytes.NewReader(clustertest.QuorumEnvelope([]byte(stdout), []byte(stderr), code, false, false)), nil
	}
	return code, noInitialOSDStream(stdout), nil
}

// release lets every pending removal run: it unlinks the socket path.
func (c *cleanupControl) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) > 0 {
		c.socket = false
	}
	c.pending = nil
}

func (c *cleanupControl) set(mode string, cancelFails int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mode, c.cancelFails = mode, cancelFails
}

func (c *cleanupControl) removeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.removes)
}

func (c *cleanupControl) hasSocket() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.socket
}

// newCleanupFixture is the retained fixture with a run volume, so that the
// stale socket cleanup actually reaches the control container. A created
// replacement opens its socket.
func newCleanupFixture(t *testing.T) (*retainedFixture, *cleanupControl) {
	t.Helper()
	f := newRetainedFixture(t)
	control := &cleanupControl{retainedControl: f.control}
	f.cluster.Container = control
	f.cluster.run = &runVolume{name: "synthetic-run"}
	inner := f.cluster.stores.runner
	f.cluster.stores.runner = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		control.mu.Lock()
		control.socket = true
		control.mu.Unlock()
		return inner(ctx, image, opts...)
	}
	return f, control
}

// runnerOutcome replaces the fixture runner by one that counts dispatches and
// either returns no container with an error or the fixture candidate with one.
func runnerOutcome(f *retainedFixture, withCandidate bool) *int {
	dispatches := new(int)
	inner := f.cluster.stores.runner
	var mu sync.Mutex
	f.cluster.stores.runner = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
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
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	control.set("exit", 0)
	osd, err := detached.Reopen(t.Context())
	if err == nil || osd != nil || !strings.Contains(err.Error(), "rm failed") {
		t.Fatalf("failed cleanup: %v %v", err, osd)
	}
	if f.runs != 0 || control.removeCount() != 1 || detached.reopened || f.cluster.detachedOSDs[3] != detached || f.cluster.osds[3] != nil {
		t.Fatalf("failed cleanup consumed the handle: runs=%d removes=%d reopened=%v", f.runs, control.removeCount(), detached.reopened)
	}
	requireNoRegistrationOrStoreWrites(t, f)

	control.set("", 0)
	reopened, err := detached.Reopen(t.Context())
	if err != nil {
		t.Fatal("retry after the known zero-dispatch failure:", err)
	}
	if f.runs != 1 || control.removeCount() != 2 || f.cluster.osds[3] != reopened || reopened.GetContainerID() != "osd-reopened-3" || !detached.reopened {
		t.Fatalf("retry state runs=%d removes=%d", f.runs, control.removeCount())
	}
	if _, err := detached.Reopen(t.Context()); err == nil || !strings.Contains(err.Error(), "already reopened") || f.runs != 1 {
		t.Fatalf("a consumed handle was replayed: %v runs=%d", err, f.runs)
	}
}

// A cleanup whose Exec stream broke may still run. Its cancellation, not the
// zero creation dispatch, decides whether a retry can overlap it.
func TestReopenSettlesAnUnconfirmedCleanupBeforeRetrying(t *testing.T) {
	setup := func(t *testing.T) (*retainedFixture, *cleanupControl, *DetachedOSD) {
		f, control := newCleanupFixture(t)
		detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		// The first cleanup is lost, and its cancellation fails too.
		control.set("stream", 1)
		osd, err := detached.Reopen(t.Context())
		if err == nil || osd != nil || f.runs != 0 || detached.reopened || f.cluster.detachedOSDs[3] != detached || control.removeCount() != 1 {
			t.Fatalf("unconfirmed cleanup: %v %v runs=%d removes=%d", err, osd, f.runs, control.removeCount())
		}
		requireNoRegistrationOrStoreWrites(t, f)
		return f, control, detached
	}

	t.Run("cancellation cannot settle it", func(t *testing.T) {
		f, control, detached := setup(t)
		// The retry must not create a container nor start another cleanup.
		control.set("", 1)
		osd, err := detached.Reopen(t.Context())
		if err == nil || osd != nil || !strings.Contains(err.Error(), "may still run") || f.runs != 0 || control.removeCount() != 1 || detached.reopened {
			t.Fatalf("retry over an unsettled cleanup: %v %v runs=%d removes=%d", err, osd, f.runs, control.removeCount())
		}
		// The old removal runs late; no successor exists to be hit.
		control.release()
		if f.cluster.osds[3] != nil || f.cluster.detachedOSDs[3] != detached {
			t.Fatal("the handle was consumed")
		}
	})

	t.Run("cancellation settles it", func(t *testing.T) {
		f, control, detached := setup(t)
		control.set("", 0)
		osd, err := detached.Reopen(t.Context())
		if err != nil || osd == nil || f.runs != 1 || control.removeCount() != 2 {
			t.Fatalf("retry after settlement: %v runs=%d removes=%d", err, f.runs, control.removeCount())
		}
		// The settled removal can no longer unlink the replacement's socket.
		control.release()
		if !control.hasSocket() {
			t.Fatal("a late cleanup unlinked the replacement's socket")
		}
	})
}

// A cleanup that ended with a failure report, or whose Exec failed and was
// confirmed stopped, is known finished: the retry needs no settlement.
func TestReopenRetriesAfterAConfirmedFinishedCleanup(t *testing.T) {
	f, control := newCleanupFixture(t)
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	control.set("stream", 0)
	if osd, err := detached.Reopen(t.Context()); err == nil || osd != nil || len(detached.staleCleanups) != 0 || f.runs != 0 {
		t.Fatalf("stopped cleanup: %v %v", err, osd)
	}
	control.release()
	control.set("", 0)
	if _, err := detached.Reopen(t.Context()); err != nil || f.runs != 1 || !control.hasSocket() {
		t.Fatalf("retry after a confirmed stop: %v runs=%d", err, f.runs)
	}
}

// A cancellation that arrives during the cleanup is rechecked before the
// irreversible transition.
func TestReopenRechecksCancellationAfterTheStaleSocketCleanup(t *testing.T) {
	f, control := newCleanupFixture(t)
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	control.onClear = cancel
	osd, err := detached.Reopen(ctx)
	if !errors.Is(err, context.Canceled) || osd != nil {
		t.Fatalf("canceled reopen: %v %v", err, osd)
	}
	if f.runs != 0 || detached.reopened || f.cluster.detachedOSDs[3] != detached || f.cluster.osds[3] != nil {
		t.Fatalf("cancellation consumed the handle: runs=%d", f.runs)
	}
	requireNoRegistrationOrStoreWrites(t, f)
	control.onClear = nil
	if _, err := detached.Reopen(t.Context()); err != nil || f.runs != 1 {
		t.Fatalf("retry after cancellation: %v runs=%d", err, f.runs)
	}
}

// A dispatched creation that returns no container may have lost its reply, so
// it stays consumed and is never replayed.
func TestReopenKeepsADispatchedCreationErrorConsumed(t *testing.T) {
	f, _ := newCleanupFixture(t)
	dispatches := runnerOutcome(f, false)
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	osd, err := detached.Reopen(t.Context())
	if err == nil || osd == nil || osd.Container != nil || f.cluster.osds[3] != osd || !detached.reopened || f.cluster.detachedOSDs[3] != nil {
		t.Fatalf("dispatched nil,error: %v %+v", err, osd)
	}
	if _, err := detached.Reopen(t.Context()); err == nil || !strings.Contains(err.Error(), "already reopened") {
		t.Fatal("retry of the dispatched creation:", err)
	}
	if _, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context()); err == nil || !strings.Contains(err.Error(), "no container") {
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
			detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
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
					osd, err := detached.Reopen(t.Context())
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

// reopenOrderOK reports whether the reopenOSD source cleans the stale socket
// and rechecks the context before it consumes the detached handle.
func reopenOrderOK(src string) bool {
	start := strings.Index(src, "func (c *Container) reopenOSD(")
	if start < 0 {
		return false
	}
	body := src[start:]
	if end := strings.Index(body, "\n}\n"); end >= 0 {
		body = body[:end]
	}
	clean := strings.Index(body, "c.clearStaleOSDSocket(")
	recheck := strings.Index(body[max(clean, 0):], "ctx.Err()")
	consume := strings.Index(body, "detached.reopened = true")
	return clean >= 0 && consume >= 0 && recheck >= 0 && clean+recheck < consume &&
		strings.Count(body, "c.clearStaleOSDSocket(") == 1 && !strings.Contains(body, "c.clearAdminSocket(")
}

// The exact production source keeps the order; the control sources do not.
func TestReopenSourceCleansBeforeConsuming(t *testing.T) {
	src, err := os.ReadFile("osd_retained.go")
	if err != nil {
		t.Fatal(err)
	}
	if !reopenOrderOK(string(src)) {
		t.Fatal("reopenOSD no longer cleans the stale socket and rechecks the context before consuming the handle")
	}
	old := `func (c *Container) reopenOSD(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	detached.reopened = true
	c.osds[id] = osd
	if err := c.clearStaleOSDSocket(ctx, detached); err != nil {
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
