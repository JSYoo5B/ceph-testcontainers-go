package mirror

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/rbdobservation"
)

// A status observation or a binding that waits for the gate longer than
// rbdGateBound still makes its reads: the bound starts when the gate is
// taken. Two other observations ahead in the queue, each holding the gate for
// most of the bound, must not turn a later one into a deadline.
func TestRBDGateBoundStartsAfterTheGateIsTaken(t *testing.T) {
	queued := rbdGateBound + 10*time.Second
	run := func(t *testing.T, gate *sync.Mutex, operation string, call func(context.Context) error) {
		t.Helper()
		var sample rbdobservation.Sample
		ctx := rbdobservation.WithRecorder(t.Context(), func(record rbdobservation.Sample) { sample = record })
		gate.Lock()
		result := make(chan error, 1)
		go func() { result <- call(ctx) }()
		time.Sleep(queued)
		select {
		case err := <-result:
			t.Fatalf("the queued call returned before the gate was free: %v", err)
		default:
		}
		gate.Unlock()
		if err := <-result; err != nil {
			t.Fatalf("a call that waited %s for the gate failed: %v", queued, err)
		}
		if operation == "" {
			return
		}
		if sample.Operation != operation || sample.Outcome != "ok" || sample.AttemptContext != "ok" ||
			sample.Acquired.Sub(sample.Started) < queued {
			t.Fatalf("unexpected sample after a %s wait: %+v", queued, sample)
		}
	}
	t.Run("owner image", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			link, _, _, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
			run(t, &link.mu, "owner-image", func(ctx context.Context) error {
				status, err := link.ImageStatus(ctx, "volume")
				if err == nil && !status.ReplayReady {
					t.Errorf("the queued observation is not ready: %+v", status)
				}
				return err
			})
		})
	})
	t.Run("namespace image", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-a", "ns-b")
			run(t, &h.m.mu, "namespace-image", func(ctx context.Context) error {
				status, err := view.ImageStatus(ctx, "volume")
				if err == nil && !status.ReplayReady {
					t.Errorf("the queued observation is not ready: %+v", status)
				}
				return err
			})
		})
	})
	t.Run("receiver", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			run(t, &h.m.mu, "receiver", func(ctx context.Context) error {
				status, err := h.m.ReceiverStatus(ctx)
				if err == nil && !status.Ready {
					t.Errorf("the queued observation is not ready: %+v", status)
				}
				return err
			})
		})
	})
	t.Run("namespace binding", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			run(t, &h.m.mu, "", func(ctx context.Context) error {
				_, err := h.m.BindNamespace(ctx, "ns-a", "ns-b")
				return err
			})
		})
	})
}

// The bound still ends a read that hangs once the gate is held, and the
// sample names the read stage, not admission.
func TestRBDGateBoundEndsAHungRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		link, source, _, _ := newRBDImageStatusFixture(t, ModeSnapshot, "", "")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var sample rbdobservation.Sample
		ctx = rbdobservation.WithRecorder(ctx, func(record rbdobservation.Sample) { sample = record })
		source.hang = true
		began := time.Now()
		_, err := link.ImageStatus(ctx, "volume")
		if elapsed := time.Since(began); err == nil || elapsed != rbdGateBound {
			t.Fatalf("a hung read ended after %s with %v, want the %s bound", elapsed, err, rbdGateBound)
		}
		if sample.Stage == "admission" || sample.Acquired.IsZero() || sample.Outcome != "deadline-exceeded" ||
			sample.CallerContext != "ok" || sample.AttemptContext != "deadline-exceeded" {
			t.Fatalf("unexpected sample of a hung read: %+v", sample)
		}
	})
}
