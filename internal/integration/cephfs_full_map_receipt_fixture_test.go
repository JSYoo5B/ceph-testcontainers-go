//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// These controls drive the retained-client map wait with synthetic status
// sequences. They test the receipt and the readiness predicate only; they do
// not reproduce the native timeout.

const retainedClientID = 4261

func statusSequence(statuses ...harness.CephFSClientStatus) func(context.Context) (harness.CephFSClientStatus, error) {
	next := 0
	return func(context.Context) (harness.CephFSClientStatus, error) {
		status := statuses[min(next, len(statuses)-1)]
		next++
		return status, nil
	}
}

func clientAt(id uint64, epoch, barrier uint32, blocklisted bool) harness.CephFSClientStatus {
	return harness.CephFSClientStatus{ID: id, OSDEpoch: epoch, Barrier: barrier, Blocklisted: blocklisted}
}

func waitFor(r *harness.RetainedClientReceipt, target uint32, bound time.Duration, read func(context.Context) (harness.CephFSClientStatus, error)) error {
	return r.WaitMap(context.Background(), "full map", target, true, bound, time.Second, time.Millisecond, read)
}

func TestRetainedMapWaitStaleEpochTimesOutWithTarget(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1099511627776, 3)
	err := waitFor(r, 26, 40*time.Millisecond, statusSequence(clientAt(retainedClientID, 24, 20, false), clientAt(retainedClientID, 25, 20, false)))
	if err == nil {
		t.Fatal("stale epoch accepted")
	}
	for _, want := range []string{"target e26", "want client 4261", "e25 barrier 20", "blocklisted=false", "poke completed=true"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
	wait := r.Waits[0]
	if wait.Outcome != "timeout" || wait.TargetEpoch != 26 || wait.Probes < 2 || !wait.PokeCompleted {
		t.Fatalf("wait = %+v", wait)
	}
	// The history keeps each distinct status once, in order.
	if len(wait.Samples) != 2 || wait.Samples[0].OSDEpoch != 24 || wait.Samples[1].OSDEpoch != 25 || wait.Samples[1].Barrier != 20 {
		t.Fatalf("samples = %+v", wait.Samples)
	}
}

func TestRetainedMapWaitReachedWithOlderBarrier(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	if err := waitFor(r, 24, time.Second, statusSequence(clientAt(retainedClientID, 23, 20, false), clientAt(retainedClientID, 24, 20, false))); err != nil {
		t.Fatal(err)
	}
	if wait := r.Waits[0]; wait.Outcome != "reached" || wait.Probes != 2 {
		t.Fatalf("wait = %+v", wait)
	}
}

func TestRetainedMapWaitAdvancedBarrierDoesNotSubstituteForMap(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	if err := waitFor(r, 24, 30*time.Millisecond, statusSequence(clientAt(retainedClientID, 23, 30, false))); err == nil {
		t.Fatal("a barrier above the target made a stale map ready")
	}
	if r.Waits[0].Outcome != "timeout" {
		t.Fatalf("wait = %+v", r.Waits[0])
	}
}

func TestRetainedMapWaitIdentityMismatchIsTerminal(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	err := waitFor(r, 24, time.Second, statusSequence(clientAt(4262, 24, 20, false), clientAt(retainedClientID, 24, 20, false)))
	if err == nil || !strings.Contains(err.Error(), "client 4262") {
		t.Fatalf("err = %v", err)
	}
	if wait := r.Waits[0]; wait.Outcome != "identity-mismatch" || wait.Probes != 1 {
		t.Fatalf("wait = %+v", wait)
	}
}

func TestRetainedMapWaitBlocklistingIsNotRevivedByClearance(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	err := waitFor(r, 24, time.Second, statusSequence(clientAt(retainedClientID, 24, 20, true), clientAt(retainedClientID, 24, 20, false)))
	if err == nil || !strings.Contains(err.Error(), "blocklisted") {
		t.Fatalf("err = %v", err)
	}
	if wait := r.Waits[0]; wait.Outcome != "blocklisted" || wait.Probes != 1 || !wait.Samples[0].Blocklisted {
		t.Fatalf("wait = %+v", wait)
	}
}

func TestRetainedMapWaitLateResultIsNotEvidence(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	err := waitFor(r, 24, 20*time.Millisecond, func(ctx context.Context) (harness.CephFSClientStatus, error) {
		<-ctx.Done()
		time.Sleep(5 * time.Millisecond)
		return clientAt(retainedClientID, 24, 20, false), nil
	})
	if err == nil {
		t.Fatal("a result after the bound was accepted")
	}
	if wait := r.Waits[0]; wait.Outcome != "timeout-late-result" {
		t.Fatalf("wait = %+v", wait)
	}
}

func TestRetainedMapWaitProbeErrorIsRecorded(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	err := waitFor(r, 24, time.Second, func(context.Context) (harness.CephFSClientStatus, error) {
		return harness.CephFSClientStatus{}, errors.New("client status: exit=1")
	})
	if err == nil {
		t.Fatal("probe error accepted")
	}
	if wait := r.Waits[0]; wait.Outcome != "probe-error" || wait.LastError != "client status: exit=1" {
		t.Fatalf("wait = %+v", wait)
	}
}

func TestRetainedMapWaitPhaseExpiryDuringProbeIsATimeout(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	err := r.WaitMap(context.Background(), "drained map", 31, true, 20*time.Millisecond, time.Second, time.Millisecond, func(ctx context.Context) (harness.CephFSClientStatus, error) {
		<-ctx.Done()
		return harness.CephFSClientStatus{}, ctx.Err()
	})
	if err == nil || !strings.Contains(err.Error(), "not observed within") {
		t.Fatalf("err = %v, want the phase expiry", err)
	}
	if wait := r.Waits[0]; wait.Outcome != "timeout" || wait.LastError == "" {
		t.Fatalf("wait = %+v, want outcome timeout with the raw error kept", wait)
	}
}

func TestRetainedMapWaitShorterProbeBoundIsAProbeError(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	err := r.WaitMap(context.Background(), "drained map", 31, true, time.Second, 20*time.Millisecond, time.Millisecond, func(ctx context.Context) (harness.CephFSClientStatus, error) {
		<-ctx.Done()
		return harness.CephFSClientStatus{}, ctx.Err()
	})
	if err == nil || strings.Contains(err.Error(), "not observed within") {
		t.Fatalf("err = %v, want an independent probe failure", err)
	}
	if wait := r.Waits[0]; wait.Outcome != "probe-error" {
		t.Fatalf("wait = %+v", wait)
	}
}

func TestRetainedMapWaitSampleHistoryKeepsLastStatus(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	epoch := uint32(0)
	_ = waitFor(r, 1000, 200*time.Millisecond, func(context.Context) (harness.CephFSClientStatus, error) {
		epoch++
		return clientAt(retainedClientID, epoch, 20, false), nil
	})
	wait := r.Waits[0]
	if len(wait.Samples) != harness.RetainedMapSampleLimit || wait.Dropped == 0 || wait.Samples[len(wait.Samples)-1].OSDEpoch != uint32(wait.Probes) {
		t.Fatalf("samples=%d dropped=%d probes=%d", len(wait.Samples), wait.Dropped, wait.Probes)
	}
}

func TestRetainedClientReceiptKeepsUnreachedPhasesAbsent(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	notFull := uint32(23)
	r.NotFullEpoch = &notFull
	r.Phase("pool-not-full")
	line := r.Line()
	for _, absent := range []string{`"full_epoch"`, `"drained_epoch"`, `"waits"`, `"unprompted"`, `"restore_error"`, `"restore_started"`, "poke"} {
		if strings.Contains(line, absent) {
			t.Errorf("unreached %q appears: %s", absent, line)
		}
	}
	if !strings.HasPrefix(line, "CEPHFS_OSD_FULL_PHASE_RECEIPT pre-restore ") || !strings.Contains(line, `"not_full_epoch":23`) || !strings.Contains(line, `"phases":["pool-not-full"]`) {
		t.Fatalf("line = %s", line)
	}
}

func TestRetainedClientReceiptKeepsRestoreFailureBesideWaitFailure(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	full := uint32(26)
	r.FullEpoch = &full
	if err := waitFor(r, 26, 20*time.Millisecond, statusSequence(clientAt(retainedClientID, 25, 20, false))); err == nil {
		t.Fatal("stale epoch accepted")
	}
	r.RestoreStarted = true
	r.RestoreError = "restore: mon unreachable"
	line := r.Line()
	for _, want := range []string{" restore-started ", `"outcome":"timeout"`, `"target_epoch":26`, `"restore_error":"restore: mon unreachable"`} {
		if !strings.Contains(line, want) {
			t.Errorf("line lacks %q: %s", want, line)
		}
	}
}
