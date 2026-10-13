//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// What a writer observes and what the OSD did can differ. The fixture supplies
// an owned pool on a file-backed BlueStore OSD, the OSD's own operation
// records and supervised native clients; an independent client decides every
// outcome by reading it back.
func TestRADOSMutationOutcomes(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(1))
	const pool, namespace = "tc-outcomes", "fixture"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", PGNum: 1}); err != nil {
		t.Fatal(err)
	}
	poolState, err := cluster.PoolStatus(ctx, pool)
	if err != nil || poolState.ID <= 0 {
		t.Fatal("pool identity unavailable", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	pg := fmt.Sprintf("%d.0", poolState.ID)
	osd := cluster.OSDs()[0]
	osdName := fmt.Sprintf("osd.%d", osd.ID)
	phase := func(out any, name string, args ...string) []byte {
		t.Helper()
		return mutationOutcomePhase(t, ctx, cluster, client, out, append([]string{name, pool, namespace}, args...)...)
	}

	// A completion that returned 0 is the durable receipt: the writes survive
	// a SIGKILL of the OSD.
	var acked struct{}
	receipt := phase(&acked, "acked")
	booted := osdUpFrom(t, ctx, cluster, osd.ID)
	signalContainer(t, ctx, osd, "KILL")
	for deadline := time.Now().Add(30 * time.Second); ; {
		state, err := osd.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Running {
			if state.ExitCode != 137 {
				t.Fatalf("%s exited %d after SIGKILL", osdName, state.ExitCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still runs after SIGKILL", osdName)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// The MONs never saw the killed process go down, so the PG statistics stay
	// clean until the new process boots and peers.
	for deadline := time.Now().Add(2 * time.Minute); osdUpFrom(t, ctx, cluster, osd.ID) <= booted; {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not boot again", osdName)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	phase(&acked, "verify", string(receipt))

	// The OSD delays its operations, so the writer's own timeout ends the
	// call while the OSD still holds the write. The writer stays connected,
	// and the OSD applies the write after the writer gave up.
	var base, after mutationOutcomeObject
	phase(&base, "put", "late", "base;")
	restore := holdDispatchDelay(t, ctx, cluster, osdName, 2*time.Second)
	var late struct {
		Client    string       `json:"client"`
		Sent      float64      `json:"sent"`
		Completed float64      `json:"completed"`
		Return    int          `json:"return"`
		Callbacks [][2]float64 `json:"callbacks"`
	}
	phase(&late, "late", "late", "2", "15")
	restore()
	if late.Return != -110 || len(late.Callbacks) != 1 || late.Callbacks[0][1] != -110 {
		t.Fatalf("the writer's timeout did not end its append once with ETIMEDOUT: %+v", late)
	}
	record := finishedClientWrite(t, ctx, cluster, osdName, pg, late.Client)
	started, ok := record.event("started")
	_, committed := record.event("commit_sent")
	gaveUp := time.Unix(0, int64(late.Completed*1e9))
	if !ok || !committed || !started.After(gaveUp) {
		t.Fatalf("%s did not start and commit the append after the writer gave up at %v: %+v", osdName, gaveUp, record)
	}
	phase(&after, "state", "late")
	if after.Data != base.Data+"late;" || after.Version <= base.Version {
		t.Fatalf("the late append was not applied exactly once: %+v, before %+v", after, base)
	}
	received, _ := record.event("initiated")
	t.Logf("%s received the append of %s at %s and started it at %s, %.1fs after the writer's ETIMEDOUT",
		osdName, late.Client, received.Format(time.StampMicro), started.Format(time.StampMicro), started.Sub(gaveUp).Seconds())

	// Retrying the uncertain append unguarded appends twice. Guarded by the
	// version read before the first attempt, the retry is refused with ERANGE;
	// a version newer than the object's is refused with EOVERFLOW. Each
	// refused write still takes a version of the PG, because the PG log
	// records it for duplicate detection.
	var retry struct {
		FutureErrno  int                   `json:"future_errno"`
		GuardedErrno int                   `json:"guarded_errno"`
		AfterGuarded mutationOutcomeObject `json:"after_guarded"`
		Unguarded    mutationOutcomeObject `json:"after_unguarded"`
	}
	phase(&retry, "retry", "late", fmt.Sprint(base.Version))
	if retry.FutureErrno != 75 || retry.GuardedErrno != 34 || retry.AfterGuarded != after {
		t.Fatalf("the guarded retries were not refused with EOVERFLOW and ERANGE without a change: %+v", retry)
	}
	if retry.Unguarded.Data != after.Data+"late;" || retry.Unguarded.Version != after.Version+3 {
		t.Fatalf("the unguarded retry did not append again after the two refused versions: %+v", retry)
	}

	// A writer killed while the OSD holds its write leaves nothing behind:
	// the OSD discards an operation whose connection closed before the OSD
	// started it (osd_discard_disconnected_ops). The longer delay leaves the
	// OSD about 9 seconds between receiving the write and starting it.
	var killedBase, killed mutationOutcomeObject
	phase(&killedBase, "put", "killed", "base;")
	restore = holdDispatchDelay(t, ctx, cluster, osdName, 3*time.Second)
	writerCtx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	done := make(chan error, 1)
	go func() {
		_, err := cluster.RunNativeCommand(writerCtx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", radosMutationOutcomeScript, "late", pool, namespace, "killed", "0", "600"},
			Timeout: 5 * time.Minute, MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "mutation outcome killed writer",
		})
		done <- err
	}()
	var held osdTrackedOp
	for deadline := time.Now().Add(time.Minute); ; {
		if ops := osdClientWrites(t, ctx, cluster, osdName, "dump_ops_in_flight", pg); len(ops) > 0 {
			held = ops[0]
			if _, ok := held.event("reached_pg"); ok {
				t.Fatalf("%s already reached the PG with the writer's append when it was first seen: %+v", osdName, held)
			}
			break
		}
		select {
		case err := <-done:
			t.Fatalf("the writer ended before %s held its write: %v", osdName, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never held the writer's append", osdName)
		}
		time.Sleep(50 * time.Millisecond)
	}
	stopWriter()
	var unstopped *ceph.NativeCommandUnstoppedError
	if err := <-done; !errors.Is(err, context.Canceled) || errors.As(err, &unstopped) {
		t.Fatalf("the writer was not confirmed stopped: %v", err)
	}
	writer := trackedClient(held.Description)
	for deadline := time.Now().Add(time.Minute); ; {
		running := false
		for _, op := range osdClientWrites(t, ctx, cluster, osdName, "dump_ops_in_flight", pg) {
			running = running || trackedClient(op.Description) == writer
		}
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s still holds the killed writer's append", osdName)
		}
		time.Sleep(200 * time.Millisecond)
	}
	restore()
	discarded := finishedClientWrite(t, ctx, cluster, osdName, pg, writer)
	if _, ran := discarded.event("started"); ran {
		t.Fatalf("%s started the killed writer's append: %+v", osdName, discarded)
	}
	phase(&killed, "state", "killed")
	if killed != killedBase {
		t.Fatalf("the killed writer's append changed the object: %+v, before %+v", killed, killedBase)
	}
	t.Logf("%s held %s and finished it without starting it: %+v", osdName, held.Description, discarded.TypeData.Events)

	if err := cluster.RemovePool(ctx, pool); err != nil {
		t.Fatal(err)
	}
}
