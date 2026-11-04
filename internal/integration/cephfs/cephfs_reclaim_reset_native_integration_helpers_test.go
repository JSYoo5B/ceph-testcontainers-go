//go:build all || integration

package cephfs_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// reclaimRegisteredReceipt waits for receipt phase of an actor, stamps the
// time at which the test saw it and rejects a receipt of another run or phase.
// It fails at once when the actor ends without publishing it.
func reclaimRegisteredReceipt(t *testing.T, ctx context.Context, run *cephFSActorRun, phase int) (harness.ReclaimReceipt, time.Time) {
	t.Helper()
	path := run.workdir + "/receipt-" + strconv.Itoa(phase)
	deadline := time.Now().Add(3 * time.Minute)
	for {
		code, out, err := harness.SessionExec(ctx, run.client, "cat", path)
		if err == nil && code == 0 {
			seen := time.Now()
			decoder := json.NewDecoder(strings.NewReader(out))
			decoder.DisallowUnknownFields()
			var receipt harness.ReclaimReceipt
			if err := decoder.Decode(&receipt); err != nil {
				t.Fatalf("receipt %d: %v: %q", phase, err, out)
			}
			if receipt.Token != run.Token || receipt.Phase != phase || receipt.PID <= 0 {
				t.Fatalf("receipt %d belongs to another run or phase: %+v", phase, receipt)
			}
			return receipt, seen
		}
		select {
		case <-run.Done:
			t.Fatalf("actor ended without receipt %d: %s", phase, run.exited(ctx))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("receipt %d did not appear", phase)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("receipt %d: %v", phase, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// reclaimAttemptOf turns an attempt result into the judge's attempt. The
// release code is checked separately: a handle that cannot be released is not
// a clean attempt.
func reclaimAttemptOf(r harness.ReclaimAttemptResult, start, end time.Time) harness.ResetAttempt {
	return harness.ResetAttempt{Handle: r.Handle, Initialized: r.Initialized, PriorReclaimCalls: r.PriorReclaimCalls,
		Flags: r.Flags, Target: r.Target, OwnUUID: r.OwnUUID, RC: r.RC, Replies: r.MDSReplies, Start: start, End: end}
}

// runReclaimAttempt runs ReclaimAttempt in the helper container, requires a
// clean release and returns the timed attempt.
func runReclaimAttempt(t *testing.T, ctx context.Context, m *cephFSSingleMDS, helper testcontainers.Container, name, target, own string, flags uint) harness.ResetAttempt {
	t.Helper()
	log := "/tmp/reclaim-attempt-" + own + ".log"
	start := time.Now()
	out := harness.ExecOutput(t, ctx, helper, "python3", "-c", harness.ReclaimAttempt, m.FS.FilesystemName, target, own, strconv.FormatUint(uint64(flags), 10), log)
	end := time.Now()
	r, err := harness.ParseReclaimAttempt(out)
	if err != nil {
		t.Fatal(err)
	}
	if r.ReleaseRC == nil || *r.ReleaseRC != 0 || r.MDSReplies == nil {
		t.Fatalf("the %s handle was not released cleanly or its log was not read: %+v", name, r)
	}
	return reclaimAttemptOf(r, start, end)
}

// reclaimAttemptString renders an attempt for the receipt, with the return
// value as a number or "unrecorded".
func reclaimAttemptString(a harness.ResetAttempt) string {
	rc := "unrecorded"
	if a.RC != nil {
		rc = strconv.Itoa(*a.RC)
	}
	return fmt.Sprintf("handle=%s initialized=%t prior_reclaim_calls=%d flags=%d target=%s own_uuid=%s rc=%s mds_replies=%v", a.Handle, a.Initialized, a.PriorReclaimCalls, a.Flags, a.Target, a.OwnUUID, rc, a.Replies)
}

// reclaimExecutor is one container that runs native clients, with its
// verified eligibility probe.
type reclaimExecutor struct {
	name      string
	container testcontainers.Container
	probe     harness.ResetABIProbe
}

// probeReclaimExecutor runs the eligibility probe in a container, binds it to
// the container's actual image config ID and applies the ABI gate. A probe
// that fails or does not match is a blocker, never a reason to switch library.
func probeReclaimExecutor(t *testing.T, ctx context.Context, name string, c testcontainers.Container, major, minor, patch int64) reclaimExecutor {
	t.Helper()
	inspect, err := c.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect %s: %v", name, err)
	}
	out := harness.ExecOutput(t, ctx, c, "python3", "-c", harness.ReclaimABIProbe, inspect.Image)
	doc, err := harness.ReclaimJSONLine(out)
	if err != nil {
		t.Fatalf("%s ABI probe: %v", name, err)
	}
	probe, err := harness.ParseResetABIProbe(doc)
	if err != nil {
		t.Fatalf("%s ABI probe unreadable: %v", name, err)
	}
	if err := harness.CheckResetABIProbe(probe, major, minor, patch); err != nil {
		t.Fatalf("%s is not eligible for the RESET recipe: %v", name, err)
	}
	t.Logf("RECLAIM_RESET_EXECUTOR %s image_config=%s library=%s library_sha256=%s extension_sha256=%s version=%d.%d.%d",
		name, probe.ImageConfigID, probe.LibraryPath, probe.LibrarySHA256, probe.ExtensionSHA256, probe.Major, probe.Minor, probe.Patch)
	return reclaimExecutor{name: name, container: c, probe: probe}
}

// reclaimObserver takes the timed observations of the RESET sequence: one
// session listing of the active MDS and one independent lock probe of the
// two held files, bracketed as a whole.
type reclaimObserver struct {
	m      *cephFSSingleMDS
	helper testcontainers.Container
	aPath  string
	uPath  string
}

func (o *reclaimObserver) observe(t *testing.T, ctx context.Context) harness.ResetObservation {
	t.Helper()
	obs := harness.ResetObservation{Start: time.Now()}
	data, err := o.m.Cluster.Ceph(ctx, "tell", "mds."+o.m.MDS.Name, "session", "ls", "--format", "json")
	if err != nil {
		t.Fatalf("session ls: %v", err)
	}
	if obs.Sessions, err = harness.ParseResetSessions(data); err != nil {
		t.Fatalf("session ls: %v: %.512s", err, data)
	}
	out := harness.ExecOutput(t, ctx, o.helper, "python3", "-c", harness.ReclaimLockProbe, o.m.FS.FilesystemName, o.aPath, o.uPath)
	if obs.ALock, obs.ULock, err = harness.ParseReclaimLock(out); err != nil {
		t.Fatalf("lock probe: %v", err)
	}
	obs.End = time.Now()
	return obs
}

// reclaimNonce returns a short random token that keeps the UUIDs of one run
// apart from any leftover of another.
func reclaimNonce(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 6)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw)
}

// runCephFSReclaimReset runs the native RESET recipe on one active MDS:
//
//  1. The release and the ABI of both executors are qualified.
//  2. A seed client fsyncs the payload. Client A (UUID, 600-second session
//     timeout) holds an exclusive flock on the seeded file; the unrelated
//     client U holds another file's lock. Independent probes see both held.
//  3. A's actor is SIGKILLed after its process ownership is verified. Its
//     session and lock must still stand, so ordinary timeout cannot stand in
//     for the RESET.
//  4. A fresh handle asks to RESET a UUID that no session carries and must get
//     -ENOENT, with A and U untouched.
//  5. A second fresh handle asks to reclaim A's UUID with flags 0, the
//     state-preserving request. The MDS must refuse it with -EINVAL, and A's
//     session and lock must still stand.
//  6. A third fresh handle RESETs A's UUID and must get 0. Before it mounts,
//     A's session is gone, A's lock is free and U's session and lock stand.
//  7. The same handle finishes the reclaim, takes over the former UUID,
//     mounts as a new client and reads the original bytes through a new
//     descriptor.
//
// JudgeResetSequence decides; the receipt is logged on every outcome.
func runCephFSReclaimReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := startCephFSSingleMDS(t, ctx)
	helper := harness.ToolkitContainer(t, m.Cluster)
	major, minor, patch, err := harness.ReclaimExpectedRelease(m.Version)
	if err != nil {
		t.Fatal(err)
	}
	executors := []reclaimExecutor{
		probeReclaimExecutor(t, ctx, "client", m.Client, major, minor, patch),
		probeReclaimExecutor(t, ctx, "toolkit", helper, major, minor, patch),
	}

	nonce := reclaimNonce(t)
	const aPath, uPath = "/reclaim-reset-a", "/reclaim-reset-u"
	payload := []byte("reclaim-acknowledged-data\x00\xff")
	seedSum := sha256.Sum256(payload)
	seq := harness.ResetSequence{
		AUUID:      "reclaim-a-" + nonce,
		UUUID:      "reclaim-u-" + nonce,
		SeedSize:   len(payload),
		SeedSHA256: hex.EncodeToString(seedSum[:]),
	}
	missingTarget, missingOwn, preserveOwn, resetOwn := "reclaim-missing-"+nonce, "reclaim-m-"+nonce, "reclaim-p-"+nonce, "reclaim-b-"+nonce
	var outcome harness.ResetOutcome
	var killedToReset time.Duration
	defer func() {
		// The receipt is emitted before the cluster is torn down, whatever the
		// outcome, so a failed run keeps the ledger.
		t.Logf("RECLAIM_RESET_RECEIPT mds=%s executors=[%s %s; %s %s] verdict=%d reason=%q a=client.%d u=client.%d b=client.%d kill_to_reset_observed=%s missing={%s} preserve={%s} reset={%s} observations(after_kill=%d after_missing=%d after_preserve=%d after_reset=%d); not observed: blocklist and OSD epoch effects, RESET call latency, any adopted capability or lock (the MDS refuses state-preserving reclaim)",
			m.Version, executors[0].name, executors[0].probe.ImageConfigID, executors[1].name, executors[1].probe.ImageConfigID,
			outcome.Verdict, outcome.Reason, seq.A, seq.U, seq.B, killedToReset, reclaimAttemptString(seq.Missing), reclaimAttemptString(seq.Preserve), reclaimAttemptString(seq.Reset),
			len(seq.AfterKill), len(seq.AfterMissing), len(seq.AfterPreserve), len(seq.AfterReset))
	}()

	seedOut := harness.ExecOutput(t, ctx, helper, "python3", "-c", harness.ReclaimSeed, m.FS.FilesystemName, hex.EncodeToString(payload), aPath, uPath)
	seedDoc, err := harness.ReclaimJSONLine(seedOut)
	if err != nil {
		t.Fatal("seed:", err)
	}
	var seeded struct {
		Client uint64 `json:"client"`
		Size   int    `json:"size"`
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal(seedDoc, &seeded); err != nil || seeded.Size != seq.SeedSize || seeded.SHA256 != seq.SeedSHA256 {
		t.Fatalf("seed wrote %s, want %d bytes with sha256 %s (%v)", seedDoc, seq.SeedSize, seq.SeedSHA256, err)
	}

	holderA := startCephFSActor(t, ctx, m.Cluster, m.Client, harness.ReclaimHolder, m.FS.FilesystemName, "/tmp/reclaim-holder-a", aPath, seq.AUUID)
	a, _ := reclaimRegisteredReceipt(t, ctx, holderA, 1)
	holderU := startCephFSActor(t, ctx, m.Cluster, helper, harness.ReclaimHolder, m.FS.FilesystemName, "/tmp/reclaim-holder-u", uPath, seq.UUUID)
	u, _ := reclaimRegisteredReceipt(t, ctx, holderU, 1)
	if a.Client == 0 || u.Client == 0 || a.Client == u.Client || a.UUID != seq.AUUID || u.UUID != seq.UUUID {
		t.Fatalf("holder identities: A %+v, U %+v", a, u)
	}
	seq.A, seq.U = a.Client, u.Client

	observer := &reclaimObserver{m: m, helper: helper, aPath: aPath, uPath: uPath}
	before := observer.observe(t, ctx)
	if before.Sessions.Status(seq.A) != harness.ResetPresent || before.Sessions.Status(seq.U) != harness.ResetPresent ||
		before.ALock != harness.ResetLockHeld || before.ULock != harness.ResetLockHeld {
		t.Fatalf("holders are not established before the kill: sessions %v, locks %d/%d", before.Sessions, before.ALock, before.ULock)
	}

	cmdline := harness.ExecOutput(t, ctx, m.Client, "cat", "/proc/"+strconv.Itoa(a.PID)+"/cmdline")
	if !strings.Contains(cmdline, holderA.Token) {
		t.Fatal("holder A's PID does not belong to its actor")
	}
	harness.ExecOutput(t, ctx, m.Client, "sh", "-c", `kill -9 "$1"`, "sh", strconv.Itoa(a.PID))
	select {
	case <-holderA.Done:
	case <-time.After(30 * time.Second):
		t.Fatal("holder A was not joined after SIGKILL")
	}
	exit := holderA.Result.ExitCode
	seq.AExitStatus = &exit
	seq.AKilledAt = time.Now()
	if holderA.Err != nil || holderA.Result.TimedOut {
		t.Fatalf("holder A outcome: %s", holderA.exited(ctx))
	}

	seq.AfterKill = append(seq.AfterKill, observer.observe(t, ctx), observer.observe(t, ctx))

	seq.Missing = runReclaimAttempt(t, ctx, m, helper, "missing-UUID", missingTarget, missingOwn, harness.CephReclaimReset)
	seq.AfterMissing = append(seq.AfterMissing, observer.observe(t, ctx), observer.observe(t, ctx))
	seq.Preserve = runReclaimAttempt(t, ctx, m, helper, "state-preserving", seq.AUUID, preserveOwn, 0)
	seq.AfterPreserve = append(seq.AfterPreserve, observer.observe(t, ctx), observer.observe(t, ctx))

	resetStart := time.Now()
	reclaimer := startCephFSActor(t, ctx, m.Cluster, helper, harness.ReclaimReclaimer, m.FS.FilesystemName, "/tmp/reclaim-reset", seq.AUUID, resetOwn, aPath)
	attempt, resetSeen := reclaimRegisteredReceipt(t, ctx, reclaimer, 1)
	seq.Reset = harness.ResetAttempt{Handle: attempt.Handle, Initialized: attempt.Initialized, PriorReclaimCalls: attempt.PriorReclaimCalls,
		Flags: attempt.Flags, Target: attempt.Target, OwnUUID: attempt.OwnUUID, RC: attempt.RC, Start: resetStart, End: resetSeen}
	seq.AfterReset = append(seq.AfterReset, observer.observe(t, ctx), observer.observe(t, ctx))
	killedToReset = time.Since(seq.AKilledAt)
	if killedToReset > 2*time.Minute {
		t.Fatalf("RESET was observed %s after the kill, too late to exclude the 600-second session timeout from attribution", killedToReset)
	}

	seq.MountStart = time.Now()
	reclaimer.Open(t, ctx, "mount")
	read, _ := reclaimRegisteredReceipt(t, ctx, reclaimer, 2)
	seq.B, seq.ReadByClientID, seq.ReadSize, seq.ReadSHA256, seq.ReadFromNewFd = read.Client, read.Client, read.Size, read.SHA256, read.NewFd

	status, statusErr := m.FS.MDSStatus(ctx)
	if result, err := evaluateMDSs(status, statusErr, sameMDSRank(m.MDS)); err != nil || !result.Matched {
		t.Fatalf("the active MDS changed during the recipe: %v %v", result, err)
	}

	outcome = harness.JudgeResetSequence(seq)

	reclaimer.Open(t, ctx, "finish")
	if closed, _ := reclaimRegisteredReceipt(t, ctx, reclaimer, 3); !closed.Closed {
		t.Fatalf("replacement client did not close: %+v", closed)
	}
	reclaimer.Wait(t, ctx)
	holderU.Open(t, ctx, "finish")
	if closed, _ := reclaimRegisteredReceipt(t, ctx, holderU, 2); !closed.Closed {
		t.Fatalf("unrelated client did not close: %+v", closed)
	}
	holderU.Wait(t, ctx)

	switch outcome.Verdict {
	case harness.ResetEstablished:
	case harness.ResetContradicted:
		t.Fatalf("RESET contradicted: %s", outcome.Reason)
	default:
		t.Fatalf("RESET inconclusive: %s", outcome.Reason)
	}
}
