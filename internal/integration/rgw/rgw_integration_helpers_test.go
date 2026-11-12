//go:build all || integration

package rgw_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/expect"
)

// rgwSyncStatsBound bounds one explicit stats synchronization of a few small
// buckets. It is its own phase inside the test's context, so a stalled
// synchronization fails with its own deadline and leaves the test's remaining
// time for the failure's evidence.
const rgwSyncStatsBound = 2 * time.Minute

// rgwSyncStats runs one native stats synchronization, user or account stats
// with --sync-stats, through gateway.Admin under rgwSyncStatsBound. It never
// repeats the command: a stopped or failed synchronization may have changed
// native counters, and its outcome is unknown. A failure names how the command
// ended, its phase deadline and elapsed time, and the gateway container's
// generation and state and the cluster's OSD map summary, each read once
// under its own short bound after the failure.
func rgwSyncStats(t *testing.T, ctx context.Context, cluster *ceph.Container, gateway *rgw.Gateway, args ...string) {
	t.Helper()
	phase, cancel := context.WithTimeout(ctx, rgwSyncStatsBound)
	defer cancel()
	deadline, _ := phase.Deadline()
	started := time.Now()
	_, err := gateway.Admin(phase, args...)
	if err == nil {
		return
	}
	elapsed := time.Since(started)
	end := rgwSyncStatsEnd(err, ctx.Err(), phase.Err())
	t.Fatalf("native %s: %s after %s (phase bound %s, deadline %s); %s; %s; error: %v", strings.Join(args, " "), end,
		elapsed.Round(time.Millisecond), rgwSyncStatsBound, deadline.UTC().Format(time.RFC3339Nano),
		rgwSyncStatsGateway(ctx, gateway), rgwSyncStatsOSDs(ctx, cluster), err)
}

// rgwSyncStatsEnd classifies how a failed synchronization ended: by the
// supervisor at its own limit, by the phase deadline, by the test's context,
// with a stop that could not be confirmed, or by a native exit.
func rgwSyncStatsEnd(err, testErr, phaseErr error) string {
	var unstopped *ceph.NativeCommandUnstoppedError
	var native *ceph.NativeFailure
	switch {
	case errors.As(err, &unstopped):
		return "radosgw-admin may still run: its stop was not confirmed"
	case errors.Is(err, ceph.ErrNativeFailure) && strings.Contains(err.Error(), "still ran after"):
		return "radosgw-admin did not finish within the phase bound and the gateway's supervisor stopped it"
	case testErr != nil:
		return "the test's context ended the call: " + testErr.Error()
	case phaseErr != nil:
		return "the phase deadline ended the call, which stopped radosgw-admin"
	case errors.As(err, &native):
		return fmt.Sprintf("radosgw-admin exited %d (errno %q)", native.ExitCode, native.Errno)
	}
	return "the call failed before a native result"
}

func rgwSyncStatsGateway(parent context.Context, gateway *rgw.Gateway) string {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer cancel()
	state, err := gateway.State(ctx)
	switch {
	case err != nil:
		return fmt.Sprintf("gateway %s: state unavailable: %v", gateway.GetContainerID(), err)
	case state == nil:
		return fmt.Sprintf("gateway %s: state unavailable: no state", gateway.GetContainerID())
	}
	return fmt.Sprintf("gateway %s: status=%s exit=%d oom=%t started=%s finished=%s", gateway.GetContainerID(), state.Status, state.ExitCode, state.OOMKilled, state.StartedAt, state.FinishedAt)
}

func rgwSyncStatsOSDs(parent context.Context, cluster *ceph.Container) string {
	if cluster == nil {
		return "OSD map: not read"
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 15*time.Second)
	defer cancel()
	data, err := cluster.Ceph(ctx, "osd", "stat", "--format", "json")
	if err != nil {
		return "OSD map: unavailable: " + err.Error()
	}
	text := strings.TrimSpace(string(data))
	if len(text) > 512 {
		text = text[:512] + " [truncated]"
	}
	return "OSD map: " + text
}

// Explicit synchronization prepares deterministic native accounting for the
// fixture assertion. User.Usage itself performs only read-only stats queries.
func rgwFixtureUserUsage(t *testing.T, ctx context.Context, gateway *rgw.Gateway, user *rgw.User, stage string, objects, sizeBytes uint64) {
	t.Helper()
	rgwSyncStats(t, ctx, nil, gateway, "user", "stats", "--uid", user.ID(), "--sync-stats")
	usage, err := user.Usage(ctx)
	if err != nil {
		t.Fatalf("native user storage read %s: %v", stage, err)
	}
	result, err := expect.UserUsage{UserID: user.ID(), Scope: expect.UsageScopeUser, Objects: objects, SizeBytes: sizeBytes}.Evaluate(usage)
	if err != nil || !result.Matched {
		t.Fatalf("native user storage assertion %s: %v; usage=%+v error=%v", stage, result, usage, err)
	}
	t.Logf("RGW_USER_USAGE stage=%s scope=%s owner_id=%q user_id=%q num_objects=%d size_bytes=%d size_actual_bytes=%d", stage, usage.Scope, usage.OwnerID, usage.UserID, usage.NumObjects, usage.SizeBytes, usage.SizeActualBytes)
}
