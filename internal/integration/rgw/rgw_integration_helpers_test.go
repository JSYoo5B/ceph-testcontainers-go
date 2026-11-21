//go:build all || integration

package rgw_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/expect"
)

// rgwAdminStepBound bounds one radosgw-admin command of a scenario whose
// whole run, cluster start included, stays under this bound in CI: there a
// command still running after it hangs. A scenario that runs longer than the
// bound as a whole says nothing about its commands, so its commands, such as
// gc process and period update, keep the test's context. The bound is its
// own phase inside the test's context: a command that hangs fails at it,
// with the main thread's frames that Gateway.Admin reports for a command its
// supervisor stopped, and leaves the test's remaining time for the failure's
// evidence. Under the test's whole context the first such command would use
// up the budget of every later step.
const rgwAdminStepBound = 2 * time.Minute

// rgwAdminStep runs one native command through gateway.Admin under
// rgwAdminStepBound and returns its result unchanged, for a caller that
// examines the error. It never repeats the command.
func rgwAdminStep(ctx context.Context, gateway *rgw.Gateway, args ...string) ([]byte, error) {
	phase, cancel := context.WithTimeout(ctx, rgwAdminStepBound)
	defer cancel()
	return gateway.Admin(phase, args...)
}

// rgwRequireAdminStep runs one native command as rgwAdminStep does and fails
// the test when it fails. It never repeats the command: a stopped or failed
// mutation may have changed native state, and its outcome is unknown. A
// failure names how the command ended, its phase deadline and elapsed time,
// and the gateway container's generation and state and the cluster's OSD
// map summary, pools and placement group states, read once under a short
// bound after the failure.
func rgwRequireAdminStep(t *testing.T, ctx context.Context, cluster *ceph.Container, gateway *rgw.Gateway, args ...string) []byte {
	t.Helper()
	phase, cancel := context.WithTimeout(ctx, rgwAdminStepBound)
	defer cancel()
	deadline, _ := phase.Deadline()
	started := time.Now()
	data, err := gateway.Admin(phase, args...)
	if err == nil {
		return data
	}
	elapsed := time.Since(started)
	end := rgwAdminStepEnd(err, ctx.Err(), phase.Err())
	t.Fatalf("native %s: %s after %s (phase bound %s, deadline %s); %s; %s; error: %v", strings.Join(args, " "), end,
		elapsed.Round(time.Millisecond), rgwAdminStepBound, deadline.UTC().Format(time.RFC3339Nano),
		rgwSyncStatsGateway(ctx, gateway), rgwSyncStatsOSDs(ctx, cluster), err)
	return nil
}

// rgwSyncStats runs one native stats synchronization, user or account stats
// with --sync-stats, as rgwRequireAdminStep runs any command.
func rgwSyncStats(t *testing.T, ctx context.Context, cluster *ceph.Container, gateway *rgw.Gateway, args ...string) {
	t.Helper()
	rgwRequireAdminStep(t, ctx, cluster, gateway, args...)
}

// rgwAdminStepEnd classifies how a failed command ended: by the
// supervisor at its own limit, by the phase deadline, by the test's context,
// with a stop that could not be confirmed, or by a native exit.
func rgwAdminStepEnd(err, testErr, phaseErr error) string {
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
	return "OSD map: " + text + "; " + rgwSyncStatsPools(ctx, cluster)
}

// rgwSyncStatsPools names each pool with its ID, placement group count and
// the OSD map epoch of its last change, and the placement group states. A
// stopped command's error names the pool and placement group of the request
// it waited for; this says what state they were in right after.
func rgwSyncStatsPools(ctx context.Context, cluster *ceph.Container) string {
	data, err := cluster.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
	var pools []struct {
		ID      int64  `json:"pool_id"`
		Name    string `json:"pool_name"`
		PGs     int    `json:"pg_num"`
		Changed any    `json:"last_change"`
	}
	if err == nil {
		err = json.Unmarshal(data, &pools)
	}
	if err != nil {
		return "pools: unavailable: " + err.Error()
	}
	names := make([]string, 0, len(pools))
	for _, pool := range pools {
		names = append(names, fmt.Sprintf("%d %s (%d PGs, changed e%v)", pool.ID, pool.Name, pool.PGs, pool.Changed))
	}
	text := "pools: " + strings.Join(names, ", ")
	if len(text) > 1024 {
		text = text[:1024] + " [truncated]"
	}
	states, err := cluster.Ceph(ctx, "pg", "stat")
	if err != nil {
		return text + "; PGs: unavailable: " + err.Error()
	}
	summary := strings.TrimSpace(string(states))
	if len(summary) > 256 {
		summary = summary[:256] + " [truncated]"
	}
	return text + "; PGs: " + summary
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
