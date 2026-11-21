package rgw

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// Admin's budget is its caller's: the cluster's startup timeout bounds the
// typed operations, not a native command such as user stats --sync-stats.
func TestRGWAdminBudgetIsTheCallersContext(t *testing.T) {
	gateway, ctr := newRGWAdminTestGateway(Config{}, rgwAdminResponse{data: "{}"}, rgwAdminResponse{data: "{}"})
	testhelper.TestSetStartupTimeout(gateway.base().Cluster(), 20*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	if _, err := gateway.Admin(ctx, "user", "stats", "--uid", "u", "--sync-stats"); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Admin(context.Background(), "user", "list"); err != nil {
		t.Fatal(err)
	}
	if len(ctr.limits) != 2 {
		t.Fatalf("supervised calls: %v", ctr.limits)
	}
	// 8 minutes less the 15 s stop margin, not the 20 s startup timeout.
	if limit, err := strconv.Atoi(ctr.limits[0]); err != nil || limit < 460 || limit > 465 {
		t.Fatalf("an 8-minute caller deadline gave the command %s s", ctr.limits[0])
	}
	if ctr.limits[1] != "inf" {
		t.Fatalf("a caller without a deadline gave the command %s s", ctr.limits[1])
	}
}

func TestRGWAdminReportsAStoppedCommand(t *testing.T) {
	gateway, _ := newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 143, stopped: true})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	_, err := gateway.Admin(ctx, "user", "stats", "--uid", "u", "--sync-stats")
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), "still ran after") {
		t.Fatalf("a stopped radosgw-admin: %v", err)
	}
	// A command that reported where it was when the supervisor stopped it:
	// the error names the thread and its frames and nothing else of the
	// native output.
	const stopped = "ERROR: secret PRIVATE\n*** Caught signal (Aborted) **\n in thread 7f0e5c0d5980 thread_name:radosgw-admin\n" +
		" 1: /lib64/libc.so.6(+0x8b3ba) [0x7f0e5d08b3ba]\n 2: pthread_cond_wait()\n" +
		" 3: (RGWSI_BucketIndex_RADOS::cls_bucket_head(DoutPrefixProvider const*, RGWBucketInfo const&, int, optional_yield)+0x2e1) [0x5560f1d07e41]\n 4: main()\n"
	gateway, ctr := newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 134, stopped: true, data: stopped})
	_, err = gateway.Admin(ctx, "bucket", "stats", "--bucket", "b")
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.HasSuffix(err.Error(), "s and was stopped; when it was stopped, thread radosgw-admin was in "+
		"libc.so.6(+0x8b3ba) < pthread_cond_wait < RGWSI_BucketIndex_RADOS::cls_bucket_head+0x2e1 < main") {
		t.Fatalf("a stopped radosgw-admin with a report: %v", err)
	}
	for _, leaked := range []string{"PRIVATE", "0x7f0e5d08b3ba", "/lib64", "RGWBucketInfo"} {
		if strings.Contains(err.Error(), leaked) {
			t.Fatalf("the stop report exposes %q: %v", leaked, err)
		}
	}
	// The supervisor is asked to stop the command with SIGABRT at its limit,
	// and to ask the command's admin socket for its RADOS requests first.
	if len(ctr.signals) != 1 || ctr.signals[0] != "6" || ctr.queries[0] != `/tmp/tc-supervised/radosgw-admin.{pid}.asok {"prefix":"objecter_requests"}` {
		t.Fatalf("deadline signals: %v, questions: %v", ctr.signals, ctr.queries)
	}
	// A command that also answered the question and dumped its recent
	// events: the error says what it waited for and what its objecter did,
	// in that order after the frames, without the other events.
	const events = "--- begin dump of recent events ---\n" +
		"   -3> 2026-11-21T12:57:48.233+0000 ffff91307040 20 rgw: signing with secret PRIVATE\n" +
		"   -2> 2026-11-21T12:57:48.234+0000 ffff91307040 15 client.4440.objecter _send_op 1 to 2.2 on osd.0\n" +
		"   -1> 2026-11-21T12:57:53.269+0000 ffff6dfb8c40  2 client.4440.objecter  tid 1 on osd.0 is laggy\n--- logging levels ---\n"
	gateway, _ = newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 134, stopped: true, data: stopped + events,
		reply: `{"ops":[{"tid":1,"pg":"2.85fca992","osd":0,"object_id":"default.realm","object_locator":"@2","age":104.5,"attempts":1,"osd_ops":["read 0~0"]}]}`})
	_, err = gateway.Admin(ctx, "bucket", "stats", "--bucket", "b")
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.HasSuffix(err.Error(), "RGWSI_BucketIndex_RADOS::cls_bucket_head+0x2e1 < main; "+
		"it waited for 1 RADOS object requests: tid 1 [read 0~0] of default.realm in @2, pg 2.85fca992, on osd.0, sent 1 times, 104.5s old; "+
		"the last 2 of the 2 different objecter events among its 3 recent events (2 of the objecter): 12:57:48.234 _send_op 1 to 2.2 on osd.0 | 12:57:53.269 tid 1 on osd.0 is laggy") ||
		strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("a stopped radosgw-admin with an answer and events: %v", err)
	}
	// Another failure stays redacted.
	gateway, _ = newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 22, data: "Error: invalid secret PRIVATE"})
	if _, err := gateway.Admin(ctx, "user", "list"); err == nil || strings.Contains(err.Error(), "still ran after") || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("an ordinary failure: %v", err)
	}
	// A gateway image without radosgw-admin lacks a capability.
	gateway, _ = newRGWAdminTestGateway(Config{}, rgwAdminResponse{launchExit: supervised.LaunchFailedExit})
	if _, err := gateway.Admin(ctx, "user", "list"); !errors.Is(err, ceph.ErrImageCapability) {
		t.Fatalf("a missing radosgw-admin: %v", err)
	}
}
