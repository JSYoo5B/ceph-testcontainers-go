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
	// The supervisor is asked to stop the command with SIGABRT at its limit.
	if len(ctr.signals) != 1 || ctr.signals[0] != "6" {
		t.Fatalf("deadline signals: %v", ctr.signals)
	}
	// A command that also dumped its recent events: after the frames the
	// error says which request of the stopped thread has no reply, what the
	// thread's objecter did last and what the other threads logged for that
	// request, without the other events.
	const events = "--- begin dump of recent events ---\n" +
		"   -4> 2026-11-21T12:57:48.233+0000 7f0e5c0d5980 20 rgw: signing with secret PRIVATE\n" +
		"   -3> 2026-11-21T12:57:48.234+0000 7f0e5c0d5980 10 client.4440.objecter _op_submit oid .dir.b.1 '@6' '@6' [call rgw.bucket_list] tid 7 osd.0\n" +
		"   -2> 2026-11-21T12:57:48.234+0000 7f0e5c0d5980 15 client.4440.objecter _send_op 7 to 6.2 on osd.0\n" +
		"   -1> 2026-11-21T12:57:58.269+0000 ffff6dfb8c40  2 client.4440.objecter  tid 7 on osd.0 is laggy\n--- logging levels ---\n"
	gateway, _ = newRGWAdminTestGateway(Config{}, rgwAdminResponse{code: 134, stopped: true, data: stopped + events})
	_, err = gateway.Admin(ctx, "bucket", "stats", "--bucket", "b")
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.HasSuffix(err.Error(), "RGWSI_BucketIndex_RADOS::cls_bucket_head+0x2e1 < main; "+
		"of its 4 recent events, 3 of the objecter; through client.4440 the signalled thread submitted 1 requests in them, 1 without a reply: "+
		"tid 7 [call rgw.bucket_list] of .dir.b.1 in @6 to pg 6.2 on osd.0 at 12:57:48.234; "+
		"its last 2 events: 12:57:48.234 _op_submit oid .dir.b.1 '@6' '@6' [call rgw.bucket_list] tid 7 osd.0 | 12:57:48.234 _send_op 7 to 6.2 on osd.0; "+
		"then of other threads, for these requests and OSD maps, the first 1 of 1: 12:57:58.269 tid 7 on osd.0 is laggy") ||
		strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("a stopped radosgw-admin with events: %v", err)
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
