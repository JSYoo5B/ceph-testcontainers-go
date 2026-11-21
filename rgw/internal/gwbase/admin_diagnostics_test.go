package gwbase

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

func TestDiagnosticArgv(t *testing.T) {
	got := diagnosticArgv(AdminArgv(Scope{}, "--format", "json", "bucket", "stats"))
	want := []string{"radosgw-admin", "--admin-socket=/tmp/tc-supervised/radosgw-admin.$pid.asok", "--debug-objecter=0/20", "--log-max-recent=10000",
		"--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json", "bucket", "stats"}
	if !slices.Equal(got, want) {
		t.Fatalf("radosgw-admin command line: %q", got)
	}
	// The supervisor asks the socket that the command opens.
	if adminSocketPath != "/tmp/tc-supervised/radosgw-admin.{pid}.asok" {
		t.Fatalf("the supervisor's socket path: %s", adminSocketPath)
	}
	if other := []string{"sh", "-c", "true"}; !slices.Equal(diagnosticArgv(other), other) || diagnosticArgv(nil) != nil {
		t.Fatal("another command line was changed")
	}
}

// objecterAnswer is the answer of a radosgw-admin of the Umbrella 21.2.0 rgw
// role image to objecter_requests while its first read waited for a paused
// OSD, with a second request, a watch and a pool operation added.
const objecterAnswer = `{
    "ops": [
        {
            "tid": 1,
            "pg": "2.85fca992",
            "osd": 0,
            "object_id": "default.realm",
            "object_locator": "@2",
            "target_object_id": "default.realm",
            "target_object_locator": "@2",
            "paused": 0,
            "used_replica": 0,
            "precalc_pgid": 0,
            "last_sent": "161.390922s",
            "age": 6.3310000029999998,
            "attempts": 1,
            "snapid": "head",
            "snap_context": "0=[]",
            "mtime": "1970-01-01T00:00:00.000000+0000",
            "osd_ops": [
                "read 0~0"
            ]
        },
        {
            "tid": 7,
            "pg": "5.aaabba1b",
            "osd": -1,
            "object_id": "PRIVATEACCESSKEY",
            "object_locator": "@5;users.keys",
            "paused": 1,
            "age": 104.96,
            "attempts": 3,
            "osd_ops": ["stat", "read 0~0", "call a.b", "getxattrs", "fifth", "sixth"]
        }
    ],
    "linger_ops": [{"linger_id": 2, "object_id": "notify.0"}],
    "pool_ops": [{"tid": 9, "pool": 6, "pool_name": "default.rgw.buckets.index", "operation_type": "create", "last_sent": "1s"}],
    "pool_stat_ops": [],
    "statfs_ops": [],
    "command_ops": []
}`

func TestWaitReport(t *testing.T) {
	const want = "it waited for 2 RADOS object requests: tid 1 [read 0~0] of default.realm in @2, pg 2.85fca992, on osd.0, sent 1 times, 6.3s old; " +
		"tid 7 [stat,read 0~0,call a.b,getxattrs,...] of a key object in @5;users.keys, pg 5.aaabba1b, on no OSD, paused, sent 3 times, 105.0s old, " +
		"and had pool operation tid 9 create of pool 6, 1 watches"
	if got := waitReport([]byte(objecterAnswer), false); got != want {
		t.Fatalf("wait report:\n got %q\nwant %q", got, want)
	}
	many := `{"ops":[` + strings.TrimSuffix(strings.Repeat(`{"tid":3,"object_id":"`+strings.Repeat("o", 80)+`","osd":2},`, 6), ",") + `]}`
	got := waitReport([]byte(many), false)
	if strings.Count(got, "tid 3") != maxWaitRequests || !strings.Contains(got, "; 2 more") || !strings.Contains(got, " of "+strings.Repeat("o", maxWaitNameBytes)+"... in ") || len(got) > 1024 {
		t.Fatalf("a bounded wait report: %d bytes, %q", len(got), got)
	}
	for name, c := range map[string]struct {
		reply string
		cut   bool
		want  string
	}{
		"no answer":        {"", false, ""},
		"nothing waits":    {`{"ops":[],"linger_ops":[{},{}]}`, false, "it had no RADOS object request outstanding, and had 2 watches"},
		"an answer cut":    {objecterAnswer, true, "its answer about its RADOS requests could not be read"},
		"not the document": {"ERROR: secret PRIVATE", false, "its answer about its RADOS requests could not be read"},
		"unprintable name": {`{"ops":[{"tid":1,"object_id":"a\nb","osd_ops":["x\ty"]}]}`, false,
			"it waited for 1 RADOS object requests: tid 1 [x?y] of a?b in , pg , on osd.0, sent 0 times, 0.0s old"},
	} {
		if got := waitReport([]byte(c.reply), c.cut); got != c.want {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
}

// eventsStderr is standard error of a radosgw-admin of the Umbrella 21.2.0
// rgw role image that the supervisor stopped while its first read waited
// for a paused OSD, shortened: the handler's report, then its dump of recent
// events, twice. A line of another subsystem with a made-up secret, an
// event of a key object and two more ticks are added.
const eventsStderr = `*** Caught signal (Aborted) **
 in thread ffff91307040 thread_name:radosgw-admin
 1: main()
--- begin dump of recent events ---
   -91> 2026-11-21T12:57:48.233+0000 ffff91307040  1 librados: starting objecter
   -90> 2026-11-21T12:57:48.233+0000 ffff91307040  5 asok(0xaaaaf07519b0) register_command objecter_requests hook 0xaaaaf08f31c0
   -75> 2026-11-21T12:57:48.233+0000 ffff91307040 10 monclient: dump:
epoch 1
fsid 49443109-0dc8-4c7e-8ffd-8e9ceebf2751
   -60> 2026-11-21T12:57:48.233+0000 ffff91307040 20 rgw: signing with secret AQBsecretsecretsecretsecretsecret==
   -50> 2026-11-21T12:57:48.234+0000 ffff6d7a8c40 10 client.?.objecter ms_handle_connect 0xaaaaf0900890
   -49> 2026-11-21T12:57:48.234+0000 ffff6d7a8c40 10 client.?.objecter resend_mon_ops
   -35> 2026-11-21T12:57:48.234+0000 ffff91307040 10 client.4440.objecter _maybe_request_map subscribing (onetime) to next osd map
   -28> 2026-11-21T12:57:48.234+0000 ffff6d7a8c40 10 client.4440.objecter ms_dispatch2 0xaaaaf07519b0 osd_map(16..16 src has 1..16)
   -27> 2026-11-21T12:57:48.234+0000 ffff6d7a8c40  3 client.4440.objecter handle_osd_map got epochs [16,16] > 0
   -26> 2026-11-21T12:57:48.234+0000 ffff6d7a8c40  3 client.4440.objecter handle_osd_map decoding full epoch 16
   -25> 2026-11-21T12:57:48.234+0000 ffff6d7a8c40 20 client.4440.objecter dump_active .. 0 homeless
   -24> 2026-11-21T12:57:48.234+0000 ffff91307040 20 client.4440.objecter _calc_target epoch 16 base PRIVATEACCESSKEY @5;users.keys precalc_pgid 0 pgid 0.0 is_read
   -22> 2026-11-21T12:57:48.234+0000 ffff91307040 10 client.4440.objecter _op_submit op 0xaaaaf092ee50
   -21> 2026-11-21T12:57:48.234+0000 ffff91307040 20 client.4440.objecter _calc_target epoch 16 base default.realm @2 precalc_pgid 0 pgid 0.0 is_read
   -20> 2026-11-21T12:57:48.234+0000 ffff91307040 20 client.4440.objecter _calc_target target default.realm @2 -> pgid 2.85fca992
   -19> 2026-11-21T12:57:48.234+0000 ffff91307040 10 client.4440.objecter _calc_target  raw pgid 2.85fca992 -> actual 2.2 acting [0] primary 0
   -18> 2026-11-21T12:57:48.234+0000 ffff91307040 20 client.4440.objecter _get_session s=0xaaaaf06ed7d0 osd=0 3
   -17> 2026-11-21T12:57:48.234+0000 ffff91307040 10 client.4440.objecter _op_submit oid default.realm '@2' '@2' [read 0~0] tid 1 osd.0
   -16> 2026-11-21T12:57:48.234+0000 ffff91307040 20 client.4440.objecter get_session s=0xaaaaf06ed7d0 osd=0 3
   -15> 2026-11-21T12:57:48.234+0000 ffff91307040 15 client.4440.objecter _session_op_assign 0 1
   -14> 2026-11-21T12:57:48.234+0000 ffff91307040 15 client.4440.objecter _send_op 1 to 2.2 on osd.0
   -13> 2026-11-21T12:57:48.234+0000 ffff91307040 20 client.4440.objecter put_session s=0xaaaaf06ed7d0 osd=0 4
   -12> 2026-11-21T12:57:48.234+0000 ffff91307040  5 client.4440.objecter 1 in flight
   -10> 2026-11-21T12:57:48.235+0000 ffff6d7a8c40 10 client.4440.objecter ms_handle_connect 0xffff68014df0
    -9> 2026-11-21T12:57:53.269+0000 ffff6dfb8c40 10 client.4440.objecter tick
    -8> 2026-11-21T12:57:53.269+0000 ffff6dfb8c40 20 client.4440.objecter tick: elapsed since last osdmap request 5.035s, stale_window 10s, epoch 17, monc->get_start(osdmap) 0
    -7> 2026-11-21T12:57:53.269+0000 ffff6dfb8c40  2 client.4440.objecter  tid 1 on osd.0 is laggy
    -6> 2026-11-21T12:57:58.270+0000 ffff6dfb8c40 10 client.4440.objecter tick
    -5> 2026-11-21T12:57:58.270+0000 ffff6dfb8c40 20 client.4440.objecter tick: elapsed since last osdmap request 10.036s, stale_window 10s, epoch 17, monc->get_start(osdmap) 0
    -4> 2026-11-21T12:57:58.270+0000 ffff6dfb8c40  2 client.4440.objecter  tid 1 on osd.0 is laggy
    -3> 2026-11-21T12:58:03.271+0000 ffff6dfb8c40 10 client.4440.objecter tick
    -2> 2026-11-21T12:58:03.271+0000 ffff6dfb8c40  2 client.4440.objecter  tid 1 on osd.0 is laggy
     0> 2026-11-21T12:58:03.917+0000 ffff91307040 -1 *** Caught signal (Aborted) **
 in thread ffff91307040 thread_name:radosgw-admin
--- logging levels ---
   0/20 objecter
  max_recent     10000
--- end dump of recent events ---
--- begin dump of recent events ---
    -1> 2026-11-21T12:58:03.271+0000 ffff6dfb8c40 10 client.4440.objecter only in the second dump
--- logging levels ---
--- end dump of recent events ---
`

func TestEventReport(t *testing.T) {
	const want = "the last 18 of the 18 different objecter events among its 33 recent events (28 of the objecter): " +
		"12:57:48.234 ms_handle_connect 0x (2 times until 12:57:48.235) | 12:57:48.234 resend_mon_ops | " +
		"12:57:48.234 _maybe_request_map subscribing (onetime) to next osd map | 12:57:48.234 ms_dispatch2 0x osd_map(16..16 src has 1..16) | " +
		"12:57:48.234 handle_osd_map got epochs [16,16] > 0 | 12:57:48.234 handle_osd_map decoding full epoch 16 | 12:57:48.234 dump_active .. 0 homeless | " +
		"12:57:48.234 an event of a key object | 12:57:48.234 _op_submit op 0x | " +
		"12:57:48.234 _calc_target epoch 16 base default.realm @2 precalc_pgid 0 pgid 0.0 is_read | 12:57:48.234 _calc_target target default.realm @2 -> pgid 2.85fca992 | " +
		"12:57:48.234 _calc_target  raw pgid 2.85fca992 -> actual 2.2 acting [0] primary 0 | 12:57:48.234 _op_submit oid default.realm '@2' '@2' [read 0~0] tid 1 osd.0 | " +
		"12:57:48.234 _send_op 1 to 2.2 on osd.0 | 12:57:48.234 1 in flight | 12:57:53.269 tick (3 times until 12:58:03.271) | " +
		"12:57:53.269 tick: elapsed since last osdmap request 10.036s, stale_window 10s, epoch 17, monc->get_start(osdmap) 0 (2 times until 12:57:58.270) | " +
		"12:57:53.269 tid 1 on osd.0 is laggy (3 times until 12:58:03.271)"
	got := eventReport([]byte(eventsStderr))
	if got != want {
		t.Fatalf("event report:\n got %q\nwant %q", got, want)
	}
	for _, leaked := range []string{"AQBsecret", "PRIVATEACCESSKEY", "0xaaaaf", "fsid", "session", "second dump", "librados"} {
		if strings.Contains(got, leaked) {
			t.Errorf("event report exposes %q: %s", leaked, got)
		}
	}
	// Many different events and a long one: the last maxEvents, each cut.
	var dump strings.Builder
	dump.WriteString(recentEventsBegin + "\n")
	for i := range 3 * maxEvents {
		fmt.Fprintf(&dump, "  -%d> 2026-11-21T12:57:48.234+0000 ffff91307040 10 client.4440.objecter request %d %s\n", 3*maxEvents-i, i, strings.Repeat("x", 2*maxEventBytes))
	}
	got = eventReport([]byte(dump.String()))
	if strings.Count(got, " | ") != maxEvents-1 || !strings.Contains(got, fmt.Sprintf("request %d ", 3*maxEvents-1)) || strings.Contains(got, fmt.Sprintf("request %d ", 2*maxEvents-1)) ||
		!strings.HasPrefix(got, fmt.Sprintf("the last %d of the %d different", maxEvents, 3*maxEvents)) || len(got) > maxEvents*(maxEventBytes+32)+256 {
		t.Fatalf("a bounded event report: %d bytes, %.300q", len(got), got)
	}
	for name, c := range map[string]struct{ stderr, want string }{
		"no dump":           {"ERROR: could not init bucket\n   -1> 2026-11-21T12:57:48.234+0000 ffff 10 client.1.objecter tick\n", ""},
		"no objecter event": {recentEventsBegin + "\n   -1> 2026-11-21T12:57:48.233+0000 ffff91307040 10 monclient: tick\n", "none of its 1 recent events is of the objecter"},
	} {
		if got := eventReport([]byte(c.stderr)); got != c.want {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
}

// A command that crashed dumps its recent events as a stopped one does, and
// its error ends with the objecter's; one that only failed has none.
func TestAdminRunFailureEndsWithTheObjecterEvents(t *testing.T) {
	crashed := strings.Replace(eventsStderr, "(Aborted)", "(Segmentation fault)", 1)
	err := adminRunFailure(supervised.Report{ExitCode: 139, Stderr: []byte(crashed)})
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), "radosgw-admin reported fatal signal Segmentation fault in thread radosgw-admin, backtrace main; "+
		"it wrote no standard output; the last 18 of the 18 different objecter events among its 33 recent events") ||
		!strings.HasSuffix(err.Error(), "tid 1 on osd.0 is laggy (3 times until 12:58:03.271); native output is redacted") || strings.Contains(err.Error(), "AQBsecret") {
		t.Fatalf("a crashed radosgw-admin: %v", err)
	}
	_, dump, _ := strings.Cut(eventsStderr, recentEventsBegin)
	if err := adminRunFailure(supervised.Report{ExitCode: 22, Stderr: []byte(recentEventsBegin + dump)}); err == nil || strings.Contains(err.Error(), "objecter") {
		t.Fatalf("a failed radosgw-admin without a signal report: %v", err)
	}
}
