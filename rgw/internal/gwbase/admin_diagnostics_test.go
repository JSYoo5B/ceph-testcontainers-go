package gwbase

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

func TestDiagnosticArgv(t *testing.T) {
	got := diagnosticArgv(AdminArgv(Scope{}, "--format", "json", "bucket", "stats"))
	want := []string{"radosgw-admin", "--debug-objecter=0/20", "--log-max-recent=10000",
		"--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json", "bucket", "stats"}
	if !slices.Equal(got, want) {
		t.Fatalf("radosgw-admin command line: %q", got)
	}
	if other := []string{"sh", "-c", "true"}; !slices.Equal(diagnosticArgv(other), other) || diagnosticArgv(nil) != nil {
		t.Fatal("another command line was changed")
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
	const want = "of its 33 recent events, 28 of the objecter; through client.4440 the signalled thread submitted 1 requests in them, 1 without a reply: " +
		"tid 1 [read 0~0] of default.realm in @2 to pg 2.2 on osd.0 at 12:57:48.234; its last 6 events: " +
		"12:57:48.234 _maybe_request_map subscribing (onetime) to next osd map | 12:57:48.234 an event of a key object | " +
		"12:57:48.234 _calc_target epoch 16 base default.realm @2 precalc_pgid 0 pgid 0.0 is_read | " +
		"12:57:48.234 _calc_target  raw pgid 2.85fca992 -> actual 2.2 acting [0] primary 0 | 12:57:48.234 _op_submit oid default.realm '@2' '@2' [read 0~0] tid 1 osd.0 | " +
		"12:57:48.234 _send_op 1 to 2.2 on osd.0; then of other threads, for these requests and OSD maps, the first 2 of 2: " +
		"12:57:48.234 handle_osd_map got epochs [16,16] > 0 | 12:57:53.269 tid 1 on osd.0 is laggy (3 times until 12:58:03.271)"
	got := eventReportOf([]byte(eventsStderr))
	if got != want {
		t.Fatalf("event report:\n got %q\nwant %q", got, want)
	}
	for _, leaked := range []string{"AQBsecret", "PRIVATEACCESSKEY", "0xaaaaf", "fsid", "session", "second dump", "librados"} {
		if strings.Contains(got, leaked) {
			t.Errorf("event report exposes %q: %s", leaked, got)
		}
	}
}

// A wait under which the command's watches are pinged and OSD maps arrive,
// in a command with two RADOS clients whose transaction IDs overlap: the
// requests of the stopped thread that have no reply are named, whatever the
// other threads and the other client logged since, and more than fits is
// cut.
func TestEventReportFindsTheRequestsWithoutAReply(t *testing.T) {
	const stopped, messenger, timer = "ffff91307040", "ffff6d7a8c40", "ffff6dfb8c40"
	var dump strings.Builder
	dump.WriteString(" in thread " + stopped + " thread_name:radosgw-admin\n" + recentEventsBegin + "\n")
	count := 0
	event := func(thread, client, format string, args ...any) {
		count++
		fmt.Fprintf(&dump, "  -1> 2026-11-21T12:57:%02d.234+0000 %s 10 client.%s.objecter %s\n", count/100, thread, client, fmt.Sprintf(format, args...))
	}
	// The other client used the same transaction IDs and got its replies.
	const requests, answered = 3 * maxThreadEvents, 20
	for i := range requests {
		event(stopped, "4439", "_op_submit oid config.%d '@2' '@2' [read 0~0] tid %d osd.0", i, 100+i)
		event(messenger, "4439", "_finish_op %d", 100+i)
	}
	// A watch of the bucket client, which never ends, by another thread.
	event(timer, "4440", "_op_submit oid notify.1 '@4' '@4' [watch ping cookie 1] tid 50 osd.0")
	long := strings.Repeat("x", 2*maxEventBytes)
	for i := range requests {
		locator := "@60"
		if i == requests-1 {
			locator = "@5;users.keys"
		}
		event(stopped, "4440", "_calc_target epoch 91 base .dir.bucket.%d %s precalc_pgid 0 pgid 0.0 is_read", i, locator)
		event(stopped, "4440", "_op_submit oid .dir.bucket.%d%s '%s' '%s' [call rgw.bucket_list] tid %d osd.0", i, long, locator, locator, 100+i)
		event(stopped, "4440", "_send_op %d to 60.%d on osd.0", 100+i, i%8)
	}
	for i := range answered {
		event(messenger, "4440", "ms_dispatch2 0xaaaaf07519b0 osd_op_reply(%d .dir.bucket [call] v0'0 uv1 ondisk = 0)", 100+i)
		event(messenger, "4440", "handle_osd_op_reply completed tid %d", 100+i)
		event(messenger, "4440", "_finish_op %d", 100+i)
	}
	for i := range 3 * maxOtherEvents {
		event(timer, "4440", "_send_op %d to 4.4 on osd.0", 1000+i)
		event(messenger, "4440", "ms_dispatch2 0xaaaaf07519b0 osd_op_reply(%d notify.1 [watch ping cookie 1] v0'0 uv1 ondisk = 0)", 1000+i)
		event(messenger, "4440", "handle_osd_map got epochs [%d,%d] > %d", 200+i, 200+i, 199+i)
		event(timer, "4440", " tid %d on osd.0 is laggy", 100+requests-1)
	}
	got := eventReportOf([]byte(dump.String()))
	open := requests - answered
	if !strings.HasPrefix(got, fmt.Sprintf("of its %d recent events, %d of the objecter; through client.4440 the signalled thread submitted %d requests in them, %d without a reply: "+
		"tid %d [call rgw.bucket_list] of .dir.bucket.%d%s... in @60 to pg 60.%d on osd.0 at 12:57:", count, count, requests, open, 100+answered, answered, strings.Repeat("x", maxNameBytes-len(".dir.bucket.20")), answered%8)) ||
		strings.Count(got, " [call rgw.bucket_list] of ") != maxOpenRequests || !strings.Contains(got, fmt.Sprintf("; %d more (other threads: 1 without a reply); its last %d events: ", open-maxOpenRequests, maxThreadEvents-1)) {
		t.Fatalf("the requests without a reply: %q", got)
	}
	_, events, _ := strings.Cut(got, "; its last ")
	own, others, _ := strings.Cut(events, "; then of other threads, for these requests and OSD maps, ")
	// The last request is of a key object: its two events that name it
	// are one replaced entry.
	if strings.Count(own, " | ") != maxThreadEvents-2 || !strings.Contains(own, " an event of a key object (2 times until 12:57:") ||
		!strings.HasSuffix(own, fmt.Sprintf(" _send_op %d to 60.%d on osd.0", 100+requests-1, (requests-1)%8)) || strings.Contains(own, "config.") {
		t.Fatalf("the stopped thread's last events: %.3000q", own)
	}
	if !strings.HasPrefix(others, fmt.Sprintf("the first %d of %d: ", maxOtherEvents, 1+3*maxOtherEvents)) || strings.Count(others, " | ") != maxOtherEvents-1 ||
		!strings.Contains(others, fmt.Sprintf(" tid %d on osd.0 is laggy (%d times until ", 100+requests-1, 3*maxOtherEvents)) || !strings.Contains(others, " handle_osd_map got epochs [200,200] > 199 | ") {
		t.Fatalf("the other threads' events: %.3000q", others)
	}
	for _, absent := range []string{"watch ping", "_send_op 1000", "completed tid", "PRIVATE", "config.", "users.keys:"} {
		if strings.Contains(got, absent) {
			t.Errorf("the report holds %q", absent)
		}
	}
	if limit := maxOpenRequests*(3*maxNameBytes+96) + (maxThreadEvents+maxOtherEvents)*(maxEventBytes+48) + 512; len(got) > limit {
		t.Fatalf("the report takes %d bytes, over %d", len(got), limit)
	}
}

// eventDump returns standard error with a dump of the objecter events of
// client.4440, each given as its thread and message. "main" took the signal.
func eventDump(events ...[2]string) []byte {
	threads := map[string]string{"main": "ffff91307040", "messenger": "ffff6d7a8c40"}
	var dump strings.Builder
	dump.WriteString(" in thread " + threads["main"] + " thread_name:radosgw-admin\n" + recentEventsBegin + "\n")
	for i, e := range events {
		fmt.Fprintf(&dump, "  -1> 2026-11-21T12:57:%02d.234+0000 %s 10 client.4440.objecter %s\n", i, threads[e[0]], e[1])
	}
	return []byte(dump.String())
}

func TestEventReportWithholdsAKeyObjectInEveryEvent(t *testing.T) {
	const reply = "ms_dispatch2 0x123 osd_op_reply(%d PRIVATEACCESSKEY [read 0~0] v0'0 uv0 ondisk = 0)"
	cases := map[string][][2]string{
		// The reply names the object without its namespace.
		"a completed request": {
			{"main", "_op_submit oid PRIVATEACCESSKEY '@5;users.keys' '@5;users.keys' [read 0~0] tid 1 osd.0"},
			{"main", "_send_op 1 to 5.1 on osd.0"},
			{"messenger", fmt.Sprintf(reply, 1)},
			{"messenger", "_finish_op 1"},
		},
		// Another request of the same object, whose reply names another
		// transaction than the one the thread submitted.
		"the same name under another transaction": {
			{"main", "_op_submit oid PRIVATEACCESSKEY '@5;users.swift' '@5;users.swift' [read 0~0] tid 1 osd.0"},
			{"main", "_op_submit oid other '@2' '@2' [read 0~0] tid 2 osd.0"},
			{"main", "laggy tid 2 for PRIVATEACCESSKEY"},
			{"messenger", "tid 2 moved, formerly PRIVATEACCESSKEY"},
		},
		// The submission is older than the events.
		"a reply without its submission": {
			{"main", "_op_submit oid other '@2' '@2' [read 0~0] tid 2 osd.0"},
			{"main", fmt.Sprintf(reply, 1)},
			{"messenger", fmt.Sprintf(reply, 2)[:len("ms_dispatch2 0x123 osd_op_reply(2")] + " other [read 0~0] v0'0 uv0 ondisk = 0)"},
			{"messenger", strings.Replace(fmt.Sprintf(reply, 1), "(1 ", "(", 1)},
		},
		// The objecter's list of its active requests, logged with every OSD
		// map: a row has no namespace either.
		"an active request": {
			{"main", "_op_submit oid PRIVATEACCESSKEY '@5;users.keys' '@5;users.keys' [read 0~0] tid 1 osd.0"},
			{"main", "dump_active .. 0 homeless"},
			{"main", "1\t5.1\tosd.0\tPRIVATEACCESSKEY\t[read 0~0]"},
		},
		"an active request without its submission": {
			{"main", "dump_active .. 0 homeless"},
			{"main", "1\t5.1\tosd.0\tPRIVATEACCESSKEY\t[read 0~0]"},
		},
		// Without an event of the signalled thread the events of all
		// threads are shown.
		"an active request without its submission, of other threads": {
			{"messenger", "dump_active .. 0 homeless"},
			{"messenger", "1\t5.1\tosd.0\tPRIVATEACCESSKEY\t[read 0~0]"},
			{"messenger", "tick"},
		},
		// Shapes that this package does not know: a backoff with its object,
		// a listing with its entries, a redirect, any other message.
		"other shapes": {
			{"main", "_op_submit oid other '@2' '@2' [read 0~0] tid 2 osd.0"},
			{"main", "_send_op backoff 5.1 id 3 on 5:1:users.key:PRIVATEACCESSKEY:head, queuing 0x tid 2"},
			{"main", "_nlist_reply response.entries.size 1, response.entries [PRIVATEACCESSKEY], handle 5:1, tentative new pos MIN"},
			{"main", "handle_osd_op_reply redirect for PRIVATEACCESSKEY previous=1 new=2"},
			{"messenger", "ms_dispatch2 0x123 watch-notify(notify (1) cookie 2 notify 3 ret 0) PRIVATEACCESSKEY tid 2"},
			{"messenger", "_send_op 2 to 5.1 on osd.0 PRIVATEACCESSKEY"},
			{"messenger", " tid 2 on osd.0 is laggy PRIVATEACCESSKEY"},
		},
	}
	for name, events := range cases {
		got := eventReportOf(eventDump(events...))
		if got == "" || strings.Contains(got, "PRIVATE") {
			t.Errorf("%s: %q", name, got)
		}
	}
	// What is not a key object's stays: the reply to a request whose
	// submission is among the events.
	got := eventReportOf(eventDump(cases["a reply without its submission"]...))
	for _, want := range []string{"osd_op_reply(2 other [read 0~0]", "an event of request 1, which was submitted before these events"} {
		if !strings.Contains(got, want) {
			t.Errorf("no %q in %q", want, got)
		}
	}
	// An event that is withheld keeps its place.
	got = eventReportOf(eventDump(cases["an active request without its submission, of other threads"]...))
	if want := "the last 3 of 3 different ones: 12:57:00.234 dump_active .. 0 homeless | 12:57:01.234 an event of request 1, which was submitted before these events | 12:57:02.234 tick"; !strings.HasSuffix(got, want) {
		t.Errorf("no %q in %q", want, got)
	}
	got = eventReportOf(eventDump(cases["other shapes"]...))
	for _, want := range []string{"an event whose text may name an object (2 times until 12:57:02.234)", "an event whose text may name an object (3 times until 12:57:06.234)"} {
		if !strings.Contains(got, want) || strings.Contains(got, "laggy") || strings.Contains(got, "_send_op") {
			t.Errorf("no %q in %q", want, got)
		}
	}
}

func TestEventReportNamesTheLatestDestination(t *testing.T) {
	cases := map[string]struct {
		events [][2]string
		want   string
	}{
		"a resend to another OSD": {
			events: [][2]string{
				{"main", "_op_submit oid object '@5' '@5' [read 0~0] tid 1 osd.0"},
				{"main", "_send_op 1 to 5.1 on osd.0"},
				{"messenger", "_send_op 1 to 5.1 on osd.2"},
			},
			want: " in @5 to pg 5.1 on osd.2 at ",
		},
		"a request that had no OSD at first": {
			events: [][2]string{
				{"main", "_op_submit oid object '@5' '@5' [read 0~0] tid 1 osd.-1"},
				{"messenger", "_send_op 1 to 5.3 on osd.4"},
			},
			want: " in @5 to pg 5.3 on osd.4 at ",
		},
		"a request that has no OSD yet": {
			events: [][2]string{{"main", "_op_submit oid object '@5' '@5' [read 0~0] tid 1 osd.-1"}},
			want:   " in @5 to no OSD at ",
		},
	}
	for name, c := range cases {
		if got := eventReportOf(eventDump(c.events...)); !strings.Contains(got, c.want) {
			t.Errorf("%s: no %q in %q", name, c.want, got)
		}
	}
}

func TestEventReportWithoutTheStoppedThread(t *testing.T) {
	for name, c := range map[string]struct{ stderr, want string }{
		"no dump":           {"ERROR: could not init bucket\n   -1> 2026-11-21T12:57:48.234+0000 ffff 10 client.1.objecter tick\n", ""},
		"no objecter event": {recentEventsBegin + "\n   -1> 2026-11-21T12:57:48.233+0000 ffff91307040 10 monclient: tick\n", "none of its 1 recent events is of the objecter"},
		"no event of the thread": {" in thread aaaa thread_name:radosgw-admin\n" + recentEventsBegin + "\n   -3> 2026-11-21T12:57:48.233+0000 ffff91307040 10 client.1.objecter tick\n" +
			"   -2> 2026-11-21T12:57:50.233+0000 ffff91307040 20 client.1.objecter put_session s=0xaaaaf06ed7d0 osd=0 4\n" +
			"   -1> 2026-11-21T12:57:53.233+0000 ffff91307040 10 client.1.objecter tick\n",
			"of its 3 recent events, 3 of the objecter, none of the signalled thread, the last 1 of 1 different ones: 12:57:48.233 tick (2 times until 12:57:53.233)"},
		// Every request of the thread has its reply: it waits for something else.
		"nothing without a reply": {" in thread ffff91307040 thread_name:radosgw-admin\n" + recentEventsBegin + "\n" +
			"   -3> 2026-11-21T12:57:48.233+0000 ffff91307040 10 client.1.objecter _op_submit oid a '@2' '@2' [read 0~0] tid 1 osd.0\n" +
			"   -2> 2026-11-21T12:57:48.233+0000 ffff91307040 15 client.1.objecter _send_op 1 to 2.2 on osd.0\n" +
			"   -1> 2026-11-21T12:57:48.234+0000 ffff6d7a8c40 15 client.1.objecter _finish_op 1\n",
			"of its 3 recent events, 3 of the objecter; through client.1 the signalled thread submitted 1 requests in them, 0 without a reply; " +
				"its last 2 events: 12:57:48.233 _op_submit oid a '@2' '@2' [read 0~0] tid 1 osd.0 | 12:57:48.233 _send_op 1 to 2.2 on osd.0; then no event of another thread for these requests, and no OSD map"},
		// A request that has no target in the client's OSD map.
		"no OSD": {" in thread ffff91307040 thread_name:radosgw-admin\n" + recentEventsBegin + "\n" +
			"   -1> 2026-11-21T12:57:48.233+0000 ffff91307040 10 client.1.objecter _op_submit oid a '@9' '@9' [read 0~0] tid 1 osd.-1\n",
			"of its 1 recent events, 1 of the objecter; through client.1 the signalled thread submitted 1 requests in them, 1 without a reply: tid 1 [read 0~0] of a in @9 to no OSD at 12:57:48.233; " +
				"its last 1 events: 12:57:48.233 _op_submit oid a '@9' '@9' [read 0~0] tid 1 osd.-1; then no event of another thread for these requests, and no OSD map"},
	} {
		if got := eventReportOf([]byte(c.stderr)); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", name, got, c.want)
		}
	}
}

// A command that crashed dumps its recent events as a stopped one does, and
// its error ends with what the crashed thread had asked of RADOS; one that
// only failed has none.
func TestAdminRunFailureEndsWithTheObjecterEvents(t *testing.T) {
	crashed := strings.Replace(eventsStderr, "(Aborted)", "(Segmentation fault)", 1)
	err := adminRunFailure(supervised.Report{ExitCode: 139, Stderr: []byte(crashed)})
	if !errors.Is(err, ceph.ErrNativeFailure) || !strings.Contains(err.Error(), "radosgw-admin reported fatal signal Segmentation fault in thread radosgw-admin, backtrace main; "+
		"it wrote no standard output; of its 33 recent events, 28 of the objecter; through client.4440 the signalled thread submitted 1 requests in them, 1 without a reply: tid 1 ") ||
		!strings.HasSuffix(err.Error(), "tid 1 on osd.0 is laggy (3 times until 12:58:03.271); native output is redacted") || strings.Contains(err.Error(), "AQBsecret") {
		t.Fatalf("a crashed radosgw-admin: %v", err)
	}
	_, dump, _ := strings.Cut(eventsStderr, recentEventsBegin)
	if err := adminRunFailure(supervised.Report{ExitCode: 22, Stderr: []byte(recentEventsBegin + dump)}); err == nil || strings.Contains(err.Error(), "objecter") {
		t.Fatalf("a failed radosgw-admin without a signal report: %v", err)
	}
}

// A crash is no errno: the dump of a crashed command holds the replies to
// its reads of objects that do not exist, whose "(2) No such file or
// directory" would otherwise classify the crash as a missing object.
func TestAdminCrashHasNoErrno(t *testing.T) {
	const reply = "   -1> 2026-11-21T23:51:00.294+0000 ffff9a39ac40 10 client.71152.objecter ms_dispatch2 0xaaaaf6aa39b0 osd_op_reply(1 default.realm [read 0~0] v0'0 uv0 ondisk = -2 ((2) No such file or directory))\n"
	crashed := "*** Caught signal (Segmentation fault) **\n in thread ffff99b8ac40 thread_name:msgr-worker-1\n 1: main()\n" + recentEventsBegin + "\n" + reply
	crash := adminRunFailure(supervised.Report{ExitCode: 139, Stderr: []byte(crashed)})
	var failure *ceph.NativeFailure
	if !errors.As(crash, &failure) || failure.ExitCode != 139 || failure.Errno != "" || errors.Is(crash, fs.ErrNotExist) ||
		!strings.HasPrefix(crash.Error(), "radosgw-admin exited 139; radosgw-admin reported fatal signal Segmentation fault in thread msgr-worker-1") {
		t.Errorf("a crashed radosgw-admin: %v", crash)
	}
	// The same line as the last word of a command that exited by itself
	// stays its errno.
	err := adminRunFailure(supervised.Report{ExitCode: 2, Stderr: []byte(reply)})
	if !errors.Is(err, fs.ErrNotExist) || !strings.HasPrefix(err.Error(), "radosgw-admin exited 2 (ENOENT)") {
		t.Fatalf("a failed radosgw-admin: %v", err)
	}
}

// eventReportOf is eventReport for the thread that the first thread line of
// the fatal signal report in stderr names. Most inputs here hold that line
// without the report's header, which adminRunFailure would need.
func eventReportOf(stderr []byte) string {
	thread := ""
	for line := range strings.SplitSeq(string(stderr), "\n") {
		if match := fatalSignalThread.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			thread = match[1]
			break
		}
	}
	return eventReport(thread, stderr)
}

// TestSignalReportNamesOneThreadForBothReports gives the thread ID with and
// without "0x": the crash summary and the event report name the same thread,
// because both read it from the one parsed report header.
func TestSignalReportNamesOneThreadForBothReports(t *testing.T) {
	events := "  -2> 2026-11-21T12:57:00.234+0000 ffff91307040 10 client.4440.objecter _op_submit oid object '@5' '@5' [read 0~0] tid 1 osd.0\n" +
		"  -1> 2026-11-21T12:57:01.234+0000 ffff6d7a8c40 10 client.4440.objecter tick\n"
	for _, spelling := range []string{"ffff91307040", "0xffff91307040"} {
		stderr := "*** Caught signal (Segmentation fault) **\n in thread " + spelling + " thread_name:radosgw-admin\n" + recentEventsBegin + "\n" + events
		for name, err := range map[string]error{
			"crash": adminRunFailure(supervised.Report{ExitCode: 139, Stderr: []byte(stderr)}),
			"stop":  AdminReportFailure(supervised.Report{TimedOut: true, Stderr: []byte(strings.Replace(stderr, "Segmentation fault", "Aborted", 1))}, 5),
		} {
			text := err.Error()
			if name == "crash" && !strings.Contains(text, "fatal signal Segmentation fault in thread radosgw-admin") {
				t.Errorf("%s %s: the summary names no thread: %s", spelling, name, text)
			}
			if !strings.Contains(text, "through client.4440 the signalled thread submitted 1 requests in them, 1 without a reply") || strings.Contains(text, "none of the signalled thread") {
				t.Errorf("%s %s: the event report lost the thread: %s", spelling, name, text)
			}
		}
	}
}

// TestRedactionShowsOnlyEventsThatNameNoKeyObject is a table over the
// classifier alone: which event texts are shown and what replaces the
// others.
func TestRedactionShowsOnlyEventsThatNameNoKeyObject(t *testing.T) {
	const keyObject, unknown = "an event of a key object", "an event whose text may name an object"
	r := newRedaction([]dumpEvent{
		{client: "1", text: "_op_submit oid PRIVATEACCESSKEY '@5;users.keys' '@5;users.keys' [read 0~0] tid 7 osd.0"},
		{client: "1", text: "_op_submit oid bucket.index '@3' '@3' [read 0~0] tid 8 osd.0"},
	})
	for _, c := range []struct{ client, text, want string }{
		{"1", "_send_op 8 to 3.1 on osd.0", ""},
		{"1", "tid 8 on osd.0 is laggy", ""},
		{"1", "tick", ""},
		{"1", "_op_submit oid bucket.index '@3' '@3' [read 0~0] tid 8 osd.0", ""},
		{"1", "ms_dispatch2 0x osd_op_reply(8 bucket.index [read 0~0] v0'0 uv0 ondisk = 0)", ""},
		{"1", "8\t3.1\tosd.0\tbucket.index\t[read 0~0]", ""},
		// A key namespace, a key object's name or a key object's
		// transaction is never shown.
		{"1", "_op_submit oid OTHERKEY '@5;users.swift' '@5;users.swift' [read 0~0] tid 9 osd.0", keyObject},
		{"1", "_calc_target epoch 5 base PRIVATEACCESSKEY @5;users.keys precalc_pgid 0", keyObject},
		{"1", "laggy tid 2 for PRIVATEACCESSKEY", keyObject},
		{"1", "ms_dispatch2 0x osd_op_reply(7 SOMEKEY [read 0~0] v0'0 uv0 ondisk = 0)", keyObject},
		// A transaction of another client is another request.
		{"2", "ms_dispatch2 0x osd_op_reply(8 bucket.index [read 0~0] v0'0 uv0 ondisk = 0)", "an event of request 8, which was submitted before these events"},
		{"1", "ms_dispatch2 0x osd_op_reply(3 something [read 0~0] v0'0 uv0 ondisk = 0)", "an event of request 3, which was submitted before these events"},
		// Any other shape may hold an object name.
		{"1", "list_nobjects pool 3 nspace", unknown},
		{"1", "handle_osd_backoff something", unknown},
	} {
		want := c.want
		if want == "" {
			want = c.text
		}
		if got := r.shown(dumpEvent{client: c.client, text: c.text}); got != want {
			t.Errorf("%q: shown as %q, want %q", c.text, got, want)
		}
	}
}
