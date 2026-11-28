package gwbase

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// A longer admin operation keeps its full supervisor limit. A nearly spent,
// expired, or unbounded operation gets a bounded chance to confirm the write.
func TestSettlementReadbackBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			bound time.Duration
			want  time.Duration
			limit int
		}{
			{3 * time.Minute, 3 * time.Minute, 165},
			{time.Second, 10 * time.Second, 5},
			{-time.Second, 10 * time.Second, 5},
			{0, 10 * time.Second, 5},
		} {
			parent := t.Context()
			if tc.bound != 0 {
				var cancel context.CancelFunc
				parent, cancel = context.WithTimeout(parent, tc.bound)
				cancel()
			}
			ctx, stop := SettlementContext(parent)
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) != tc.want || multicluster.Limit(ctx) != tc.limit || ctx.Err() != nil {
				t.Errorf("input %v: deadline in %v, supervisor limit %ds, error %v", tc.bound, time.Until(deadline), multicluster.Limit(ctx), ctx.Err())
			}
			stop()
		}
	})
}

// crashStderr is standard error of a radosgw-admin of the Tentacle 20.2.4 rgw
// role image that a SIGSEGV killed, with long names shortened: a log line,
// then the report of Ceph's signal handler, which it repeats in its dump of
// recent events.
const crashStderr = `2026-11-06T01:02:03.456+0000 73c9913528c0 -1 auth: unable to find a keyring val=AQBsecretsecretsecretsecretsecret==
*** Caught signal (Segmentation fault) **
 in thread 73c9913528c0 thread_name:radosgw-admin
 ceph version 20.2.4 (0123456789abcdef0123456789abcdef01234567) tentacle (stable - RelWithDebInfo)
 1: /lib64/libc.so.6(+0x3fb30) [0x73c9934bab30]
 2: /lib64/libc.so.6(+0x8854a) [0x73c99350354a]
 3: pthread_cond_clockwait()
 4: (MonClient::authenticate(double)+0x436) [0x73c994a8ee86]
 5: (MonClient::get_monmap_and_config()+0x286) [0x73c994a89df6]
 6: (global_init(std::map<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >, std::less<std::__cxx11::basic_string<char> > > const*, std::vector<char const*, std::allocator<char const*> >&, unsigned int, code_environment_t, int, bool)+0x276) [0x651d43a39216]
 7: (rgw::AppMain::shutdown(std::function<void ()>) const+0x9f) [0x651d431215dc]
 8: main()
 9: radosgw-admin(+0x4a1c2f) [0x651d434a1c2f]
 10: __libc_start_main()
 11: _start()
 in thread 73c9913528c0 thread_name:radosgw-admin

 ceph version 20.2.4 (0123456789abcdef0123456789abcdef01234567) tentacle (stable - RelWithDebInfo)
 1: /lib64/libc.so.6(+0x3fb30) [0x73c9934bab30]
 NOTE: a copy of the executable, or ` + "`objdump -rdS <executable>`" + ` is needed to interpret this.

  -115> 2026-11-06T01:02:03.457+0000 73c9913528c0 -1 *** Caught signal (Segmentation fault) **
 in thread 73c9913528c0 thread_name:radosgw-admin
`

func TestFatalSignalReport(t *testing.T) {
	const want = "radosgw-admin reported fatal signal Segmentation fault in thread radosgw-admin, backtrace " +
		"libc.so.6(+0x3fb30) < libc.so.6(+0x8854a) < pthread_cond_clockwait < MonClient::authenticate+0x436 < " +
		"MonClient::get_monmap_and_config+0x286 < global_init+0x276 < rgw::AppMain::shutdown+0x9f < main < 3 more frames"
	if got := fatalSignalReport(strings.Split(crashStderr, "\n")); got != want {
		t.Fatalf("report:\n got %q\nwant %q", got, want)
	}
	for name, lines := range map[string][]string{
		"no report":         {"ERROR: could not init bucket: (2) No such file or directory"},
		"logged header":     {"  -115> 2026-11-06T01:02:03.457+0000 73c9913528c0 -1 *** Caught signal (Segmentation fault) **", " 1: (f()+0x1) [0x1]"},
		"unrecognized name": {"*** Caught signal (Segmentation fault; rm -rf) **"},
	} {
		if got := fatalSignalReport(lines); got != "" {
			t.Errorf("%s: %q", name, got)
		}
	}
	long := "(" + strings.Repeat("x", 300) + "::f(int)+0x1)"
	got := fatalSignalReport([]string{"*** Caught signal (Aborted) **", "1: " + long + " [0x1]", "unrelated 2: (g()+0x1) [0x2]"})
	if !strings.HasPrefix(got, "radosgw-admin reported fatal signal Aborted, backtrace xxx") || len(got) > 200 || strings.Contains(got, "g()") {
		t.Fatalf("bounded single frame: %q", got)
	}
	// Frames that do not follow the header closely are not its backtrace.
	if got := fatalSignalReport([]string{"*** Caught signal (Bus error) **", "a", "b", "c", "d", "e", "1: (f()+0x1) [0x1]"}); got != "radosgw-admin reported fatal signal Bus error, without a backtrace" {
		t.Fatalf("distant frames: %q", got)
	}
}

func TestAdminRunFailureNamesTheSignalWithoutOutput(t *testing.T) {
	stdout := []byte(`{"bucket":"tc-user-existing","placement_rule":"default-placement","owner":"tc-placement-writer","secret_key":"stdout-secret"}` + "\n")
	err := adminRunFailure(supervised.Report{Stdout: stdout, Stderr: []byte(crashStderr), ExitCode: 139})
	text := err.Error()
	for _, leaked := range []string{"stdout-secret", "tc-user-existing", "AQBsecret", "0x73c994a8ee86", "/lib64", "0123456789abcdef", "std::"} {
		if strings.Contains(text, leaked) {
			t.Errorf("error exposes %q: %s", leaked, text)
		}
	}
	for _, part := range []string{"radosgw-admin exited 139; radosgw-admin reported fatal signal Segmentation fault in thread radosgw-admin, backtrace libc.so.6(+0x3fb30) < libc.so.6(+0x8854a) < pthread_cond_clockwait < MonClient::authenticate+0x436",
		"; it wrote " + strconv.Itoa(len(stdout)) + " bytes of standard output, a complete JSON document; native output is redacted"} {
		if !strings.Contains(text, part) {
			t.Errorf("error lacks %q: %s", part, text)
		}
	}
	var failure *ceph.NativeFailure
	if !errors.Is(err, ceph.ErrNativeFailure) || !errors.As(err, &failure) || failure.ExitCode != 139 || failure.Reason != "" {
		t.Fatalf("classification: %#v", failure)
	}
	for _, c := range []struct {
		stdout    string
		truncated bool
		want      string
	}{
		{"", false, "it wrote no standard output"},
		{`{"bucket":`, false, "it wrote 10 bytes of standard output, not a complete JSON document"},
		{`{"bucket":`, true, "it wrote more than 10 bytes of standard output"},
	} {
		got := adminRunFailure(supervised.Report{Stdout: []byte(c.stdout), StdoutTruncated: c.truncated, Stderr: []byte(crashStderr), ExitCode: 139}).Error()
		if !strings.HasSuffix(got, "; "+c.want+"; native output is redacted") {
			t.Errorf("stdout %q truncated=%v: %s", c.stdout, c.truncated, got)
		}
	}
	// Without a signal report the text keeps its exit and errno shape.
	plain := adminRunFailure(supervised.Report{Stdout: stdout, Stderr: []byte("ERROR: failed to sync bucket: (5) Input/output error\n"), ExitCode: 5}).Error()
	if plain != "radosgw-admin exited 5 (EIO); native output is redacted" {
		t.Fatalf("plain failure: %s", plain)
	}
}

// frameLeaks are parts of crashStderr that a summary never names: a
// directory, an address, an argument list and the version.
var frameLeaks = []string{"/lib64", "/opt", "[0x", "0x73c9", "0x651d", "std::", "(double", "(std", "0123456789abcdef"}

func TestFatalSignalReportRejectsIncompleteFrames(t *testing.T) {
	for frame, want := range map[string]string{
		"/opt/example/lib/libfoo.so(+0x12":                                    incompleteFrame,
		"(global_init(std::vector<char const*, std::allocator<char const*> >": incompleteFrame,
		"(MonClient::authenticate(double)+0x436) [0x1234":                     incompleteFrame,
		"(MonClient::authenticate(double)+0x4":                                incompleteFrame,
		"main() [0x":                                                          incompleteFrame,
		"(f(int))+0x1)":                                                       incompleteFrame,
		"opt/example/libfoo.so(+0x12)":                                        incompleteFrame,
		"(__libc_start_main+0x80)":                                            "__libc_start_main+0x80",
		"(std::_Function_handler<void (), F>::_M_invoke(std::_Any_data const&)+0x2)": "std::_Function_handler<void (), F>::_M_invoke+0x2",
	} {
		got := fatalSignalReport([]string{"*** Caught signal (Aborted) **", "1: " + frame, "2: main()"})
		wantText := "radosgw-admin reported fatal signal Aborted, backtrace " + want
		if want != incompleteFrame {
			wantText += " < main"
		}
		if got != wantText {
			t.Errorf("frame %q:\n got %q\nwant %q", frame, got, wantText)
		}
	}
	// Every cut of the real report yields complete frames or the marker only.
	for n := range len(crashStderr) {
		checkFrameSummary(t, fmt.Sprintf("stderr cut at %d", n), fatalSignalReport(strings.Split(crashStderr[:n], "\n")))
	}
}

func checkFrameSummary(t *testing.T, name, text string) {
	t.Helper()
	for _, leaked := range frameLeaks {
		if strings.Contains(text, leaked) {
			t.Fatalf("%s exposes %q: %s", name, leaked, text)
		}
	}
	_, frames, ok := strings.Cut(text, ", backtrace ")
	if !ok {
		return
	}
	frames, _, _ = strings.Cut(frames, "; ")
	for _, frame := range strings.Split(frames, " < ") {
		if frame != incompleteFrame && !strings.HasSuffix(frame, " more frames") && !summaryFrames[frame] {
			t.Fatalf("%s names frame %q not in the report: %s", name, frame, text)
		}
	}
}

// summaryFrames are the frames that the complete report names, and the names
// that a cut can leave of them.
var summaryFrames = func() map[string]bool {
	frames := map[string]bool{}
	for _, frame := range []string{"libc.so.6(+0x3fb30)", "libc.so.6(+0x8854a)", "pthread_cond_clockwait", "MonClient::authenticate+0x436",
		"MonClient::get_monmap_and_config+0x286", "global_init+0x276", "rgw::AppMain::shutdown+0x9f", "main", "radosgw-admin(+0x4a1c2f)", "__libc_start_main", "_start"} {
		for i := 1; i <= len(frame); i++ {
			frames[frame[:i]] = true
		}
	}
	return frames
}()

func TestRedactedAdminErrnoRequiresNativeEvidence(t *testing.T) {
	for _, tc := range []struct{ text, errno string }{
		{"failed to read bucket info for PRIVATE-BUCKET", ""},
		{"ERROR: could not get topic PRIVATE-TOPIC: (2) No such file or directory", "ENOENT"},
	} {
		err := adminRunFailure(supervised.Report{ExitCode: 2, Stderr: []byte(tc.text)})
		var failure *ceph.NativeFailure
		if !errors.As(err, &failure) || failure.ExitCode != 2 || failure.Errno != tc.errno || failure.Reason != "" || strings.Contains(err.Error(), "PRIVATE") || errors.Is(err, fs.ErrNotExist) != (tc.errno == "ENOENT") {
			t.Fatalf("native evidence for errno %q returned %v", tc.errno, err)
		}
	}
}

// stoppedStderr is standard error of a radosgw-admin that waited for an OSD
// when the supervisor stopped it with SIGABRT: a log line, then the report of
// Ceph's signal handler for the main thread.
const stoppedStderr = `2026-11-20T04:33:25.001+0000 7f0e5c0d5980 -1 auth: unable to find a keyring val=AQBsecretsecretsecretsecretsecret==
*** Caught signal (Aborted) **
 in thread 7f0e5c0d5980 thread_name:radosgw-admin
 ceph version 21.2.0 (0123456789abcdef0123456789abcdef01234567) umbrella (dev - RelWithDebInfo)
 1: /lib64/libc.so.6(+0x3fc30) [0x7f0e5d03fc30]
 2: /lib64/libc.so.6(+0x8b3ba) [0x7f0e5d08b3ba]
 3: /lib64/libc.so.6(+0x8dc10) [0x7f0e5d08dc10]
 4: pthread_cond_wait()
 5: (std::condition_variable::wait(std::unique_lock<std::mutex>&)+0x10) [0x7f0e5d4f1b40]
 6: (librados::v14_2_0::IoCtxImpl::operate_read(object_t const&, ObjectOperation*, ceph::buffer::v15_2_0::list*, int)+0x1f4) [0x7f0e5e6d1a14]
 7: (librados::v14_2_0::IoCtx::operate(std::__cxx11::basic_string<char> const&, librados::v14_2_0::ObjectReadOperation*, ceph::buffer::v15_2_0::list*)+0x4a) [0x7f0e5e6a2f0a]
 8: (rgw_rados_operate(DoutPrefixProvider const*, librados::v14_2_0::IoCtx&, std::__cxx11::basic_string<char> const&, librados::v14_2_0::ObjectReadOperation&&, optional_yield, int)+0x1b7) [0x5560f1c5a2d7]
 9: (RGWSI_BucketIndex_RADOS::cls_bucket_head(DoutPrefixProvider const*, RGWBucketInfo const&, int, optional_yield)+0x2e1) [0x5560f1d07e41]
 10: (RGWSI_BucketIndex_RADOS::read_stats(DoutPrefixProvider const*, RGWBucketInfo const&, RGWBucketEnt*, optional_yield)+0xd5) [0x5560f1d0a015]
 11: (RGWBucketAdminOp::info(rgw::sal::Driver*, RGWBucketAdminOpState&, RGWFormatterFlusher&, optional_yield, DoutPrefixProvider const*)+0x402) [0x5560f1a40bf2]
 12: main()
 13: /lib64/libc.so.6(+0x295d0) [0x7f0e5d0295d0]
 14: __libc_start_main()
 15: _start()
 16: radosgw-admin(+0x4a1c2f) [0x5560f14a1c2f]
 17: (extra()+0x1) [0x1]
 NOTE: a copy of the executable, or ` + "`objdump -rdS <executable>`" + ` is needed to interpret this.
`

func TestStopReport(t *testing.T) {
	const want = "when it was stopped, thread radosgw-admin was in libc.so.6(+0x3fc30) < libc.so.6(+0x8b3ba) < libc.so.6(+0x8dc10) < pthread_cond_wait < " +
		"std::condition_variable::wait+0x10 < librados::v14_2_0::IoCtxImpl::operate_read+0x1f4 < librados::v14_2_0::IoCtx::operate+0x4a < rgw_rados_operate+0x1b7 < " +
		"RGWSI_BucketIndex_RADOS::cls_bucket_head+0x2e1 < RGWSI_BucketIndex_RADOS::read_stats+0xd5 < RGWBucketAdminOp::info+0x402 < main < " +
		"libc.so.6(+0x295d0) < __libc_start_main < _start < radosgw-admin(+0x4a1c2f) < 1 more frames"
	got := stopReport([]byte(stoppedStderr))
	if got != want {
		t.Fatalf("stop report:\n got %q\nwant %q", got, want)
	}
	for _, leaked := range []string{"AQBsecret", "0x7f0e5e6d1a14", "/lib64", "0123456789abcdef", "object_t", "std::__cxx11"} {
		if strings.Contains(got, leaked) {
			t.Errorf("stop report exposes %q: %s", leaked, got)
		}
	}
	for name, c := range map[string]struct{ stderr, want string }{
		"no report":    {"ERROR: could not init bucket: (2) No such file or directory\n", ""},
		"no backtrace": {"*** Caught signal (Aborted) **\n in thread 7f0e5c0d5980 thread_name:radosgw-admin\n", ""},
		"no thread":    {"*** Caught signal (Aborted) **\n 1: main()\n", "when it was stopped, a thread was in main"},
		// A crash that the stop found is not where the command waited.
		"another signal": {crashStderr, fatalSignalReport(strings.Split(crashStderr, "\n"))},
	} {
		if got := stopReport([]byte(c.stderr)); got != c.want {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
}
