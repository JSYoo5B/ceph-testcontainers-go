package gwbase

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

func TestRGWAdminLimit(t *testing.T) {
	if limit := adminLimit(context.Background()); limit != 0 {
		t.Fatalf("no deadline: %d", limit)
	}
	for bound, want := range map[time.Duration][2]int{3 * time.Minute: {164, 165}, 20 * time.Second: {9, 10}, time.Second: {1, 1}} {
		ctx, cancel := context.WithTimeout(t.Context(), bound)
		limit := adminLimit(ctx)
		cancel()
		if limit < want[0] || limit > want[1] {
			t.Errorf("bound %s: limit %d", bound, limit)
		}
	}
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
	err := adminRunFailure(supervised.ShellReport{Stdout: stdout, Stderr: []byte(crashStderr), ExitCode: 139})
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
		got := adminRunFailure(supervised.ShellReport{Stdout: []byte(c.stdout), StdoutTruncated: c.truncated, Stderr: []byte(crashStderr), ExitCode: 139}).Error()
		if !strings.HasSuffix(got, "; "+c.want+"; native output is redacted") {
			t.Errorf("stdout %q truncated=%v: %s", c.stdout, c.truncated, got)
		}
	}
	// Without a signal report the text keeps its exit and errno shape.
	plain := adminRunFailure(supervised.ShellReport{Stdout: stdout, Stderr: []byte("ERROR: failed to sync bucket: (5) Input/output error\n"), ExitCode: 5}).Error()
	if plain != "radosgw-admin exited 5 (EIO); native output is redacted" {
		t.Fatalf("plain failure: %s", plain)
	}
}

func TestRedactAdminFailureReadsTheSignalFromTheReason(t *testing.T) {
	native := servicehook.NativeFailure("radosgw-admin", 139, nil, []byte(crashStderr))
	got := RedactAdminFailure(fmt.Errorf("zone admin: %w", native)).Error()
	if !strings.HasPrefix(got, "radosgw-admin exited 139; radosgw-admin reported fatal signal Segmentation fault in thread radosgw-admin, backtrace libc.so.6(+0x3fb30)") ||
		!strings.HasSuffix(got, "; native output is redacted") || strings.Contains(got, "AQB") || strings.Contains(got, "standard output") {
		t.Fatalf("redacted: %s", got)
	}
}
