package gwbase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
)

// AdminArgv returns the radosgw-admin command line that addresses scope with
// the cluster configuration and admin keyring of a gateway or CLI client
// container, followed by args. Empty scope fields select the native defaults.
func AdminArgv(scope Scope, args ...string) []string {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring"}
	for _, field := range []struct{ flag, value string }{
		{"--rgw-realm", scope.Realm}, {"--rgw-zonegroup", scope.Zonegroup}, {"--rgw-zone", scope.Zone},
	} {
		if field.value != "" {
			argv = append(argv, field.flag, field.value)
		}
	}
	if scope.Realm != "" {
		argv = append(argv, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0")
	}
	return append(argv, args...)
}

// ValidateAdminArgs refuses caller-supplied radosgw-admin arguments of a
// native escape hatch that are empty, contain control characters, or would
// change the scope or Ceph connection that AdminArgv sets.
func ValidateAdminArgs(args []string) error {
	if len(args) == 0 {
		return servicehook.Invalid(errors.New("radosgw-admin operation is required"))
	}
	for _, arg := range args {
		if strings.IndexFunc(arg, unicode.IsControl) != -1 {
			return servicehook.Invalid(errors.New("radosgw-admin arguments must not contain control characters"))
		}
		flag, _, _ := strings.Cut(strings.ReplaceAll(arg, "_", "-"), "=")
		switch flag {
		case "--rgw-realm", "--rgw-realm-id", "--realm-id", "--realm-name",
			"--rgw-zonegroup", "--rgw-zonegroup-id", "--zonegroup-id", "--zonegroup-name",
			"--rgw-zone", "--rgw-zone-id", "--zone-id", "--zone-name",
			"--conf", "--cluster", "--keyring", "--keyfile", "--key-file", "--key", "--id", "--name", "--client-id", "--mon-host", "--monmap", "--no-config-file":
			return servicehook.Invalid(errors.New("radosgw-admin cannot override the gateway scope or Ceph connection"))
		}
		for _, short := range []string{"-c", "-m", "-n", "-k"} {
			if strings.HasPrefix(arg, short) && !strings.HasPrefix(arg, "--") {
				return servicehook.Invalid(errors.New("radosgw-admin cannot override the gateway Ceph connection"))
			}
		}
	}
	return nil
}

// RedactAdminFailure hides the native output of a failed radosgw-admin
// command, which can include secret keys, and keeps the classification of err,
// including the *ceph.NativeFailure exit code and errno, which its text also
// names. When the command reported a fatal signal, the text also names the
// signal, the thread and the first backtrace frames, read from the
// already redacted and bounded Reason. Other errors are returned unchanged.
func RedactAdminFailure(err error) error {
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) {
		return err
	}
	return redactAdminFailure(err, failure, fatalSignalReport(strings.Split(failure.Reason, "; ")))
}

// adminRunFailure describes a radosgw-admin run that exited nonzero as
// RedactAdminFailure does, reading a fatal signal report from its whole
// standard error. With such a report it also describes the shape of its
// standard output, but never its content, which tells a command that crashed
// after it wrote its result from one that crashed before.
func adminRunFailure(report supervised.ShellReport) error {
	err := servicehook.NativeFailure("radosgw-admin", report.ExitCode, report.Stdout, report.Stderr)
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) {
		return err
	}
	crash := fatalSignalReport(strings.Split(string(report.Stderr), "\n"))
	output := ""
	if crash != "" {
		output = adminOutputShape(report.Stdout, report.StdoutTruncated)
	}
	return redactAdminFailure(err, failure, crash, output)
}

func redactAdminFailure(err error, failure *ceph.NativeFailure, details ...string) error {
	exit := ceph.NativeFailure{Program: "radosgw-admin", ExitCode: failure.ExitCode, Errno: failure.Errno}
	text := exit.Error()
	for _, detail := range details {
		if detail != "" {
			text += "; " + detail
		}
	}
	return clusterutil.Quiet(fmt.Errorf("%s; native output is redacted", text), err)
}

// The fatal signal report that Ceph's signal handler writes to standard error
// before the process dies of the signal: a header, the thread, the version,
// then numbered frames, each a demangled symbol with its offset, a file with
// its offset, or a bare symbol, most with their address.
var (
	fatalSignalHeader = regexp.MustCompile(`^\*\*\* Caught signal \(([A-Za-z][A-Za-z -]{0,39})\) \*\*$`)
	fatalSignalThread = regexp.MustCompile(`^in thread [0-9a-fx]{1,18} thread_name:([!-~]{1,15})$`)
	backtraceFrame    = regexp.MustCompile(`^\d{1,3}: ([ -~]+?)(?: \[0x[0-9a-f]{1,16}\])?$`)
	symbolFrame       = regexp.MustCompile(`^\(([ -~]+)\+(0x[0-9a-f]{1,16})\)$`)
	fileFrame         = regexp.MustCompile(`^(?:/[!-~]*/)?([!-'*-~]+\([!-'*-~]*\+0x[0-9a-f]{1,16}\))$`)
)

const (
	// maxSignalFrames bounds the backtrace frames of a fatal signal report.
	maxSignalFrames = 8
	// maxFrameBytes bounds one frame.
	maxFrameBytes = 120
	// maxFrameGap is how many lines (the thread, the version and blank
	// lines) may separate the header from the first frame.
	maxFrameGap = 5
)

// fatalSignalReport summarizes the first fatal signal report in lines: the
// signal, the thread and at most maxSignalFrames frames, each a function name
// with its offset, a file name with its offset or a bare symbol, never an
// address, a directory or an argument list. It returns "" when lines hold no
// report. Frames are names from the binary, not data of the request.
func fatalSignalReport(lines []string) string {
	start := -1
	signal := ""
	for i, line := range lines {
		if match := fatalSignalHeader.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			start, signal = i, match[1]
			break
		}
	}
	if start < 0 {
		return ""
	}
	thread := ""
	var frames []string
	extra := 0
	for i := start + 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if len(frames) == 0 && i-start > maxFrameGap {
			break
		}
		if match := backtraceFrame.FindStringSubmatch(line); match != nil {
			if len(frames) == maxSignalFrames {
				extra++
				continue
			}
			frames = append(frames, backtraceFrameName(match[1]))
			continue
		}
		if len(frames) > 0 {
			break
		}
		if match := fatalSignalThread.FindStringSubmatch(line); match != nil {
			thread = match[1]
		}
	}
	text := "radosgw-admin reported fatal signal " + signal
	if thread != "" {
		text += " in thread " + thread
	}
	if len(frames) == 0 {
		return text + ", without a backtrace"
	}
	text += ", backtrace " + strings.Join(frames, " < ")
	if extra > 0 {
		text += fmt.Sprintf(" < %d more frames", extra)
	}
	return text
}

// backtraceFrameName shortens one frame: "(ns::f(args) const+0x1f)" to
// "ns::f+0x1f" and "/lib64/libc.so.6(+0x3fb30)" to "libc.so.6(+0x3fb30)",
// bounded by maxFrameBytes.
func backtraceFrameName(frame string) string {
	if match := symbolFrame.FindStringSubmatch(frame); match != nil {
		frame = withoutArguments(match[1]) + "+" + match[2]
	} else if match := fileFrame.FindStringSubmatch(frame); match != nil {
		frame = match[1]
	} else {
		frame = withoutArguments(frame)
	}
	if len(frame) > maxFrameBytes {
		frame = frame[:maxFrameBytes-len("...")] + "..."
	}
	return frame
}

// withoutArguments removes the trailing argument list, and a const after it,
// of a demangled function name.
func withoutArguments(symbol string) string {
	symbol = strings.TrimSuffix(symbol, " const")
	if !strings.HasSuffix(symbol, ")") {
		return symbol
	}
	depth := 0
	for i := len(symbol) - 1; i >= 0; i-- {
		switch symbol[i] {
		case ')':
			depth++
		case '(':
			depth--
			if depth == 0 {
				return symbol[:i]
			}
		}
	}
	return symbol
}

// adminOutputShape describes standard output by its size and whether it is
// one complete JSON document.
func adminOutputShape(stdout []byte, truncated bool) string {
	switch {
	case truncated:
		return fmt.Sprintf("it wrote more than %d bytes of standard output", len(stdout))
	case len(bytes.TrimSpace(stdout)) == 0:
		return "it wrote no standard output"
	case json.Valid(stdout):
		return fmt.Sprintf("it wrote %d bytes of standard output, a complete JSON document", len(stdout))
	}
	return fmt.Sprintf("it wrote %d bytes of standard output, not a complete JSON document", len(stdout))
}

// AdminCommand runs radosgw-admin in the gateway container, in the gateway's
// configured scope, with --format json. It is called with the owning
// cluster's mutex held. Native output can include secret keys even in errors,
// so it never wraps the original error.
func (c *Core) AdminCommand(ctx context.Context, args ...string) ([]byte, error) {
	return c.AdminCommandIn(ctx, c.Container(), args...)
}

// AdmitAdmin validates c as ValidateAdmin does and returns the gateway
// container that a later AdminCommandIn runs in. The caller holds the cluster
// mutex; AdminCommandIn does not need it.
func (c *Core) AdmitAdmin() (testcontainers.Container, error) {
	if err := c.ValidateAdmin(); err != nil {
		return nil, err
	}
	return c.Container(), nil
}

// AdminCommandIn runs radosgw-admin as AdminCommand does, in target, a
// container that AdmitAdmin returned. It does not need the cluster mutex: the
// cluster's native command tracking refuses a terminated cluster and stops the
// command when Terminate starts, and a target removed meanwhile fails the
// command with a classified error.
func (c *Core) AdminCommandIn(ctx context.Context, target testcontainers.Container, args ...string) ([]byte, error) {
	data, _, err := c.AdminPrefixIn(ctx, target, 0, args...)
	return data, err
}

// AdminPrefixIn runs radosgw-admin as AdminCommandIn does and keeps at most
// the first maxBytes bytes of its output (0 for all); truncated reports that
// the command wrote more, which was discarded as it arrived. A failed command
// is reported as by AdminCommandIn, from what was kept.
func (c *Core) AdminPrefixIn(ctx context.Context, target testcontainers.Container, maxBytes int, args ...string) (data []byte, truncated bool, err error) {
	argv := AdminArgv(c.scope, append([]string{"--format", "json"}, args...)...)
	return c.supervisedPrefix(ctx, target, maxBytes, argv)
}

// supervised runs the already scoped command line argv without limiting stdout.
func (c *Core) supervised(ctx context.Context, target testcontainers.Container, argv []string) ([]byte, error) {
	data, _, err := c.supervisedPrefix(ctx, target, 0, argv)
	return data, err
}

// supervisedPrefix preserves supervisor tracking while retaining a bounded stdout prefix.
func (c *Core) supervisedPrefix(ctx context.Context, target testcontainers.Container, maxBytes int, argv []string) ([]byte, bool, error) {
	if servicehook.DiagnosticHandle(target) == nil {
		return nil, false, servicehook.Invalid(errors.New("RGW gateway is no longer owned by this cluster"))
	}
	limit := adminLimit(ctx)
	report, err := servicehook.RunOwnedShell(ctx, c.cluster, target, "radosgw-admin", limit, maxBytes, argv...)
	var unstopped *ceph.NativeCommandUnstoppedError
	switch {
	case errors.As(err, &unstopped):
		// Its Stop, or Terminate, confirms the stop later.
		return nil, false, err
	case err != nil && ctx.Err() != nil:
		return nil, false, fmt.Errorf("radosgw-admin: the call ended before the command, which was stopped: %w", ctx.Err())
	case err != nil:
		// The command may have run: its outcome is unknown.
		return nil, false, servicehook.ClassifyExec(ctx, err)
	case report.Stopped:
		return nil, false, &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "radosgw-admin",
			Err: fmt.Errorf("radosgw-admin still ran after %ds and was stopped", limit)}
	case report.ExitCode == 127:
		// The gateway's sh found no radosgw-admin, which never exits 127 itself.
		return nil, false, &ceph.OperationError{Kind: ceph.ErrImageCapability, Op: "radosgw-admin", Capability: "radosgw-admin",
			Err: errors.New("the gateway image has no radosgw-admin")}
	case report.ExitCode != 0:
		return nil, false, adminRunFailure(report)
	}
	return report.Stdout, report.StdoutTruncated, nil
}

// adminLimit is how long radosgw-admin may run before the gateway's
// supervisor stops it on its own: the call's bound less a margin for stopping
// it and returning, so that a command that runs too long is reported as such
// within the bound. Without a bound, it is 0. A call whose context ends first
// stops the command itself.
func adminLimit(ctx context.Context) int {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	limit := remaining - 15*time.Second
	if limit < remaining/2 {
		limit = remaining / 2
	}
	return max(1, int(limit/time.Second))
}
