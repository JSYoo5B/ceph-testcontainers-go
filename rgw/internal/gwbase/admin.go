package gwbase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
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

// adminRunFailure describes a radosgw-admin run that exited nonzero without
// its native output, which can include secret keys: the *ceph.NativeFailure
// exit code and errno, which its text also names. When the command reported
// a fatal signal, read from its whole standard error, the text also names the
// signal, the thread and the first backtrace frames, and the failure has no
// errno. With such a report it also describes the shape of its
// standard output, but never its content, which tells a command that crashed
// after it wrote its result from one that crashed before, and what the
// crashed thread had asked of RADOS (eventReport).
func adminRunFailure(report supervised.Report) error {
	err := servicehook.NativeFailure("radosgw-admin", report.ExitCode, report.Stdout, report.Stderr)
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) {
		return err
	}
	// The report header is read once: it names the thread for both the
	// crash summary and the event report.
	signal, found := parseSignalReport(strings.Split(string(report.Stderr), "\n"), maxSignalFrames)
	crash, output, events := "", "", ""
	if found {
		crash = signal.crash()
		output, events = adminOutputShape(report.Stdout, report.StdoutTruncated), eventReport(signal.threadID, report.Stderr)
	}
	return redactAdminFailure(err, failure, crash, output, events)
}

func redactAdminFailure(err error, failure *ceph.NativeFailure, details ...string) error {
	// A command that died of a signal reported no errno: an "(N) text" in
	// its output is of an event in its dump, such as the reply to a read of
	// an object that does not exist, not the reason it ended.
	if len(details) > 0 && details[0] != "" {
		failure.Errno = ""
	}
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
	// The thread ID is hexadecimal, with or without "0x"; the dump of
	// recent events spells it without.
	fatalSignalThread = regexp.MustCompile(`^in thread (?:0x)?([0-9a-f]{1,16}) thread_name:([!-~]{1,15})$`)
	backtraceFrame    = regexp.MustCompile(`^\d{1,3}: ([ -~]+?)(?: \[0x[0-9a-f]{1,16}\])?$`)
	symbolFrame       = regexp.MustCompile(`^\(([ -~]+)\+(0x[0-9a-f]{1,16})\)$`)
	fileFrame         = regexp.MustCompile(`^(?:/[!-~]*/)?([!-'*-.0-~]+\([!-'*-.0-~]*\+0x[0-9a-f]{1,16}\))$`)
	// A function name without its argument list. Template arguments may
	// hold spaces and parentheses, never a directory or an address.
	functionName = regexp.MustCompile(`^[A-Za-z_~][ -'*-.0-Z^-~]*(?:\([ -'*-.0-Z^-~]*\)[ -'*-.0-Z^-~]*)*$`)
	// A bare symbol, with an empty argument list when it has one.
	bareSymbol = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_.$@]*)(?:\(\))?$`)
)

// incompleteFrame stands for a frame that is not one of the complete shapes,
// as a line that stderr's end cut short can be, and ends the backtrace.
const incompleteFrame = "an incomplete frame"

const (
	// maxSignalFrames bounds the backtrace frames of a fatal signal report.
	maxSignalFrames = 8
	// maxStopFrames bounds the frames of a stopped command's report. A
	// command that waits for RADOS spends about eight frames in the C
	// library and librados before the first RGW function.
	maxStopFrames = 16
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
	report, ok := parseSignalReport(lines, maxSignalFrames)
	if !ok {
		return ""
	}
	return report.crash()
}

// crash is the fatalSignalReport text of r.
func (r signalReport) crash() string {
	report := r
	text := "radosgw-admin reported fatal signal " + report.signal
	if report.thread != "" {
		text += " in thread " + report.thread
	}
	if len(report.frames) == 0 {
		return text + ", without a backtrace"
	}
	return text + ", backtrace " + report.backtrace()
}

// stopReport says where a command that the supervisor stopped with
// multicluster.DeadlineSignal was: the thread that took the signal and its
// first frames, read from the
// fatal signal report in the command's standard error as fatalSignalReport
// reads it. It returns "" when stderr holds no report with a backtrace, as
// for a command that the signal could not reach in time. A report of another
// signal is of a crash that the stop found, and is described as one.
func stopReport(stderr []byte) string {
	lines := strings.Split(string(stderr), "\n")
	report, ok := parseSignalReport(lines, maxStopFrames)
	return report.stop(ok, lines)
}

// stop is the stopReport text of r, the report in lines with maxStopFrames
// frames, which found says lines hold.
func (r signalReport) stop(found bool, lines []string) string {
	report, ok := r, found
	switch {
	case !ok:
		return ""
	case report.signal != "Aborted":
		return fatalSignalReport(lines)
	case len(report.frames) == 0:
		return ""
	}
	thread := "a thread"
	if report.thread != "" {
		thread = "thread " + report.thread
	}
	return "when it was stopped, " + thread + " was in " + report.backtrace()
}

// signalReport is the first fatal signal report of a command's output: the
// signal, the name and the ID of its thread, and its frames.
type signalReport struct {
	signal, thread, threadID string
	frames                   []string
	extra                    int
}

func (r signalReport) backtrace() string {
	text := strings.Join(r.frames, " < ")
	if r.extra > 0 {
		text += fmt.Sprintf(" < %d more frames", r.extra)
	}
	return text
}

// parseSignalReport reads the first fatal signal report in lines, keeping at
// most maxFrames frames, under the rules of fatalSignalReport.
func parseSignalReport(lines []string, maxFrames int) (signalReport, bool) {
	start := -1
	signal := ""
	for i, line := range lines {
		if match := fatalSignalHeader.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			start, signal = i, match[1]
			break
		}
	}
	if start < 0 {
		return signalReport{}, false
	}
	thread, threadID := "", ""
	var frames []string
	extra := 0
	for i := start + 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if len(frames) == 0 && i-start > maxFrameGap {
			break
		}
		if match := backtraceFrame.FindStringSubmatch(line); match != nil {
			if len(frames) == maxFrames {
				extra++
				continue
			}
			name, ok := backtraceFrameName(match[1])
			if !ok {
				frames = append(frames, incompleteFrame)
				break
			}
			frames = append(frames, name)
			continue
		}
		if len(frames) > 0 {
			break
		}
		if match := fatalSignalThread.FindStringSubmatch(line); match != nil {
			threadID, thread = match[1], match[2]
		}
	}
	return signalReport{signal: signal, thread: thread, threadID: threadID, frames: frames, extra: extra}, true
}

// backtraceFrameName shortens one frame: "(ns::f(args) const+0x1f)" to
// "ns::f+0x1f", "/lib64/libc.so.6(+0x3fb30)" to "libc.so.6(+0x3fb30)" and
// "main()" to "main", bounded by maxFrameBytes. It reports false for a frame
// of any other shape, such as one cut inside its name, its argument list, its
// directory or its address, whose text it never returns.
func backtraceFrameName(frame string) (string, bool) {
	if match := symbolFrame.FindStringSubmatch(frame); match != nil {
		name, ok := withoutArguments(match[1])
		if !ok || !functionName.MatchString(name) {
			return "", false
		}
		frame = name + "+" + match[2]
	} else if match := fileFrame.FindStringSubmatch(frame); match != nil {
		frame = match[1]
	} else if match := bareSymbol.FindStringSubmatch(frame); match != nil {
		frame = match[1]
	} else {
		return "", false
	}
	if len(frame) > maxFrameBytes {
		frame = frame[:maxFrameBytes-len("...")] + "..."
	}
	return frame, true
}

// withoutArguments removes the trailing argument list, and a const after it,
// of a demangled function name. It reports false when the name has an
// unbalanced trailing argument list or a parenthesis without one.
func withoutArguments(symbol string) (string, bool) {
	symbol = strings.TrimSuffix(symbol, " const")
	if !strings.HasSuffix(symbol, ")") {
		return symbol, !strings.ContainsAny(symbol, "()")
	}
	depth := 0
	for i := len(symbol) - 1; i >= 0; i-- {
		switch symbol[i] {
		case ')':
			depth++
		case '(':
			depth--
			if depth == 0 {
				return symbol[:i], i > 0
			}
		}
	}
	return "", false
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
	return AdminPrefix(ctx, c.cluster, target, maxBytes, argv)
}

// RunAdmin runs the scoped radosgw-admin command line argv, from AdminArgv
// or CommandArgs, in target, a gateway of owner or a setup client that a
// fixture recorded for owner, through multicluster.Run: the supervisor stops
// it with multicluster.DeadlineSignal at multicluster.Limit(ctx), when ctx
// ends first it is stopped before RunAdmin returns, and a stop that cannot be
// confirmed returns a *ceph.NativeCommandUnstoppedError. It keeps at most
// maxBytes bytes of standard output (0 for all). The report is returned for
// a caller that classifies the exit status itself, and never in an error.
func RunAdmin(ctx context.Context, owner *ceph.Container, target testcontainers.Container, maxBytes int, argv []string) (supervised.Report, int, error) {
	limit := multicluster.Limit(ctx)
	report, err := multicluster.Run(ctx, multicluster.Command{Owner: owner, Target: target, Label: "radosgw-admin", StdoutLimit: maxBytes, Limit: limit, Argv: diagnosticArgv(argv)})
	return report, limit, err
}

// AdminPrefix runs argv as RunAdmin does and returns its standard output and
// whether it wrote more than maxBytes. A command that the supervisor stopped
// is an ErrNativeFailure that says where it was, and a nonzero exit is
// reported by adminRunFailure, without the native output.
func AdminPrefix(ctx context.Context, owner *ceph.Container, target testcontainers.Container, maxBytes int, argv []string) ([]byte, bool, error) {
	report, limit, err := RunAdmin(ctx, owner, target, maxBytes, argv)
	if err != nil {
		return nil, false, err
	}
	if err := AdminReportFailure(report, limit); err != nil {
		return nil, false, err
	}
	return report.Stdout, report.StdoutTruncated, nil
}

// AdminReportFailure classifies a RunAdmin report with limit seconds, as
// AdminPrefix does: nil for a command that exited 0, an ErrNativeFailure that
// says where it was for one that the supervisor stopped, and adminRunFailure
// for a nonzero exit.
func AdminReportFailure(report supervised.Report, limit int) error {
	switch {
	case report.TimedOut:
		text := fmt.Sprintf("radosgw-admin still ran after %ds and was stopped", limit)
		lines := strings.Split(string(report.Stderr), "\n")
		signal, found := parseSignalReport(lines, maxStopFrames)
		for _, detail := range []string{signal.stop(found, lines), eventReport(signal.threadID, report.Stderr)} {
			if detail != "" {
				text += "; " + detail
			}
		}
		return &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "radosgw-admin", Err: errors.New(text)}
	case report.ExitCode != 0:
		return adminRunFailure(report)
	}
	return nil
}

// StagingPeriod reads the staging period of scope's realm, the exact object
// <realm ID>:staging at epoch 1, which avoids a separate latest-epoch lookup,
// in target as RunAdmin does. It returns nil when there is none: radosgw-admin
// exits 2, with no output, only for a missing object. Every other failure is
// a failure, never an empty staging area, and is reported as by AdminPrefix,
// without the native output. A period of another realm is refused.
func StagingPeriod(ctx context.Context, owner *ceph.Container, target testcontainers.Container, scope NativeScope) (map[string]any, error) {
	id := scope.RealmID + ":staging"
	report, limit, err := RunAdmin(ctx, owner, target, 0, CommandArgs(scope, "period", "get", "--period", id, "--epoch", "1"))
	if err != nil {
		return nil, err
	}
	if !report.TimedOut && report.ExitCode == 2 && len(report.Stdout) == 0 {
		return nil, nil
	}
	if err := AdminReportFailure(report, limit); err != nil {
		return nil, err
	}
	period, err := DecodeObject(report.Stdout)
	if err != nil {
		return nil, err
	}
	if period["id"] != id || period["realm_id"] != scope.RealmID {
		return nil, errors.New("RGW staging period identity differs from the owned realm")
	}
	return period, nil
}
