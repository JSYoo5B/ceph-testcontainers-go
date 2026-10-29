package ceph

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Error categories. A classified error matches one of them with errors.Is,
// and errors.As with *OperationError or *NativeFailure returns its details.
// An error joined from several failures, such as a failed change and a failed
// restoration, can match several. Context errors, ErrNativeCommandIncomplete
// and *NativeCommandUnstoppedError keep their own contracts and belong to no
// category: a call that ctx ended is never classified by what it could not
// finish. An error that matches no category is a failed check of the
// fixture's own, such as output it could not parse, or a failure whose
// outcome is unknown, such as a container that disappeared after a native
// command was sent to it. Only ErrInvalidRequest and ErrSharedCluster
// promise that nothing changed.
var (
	// ErrInvalidRequest reports a request that the fixture refused before
	// any change: malformed arguments, or an operation that the state of the
	// cluster or of the selected resource forbids, such as a terminated
	// cluster, a container that the cluster neither owns nor has attached,
	// or an override that is already held.
	ErrInvalidRequest = errors.New("invalid request")
	// ErrUnsupported reports a native feature that the Ceph of the relevant
	// target lacks, established before any change from that target's own
	// capability, such as an option that its tool does not list, or from a
	// documented limit of the release it runs. AddressDualStack establishes
	// it after startup instead, from the addresses that the daemons
	// advertise. *OperationError names the feature, the target and its
	// version.
	ErrUnsupported = errors.New("unsupported by the target's Ceph")
	// ErrImageCapability reports an image that lacks what the operation
	// needs: an executable or runtime, such as the python3 of the
	// supervisor, or an optional extension that was not selected or does not
	// meet its contract.
	ErrImageCapability = errors.New("image capability unavailable")
	// ErrEnvironmentUnavailable reports that the Docker engine, the host, the
	// kernel or the permissions of a container cannot provide what the
	// operation needs, although the image may be valid. When it comes from a
	// call that had sent a native command, the command may have run.
	ErrEnvironmentUnavailable = errors.New("runtime environment unavailable")
	// ErrNativeFailure reports a native command that ran and failed: Ceph
	// refused the request, a resource was missing, permission was denied or
	// I/O failed. *NativeFailure carries the native status.
	ErrNativeFailure = errors.New("native operation failed")
	// ErrSharedCluster reports a method that a cluster shared through
	// SharedCluster refuses before any change: an operation that would
	// reach outside one tenant's scope, a raw handle to its containers or
	// credentials, or Terminate, since only SharedCluster.Close ends it.
	// *OperationError names the method in Op.
	ErrSharedCluster = errors.New("refused on a shared cluster")
)

// OperationError is a classified failure with the details that identify it.
// Its text never includes command arguments, raw output or credentials.
type OperationError struct {
	// Kind is the category: ErrInvalidRequest, ErrUnsupported,
	// ErrImageCapability, ErrEnvironmentUnavailable, ErrNativeFailure or
	// ErrSharedCluster.
	Kind error
	// Op names the operation, such as "create client", when the error's
	// text does not already start with it.
	Op string
	// Target names the container or cluster that the failure concerns, such
	// as "control", "osd.0", "destination cluster" or "netem helper".
	Target string
	// Version is the Ceph release observed on Target, when it matters.
	Version string
	// Image is the immutable ID of Target's image, when the fixture read it.
	Image string
	// Capability names the missing or unsupported capability, such as
	// "ceph-authtool --key-type" or "python3".
	Capability string
	// Err is the cause.
	Err error
}

func (e *OperationError) Error() string {
	var text strings.Builder
	if e.Op != "" {
		text.WriteString(e.Op)
	}
	switch {
	case e.Err != nil && e.Op != "":
		text.WriteString(": " + e.Err.Error())
	case e.Err != nil:
		text.WriteString(e.Err.Error())
	case e.Kind != nil && e.Op != "":
		text.WriteString(": " + e.Kind.Error())
	case e.Kind != nil:
		text.WriteString(e.Kind.Error())
	}
	details := []string{}
	if e.Target != "" {
		details = append(details, e.Target)
	}
	if e.Version != "" {
		details = append(details, "Ceph "+e.Version)
	}
	if e.Image != "" {
		details = append(details, "image "+e.Image)
	}
	if len(details) > 0 {
		text.WriteString(" (" + strings.Join(details, ", ") + ")")
	}
	return text.String()
}

// Is reports whether target is the error's category.
func (e *OperationError) Is(target error) bool { return e.Kind != nil && target == e.Kind }

func (e *OperationError) Unwrap() error { return e.Err }

// NativeFailure is the status of a native command that ran and exited
// nonzero. errors.Is matches it with ErrNativeFailure, and with
// fs.ErrNotExist, fs.ErrPermission or fs.ErrExist when the tool reported
// ENOENT, EACCES or EPERM, or EEXIST. RunNativeCommand returns a nonzero exit
// as its result instead; the module's own commands and Ceph return this.
type NativeFailure struct {
	// Program is the command's program, such as "ceph" or "rbd"; the rest of
	// its arguments is never recorded.
	Program string
	// ExitCode is its exit code.
	ExitCode int
	// Errno is the error code that the tool reported, such as "ENOENT", or
	// "" when it reported none. It is read from the "Error ENOENT:" that
	// starts a line of Ceph and from the last "(2) No such file or directory"
	// of a line of the other tools, never guessed from the exit code.
	Errno string
	// Reason is the tool's own explanation: the lines of its error output
	// without Ceph's log lines, joined with "; ", at most
	// MaxNativeFailureReason bytes, with keys and the secrets that
	// RedactSecrets recognizes replaced. It can name resources of the
	// request, such as a pool.
	Reason string
}

// MaxNativeFailureReason bounds NativeFailure.Reason.
const MaxNativeFailureReason = 512

func (f *NativeFailure) Error() string {
	text := fmt.Sprintf("%s exited %d", f.Program, f.ExitCode)
	if f.Errno != "" {
		text += " (" + f.Errno + ")"
	}
	if f.Reason != "" {
		text += ": " + f.Reason
	}
	return text
}

// Is reports whether target is ErrNativeFailure, or the fs error that
// matches the reported errno.
func (f *NativeFailure) Is(target error) bool {
	switch target {
	case ErrNativeFailure:
		return true
	case fs.ErrNotExist:
		return f.Errno == "ENOENT"
	case fs.ErrPermission:
		return f.Errno == "EACCES" || f.Errno == "EPERM"
	case fs.ErrExist:
		return f.Errno == "EEXIST"
	}
	return false
}

// categoryError is a sentinel of a narrower failure that belongs to a
// category.
type categoryError struct {
	text string
	kind error
}

func (e *categoryError) Error() string { return e.text }

func (e *categoryError) Is(target error) bool { return target == e.kind }

// categories are the error categories.
var categories = []error{ErrInvalidRequest, ErrUnsupported, ErrImageCapability, ErrEnvironmentUnavailable, ErrNativeFailure, ErrSharedCluster}

// classified reports whether err belongs to a category.
func classified(err error) bool {
	for _, kind := range categories {
		if errors.Is(err, kind) {
			return true
		}
	}
	return false
}

// classificationOf returns the classification of err without any native
// output, for an error whose text hides that output: a *NativeFailure
// without its Reason, or the category and details of err. It returns nil
// for an unclassified error.
func classificationOf(err error) error {
	var failure *NativeFailure
	if errors.As(err, &failure) {
		quiet := *failure
		quiet.Reason = ""
		return &quiet
	}
	var operation *OperationError
	if errors.As(err, &operation) && operation.Kind != nil {
		return &OperationError{Kind: operation.Kind, Op: operation.Op, Target: operation.Target, Version: operation.Version,
			Image: operation.Image, Capability: operation.Capability}
	}
	for _, kind := range categories {
		if errors.Is(err, kind) {
			return &OperationError{Kind: kind}
		}
	}
	return nil
}

// quietError is an error whose text hides the native output of its cause;
// it keeps the cause's classification.
type quietError struct {
	visible error
	kept    error
}

func (e *quietError) Error() string { return e.visible.Error() }

func (e *quietError) Unwrap() []error { return []error{e.visible, e.kept} }

// quiet returns visible, keeping the classification of native and its
// context.Canceled or context.DeadlineExceeded; with a nil visible, only
// those.
func quiet(visible, native error) error {
	kept := classificationOf(native)
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(native, cause) && !errors.Is(visible, cause) {
			kept = errors.Join(kept, cause)
		}
	}
	switch {
	case kept == nil:
		return visible
	case visible == nil:
		return kept
	}
	return &quietError{visible: visible, kept: kept}
}

// classify marks err with kind without changing its text.
func classify(kind, err error) error {
	if err == nil {
		return nil
	}
	return &OperationError{Kind: kind, Err: err}
}

// invalidRequest is a refusal before any change.
func invalidRequest(format string, args ...any) error {
	return newInvalid(fmt.Errorf(format, args...))
}

// newInvalid marks err as a refused request with ErrInvalidRequest and keeps
// err as the cause, as it is, also when it is nil. It is the one construction
// that invalidRequest and the shared harness error contract use.
func newInvalid(err error) error {
	return &OperationError{Kind: ErrInvalidRequest, Err: err}
}

// linuxErrnos maps the Linux errno values that Ceph's tools print as "(N)
// text" to their names and strerror texts. The container's errno values are
// Linux values whatever the platform of the test process.
var linuxErrnos = map[int][2]string{
	1: {"EPERM", "Operation not permitted"}, 2: {"ENOENT", "No such file or directory"},
	3: {"ESRCH", "No such process"}, 4: {"EINTR", "Interrupted system call"},
	5: {"EIO", "Input/output error"}, 6: {"ENXIO", "No such device or address"},
	7: {"E2BIG", "Argument list too long"}, 9: {"EBADF", "Bad file descriptor"},
	11: {"EAGAIN", "Resource temporarily unavailable"}, 12: {"ENOMEM", "Cannot allocate memory"},
	13: {"EACCES", "Permission denied"}, 16: {"EBUSY", "Device or resource busy"},
	17: {"EEXIST", "File exists"}, 18: {"EXDEV", "Invalid cross-device link"},
	19: {"ENODEV", "No such device"}, 20: {"ENOTDIR", "Not a directory"},
	21: {"EISDIR", "Is a directory"}, 22: {"EINVAL", "Invalid argument"},
	27: {"EFBIG", "File too large"}, 28: {"ENOSPC", "No space left on device"},
	30: {"EROFS", "Read-only file system"}, 34: {"ERANGE", "Numerical result out of range"},
	36: {"ENAMETOOLONG", "File name too long"}, 38: {"ENOSYS", "Function not implemented"},
	39: {"ENOTEMPTY", "Directory not empty"}, 61: {"ENODATA", "No data available"},
	62: {"ETIME", "Timer expired"}, 71: {"EPROTO", "Protocol error"},
	75: {"EOVERFLOW", "Value too large for defined data type"},
	95: {"EOPNOTSUPP", "Operation not supported"}, 103: {"ECONNABORTED", "Software caused connection abort"},
	104: {"ECONNRESET", "Connection reset by peer"}, 108: {"ESHUTDOWN", "Cannot send after transport endpoint shutdown"},
	110: {"ETIMEDOUT", "Connection timed out"}, 111: {"ECONNREFUSED", "Connection refused"},
	113: {"EHOSTUNREACH", "No route to host"}, 114: {"EALREADY", "Operation already in progress"},
	115: {"EINPROGRESS", "Operation now in progress"}, 116: {"ESTALE", "Stale file handle"},
	122: {"EDQUOT", "Disk quota exceeded"}, 125: {"ECANCELED", "Operation canceled"},
	126: {"ENOKEY", "Required key not available"}, 127: {"EKEYEXPIRED", "Key has expired"},
	129: {"EKEYREJECTED", "Key was rejected by service"},
}

var (
	// cephErrnoPattern matches the ceph CLI's "Error ENOENT: ..." lines,
	// which start with it.
	cephErrnoPattern = regexp.MustCompile(`^Error (E[A-Z0-9]+):`)
	// toolErrnoPattern matches "(2) No such file or directory" of rbd,
	// rados and radosgw-admin.
	toolErrnoPattern = regexp.MustCompile(`\((-?\d{1,3})\) ([A-Z][^()\n]*)`)
	// nativeKeyPatterns match a Cephx key, also a truncated one, the value
	// that Ceph's key parsers print as "val=...", and another padded base64
	// secret of at least 40 characters.
	nativeKeyPatterns = []*regexp.Regexp{
		regexp.MustCompile(`\bval=\S+`),
		regexp.MustCompile(`\bA[Qg][A-Za-z0-9+/]{14,}={0,2}`),
		regexp.MustCompile(`[A-Za-z0-9+/]{38,}={1,2}`),
	}
)

// knownErrno reports whether name is an errno of linuxErrnos.
func knownErrno(name string) bool {
	for _, errno := range linuxErrnos {
		if errno[0] == name {
			return true
		}
	}
	return false
}

// nativeFailure describes a command that exited nonzero from its error
// output, or from the last line of its standard output when it wrote no
// error output.
func nativeFailure(program string, exitCode int, stdout, stderr []byte) *NativeFailure {
	lines := nativeReasonLines(string(stderr))
	if len(lines) == 0 {
		lines = nativeReasonLines(string(stdout))
		if len(lines) > 1 {
			lines = lines[len(lines)-1:]
		}
	}
	failure := &NativeFailure{Program: program, ExitCode: exitCode, Reason: nativeReason(lines)}
	// The line that ends the command reports the errno; the lines before it
	// explain it.
	for i := len(lines) - 1; i >= 0 && failure.Errno == ""; i-- {
		failure.Errno = nativeErrno(lines[i])
	}
	return failure
}

// nativeErrno returns the errno that line reports, or "".
func nativeErrno(line string) string {
	if match := cephErrnoPattern.FindStringSubmatch(line); match != nil && knownErrno(match[1]) {
		return match[1]
	}
	// The tools print the errno last, after names of the request that can
	// look like one.
	matches := toolErrnoPattern.FindAllStringSubmatch(line, -1)
	for i := len(matches) - 1; i >= 0; i-- {
		code, _ := strconv.Atoi(strings.TrimPrefix(matches[i][1], "-"))
		if errno, ok := linuxErrnos[code]; ok && strings.HasPrefix(matches[i][2], errno[1]) {
			return errno[0]
		}
	}
	return ""
}

// cephLogLine matches the log lines of Ceph's tools, which start with a
// timestamp, a thread and a level.
var cephLogLine = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T[0-9:.]+[+-]\d{4} [0-9a-f]+ +-?\d+ `)

// nativeReasonLines returns the lines of output that explain a failure: its
// lines with text, without Ceph's log lines, or the last line when it has
// only those.
func nativeReasonLines(output string) []string {
	var lines []string
	last := ""
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case cephLogLine.MatchString(line):
			last = line
		default:
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 && last != "" {
		lines = []string{last}
	}
	return lines
}

// nativeReason redacts lines of native output, joins them and bounds the
// result.
func nativeReason(lines []string) string {
	redacted := make([]string, len(lines))
	for i, line := range lines {
		line = diagnosticsRedactor(nil)(line)
		line = diagnosticKeyringPattern.ReplaceAllString(line, "key = [REDACTED]")
		for _, pattern := range nativeKeyPatterns {
			line = pattern.ReplaceAllString(line, "[REDACTED]")
		}
		redacted[i] = strings.Map(func(r rune) rune {
			if unicode.IsControl(r) {
				return ' '
			}
			return r
		}, strings.ToValidUTF8(line, "?"))
	}
	reason := strings.Join(redacted, "; ")
	if len(reason) > MaxNativeFailureReason {
		cut := MaxNativeFailureReason - len("...")
		for cut > 0 && !utf8.RuneStart(reason[cut]) {
			cut--
		}
		reason = reason[:cut] + "..."
	}
	return reason
}

// invalid marks err as a refused request.
func invalid(err error) error { return classify(ErrInvalidRequest, err) }
