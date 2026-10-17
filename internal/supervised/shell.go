package supervised

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// ShellMarkerDir holds the markers of ShellScript invocations.
	ShellMarkerDir = "/tmp/tc-supervised-sh"
	// ShellStopGrace is how long, in seconds, ShellScript waits after SIGTERM
	// before it sends SIGKILL.
	ShellStopGrace = 2
	// shellCancelWait is how long, in seconds, ShellCancelScript waits for the
	// supervisor to acknowledge: a second to notice the cancel marker, the
	// grace and a second to notice the acknowledgment, with room to spare
	// within CancelTimeout.
	shellCancelWait = 8
	// ShellEndPrefix starts the last line of a ShellScript's standard error,
	// which carries the command's exit status; RunShell removes it.
	ShellEndPrefix = "tc-supervised-sh: end "
	// ShellDeadlineLine is the line of standard error before the end line of
	// a ShellScript that stopped its command at its limit; RunShell removes
	// it and reports Stopped.
	ShellDeadlineLine = "tc-supervised-sh: deadline"
)

// ShellScript is a supervisor for containers that have a POSIX shell but no
// python3, such as the RGW role image: it needs only sh, its kill builtin,
// sleep, cat, awk, mkdir and rm, and a writable /tmp. Its arguments are the
// invocation's marker, a limit in seconds (0 for none), the stop grace in
// seconds and the command.
//
// It follows Script's marker protocol. It claims the marker before it starts
// the command, writing the container instance into it, and starts nothing if
// ShellCancelScript claimed it first. A watcher stops the command, with
// SIGTERM and SIGKILL after the grace, when a cancel marker appears or the
// limit passes; it holds none of the exec's streams, so a command that ends
// returns at once. Removing the marker after the command was reaped
// acknowledges a cancellation. The command is stopped by its PID: dash gives a
// background job no process group of its own without a terminal, so a
// command that starts processes of its own may leave them behind.
//
// Both waits discard the shell's own standard error: bash, the sh of the
// official image, reports a job that a signal ended there.
//
// Its last line of standard error is ShellEndPrefix and the command's exit
// status, after ShellDeadlineLine when the limit stopped it. A run without
// that line did not finish: a signal killed the supervisor.
const ShellScript = `marker=$1 limit=$2 grace=$3
shift 3
mkdir -p "${marker%/*}" 2>/dev/null
instance=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null)/$(awk '{sub(/.*\) /, ""); print $20}' /proc/1/stat 2>/dev/null)
if ! (set -C; echo "run $instance" > "$marker") 2>/dev/null; then
    rm -f "$marker"
    exit 0
fi
"$@" &
pid=$!
(
    waited=0
    while kill -0 "$pid" 2>/dev/null; do
        [ -e "$marker.cancel" ] && break
        if [ "$limit" -gt 0 ] && [ "$waited" -ge "$limit" ]; then
            : > "$marker.deadline"
            break
        fi
        sleep 1
        waited=$((waited + 1))
    done
    if kill -TERM "$pid" 2>/dev/null; then
        waited=0
        while [ "$waited" -lt "$grace" ] && kill -0 "$pid" 2>/dev/null; do
            sleep 1
            waited=$((waited + 1))
        done
        kill -KILL "$pid" 2>/dev/null
    fi
) </dev/null >/dev/null 2>&1 &
watcher=$!
wait "$pid" 2>/dev/null
code=$?
kill "$watcher" 2>/dev/null
wait "$watcher" 2>/dev/null
stopped=
[ -e "$marker.deadline" ] && stopped=1
rm -f "$marker.deadline" "$marker"
[ -n "$stopped" ] && echo "` + ShellDeadlineLine + `" >&2
echo "` + ShellEndPrefix + `$code" >&2
exit "$code"
`

// ShellCancelScript cancels one ShellScript invocation through its marker,
// as CancelScript does for Script: it claims a marker that does not exist,
// confirms one of its own claim or of an earlier container instance, and
// otherwise writes the cancel marker and waits until the supervisor removes
// the marker. It exits 3 when that does not happen within its wait.
const ShellCancelScript = `marker=$1 wait=$2
mkdir -p "${marker%/*}" 2>/dev/null
(set -C; echo claim > "$marker") 2>/dev/null && exit 0
record=$(cat "$marker" 2>/dev/null) || exit 0
[ "$record" = claim ] && exit 0
instance=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null)/$(awk '{sub(/.*\) /, ""); print $20}' /proc/1/stat 2>/dev/null)
known() { case $1 in "" | /* | */) return 1 ;; esac; }
case $record in
"run "*)
    recorded=${record#run }
    known "$recorded" && known "$instance" && [ "$recorded" != "$instance" ] && exit 0
    ;;
esac
: > "$marker.cancel"
waited=0
while [ -e "$marker" ]; do
    [ "$waited" -ge "$wait" ] && exit 3
    sleep 1
    waited=$((waited + 1))
done
rm -f "$marker.cancel"
`

// ShellReport is the outcome of one RunShell that the supervisor finished.
type ShellReport struct {
	Stdout, Stderr []byte
	ExitCode       int
	// Stopped means that the limit passed and the supervisor stopped the
	// command.
	Stopped bool
}

// RunShell runs argv in ctr under ShellScript with a limit in seconds (0 for
// none). When the call ends before the supervisor reports, because ctx ended
// or the exec failed, CancelShell stops the invocation inside the container
// before RunShell returns, also when Docker starts it only later; when that
// fails, the error holds a NotStoppedError, and a later CancelShell with its
// marker can confirm the stop. label names the command in errors.
func RunShell(ctx context.Context, ctr testcontainers.Container, label string, limit int, argv ...string) (ShellReport, error) {
	if err := ctx.Err(); err != nil {
		return ShellReport{}, err
	}
	if ctr == nil {
		return ShellReport{}, errors.New(label + " container is unavailable")
	}
	marker := ShellMarkerDir + "/" + uuid.NewString()
	command := append([]string{"sh", "-c", ShellScript, "sh", marker, strconv.Itoa(max(limit, 0)), strconv.Itoa(ShellStopGrace)}, argv...)
	code, reader, err := ctr.Exec(ctx, command)
	var stdout, stderr bytes.Buffer
	if err != nil {
		err = &BackendError{Op: "exec " + label + " supervisor", Err: err}
	} else if reader != nil {
		if _, copyErr := stdcopy.StdCopy(&stdout, &stderr, reader); copyErr != nil {
			err = &BackendError{Op: "read " + label + " supervisor", Err: copyErr}
		}
	}
	end := -1
	if err == nil {
		end = shellEnd(&stderr)
	}
	if err == nil && ctx.Err() == nil && end == code {
		report := ShellReport{Stdout: stdout.Bytes(), ExitCode: code}
		report.Stderr, report.Stopped = bytes.CutSuffix(stderr.Bytes(), []byte(ShellDeadlineLine+"\n"))
		return report, nil
	}
	// The supervisor did not report: the exec may still run, or start later,
	// or a signal killed the supervisor and left the command.
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = fmt.Errorf("%s supervisor ended without its report: exit=%d", label, code)
	}
	if cancelErr := CancelShell(ctx, ctr, label, marker); cancelErr != nil {
		return ShellReport{}, &NotStoppedError{Marker: marker, Err: errors.Join(err, cancelErr)}
	}
	return ShellReport{}, err
}

// shellEnd removes the end line from stderr and returns its status, or -1
// when stderr does not end with one.
func shellEnd(stderr *bytes.Buffer) int {
	data := stderr.Bytes()
	if !bytes.HasSuffix(data, []byte("\n")) {
		return -1
	}
	start := bytes.LastIndexByte(data[:len(data)-1], '\n') + 1
	line := string(data[start : len(data)-1])
	status, ok := strings.CutPrefix(line, ShellEndPrefix)
	if !ok {
		return -1
	}
	code, err := strconv.Atoi(status)
	if err != nil || code < 0 {
		return -1
	}
	stderr.Truncate(start)
	return code
}

// CancelShell stops the ShellScript invocation that owns marker in ctr and
// returns once it can no longer run its command. It uses its own bounded
// context, because the caller's may have ended.
func CancelShell(ctx context.Context, ctr testcontainers.Container, label, marker string) error {
	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), CancelTimeout)
	defer cancel()
	code, reader, err := ctr.Exec(cancelCtx, []string{"sh", "-c", ShellCancelScript, "sh", marker, strconv.Itoa(shellCancelWait)})
	if err == nil && reader != nil {
		_, _ = stdcopy.StdCopy(discard{}, discard{}, reader)
	}
	if err != nil {
		return &BackendError{Op: "cancel " + label + " command", Err: err}
	}
	if code != 0 {
		return fmt.Errorf("cancel %s command: canceler exited %d", label, code)
	}
	return nil
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
