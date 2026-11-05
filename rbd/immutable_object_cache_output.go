package rbd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// immutableObjectCacheOutput is the standard output of one operation of
// immutableObjectCacheScript, before validation.
type immutableObjectCacheOutput struct {
	op  string
	out []byte
}

// immutableObjectCacheFields are the key=value lines of one operation's
// output, each key at most once and each one of the keys that the operation
// prints.
type immutableObjectCacheFields struct {
	op     string
	values map[string]string
}

// fields splits the output into its key=value lines. A line that is not
// key=value, a repeated key or a key outside keys is an error.
func (o immutableObjectCacheOutput) fields(keys ...string) (immutableObjectCacheFields, error) {
	f := immutableObjectCacheFields{op: o.op, values: map[string]string{}}
	for _, line := range strings.Split(string(o.out), "\n") {
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		switch {
		case !ok:
			return f, f.errorf("", "the line %q is not key=value", line)
		case !slices.Contains(keys, key):
			return f, f.errorf(key, "the field is not one of %s", strings.Join(keys, ", "))
		}
		if _, seen := f.values[key]; seen {
			return f, f.errorf(key, "the field is repeated")
		}
		f.values[key] = value
	}
	return f, nil
}

func (f immutableObjectCacheFields) errorf(key, format string, args ...any) error {
	where := fmt.Sprintf("parse the immutable object cache %s output", f.op)
	if key != "" {
		where += ": " + key
	}
	return fmt.Errorf("%s: %s", where, fmt.Sprintf(format, args...))
}

func (f immutableObjectCacheFields) required(key string) (string, error) {
	value, ok := f.values[key]
	if !ok {
		return "", f.errorf(key, "the field is missing")
	}
	return value, nil
}

// flag reads a required 0 or 1.
func (f immutableObjectCacheFields) flag(key string) (bool, error) {
	value, err := f.required(key)
	if err != nil {
		return false, err
	}
	switch value {
	case "0":
		return false, nil
	case "1":
		return true, nil
	}
	return false, f.errorf(key, "%q is neither 0 nor 1", value)
}

// oneOf reads a required value from allowed.
func (f immutableObjectCacheFields) oneOf(key string, allowed ...string) (string, error) {
	value, err := f.required(key)
	if err != nil {
		return "", err
	}
	if !slices.Contains(allowed, value) {
		return "", f.errorf(key, "%q is not one of %s", value, strings.Join(allowed, ", "))
	}
	return value, nil
}

// count reads a required unsigned decimal that fits in bits.
func (f immutableObjectCacheFields) count(key string, bits int) (uint64, error) {
	value, err := f.required(key)
	if err != nil {
		return 0, err
	}
	return f.parseCount(key, value, bits)
}

func (f immutableObjectCacheFields) parseCount(key, value string, bits int) (uint64, error) {
	n, err := strconv.ParseUint(value, 10, bits)
	if errors.Is(err, strconv.ErrRange) {
		return 0, f.errorf(key, "%s does not fit in %d bits", value, bits)
	}
	if err != nil {
		return 0, f.errorf(key, "%q is not an unsigned decimal", value)
	}
	return n, nil
}

// immutableObjectCacheInspection is the inspect operation's report of the
// client container. path is none, empty, full or other.
type immutableObjectCacheInspection struct {
	binary, plugin, socket bool
	path                   string
}

func inspectImmutableObjectCache(ctx context.Context, client testcontainers.Container, config ImmutableObjectCache) (immutableObjectCacheInspection, error) {
	out, err := immutableObjectCacheOp(ctx, client, "inspect", config.Path, config.Socket)
	if err != nil {
		return immutableObjectCacheInspection{}, err
	}
	return parseImmutableObjectCacheInspection(out)
}

func parseImmutableObjectCacheInspection(out immutableObjectCacheOutput) (immutableObjectCacheInspection, error) {
	var state immutableObjectCacheInspection
	f, err := out.fields("binary", "plugin", "socket", "path")
	if err != nil {
		return state, err
	}
	if state.binary, err = f.flag("binary"); err != nil {
		return state, err
	}
	if state.plugin, err = f.flag("plugin"); err != nil {
		return state, err
	}
	if state.socket, err = f.flag("socket"); err != nil {
		return state, err
	}
	state.path, err = f.oneOf("path", "none", "empty", "full", "other")
	return state, err
}

// immutableObjectCacheStart is the start operation's report: the
// daemonizing parent's exit status, the PID file's PID (0 when it is empty)
// and whether that process opened the socket.
type immutableObjectCacheStart struct {
	exit  int
	pid   int
	ready bool
}

func (l *ImmutableObjectCacheLease) start(ctx context.Context) (immutableObjectCacheStart, error) {
	c := l.config
	out, err := immutableObjectCacheOp(ctx, l.client, "start", c.ID, c.Path, c.Socket,
		strconv.FormatInt(c.MaxSize, 10), l.adminSocket, l.pidFile)
	if err != nil {
		return immutableObjectCacheStart{}, err
	}
	return parseImmutableObjectCacheStart(out)
}

func parseImmutableObjectCacheStart(out immutableObjectCacheOutput) (immutableObjectCacheStart, error) {
	var started immutableObjectCacheStart
	f, err := out.fields("exit", "pid", "ready")
	if err != nil {
		return started, err
	}
	exit, err := f.count("exit", 8)
	if err != nil {
		return started, err
	}
	started.exit = int(exit)
	pid, err := f.required("pid")
	if err != nil {
		return started, err
	}
	if pid != "" {
		n, err := f.parseCount("pid", pid, strconv.IntSize-1)
		if err != nil {
			return started, err
		}
		started.pid = int(n)
	}
	if started.ready, err = f.flag("ready"); err != nil {
		return started, err
	}
	if started.ready && started.pid == 0 {
		return started, f.errorf("pid", "a ready daemon has no PID")
	}
	return started, nil
}

// parseImmutableObjectCacheStatus validates the status operation's report:
// perf is required while the daemon runs, and files and bytes while the
// cache directory exists.
func parseImmutableObjectCacheStatus(out immutableObjectCacheOutput, pid int) (ImmutableObjectCacheStatus, error) {
	status := ImmutableObjectCacheStatus{PID: pid}
	f, err := out.fields("running", "perf", "dir", "files", "bytes")
	if err != nil {
		return status, err
	}
	if status.Running, err = f.flag("running"); err != nil {
		return status, err
	}
	dir, err := f.flag("dir")
	if err != nil {
		return status, err
	}
	if dir {
		files, err := f.count("files", strconv.IntSize-1)
		if err != nil {
			return status, err
		}
		bytes, err := f.count("bytes", 63)
		if err != nil {
			return status, err
		}
		status.CachedObjects, status.CachedBytes = int(files), int64(bytes)
	}
	if !status.Running {
		return status, nil
	}
	perf, err := f.required("perf")
	if err != nil {
		return status, err
	}
	var counters struct {
		Objecter struct {
			OpR *uint64 `json:"op_r"`
		} `json:"objecter"`
	}
	if err := json.Unmarshal([]byte(perf), &counters); err != nil {
		return status, fmt.Errorf("parse the immutable object cache counters: %w", err)
	}
	if counters.Objecter.OpR == nil {
		return status, errors.New("parse the immutable object cache counters: objecter.op_r is missing")
	}
	status.RADOSReads = *counters.Objecter.OpR
	return status, nil
}

// parseImmutableObjectCacheFlag validates an output whose only field is the
// required flag key.
func parseImmutableObjectCacheFlag(out immutableObjectCacheOutput, key string) (bool, error) {
	f, err := out.fields(key)
	if err != nil {
		return false, err
	}
	return f.flag(key)
}
