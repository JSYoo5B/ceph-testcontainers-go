package debugoverhead_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	mobycl "github.com/moby/moby/client"
)

const debugOverheadLogLimit = 16 << 20

type containerGeneration struct {
	ID        string `json:"container_id"`
	StartedAt string `json:"started_at"`
}

func (g containerGeneration) valid() bool { return g.ID != "" && g.StartedAt != "" }

type debugOverheadSample struct {
	Generation containerGeneration `json:"generation"`
	// StatsIdentity distinguishes an ID supplied by the response from a
	// one-shot response bound only by the addressed, twice-inspected endpoint.
	StatsIdentity string    `json:"stats_identity"`
	Read          time.Time `json:"read"`
	CPUNanos      uint64    `json:"cpu_total_usage_ns"`
	Memory        uint64    `json:"memory_usage_bytes"`
}

// parseDebugOverheadSample keeps absolute Docker counters, not docker stats'
// CPU percentage or a guessed process RSS. Missing counters are not zeroes.
// Moby 28's one-shot GetContainerStats path can omit id; sampleDebugOverhead
// binds that response to the addressed container and checks its generation
// before and after the request. An ID that is present must still match.
func parseDebugOverheadSample(gen containerGeneration, body []byte) (debugOverheadSample, error) {
	fields := debugOverheadFields{JSON: "invalid", Generation: "incomplete", ResponseID: "unknown", Read: "unknown", CPU: "unknown", Memory: "unknown"}
	if gen.valid() {
		fields.Generation = "complete"
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(body, &raw) != nil || raw == nil {
		return debugOverheadSample{}, fields.refusal()
	}
	fields.JSON = "valid"
	responseID, idState := debugOverheadString(raw, "id")
	fields.ResponseID = idState
	if idState == "valid" {
		switch {
		case responseID == "":
			fields.ResponseID = "empty"
		case responseID != gen.ID:
			fields.ResponseID = "contradictory"
		default:
			fields.ResponseID = "matched"
		}
	}
	stamp, readState := debugOverheadString(raw, "read")
	fields.Read = readState
	read, err := time.Parse(time.RFC3339Nano, stamp)
	if readState == "valid" && (err != nil || read.Year() < 2000) {
		fields.Read = "invalid"
	}
	cpu, cpuState := debugOverheadCounter(raw, "cpu_stats", "cpu_usage", "total_usage")
	memory, memoryState := debugOverheadCounter(raw, "memory_stats", "usage")
	fields.CPU, fields.Memory = cpuState, memoryState
	if memoryState == "valid" && memory > math.MaxInt64 {
		fields.Memory = "out_of_range"
	}
	if !gen.valid() || (fields.ResponseID != "missing" && fields.ResponseID != "matched") || fields.Read != "valid" || fields.CPU != "valid" || fields.Memory != "valid" {
		return debugOverheadSample{}, fields.refusal()
	}
	identity := "endpoint-generation"
	if fields.ResponseID == "matched" {
		identity = "response-and-endpoint-generation"
	}
	return debugOverheadSample{Generation: gen, StatsIdentity: identity, Read: read, CPUNanos: cpu, Memory: memory}, nil
}

// Refusals retain only fixed field states, never raw JSON, IDs, invalid time
// text or unknown response fields. This keeps failure evidence bounded and
// allowlisted even when a malformed response contains sensitive data.
type debugOverheadFields struct {
	JSON, Generation, ResponseID, Read, CPU, Memory string
}

func (f debugOverheadFields) refusal() error {
	return fmt.Errorf("debug accounting refused: json=%s generation=%s response_id=%s read=%s cpu_total_usage=%s memory_usage=%s", f.JSON, f.Generation, f.ResponseID, f.Read, f.CPU, f.Memory)
}

func debugOverheadString(raw map[string]json.RawMessage, name string) (string, string) {
	data, ok := raw[name]
	if !ok {
		return "", "missing"
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return "", "null"
	}
	var value string
	if json.Unmarshal(data, &value) != nil {
		return "", "invalid"
	}
	return value, "valid"
}

func debugOverheadCounter(raw map[string]json.RawMessage, path ...string) (uint64, string) {
	for i, name := range path {
		data, ok := raw[name]
		if !ok {
			return 0, "missing"
		}
		data = bytes.TrimSpace(data)
		if bytes.Equal(data, []byte("null")) {
			return 0, "null"
		}
		if i == len(path)-1 {
			value, err := strconv.ParseUint(string(data), 10, 64)
			if err != nil {
				return 0, "invalid"
			}
			return value, "valid"
		}
		var next map[string]json.RawMessage
		if json.Unmarshal(data, &next) != nil || next == nil {
			return 0, "invalid"
		}
		raw = next
	}
	return 0, "missing"
}

func debugOverheadDelta(before, after debugOverheadSample) (uint64, int64, error) {
	if !before.Generation.valid() || before.Generation != after.Generation || !after.Read.After(before.Read) || after.CPUNanos < before.CPUNanos {
		return 0, 0, errors.New("debug accounting generation, time or cumulative CPU is not comparable")
	}
	return after.CPUNanos - before.CPUNanos, int64(after.Memory) - int64(before.Memory), nil
}

func sampleDebugOverhead(parent context.Context, docker *mobycl.Client, gen containerGeneration) (debugOverheadSample, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	check := func() error {
		result, err := docker.ContainerInspect(ctx, gen.ID, mobycl.ContainerInspectOptions{})
		if err != nil {
			return err
		}
		state := result.Container.State
		if state == nil || !state.Running || state.Paused || result.Container.ID != gen.ID || state.StartedAt != gen.StartedAt {
			return errors.New("debug accounting target generation changed or is not running")
		}
		return nil
	}
	if err := check(); err != nil {
		return debugOverheadSample{}, err
	}
	stats, err := docker.ContainerStats(ctx, gen.ID, mobycl.ContainerStatsOptions{})
	if err != nil {
		return debugOverheadSample{}, err
	}
	body, readErr := io.ReadAll(io.LimitReader(stats.Body, (2<<20)+1))
	closeErr := stats.Body.Close()
	if err := errors.Join(readErr, closeErr, ctx.Err()); err != nil {
		return debugOverheadSample{}, err
	}
	if len(body) > 2<<20 {
		return debugOverheadSample{}, errors.New("debug accounting stats body exceeds 2 MiB")
	}
	if err := check(); err != nil {
		return debugOverheadSample{}, err
	}
	return parseDebugOverheadSample(gen, body)
}

// debugOverheadLogBytes counts only stdout/stderr payload bytes. Docker's
// framing and timestamps are excluded; the caller uses the engine's clock
// to select the window. A truncated/invalid/over-limit stream is not a count.
func debugOverheadLogBytes(reader io.Reader, tty bool, limit int64) (int64, error) {
	if limit <= 0 {
		return 0, errors.New("debug log limit must be positive")
	}
	if tty {
		n, err := io.Copy(io.Discard, io.LimitReader(reader, limit+1))
		if n > limit {
			return n, errors.New("debug log payload exceeds limit")
		}
		return n, err
	}
	var total int64
	var header [8]byte
	for {
		_, err := io.ReadFull(reader, header[:])
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		if (header[0] != 1 && header[0] != 2) || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return total, errors.New("invalid debug Docker log frame")
		}
		n := int64(binary.BigEndian.Uint32(header[4:]))
		if n > limit-total {
			return total, errors.New("debug log payload exceeds limit")
		}
		copied, err := io.CopyN(io.Discard, reader, n)
		total += copied
		if err != nil {
			return total, err
		}
	}
}

func debugOverheadClock(parent context.Context, docker *mobycl.Client) (time.Time, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, info.Info.SystemTime)
}

func collectDebugOverheadLogs(parent context.Context, docker *mobycl.Client, gen containerGeneration, tty bool, start, end time.Time) (int64, error) {
	if !end.After(start) {
		return 0, errors.New("debug log window did not advance")
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	stamp := func(t time.Time) string { return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond()) }
	reader, err := docker.ContainerLogs(ctx, gen.ID, mobycl.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Since: stamp(start), Until: stamp(end)})
	if err != nil {
		return 0, err
	}
	n, readErr := debugOverheadLogBytes(reader, tty, debugOverheadLogLimit)
	return n, errors.Join(readErr, reader.Close(), ctx.Err())
}

type debugOverheadWorkloadReport struct {
	Writes       int      `json:"writes"`
	Reads        int      `json:"reads"`
	ObjectBytes  int      `json:"object_bytes"`
	SHA256       string   `json:"sha256"`
	ElapsedNanos uint64   `json:"elapsed_ns"`
	PairNanos    []uint64 `json:"write_read_pair_ns"`
}

// Every arm runs exactly this script on the already-created client runtime.
// Per-pair timing is inside one Python process and excludes connection setup;
// OSD accounting includes the connection, the workload and a fixed drain wait.
const debugOverheadWorkload = `
import hashlib,json,rados,sys,time
pool=sys.argv[1]
n=int(sys.argv[2]); size=int(sys.argv[3])
payload=bytes((i*37+11)%256 for i in range(size))
cluster=rados.Rados(conffile='/etc/ceph/ceph.conf')
cluster.connect()
io=cluster.open_ioctx(pool)
samples=[]
started=time.monotonic_ns()
for _ in range(n):
    begin=time.monotonic_ns()
    io.write_full('same-object',payload)
    actual=io.read('same-object',size+1,0)
    assert actual==payload
    samples.append(time.monotonic_ns()-begin)
elapsed=time.monotonic_ns()-started
io.close(); cluster.shutdown()
print(json.dumps(dict(writes=n,reads=n,object_bytes=size,sha256=hashlib.sha256(payload).hexdigest(),elapsed_ns=elapsed,write_read_pair_ns=samples)))
`
