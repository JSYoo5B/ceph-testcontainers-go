//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These controls feed synthetic write series to judgeByteQuotaSeries. They
// are not native quota observations.
func TestLiveByteQuotaJudge(t *testing.T) {
	const limit, chunk, overshoot = 4 << 20, 64 << 10, 32 << 20
	series := func(acked int64, errno int, change func(*byteQuotaSeries)) byteQuotaSeries {
		l, size, observed, elapsed := uint64(limit), acked, int64(1), int64(10)
		s := byteQuotaSeries{Limit: &l, Samples: 3, ObservedMS: &observed, ElapsedMS: &elapsed, Acked: &acked, Attempts: uint64(acked/chunk) + 1, Errno: &errno, Size: &size}
		if change != nil {
			change(&s)
		}
		return s
	}
	for name, test := range map[string]struct {
		series byteQuotaSeries
		want   string
	}{
		"denied at the limit":        {series: series(limit, int(syscall.EDQUOT), nil)},
		"denied after overshoot":     {series: series(limit+3*chunk, int(syscall.EDQUOT), nil)},
		"denied at the bound":        {series: series(limit+overshoot, int(syscall.EDQUOT), nil)},
		"never denied":               {series: series(limit+overshoot, 0, nil), want: "no write failed"},
		"another errno":              {series: series(limit, int(syscall.ENOSPC), nil), want: "not EDQUOT"},
		"denied below the limit":     {series: series(limit-chunk, int(syscall.EDQUOT), nil), want: "still fit below"},
		"denied beyond the bound":    {series: series(limit+overshoot+chunk, int(syscall.EDQUOT), nil), want: "more than"},
		"size differs":               {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { *s.Size -= 1 }), want: "file size"},
		"partial chunk acknowledged": {series: series(limit+1, int(syscall.EDQUOT), nil), want: "chunks"},
		"retried attempts":           {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { s.Attempts++ }), want: "attempts"},
		"other limit observed": {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) {
			other := uint64(limit + 1)
			s.Limit = &other
		}), want: "observed limit"},
		"late observation": {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { *s.ObservedMS = 90_000 }), want: "deadline"},
		"late write":       {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { *s.ElapsedMS = 120_000 }), want: "deadline"},
		"missing timing":   {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { s.ElapsedMS = nil }), want: "incomplete"},
		"no sample":        {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { s.Samples = 0 }), want: "incomplete"},
		"missing errno":    {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { s.Errno = nil }), want: "incomplete"},
	} {
		t.Run(name, func(t *testing.T) {
			err := judgeByteQuotaSeries(test.series, limit, chunk, overshoot)
			switch {
			case test.want == "" && err != nil:
				t.Fatalf("refused: %v", err)
			case test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)):
				t.Fatalf("got %v, want an error containing %q", err, test.want)
			}
		})
	}
}

func TestByteQuotaPattern(t *testing.T) {
	data := byteQuotaPattern(600)
	if data[0] != 0 || data[250] != 250 || data[251] != 0 || data[599] != byte(599%251) {
		t.Fatalf("pattern: %v", data[:4])
	}
	if byteQuotaDigest(600) == byteQuotaDigest(599) {
		t.Fatal("digests of different lengths agree")
	}
}

// These controls execute the byte actor's actual observation and write helpers.
// The synchronous calls are substitutes; they establish timing admission only.
func TestLiveByteQuotaPhaseDeadlines(t *testing.T) {
	const setup = `
import types
class NativeError(Exception):
    def __init__(self, number): self.errno=number
cephfs=types.SimpleNamespace(Error=NativeError)
now=[0.0]
class FS:
    def write(self, fd, data, offset): return len(data)
    def fsync(self, fd, data): pass
fs=FS()
`
	for name, control := range map[string]string{
		"late matching observation": `
def sample():
    now[0]=90.5
    return 4096
try:
    observe_byte_quota(sample, 4096, clock=lambda:now[0])
    raise AssertionError("late observation admitted")
except TimeoutError: pass
`,
		"late write": `
def write(fd,data,offset):
    now[0]=120.5
    return len(data)
fs.write=write
try:
    byte_quota_series(fs,1,4096,1024,4096,clock=lambda:now[0])
    raise AssertionError("late write admitted")
except TimeoutError: pass
`,
		"late fsync success": `
def fsync(fd,data):
    now[0]=120.5
fs.fsync=fsync
try:
    byte_quota_write(fs,1,0,1024,120,lambda:now[0])
    raise AssertionError("late fsync admitted")
except TimeoutError: pass
`,
		"late fsync denial": `
def fsync(fd,data):
    now[0]=120.5
    raise NativeError(122)
fs.fsync=fsync
try:
    byte_quota_series(fs,1,4096,1024,4096,clock=lambda:now[0])
    raise AssertionError("late EDQUOT admitted")
except TimeoutError: pass
`,
		"timely complete series": `
observation=observe_byte_quota(lambda:4096,4096,clock=lambda:now[0])
original=fs.write
def write(fd,data,offset):
    if offset==4096: raise NativeError(122)
    return original(fd,data,offset)
fs.write=write
series=byte_quota_series(fs,1,4096,1024,4096,clock=lambda:now[0],sleep=lambda n:now.__setitem__(0,now[0]+n))
assert observation["samples"]==1 and observation["observed_ms"]==0
assert series["acked"]==4096 and series["attempts"]==5 and series["errno"]==122
assert 0 <= series["elapsed_ms"] < 120000
`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, "python3", "-c", cephFSByteQuotaOperations+setup+control).CombinedOutput()
			if err != nil {
				t.Fatalf("byte quota timing control: %v: %s", err, out)
			}
		})
	}
	for _, phase := range []string{"observation", "write", "fsync"} {
		t.Run("blocked "+phase, func(t *testing.T) {
			control := `
import ctypes
def blocked(*args):
    print("blocked-call",flush=True)
    ctypes.CDLL(None).sleep(10)
    return 4096
`
			switch phase {
			case "observation":
				control += "observe_byte_quota(blocked,4096,timeout=0.1)\n"
			case "write":
				control += "fs.write=blocked\nbyte_quota_series(fs,1,4096,1024,4096,timeout=0.1)\n"
			case "fsync":
				control += "fs.fsync=blocked\nbyte_quota_series(fs,1,4096,1024,4096,timeout=0.1)\n"
			}
			control += "print('WORKLOAD_ADMITTED',flush=True)\n"
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, "python3", "-c", cephFSByteQuotaOperations+setup+control).CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || ctx.Err() != nil || !strings.Contains(exited.ProcessState.String(), "alarm clock") || string(out) != "blocked-call\n" {
				t.Fatalf("phase watchdog: err=%v context=%v output=%q", err, ctx.Err(), out)
			}
		})
	}
}
