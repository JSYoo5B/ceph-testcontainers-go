//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"strings"
	"syscall"
	"testing"
)

// These controls feed synthetic write series to judgeByteQuotaSeries. They
// are not native quota observations.
func TestLiveByteQuotaJudge(t *testing.T) {
	const limit, chunk, overshoot = 4 << 20, 64 << 10, 32 << 20
	series := func(acked int64, errno int, change func(*byteQuotaSeries)) byteQuotaSeries {
		l, size := uint64(limit), acked
		s := byteQuotaSeries{Limit: &l, Samples: 3, Acked: &acked, Attempts: uint64(acked/chunk) + 1, Errno: &errno, Size: &size}
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
		"no sample":     {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { s.Samples = 0 }), want: "incomplete"},
		"missing errno": {series: series(limit, int(syscall.EDQUOT), func(s *byteQuotaSeries) { s.Errno = nil }), want: "incomplete"},
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
