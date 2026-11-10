//go:build all || integration

package rgw_single_part_multipart_read_test

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// spRead is one GET or HEAD of a part number: RGW's answer, or the way the
// request ended without one.
type spRead struct {
	Method string
	Part   int
	// NotSent is why the request was never made, when it was not.
	NotSent string
	// TimedOut is a request that got no answer within Deadline; Err is any
	// other transport failure.
	TimedOut bool
	Deadline time.Duration
	Err      string

	Status      int
	Length      int64
	PartsCount  string
	Code        string
	BodyMatches bool
}

func (r spRead) String() string {
	switch {
	case r.NotSent != "":
		return fmt.Sprintf("%s partNumber=%d not sent: %s", r.Method, r.Part, r.NotSent)
	case r.TimedOut:
		return fmt.Sprintf("%s partNumber=%d no answer within %s", r.Method, r.Part, r.Deadline)
	case r.Err != "":
		return fmt.Sprintf("%s partNumber=%d transport error %q", r.Method, r.Part, r.Err)
	}
	return fmt.Sprintf("%s partNumber=%d status=%d content-length=%d parts-count=%q code=%q body-matches=%t",
		r.Method, r.Part, r.Status, r.Length, r.PartsCount, r.Code, r.BodyMatches)
}

// spProcess is one reading of the gateway's process 1: when it started and
// the CPU time it has used, both in clock ticks.
type spProcess struct {
	Start, CPU uint64
}

// parseProcessStat reads the start time and the user plus system CPU time
// from the text of /proc/<pid>/stat. The command name is in parentheses and
// may itself hold spaces and parentheses, so the fields are counted from the
// last closing one.
func parseProcessStat(text string) (spProcess, error) {
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		return spProcess{}, fmt.Errorf("no command name in process stat")
	}
	// fields[0] is field 3 (state); utime, stime and starttime are fields
	// 14, 15 and 22.
	fields := strings.Fields(text[end+1:])
	if len(fields) < 20 {
		return spProcess{}, fmt.Errorf("process stat has %d fields after the command name, want at least 20", len(fields))
	}
	var values [3]uint64
	for i, index := range [3]int{11, 12, 19} {
		value, err := strconv.ParseUint(fields[index], 10, 64)
		if err != nil {
			return spProcess{}, fmt.Errorf("process stat field %d: %w", index+3, err)
		}
		values[i] = value
	}
	return spProcess{Start: values[2], CPU: values[0] + values[1]}, nil
}

// spStall is the gateway after a part read that got no answer: process 1
// before the read, and twice after the deadline, Window apart.
type spStall struct {
	Before, First, Second spProcess
	Window                time.Duration
	// TicksPerSecond is the clock tick rate the CPU times are counted in.
	TicksPerSecond uint64
	// Earlier is the number of reads of this run that had already ended
	// without an answer. Whatever they left running in the gateway is part
	// of the process-wide CPU time, too.
	Earlier int
}

// busy is the share of one core that the whole of process 1 used during
// the window. It is an observation about the process, not about the request.
func (s spStall) busy() float64 {
	if s.Window <= 0 || s.TicksPerSecond == 0 || s.Second.CPU < s.First.CPU {
		return 0
	}
	return float64(s.Second.CPU-s.First.CPU) / float64(s.TicksPerSecond) / s.Window.Seconds()
}

// judgeOnlyPart judges a read of part 1 of an object whose upload had one
// part of want bytes. It returns "" for the part's own answer: 200 or 206,
// its length, a part count of 1 and, for a GET, its bytes. A read without an
// answer from the gateway process that received it fails the contract, and
// that is all it shows: the CPU time of the process is reported with it, but
// it is counted for the whole process, earlier unanswered requests
// included, and is no call trace of this request. The verdict therefore
// leaves the cause inside the gateway open.
func judgeOnlyPart(read spRead, want int, stall *spStall) string {
	switch {
	case read.NotSent != "":
		return fmt.Sprintf("another fault: %s", read)
	case read.TimedOut && stall == nil:
		return fmt.Sprintf("another fault: %s, and the gateway was not inspected", read)
	case read.TimedOut && (stall.Before.Start != stall.First.Start || stall.First.Start != stall.Second.Start):
		return fmt.Sprintf("another fault: %s, and the gateway process is not the one that received the request", read)
	case read.TimedOut:
		return fmt.Sprintf("observed defect: %s from the gateway process that received it. The cause inside the gateway is not established: "+
			"the whole process used %.2f of a core in the %s after the deadline, with %d earlier unanswered read(s) of this run counted in, and no call trace of the request was taken",
			read, stall.busy(), stall.Window.Round(time.Millisecond), stall.Earlier)
	case read.Err != "":
		return fmt.Sprintf("another fault: %s", read)
	case read.Status != http.StatusOK && read.Status != http.StatusPartialContent,
		read.Length != int64(want), read.Method == http.MethodGet && !read.BodyMatches:
		return fmt.Sprintf("observed defect: the only part of the upload is not answered with its %d bytes: %s", want, read)
	case read.PartsCount != "1":
		return fmt.Sprintf("observed defect: the only part of the upload is answered with a part count of %q, want 1: %s", read.PartsCount, read)
	}
	return ""
}

// judgeControlPart judges a read of a part of the two-part control object,
// whose numbered reads answer on every measured build.
func judgeControlPart(read spRead, want int) string {
	if read.NotSent != "" || read.TimedOut || read.Err != "" || read.Status != http.StatusOK && read.Status != http.StatusPartialContent ||
		read.Length != int64(want) || read.PartsCount != "2" || read.Method == http.MethodGet && !read.BodyMatches {
		return fmt.Sprintf("control failed: a part of the two-part object is not answered with its %d bytes and a part count of 2: %s", want, read)
	}
	return ""
}

// judgeBeyond judges a GET of the part number after the last part, which
// must be refused as out of range: 400 with the S3 code InvalidPart. Another
// error does not show that the part number was out of range.
func judgeBeyond(read spRead) string {
	if read.NotSent != "" || read.TimedOut || read.Err != "" || read.Method != http.MethodGet || read.Status != http.StatusBadRequest || read.Code != "InvalidPart" {
		return fmt.Sprintf("control failed: a part number after the last part is not refused as 400 InvalidPart: %s", read)
	}
	return ""
}
