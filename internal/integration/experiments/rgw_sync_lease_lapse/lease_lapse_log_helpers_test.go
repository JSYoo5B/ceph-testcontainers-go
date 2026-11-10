//go:build all || integration

package rgw_sync_lease_lapse_test

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// shardTrace is what one data sync shard of a gateway logged from a time on,
// with debug_rgw_sync at 20.
type shardTrace struct {
	// LostInBatch counts the times the shard found its sync lease lost
	// while it went over a batch of data log entries it had read from the
	// source: it left the batch and finished the pass. LostAtStart counts
	// the times the shard's incremental sync ended on a lost lease, to be
	// started again from the stored marker. LostOther counts a lost lease
	// followed by neither.
	LostInBatch, LostAtStart, LostOther int
	// Entries are the ids of the data log entries the shard started.
	Entries []string
	// Marker is the marker of the shard's latest read of the source's data
	// log: the position the gateway holds in memory.
	Marker string
	// Reads are the shard's reads of the source's data log; End is zero
	// while the pass of a read has not finished.
	Reads []shardRead

	lost bool
}

// shardRead is one read of the source's data log by a shard: the time the
// shard logged its marker before the read and the time it logged the end of
// the pass.
type shardRead struct {
	Start, End time.Time
}

// held reports whether a read of the shard was started before from and its
// pass ended after to.
func (s *shardTrace) held(from, to time.Time) bool {
	if s == nil {
		return false
	}
	return slices.ContainsFunc(s.Reads, func(read shardRead) bool {
		return read.Start.Before(from) && read.End.After(to)
	})
}

var (
	// A daemon log line of a data sync shard: time, thread, level, shard
	// and text. An entry's own lines ("shard[N]:entry[...") do not match.
	shardLogLine = regexp.MustCompile(`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4}) [0-9a-f]+ +-?\d+ RGW-SYNC:data:sync:shard\[(\d+)\]: (.*)`)
	// The marker before a read, and the end of a pass.
	shardReadStart = regexp.MustCompile(`^shard_id=\d+ sync_marker=(\S*)$`)
	shardPassEnd   = regexp.MustCompile(`^shard_id=\d+ sync_marker=\S* next_marker=\S* truncated=\d+$`)
	shardLogEntry  = regexp.MustCompile(`^shard_id=\d+ log_entry: (\d+:\d+):`)
)

// readShardTraces reads the data sync shards' lines of a gateway log from
// since on.
func readShardTraces(log string, since time.Time) map[int]*shardTrace {
	traces := map[int]*shardTrace{}
	for line := range strings.SplitSeq(log, "\n") {
		m := shardLogLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		at, err := time.Parse("2006-01-02T15:04:05.000-0700", m[1])
		if err != nil || at.Before(since) {
			continue
		}
		shard, err := strconv.Atoi(m[2])
		if err != nil {
			continue
		}
		trace := traces[shard]
		if trace == nil {
			trace = &shardTrace{}
			traces[shard] = trace
		}
		text := strings.TrimRight(m[3], "\r ")
		switch {
		case text == "lease is lost, abort":
			if trace.lost {
				trace.LostOther++
			}
			trace.lost = true
		case shardPassEnd.MatchString(text):
			if trace.lost {
				trace.LostInBatch++
			}
			trace.lost = false
			if n := len(trace.Reads); n > 0 && trace.Reads[n-1].End.IsZero() {
				trace.Reads[n-1].End = at
			}
		case shardReadStart.MatchString(text):
			if trace.lost {
				trace.LostOther++
			}
			trace.lost = false
			trace.Marker = shardReadStart.FindStringSubmatch(text)[1]
			trace.Reads = append(trace.Reads, shardRead{Start: at})
		case strings.HasPrefix(text, "incremental sync failed"):
			if trace.lost {
				trace.LostAtStart++
			}
			trace.lost = false
		default:
			if e := shardLogEntry.FindStringSubmatch(text); e != nil {
				trace.Entries = append(trace.Entries, e[1])
			}
		}
	}
	for _, trace := range traces {
		if trace.lost {
			trace.LostOther++
			trace.lost = false
		}
	}
	return traces
}

// skippedBatch reports the signature of a batch that a shard left on a lost
// lease and then passed: the shard found the lease lost inside a batch, the
// marker it reads the source's data log from is the source's newest one, it
// never started the entry of that marker, and the marker it stored is not
// the source's.
func skippedBatch(trace *shardTrace, source, stored string) bool {
	return trace != nil && trace.LostInBatch > 0 && source != "" && stored != source && trace.Marker == source && !slices.Contains(trace.Entries, source)
}

// shardLines returns the last n lines of a data sync shard in a gateway
// log, without the lines of its entries.
func shardLines(log string, shard, n int) []string {
	var lines []string
	for line := range strings.SplitSeq(log, "\n") {
		if at := strings.Index(line, "shard["+strconv.Itoa(shard)+"]"); at >= 0 && strings.Contains(line, "RGW-SYNC:data:") {
			lines = append(lines, line[max(at-60, 0):min(at+260, len(line))])
		}
	}
	return lines[max(len(lines)-n, 0):]
}
