package finalize

import (
	"regexp"
	"strings"
	"time"
)

// A MON writes each cluster log entry to its standard error twice
// (LogMonitor::log_external with mon_cluster_log_to_stderr): once with
// `cerr << channel << " " << entry << endl`, and once formatted in one write.
// The first form reaches the stream in pieces, one write per piece of the
// entry's header (the channel, each part of the stamp, the name, the rank,
// the sequence number, the channel and the priority), then its message in
// one write and the newline. The daemon's log thread writes each of its own
// lines in one write to the same stream, and such a line can land between
// two pieces of the header. The entry is then split over several lines: each
// but the last ends with one whole daemon line, and the last holds the rest
// of the entry. Without the rest, neither the header nor its message has an
// envelope, and a message that quotes a severity marker is a gap.
//
// verificationDeinterleaver restores such entries on one stream: a line that
// holds a daemon line after its start is held as a fragment, and the line
// that follows the fragments completes them when the heads of the fragments
// and that line form a cluster log entry whose header holds every split.
// The daemon lines and the entry are then emitted as separate lines, the
// entry with the Docker timestamp of its first fragment. Anything else is
// emitted unchanged: a split inside a message is not restored, since a
// message may quote any text, including a daemon line. A line that has a log
// envelope of its own is a record, not the rest of an entry, and the fragments
// before it are emitted unchanged.
type verificationDeinterleaver struct {
	emit    func(stream string, data []byte, long, ended bool)
	pending map[string][]verificationFragment
}

// verificationFragment is one line of a split entry: its Docker timestamp,
// the head of the entry it holds, the whole daemon line after the head, and
// the line as read.
type verificationFragment struct {
	stamp, head, daemon string
	data                []byte
}

// maxVerificationFragments bounds the fragments held for one entry; the
// header of an entry has fewer pieces than that.
const maxVerificationFragments = 32

// embeddedDaemonLine finds the start of a daemon log line (the prefix of
// Log::_flush_logbuf: a stamp with three fractional digits, the thread and
// the level) after the start of a line. A cluster log entry's own stamp has
// six fractional digits and does not match.
var embeddedDaemonLine = regexp.MustCompile(`(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4}) +[0-9a-f]+ +-?\d+ `)

func newVerificationDeinterleaver(emit func(string, []byte, bool, bool)) *verificationDeinterleaver {
	return &verificationDeinterleaver{emit: emit, pending: map[string][]verificationFragment{}}
}

// line receives one line of a stream as read, with its Docker timestamp.
func (d *verificationDeinterleaver) line(stream string, data []byte, long, ended bool) {
	stamp, text, cut := strings.Cut(string(data), " ")
	if _, err := time.Parse(time.RFC3339Nano, stamp); long || ended || !cut || err != nil {
		d.flush(stream)
		d.emit(stream, data, long, ended)
		return
	}
	if at := daemonLineAfterStart(text); at > 0 {
		if len(d.pending[stream]) == maxVerificationFragments {
			d.flush(stream)
		}
		d.pending[stream] = append(d.pending[stream], verificationFragment{stamp: stamp, head: text[:at], daemon: text[at:], data: append([]byte(nil), data...)})
		return
	}
	if fragments := d.pending[stream]; len(fragments) > 0 {
		// A line that is a whole record on its own is never the rest of an
		// entry: a record whose message is a quoted daemon line looks like
		// a fragment, and joining the record after it would hide that record.
		if _, whole := parseVerificationEnvelope(text); whole {
			d.flush(stream)
			d.emit(stream, data, long, ended)
			return
		}
		if entry, ok := restoreClusterEntry(fragments, text); ok {
			d.pending[stream] = nil
			for _, fragment := range fragments {
				d.emit(stream, []byte(fragment.stamp+" "+fragment.daemon), false, false)
			}
			d.emit(stream, []byte(fragments[0].stamp+" "+entry), false, false)
			return
		}
		d.flush(stream)
	}
	d.emit(stream, data, long, ended)
}

// daemonLineAfterStart returns where a daemon line starts after the start of
// text, or 0.
func daemonLineAfterStart(text string) int {
	for _, m := range embeddedDaemonLine.FindAllStringSubmatchIndex(text, -1) {
		if m[0] > 0 && validCephStamp(text[m[2]:m[3]]) {
			return m[0]
		}
	}
	return 0
}

// restoreClusterEntry joins the heads of the fragments and the rest of the
// entry, and returns the entry when it has a cluster log envelope that ends
// at or after the last split.
func restoreClusterEntry(fragments []verificationFragment, rest string) (string, bool) {
	var entry strings.Builder
	for _, fragment := range fragments {
		entry.WriteString(fragment.head)
	}
	split := entry.Len()
	entry.WriteString(rest)
	text := entry.String()
	m := clusterEnvelope.FindStringSubmatchIndex(text)
	if m == nil || split > m[1] || !validCephStamp(text[m[4]:m[5]]) {
		return "", false
	}
	if m[2] >= 0 && text[m[2]:m[3]] != text[m[6]:m[7]] {
		return "", false
	}
	return text, true
}

// flush emits the held fragments of a stream unchanged.
func (d *verificationDeinterleaver) flush(stream string) {
	fragments := d.pending[stream]
	d.pending[stream] = nil
	for _, fragment := range fragments {
		d.emit(stream, fragment.data, false, false)
	}
}

// finish emits the fragments still held at the end of the read.
func (d *verificationDeinterleaver) finish() {
	d.flush("stdout")
	d.flush("stderr")
}
