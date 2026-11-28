package evidence

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"time"
)

// MaxLogLine bounds one log line read by ReadLogLines, its Docker timestamp
// included; the rest of a longer line is not read.
const MaxLogLine = 64 << 10

// LogLine is one line of a Docker log stream read with timestamps.
type LogLine struct {
	// Stream is "stdout" or "stderr"; every line of a TTY stream is
	// "stdout".
	Stream string
	// Time is the time Docker recorded for the line, and Text the line
	// after it without trailing carriage returns. A Malformed line has no
	// valid timestamp: Time is zero and Text is the line as read.
	Time      time.Time
	Text      string
	Malformed bool
	// Cut is set when the line is longer than MaxLogLine and its rest was
	// not read; Ended when the stream stopped early inside the line. Both
	// mean that the line may lack its end.
	Cut, Ended bool
}

// LogRead is what ReadLogLines read.
type LogRead struct {
	// Bytes is the number of stream bytes read, at most the limit;
	// Exceeded is set when the stream holds more than the limit.
	Bytes    int64
	Exceeded bool
	// Lines, Malformed and Cut count the lines passed to visit, those of
	// them without a valid timestamp and those cut at MaxLogLine.
	Lines, Malformed, Cut int
}

// errLogVisitDone ends a read whose visitor needs no more lines.
var errLogVisitDone = errors.New("the log visitor is done")

// ReadLogLines reads the Docker log stream r, recorded with timestamps, and
// passes each line to visit in the order received. Each stream of a
// multiplexed log is split on its own, so a frame without a newline is
// continued only by the next frame of the same stream. A positive limit
// bounds the bytes read. The read stops without error when visit returns
// false. A stream that ends inside a frame returns ErrLogFrameCut.
func ReadLogLines(r io.Reader, tty bool, limit int64, visit func(LogLine) bool) (LogRead, error) {
	var read LogRead
	counted := &countingReader{r: r}
	if limit > 0 {
		counted.r = io.LimitReader(r, limit+1)
	}
	done := false
	emit := func(line LogLine) bool {
		read.Lines++
		if line.Malformed {
			read.Malformed++
		}
		if line.Cut {
			read.Cut++
		}
		done = !visit(line)
		return !done
	}
	stdout, stderr := &logSplitter{stream: "stdout", emit: emit}, &logSplitter{stream: "stderr", emit: emit}
	err := DemuxLogs(counted, tty, stdout, stderr)
	read.Bytes, read.Exceeded = counted.n, limit > 0 && counted.n > limit
	if read.Exceeded {
		read.Bytes = limit
	}
	if done {
		return read, nil
	}
	// A stream that the limit or an error ended can stop inside a line.
	ended := err != nil || read.Exceeded
	if stdout.finish(ended) {
		stderr.finish(ended)
	}
	if read.Exceeded {
		// The limit ended the stream, whatever the demultiplexer made of it.
		err = nil
	}
	return read, err
}

// logSplitter splits one stream into lines of at most MaxLogLine bytes.
type logSplitter struct {
	stream string
	line   []byte
	cut    bool
	emit   func(LogLine) bool
}

func (l *logSplitter) Write(data []byte) (int, error) {
	n := len(data)
	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		chunk := data
		if end >= 0 {
			chunk = data[:end]
		}
		if room := MaxLogLine - len(l.line); room < len(chunk) {
			l.line = append(l.line, chunk[:max(room, 0)]...)
			l.cut = true
		} else {
			l.line = append(l.line, chunk...)
		}
		if end < 0 {
			break
		}
		data = data[end+1:]
		if !l.flush(false) {
			return n, errLogVisitDone
		}
	}
	return n, nil
}

// finish passes the line still pending at the end of the stream; ended marks
// a stream that stopped early. It reports whether the visitor wants more.
func (l *logSplitter) finish(ended bool) bool { return l.flush(ended) }

func (l *logSplitter) flush(ended bool) bool {
	if len(l.line) == 0 && !l.cut {
		return true
	}
	line := parseLogLine(l.stream, l.line)
	line.Cut, line.Ended = l.cut, ended
	l.line, l.cut = l.line[:0], false
	return l.emit(line)
}

// parseLogLine splits Docker's timestamp from a line.
func parseLogLine(stream string, data []byte) LogLine {
	stamp, text, _ := strings.Cut(string(data), " ")
	at, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return LogLine{Stream: stream, Text: string(data), Malformed: true}
	}
	return LogLine{Stream: stream, Time: at.UTC(), Text: strings.TrimRight(text, "\r")}
}

// PrivateKeyMarker replaces the text of a log line, or the part of it, that
// is or may be inside a private key block. It is the marker that the
// redaction of a whole block writes, which redaction leaves alone.
const PrivateKeyMarker = "[REDACTED PRIVATE KEY]"

// KeyBlockFollower follows the private key blocks of each stream of one log
// read across its lines. Redaction sees one line at a time, so a block whose
// delimiters are on different lines never matches it whole; the follower
// replaces what lies between them before redaction runs. Each stream has its
// own blocks: an interleaved line of the other stream neither opens nor
// closes one.
type KeyBlockFollower struct {
	streams map[string]*keyBlockState
	next    int
}

type keyBlockState struct {
	// open is set from a beginning until its end.
	open bool
	// since is the number of the first line after the last end.
	since int
}

// Follow takes the next line of stream, whether or not the caller keeps it,
// and returns its text with each part inside a block replaced by
// PrivateKeyMarker, and the line's number in the read (0, 1, ...).
// withhold is -1, or, after an end without a beginning, the number from
// which the caller withholds the earlier lines of stream: the block began on
// a line the read did not see whole (one before the window, without a
// timestamp, or cut at MaxLogLine), so every line of stream since the last
// end may hold its payload. The returned text has already lost the part
// before such an end.
func (f *KeyBlockFollower) Follow(stream, text string) (safe string, line, withhold int) {
	if f.streams == nil {
		f.streams = map[string]*keyBlockState{}
	}
	b := f.streams[stream]
	if b == nil {
		b = &keyBlockState{}
		f.streams[stream] = b
	}
	line, withhold = f.next, -1
	f.next++
	var out strings.Builder
	from, ended := 0, false
	VisitPrivateKeyDelimiters(text, func(begin bool, start, end int) {
		if begin {
			if !b.open {
				out.WriteString(text[from:start])
				b.open = true
			}
			return
		}
		if !b.open {
			// The prefix, including any earlier text on this line, may be
			// key material.
			out.Reset()
			withhold = b.since
		}
		out.WriteString(PrivateKeyMarker)
		b.open, ended, from = false, true, end
	})
	if b.open {
		out.WriteString(PrivateKeyMarker)
	} else {
		out.WriteString(text[from:])
	}
	if ended {
		// Withholding never reaches back past this line, which is safe.
		b.since = line + 1
	}
	return out.String(), line, withhold
}

// LogLineRedactor returns the redaction of one log line with secrets, the
// exact values to mask, and redact, the Redactor of the same values: a line
// that may lack its end (cut, or in a stream that stopped early) also masks
// a known value that the missing end split.
func LogLineRedactor(secrets []string, redact func(string) string) func(text string, missingEnd bool) string {
	return func(text string, missingEnd bool) string {
		return RedactLog(text, secrets, false, missingEnd, redact)
	}
}
