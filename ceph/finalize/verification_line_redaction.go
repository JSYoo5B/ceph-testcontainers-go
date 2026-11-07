package finalize

import (
	"regexp"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
)

// verificationKeyMarker replaces the text of a line, or the part of it, that
// is or may be inside a private key block. It is the marker that the
// redaction of a whole block writes, which redaction leaves alone.
const verificationKeyMarker = "[REDACTED PRIVATE KEY]"

var (
	privateKeyBegin = regexp.MustCompile(`-----BEGIN (?:[A-Z ]+)?PRIVATE KEY-----`)
	privateKeyEnd   = regexp.MustCompile(`-----END (?:[A-Z ]+)?PRIVATE KEY-----`)
)

// verificationKeyBlocks follows the private key blocks of one stream across
// its lines. Redaction sees one line at a time, so a block whose delimiters
// are on different lines never matches it whole; this replaces what lies
// between them before redaction runs.
type verificationKeyBlocks struct {
	// open is set from a beginning until its end.
	open bool
	// since is the index, in the read's lines, of the first line after the
	// last end, which an end without a beginning withholds back to.
	since int
}

// follow returns text with each part inside a block replaced by
// verificationKeyMarker, and whether text holds an end. orphan reports an
// end without a beginning: the block began on a line that the read did not
// see whole (one before the window, without a timestamp, or cut at 64 KiB),
// so the lines since the last end may hold its payload and the caller
// withholds them.
func (b *verificationKeyBlocks) follow(text string) (safe string, ended, orphan bool) {
	var out strings.Builder
	for {
		if b.open {
			end := privateKeyEnd.FindStringIndex(text)
			out.WriteString(verificationKeyMarker)
			if end == nil {
				return out.String(), ended, orphan
			}
			b.open, ended, text = false, true, text[end[1]:]
			continue
		}
		begin, end := privateKeyBegin.FindStringIndex(text), privateKeyEnd.FindStringIndex(text)
		if end != nil && (begin == nil || end[0] < begin[0]) {
			// What precedes the end on this line may be payload as well.
			out.Reset()
			out.WriteString(verificationKeyMarker)
			ended, orphan, text = true, true, text[end[1]:]
			continue
		}
		if begin == nil {
			out.WriteString(text)
			return out.String(), ended, orphan
		}
		out.WriteString(text[:begin[0]])
		b.open, text = true, text[begin[1]:]
	}
}

// verificationLineRedaction redacts one log line of the window.
type verificationLineRedaction struct {
	redact func(string) string
	// secrets are the exact values that redact replaces.
	secrets []string
}

// line masks known secrets across a truncated line's boundary.
func (r verificationLineRedaction) line(text string, truncated bool) string {
	return evidence.RedactLog(text, r.secrets, false, truncated, r.redact)
}
