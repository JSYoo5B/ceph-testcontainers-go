package finalize

import (
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
)

// verificationKeyMarker replaces the text of a line, or the part of it, that
// is or may be inside a private key block. It is the marker that the
// redaction of a whole block writes, which redaction leaves alone.
const verificationKeyMarker = "[REDACTED PRIVATE KEY]"

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
	from := 0
	evidence.VisitPrivateKeyDelimiters(text, func(begin bool, start, end int) {
		if begin {
			if !b.open {
				out.WriteString(text[from:start])
				b.open = true
			}
			return
		}
		if !b.open {
			// The prefix, including any earlier text on this line, may be key material.
			out.Reset()
			orphan = true
		}
		out.WriteString(verificationKeyMarker)
		b.open, ended, from = false, true, end
	})
	if b.open {
		out.WriteString(verificationKeyMarker)
	} else {
		out.WriteString(text[from:])
	}
	return out.String(), ended, orphan
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
