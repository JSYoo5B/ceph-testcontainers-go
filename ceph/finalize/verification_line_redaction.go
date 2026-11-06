package finalize

import (
	"regexp"
	"strings"
)

// verificationKeyMarker replaces the text of a line, or the part of it, that
// is or may be inside a private key block. It is the marker that the
// redaction of a whole block writes, which redaction leaves alone.
const verificationKeyMarker = "[REDACTED PRIVATE KEY]"

// verificationSecretMarker follows the safe prefix of a truncated line whose
// tail could be the start of a secret.
const verificationSecretMarker = "[REDACTED]"

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

// line redacts text. For a truncated line, whose rest was not read, it
// also withholds the longest tail that is the start of a secret: the
// replacement of exact values cannot match a secret that the cut split, so
// that tail would keep all of the secret but its rest. The tail and every
// whole secret are masked together on the original text before redaction
// runs, because a tail can lie inside a whole secret that ends the line (a
// secret that starts with its own last characters); trimming the tail first
// would split that secret and expose the rest of it. A masked run is
// replaced by verificationSecretMarker.
func (r verificationLineRedaction) line(text string, truncated bool) string {
	if !truncated {
		return r.redact(text)
	}
	cut := 0
	for _, secret := range r.secrets {
		cut = max(cut, secretPrefixSuffix(text, secret))
	}
	if cut == 0 {
		return r.redact(text)
	}
	masked := make([]bool, len(text))
	for i := len(text) - cut; i < len(text); i++ {
		masked[i] = true
	}
	for _, secret := range r.secrets {
		if secret == "" {
			continue
		}
		for from := 0; from+len(secret) <= len(text); {
			i := strings.Index(text[from:], secret)
			if i < 0 {
				break
			}
			for j := from + i; j < from+i+len(secret); j++ {
				masked[j] = true
			}
			from += i + 1
		}
	}
	var out strings.Builder
	for i := 0; i < len(text); {
		j := i
		for j < len(text) && masked[j] == masked[i] {
			j++
		}
		if masked[i] {
			out.WriteString(verificationSecretMarker)
		} else {
			out.WriteString(text[i:j])
		}
		i = j
	}
	return r.redact(out.String())
}

// secretPrefixSuffix returns the length of the longest suffix of text that
// is a proper prefix of secret, in linear time.
func secretPrefixSuffix(text, secret string) int {
	if len(secret) < 2 {
		return 0
	}
	if len(text) > len(secret)-1 {
		text = text[len(text)-(len(secret)-1):]
	}
	// fail[i] is the length of the longest proper border of secret[:i+1].
	fail := make([]int, len(secret))
	for i, k := 1, 0; i < len(secret); i++ {
		for k > 0 && secret[i] != secret[k] {
			k = fail[k-1]
		}
		if secret[i] == secret[k] {
			k++
		}
		fail[i] = k
	}
	// text is shorter than secret, so k never reaches its length.
	k := 0
	for i := 0; i < len(text); i++ {
		for k > 0 && text[i] != secret[k] {
			k = fail[k-1]
		}
		if text[i] == secret[k] {
			k++
		}
	}
	return k
}
