package evidence

import (
	"slices"
	"strings"
)

// RedactLog masks partial known values at the missing boundaries of
// text, together with whole values overlapping them. Match both boundaries on
// the original bytes, since a marker inserted at one end can hide a match
// extending from the other. Use the ordinary redactor when no boundary mask
// applies, so inserted markers are never passed through another substitution.
func RedactLog(text string, secrets []string, missingStart, missingEnd bool, redact func(string) string) string {
	if len(text) == 0 || (!missingStart && !missingEnd) {
		return redact(text)
	}
	startCut, endCut := 0, 0
	for _, secret := range secrets {
		if len(secret) < 2 {
			continue
		}
		if missingStart && missingEnd && len(secret) > len(text) && strings.Contains(secret, text) {
			// The retained window can lie entirely inside one secret.
			return "[REDACTED]"
		}
		if missingStart {
			startCut = max(startCut, suffixPrefixOverlap(secret[1:], text))
		}
		if missingEnd {
			endCut = max(endCut, suffixPrefixOverlap(text, secret[:len(secret)-1]))
		}
	}
	if startCut == 0 && endCut == 0 {
		return redact(text)
	}
	return maskOriginalSecrets(text, secrets, startCut, endCut)
}

// Combine the sensitive spans on the unmodified bytes. A full configured
// value can overlap syntax or another value just as a boundary fragment can.
func maskOriginalSecrets(text string, secrets []string, startCut, endCut int) string {
	masked := make([]byte, len(text))
	markBytes(masked[:startCut])
	markBytes(masked[len(text)-endCut:])
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		maskOccurrences(masked, text, secret)
	}
	if !slices.Contains(masked, byte(0)) {
		return "[REDACTED]"
	}
	// Match credential syntax before a literal or boundary mask can split a
	// field name, bearer prefix or private-key delimiter.
	if strings.ContainsAny(text, "=:") {
		for _, m := range secretPattern.FindAllStringSubmatchIndex(text, -1) {
			if !isRedactionMarker(text, m[4], m[5]) {
				markBytes(masked[m[4]:m[5]])
			}
		}
	}
	for _, m := range bearerPattern.FindAllStringSubmatchIndex(text, -1) {
		markBytes(masked[m[3]:m[1]])
	}
	for _, m := range privateKeyPattern.FindAllStringIndex(text, -1) {
		for i := m[0]; i < m[1]; i++ {
			masked[i] = privateKeyByte
		}
	}
	return renderMask(text, masked)
}

// A contiguous masked run containing a private-key block keeps its specific
// marker in reports that distinguish key blocks from individual credentials.
const privateKeyByte = 2

func renderMask(text string, masked []byte) string {
	var out strings.Builder
	for i := 0; i < len(text); {
		j := i
		if masked[i] == 0 {
			for j < len(text) && masked[j] == 0 {
				j++
			}
			out.WriteString(text[i:j])
		} else {
			for j < len(text) && masked[j] != 0 {
				j++
			}
			if slices.Contains(masked[i:j], byte(privateKeyByte)) {
				out.WriteString("[REDACTED PRIVATE KEY]")
			} else {
				out.WriteString("[REDACTED]")
			}
		}
		i = j
	}
	return out.String()
}

func markBytes(mask []byte) {
	for i := range mask {
		mask[i] = 1
	}
}

// maskOccurrences marks the union of all overlapping matches. KMP resumes
// at the longest matching border; only bytes beyond the preceding match need
// marking. Thus both matching and marking are linear for each secret.
func maskOccurrences(mask []byte, text, secret string) {
	first := strings.Index(text, secret)
	if first < 0 {
		return
	}
	end := first + len(secret)
	markBytes(mask[first:end])
	fail := prefixFailures(secret)
	matched := fail[len(secret)-1]
	for i := end; i < len(text); i++ {
		for matched > 0 && text[i] != secret[matched] {
			matched = fail[matched-1]
		}
		if text[i] != secret[matched] {
			continue
		}
		matched++
		if matched == len(secret) {
			nextEnd := i + 1
			markBytes(mask[max(end, nextEnd-len(secret)):nextEnd])
			end = nextEnd
			matched = fail[matched-1]
		}
	}
}

// suffixPrefixOverlap finds the longest suffix of the first string that is a
// prefix of the second. Only the common possible length matters.
func suffixPrefixOverlap(suffix, prefix string) int {
	n := min(len(suffix), len(prefix))
	suffix, prefix = suffix[len(suffix)-n:], prefix[:n]
	fail := prefixFailures(prefix)
	// suffix is at most as long as prefix, so a full match can only end the loop.
	k := 0
	for i := 0; i < len(suffix); i++ {
		for k > 0 && suffix[i] != prefix[k] {
			k = fail[k-1]
		}
		if suffix[i] == prefix[k] {
			k++
		}
	}
	return k
}

// prefixFailures stores the length of the longest proper border ending at
// each byte, shared by boundary matching and whole-value search.
func prefixFailures(prefix string) []int {
	fail := make([]int, len(prefix))
	for i, k := 1, 0; i < len(prefix); i++ {
		for k > 0 && prefix[i] != prefix[k] {
			k = fail[k-1]
		}
		if prefix[i] == prefix[k] {
			k++
		}
		fail[i] = k
	}
	return fail
}
