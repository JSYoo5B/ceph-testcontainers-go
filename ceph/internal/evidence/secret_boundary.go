package evidence

import "strings"

// MaskClippedStart masks a known secret whose beginning could be missing from
// the retained log tail. Call the ordinary Redactor on its result as usual.
func MaskClippedStart(text string, secrets []string) string {
	cut := 0
	for _, secret := range secrets {
		if len(secret) > 1 {
			cut = max(cut, suffixPrefixOverlap(secret[1:], text))
		}
	}
	return maskSecretBoundary(text, secrets, 0, cut)
}

// MaskClippedEnd masks a known secret whose end could be missing from a
// truncated line. Call the ordinary Redactor on its result as usual.
func MaskClippedEnd(text string, secrets []string) string {
	cut := 0
	for _, secret := range secrets {
		if len(secret) > 1 {
			cut = max(cut, suffixPrefixOverlap(text, secret[:len(secret)-1]))
		}
	}
	return maskSecretBoundary(text, secrets, len(text)-cut, len(text))
}

// Include whole secret matches on the original bytes. Replacing the cut
// first could split an overlapping whole secret and disclose its other end.
func maskSecretBoundary(text string, secrets []string, start, end int) string {
	if start == end {
		return text
	}
	masked := make([]bool, len(text))
	for i := start; i < end; i++ {
		masked[i] = true
	}
	for _, secret := range secrets {
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
			out.WriteString("[REDACTED]")
		} else {
			out.WriteString(text[i:j])
		}
		i = j
	}
	return out.String()
}

// suffixPrefixOverlap finds the longest suffix of the first string that is a
// prefix of the second. Only the common possible length matters.
func suffixPrefixOverlap(suffix, prefix string) int {
	n := min(len(suffix), len(prefix))
	suffix, prefix = suffix[len(suffix)-n:], prefix[:n]
	// fail[i] is the length of the longest proper border of prefix[:i+1].
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
