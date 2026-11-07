package evidence

import "strings"

// MaskClippedSecrets masks partial known values at the missing boundaries of
// text, together with whole values overlapping them. Match both boundaries on
// the original bytes, since a marker inserted at one end can hide a match
// extending from the other. Apply Redactor to the result as usual.
func MaskClippedSecrets(text string, secrets []string, missingStart, missingEnd bool) string {
	if len(text) == 0 || (!missingStart && !missingEnd) {
		return text
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
		return text
	}
	masked := make([]bool, len(text))
	for i := range startCut {
		masked[i] = true
	}
	for i := len(text) - endCut; i < len(text); i++ {
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
