//go:build all || integration

package rgw_multipart_completion_replay_etag_test

import (
	"fmt"
	"slices"
)

// An absent or invalid first header cannot qualify the HTTP comparison.
// The first XML ETag and stored bytes have already been verified by the caller.
func replayETagVerdict(expected, replayXML string, firstHeaders, replayHeaders []string) (string, []string, error) {
	var symptoms []string
	switch replayXML {
	case expected:
	case "", `""`:
		symptoms = append(symptoms, "empty replay XML ETag")
	default:
		return "", nil, fmt.Errorf("unexpected replay XML ETag %q", replayXML)
	}
	if slices.Equal(firstHeaders, []string{expected}) {
		switch {
		case slices.Equal(replayHeaders, firstHeaders):
		case len(replayHeaders) == 0 || slices.Equal(replayHeaders, []string{""}) || slices.Equal(replayHeaders, []string{`""`}):
			symptoms = append(symptoms, "missing or empty replay HTTP ETag")
		default:
			return "", nil, fmt.Errorf("unexpected replay HTTP ETag %q, want %q", replayHeaders, expected)
		}
	}
	if len(symptoms) > 0 {
		return "REPRODUCED", symptoms, nil
	}
	return "NOT_REPRODUCED", nil, nil
}
