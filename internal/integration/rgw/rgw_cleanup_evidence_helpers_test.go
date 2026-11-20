//go:build all || integration

package rgw_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// multisiteObjectMatches reports whether one GET answer is the state the
// caller waits for: HTTP 200 with exactly payload, HTTP 404 with an S3
// NoSuchKey error, or an explicitly expected HTTP 301 cross-zonegroup
// redirect. A redirect is judged by its status alone: some releases send a
// Location that names the wrong zonegroup endpoint, and the caller does not
// follow it. Any other status, or a status the caller did not ask for, does
// not match.
func multisiteObjectMatches(status int, body []byte, wantStatus int, payload []byte) bool {
	if status != wantStatus {
		return false
	}
	switch wantStatus {
	case http.StatusOK:
		return bytes.Equal(body, payload)
	case http.StatusNotFound:
		var failure struct {
			XMLName xml.Name `xml:"Error"`
			Code    string   `xml:"Code"`
		}
		return xml.Unmarshal(body, &failure) == nil && failure.Code == "NoSuchKey"
	case http.StatusMovedPermanently:
		return true
	default:
		return false
	}
}

func notificationLogWhileStopped(prior, observed, queued []string) error {
	if len(prior) == 0 {
		return fmt.Errorf("receiver log has no previously observed events")
	}
	for _, event := range prior {
		if !slices.Contains(observed, event) {
			return fmt.Errorf("receiver log lost previously observed event %q", event)
		}
	}
	for _, event := range queued {
		if slices.Contains(observed, event) {
			return fmt.Errorf("event recorded while receiver was stopped: %q", event)
		}
	}
	return nil
}

// rgwAdminProcessState first enumerates proc successfully. If a listed PID
// disappears before its stat is read, another successful enumeration must
// establish its absence. A failed read while it remains listed is inconclusive.
const rgwAdminProcessState = `proc=${2:-/proc}
entries=$(ls -1 "$proc") || exit
case "
$entries
" in
*"
$1
"*) ;;
*) echo absent; exit 0 ;;
esac
if state=$(awk '{sub(/.*\) /, ""); if (NF < 20) exit 1; print $1}' "$proc/$1/stat" 2>/dev/null); then
    printf '%s\n' "$state"
    exit 0
fi
entries=$(ls -1 "$proc") || exit
case "
$entries
" in
*"
$1
"*) exit 1 ;;
*) echo absent ;;
esac
`

func rgwAdminProcessGone(code int, state string) (bool, error) {
	state = strings.TrimSpace(state)
	if code != 0 {
		return false, fmt.Errorf("read admin process state: exit=%d output=%q", code, state)
	}
	switch state {
	case "absent", "Z":
		return true, nil
	case "R", "S", "D", "T", "t", "X", "x", "K", "W", "P", "I":
		return false, nil
	default:
		return false, fmt.Errorf("invalid admin process state %q", state)
	}
}
