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

func multisiteObjectMatches(status int, body []byte, wantStatus int, payload []byte) bool {
	if status != wantStatus {
		return false
	}
	if wantStatus == http.StatusNotFound {
		var failure struct {
			XMLName xml.Name `xml:"Error"`
			Code    string   `xml:"Code"`
		}
		return xml.Unmarshal(body, &failure) == nil && failure.Code == "NoSuchKey"
	}
	return wantStatus == http.StatusOK && bytes.Equal(body, payload)
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

// rgwAdminProcessState first enumerates proc successfully. A missing PID in
// that listing proves absence; a failed stat read of a listed PID does not.
const rgwAdminProcessState = `proc=${2:-/proc}
entries=$(ls -1 "$proc") || exit
case "
$entries
" in
*"
$1
"*) awk '{sub(/.*\) /, ""); if (NF < 20) exit 1; print $1}' "$proc/$1/stat" ;;
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
