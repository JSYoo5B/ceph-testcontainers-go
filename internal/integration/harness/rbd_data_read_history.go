package harness

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

// OSD descriptions contain the full hobject and a bracketed vector of
// operations. Extract whole RBD object names and include only data reads.
var rbdDataObjectToken = regexp.MustCompile(`[[:alnum:]_.-]+`)
var rbdDataReadOp = regexp.MustCompile(`(\[|,)[ ]*(read|sparse-read)[ ]`)

// RBDDataReadsInHistory selects reads of exact RBD object names. Each key
// identifies a native request by its description and start time.
func RBDDataReadsInHistory(out []byte, objects map[string]bool) (map[string]string, error) {
	var history struct {
		Ops *[]struct {
			Description string `json:"description"`
			Started     string `json:"initiated_at"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(out, &history); err != nil {
		return nil, err
	}
	if history.Ops == nil {
		return nil, errors.New("OSD history is missing ops")
	}
	reads := map[string]string{}
	for _, op := range *history.Ops {
		if !strings.HasPrefix(op.Description, "osd_op(") || !rbdDataReadOp.MatchString(op.Description) {
			continue
		}
		for _, token := range rbdDataObjectToken.FindAllString(op.Description, -1) {
			if objects[token] {
				if op.Started == "" {
					return nil, errors.New("RBD data read is missing its start time")
				}
				reads[op.Started+" "+op.Description] = token
				break
			}
		}
	}
	return reads, nil
}
