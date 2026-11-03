//go:build all || integration

package harness

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ObjecterOp is one operation in flight in an Objecter's objecter_requests:
// the OSD it was sent to, its object and locator ("@<pool>", with ";<ns>"
// or ":<key>" behind), how long ago it was sent and its OSD sub-operations.
type ObjecterOp struct {
	TID      uint64   `json:"tid"`
	OSD      int      `json:"osd"`
	Object   string   `json:"object_id"`
	Locator  string   `json:"object_locator"`
	Age      float64  `json:"age"`
	Attempts int      `json:"attempts"`
	OSDOps   []string `json:"osd_ops"`
}

// pool returns the pool ID of the op's object locator.
func (op ObjecterOp) Pool() (int64, bool) {
	rest, found := strings.CutPrefix(op.Locator, "@")
	if !found {
		return 0, false
	}
	if end := strings.IndexAny(rest, ";:"); end >= 0 {
		rest = rest[:end]
	}
	pool, err := strconv.ParseInt(rest, 10, 64)
	return pool, err == nil
}

// ParseObjecterOps reads the OSD operations of an objecter_requests dump. A
// dump without an "ops" array is an error; an empty array is an answer.
func ParseObjecterOps(data []byte) ([]ObjecterOp, error) {
	var dump struct {
		Ops *[]ObjecterOp `json:"ops"`
	}
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, err
	}
	if dump.Ops == nil {
		return nil, fmt.Errorf("objecter_requests has no ops")
	}
	return *dump.Ops, nil
}

// ObjecterOpsOn returns the ops on pool whose object starts with prefix.
func ObjecterOpsOn(ops []ObjecterOp, pool int64, prefix string) []ObjecterOp {
	var matched []ObjecterOp
	for _, op := range ops {
		if id, ok := op.Pool(); ok && id == pool && strings.HasPrefix(op.Object, prefix) {
			matched = append(matched, op)
		}
	}
	return matched
}
