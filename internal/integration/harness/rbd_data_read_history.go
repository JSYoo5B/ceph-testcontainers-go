package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
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

// OSDReadCounters is the read part of one OSD's `perf dump osd`. The counters
// are request-level evidence apart from the op history: an OSD that serves a
// data read adds the returned bytes to op_r_out_bytes, and a non-primary
// that serves a replica read also counts it in replica_read_served.
type OSDReadCounters struct {
	OpR, OpROutBytes int64
	// ReplicaReadServed is nil when the OSD does not report the counter.
	ReplicaReadServed *int64
}

// ParseOSDReadCounters decodes `perf dump osd`. op_r and op_r_out_bytes are
// required; replica_read_served is optional.
func ParseOSDReadCounters(out []byte) (OSDReadCounters, error) {
	var dump struct {
		OSD map[string]json.RawMessage `json:"osd"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return OSDReadCounters{}, err
	}
	counter := func(name string, required bool) (*int64, error) {
		raw, ok := dump.OSD[name]
		if !ok {
			if required {
				return nil, errors.New("perf dump osd lacks " + name)
			}
			return nil, nil
		}
		var value int64
		if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
			return nil, fmt.Errorf("perf dump osd %s is %s, not a non-negative integer", name, raw)
		}
		return &value, nil
	}
	var counters OSDReadCounters
	for _, c := range []struct {
		name string
		dst  *int64
	}{{"op_r", &counters.OpR}, {"op_r_out_bytes", &counters.OpROutBytes}} {
		value, err := counter(c.name, true)
		if err != nil {
			return OSDReadCounters{}, err
		}
		*c.dst = *value
	}
	served, err := counter("replica_read_served", false)
	if err != nil {
		return OSDReadCounters{}, err
	}
	counters.ReplicaReadServed = served
	return counters, nil
}

// RBDReadEvidence is what one OSD showed between the observations before
// and after a single read of one RBD data object.
type RBDReadEvidence struct {
	// History holds the op history entries for the object that were not
	// there before the read.
	History []string
	// Before and After are the OSD's read counters around the read.
	Before, After OSDReadCounters
}

// OutBytes is the number of bytes the OSD returned to read requests during
// the read.
func (e RBDReadEvidence) OutBytes() int64 { return e.After.OpROutBytes - e.Before.OpROutBytes }

// ReplicaServed is the number of replica reads the OSD served during the
// read; ok is false when the OSD does not report the counter.
func (e RBDReadEvidence) ReplicaServed() (served int64, ok bool) {
	if e.Before.ReplicaReadServed == nil || e.After.ReplicaReadServed == nil {
		return 0, false
	}
	return *e.After.ReplicaReadServed - *e.Before.ReplicaReadServed, true
}

// RBDReadServers attributes a read of a data object of size bytes. An OSD
// served it when its op history gained a data read of the object, or when it
// returned at least size bytes to read requests; either kind of evidence
// alone identifies it, so a history entry that is missing on one release
// does not hide the serving OSD. Counters that went backwards, or a history
// entry on an OSD that returned fewer than size bytes, contradict each other
// and are an error.
func RBDReadServers(evidence map[int]RBDReadEvidence, size int64) ([]int, error) {
	var servers []int
	for id, e := range evidence {
		if e.After.OpR < e.Before.OpR || e.OutBytes() < 0 {
			return nil, fmt.Errorf("osd.%d read counters went backwards: %+v, then %+v", id, e.Before, e.After)
		}
		if served, ok := e.ReplicaServed(); ok && served < 0 {
			return nil, fmt.Errorf("osd.%d replica_read_served went backwards by %d", id, -served)
		}
		if len(e.History) > 0 && e.OutBytes() < size {
			return nil, fmt.Errorf("osd.%d history gained %v but the OSD returned %d bytes, less than the %d-byte object", id, e.History, e.OutBytes(), size)
		}
		if len(e.History) > 0 || e.OutBytes() >= size {
			servers = append(servers, id)
		}
	}
	slices.Sort(servers)
	return servers, nil
}
