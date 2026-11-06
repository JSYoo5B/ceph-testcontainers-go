//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

// historicOpsDump is one parsed dump_historic_ops response of an OSD. Every
// entry is kept; Parsed counts the entries whose description is a decoded
// osd_op, Undecoded the osd_op entries whose message the tracker described
// before decoding it, and Opaque every other description.
type historicOpsDump struct {
	Size      int          `json:"size"`
	Duration  int          `json:"duration"`
	Entries   []historicOp `json:"-"`
	Total     int          `json:"total"`
	Parsed    int          `json:"parsed"`
	Undecoded int          `json:"undecoded"`
	Opaque    int          `json:"opaque"`
}

// historicOp is one history entry and, for a decoded osd_op, the fields of
// its description: osd_op(<reqid> <pgid> <hobject> [<op>,...] ...).
type historicOp struct {
	Description string    `json:"description"`
	Initiated   time.Time `json:"initiated_at"`
	Duration    float64   `json:"duration"`
	FlagPoint   string    `json:"flag_point,omitempty"`
	ReqID       string    `json:"-"`
	Entity      string    `json:"-"`
	PG          string    `json:"-"`
	Object      string    `json:"-"`
	Ops         []string  `json:"-"`
	Undecoded   bool      `json:"-"`
	Decoded     bool      `json:"-"`
}

// parseHistoricOps decodes a whole dump_historic_ops response. Trailing data,
// a missing ops array, an entry without a description or with an unreadable
// initiated_at is an error, so a truncated or reshaped dump never reads as a
// history without the operation.
func parseHistoricOps(raw []byte) (historicOpsDump, error) {
	var doc struct {
		Size     *int `json:"size"`
		Duration *int `json:"duration"`
		Ops      *[]struct {
			Description string  `json:"description"`
			Initiated   string  `json:"initiated_at"`
			Duration    float64 `json:"duration"`
			TypeData    struct {
				FlagPoint string `json:"flag_point"`
			} `json:"type_data"`
		} `json:"ops"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&doc); err != nil {
		return historicOpsDump{}, fmt.Errorf("dump_historic_ops is not one JSON object: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return historicOpsDump{}, errors.New("dump_historic_ops has data after its object")
	}
	if doc.Size == nil || doc.Duration == nil || doc.Ops == nil {
		return historicOpsDump{}, errors.New("dump_historic_ops lacks size, duration or ops")
	}
	dump := historicOpsDump{Size: *doc.Size, Duration: *doc.Duration, Total: len(*doc.Ops)}
	for i, raw := range *doc.Ops {
		if raw.Description == "" {
			return historicOpsDump{}, fmt.Errorf("history entry %d has no description", i)
		}
		initiated, err := time.Parse("2006-01-02T15:04:05.999999-0700", raw.Initiated)
		if err != nil {
			return historicOpsDump{}, fmt.Errorf("history entry %d initiated_at %q: %w", i, raw.Initiated, err)
		}
		op := parseOSDOpDescription(raw.Description)
		op.Initiated, op.Duration, op.FlagPoint = initiated, raw.Duration, raw.TypeData.FlagPoint
		switch {
		case op.Decoded:
			dump.Parsed++
		case op.Undecoded:
			dump.Undecoded++
		default:
			dump.Opaque++
		}
		dump.Entries = append(dump.Entries, op)
	}
	return dump, nil
}

// parseOSDOpDescription reads the MOSDOp description that the OSD's op
// tracker records. An undecoded message reads "osd_op(<reqid> <pgid> <raw pg>
// (undecoded) ...)"; anything else that does not have the decoded shape is
// left opaque.
func parseOSDOpDescription(description string) historicOp {
	op := historicOp{Description: description}
	body, ok := strings.CutPrefix(description, "osd_op(")
	if !ok || !strings.HasSuffix(body, ")") {
		return op
	}
	fields := strings.Fields(body)
	if len(fields) < 3 {
		return op
	}
	reqid, pg := fields[0], fields[1]
	entity, ok := reqidEntity(reqid)
	if !ok {
		return op
	}
	if slices.Contains(fields, "(undecoded)") {
		op.ReqID, op.Entity, op.PG, op.Undecoded = reqid, entity, pg, true
		return op
	}
	// The hobject follows "<reqid> <pgid> ", and the op list follows it.
	// A description without the hobject, or with its fields in another
	// order, stays opaque instead of slicing outside it.
	start := strings.Index(body, reqid+" "+pg+" ")
	if start != 0 {
		return op
	}
	start = len(reqid) + 1 + len(pg) + 1
	open := strings.Index(body[start:], " [")
	if open < 0 {
		return op
	}
	open += start
	closing := strings.Index(body[open:], "] snapc ")
	if closing < 0 {
		return op
	}
	closing += open
	object, ok := hobjectName(strings.TrimSpace(body[start:open]))
	if !ok {
		return op
	}
	op.ReqID, op.Entity, op.PG, op.Object, op.Decoded = reqid, entity, pg, object, true
	for part := range strings.SplitSeq(body[open+2:closing], ",") {
		op.Ops = append(op.Ops, strings.TrimSpace(part))
	}
	return op
}

// reqidEntity returns the entity of an osd_reqid_t "<type>.<num>.<inc>:<tid>",
// such as client.4172 for client.4172.0:1 or osd.0 for osd.0.6:3.
func reqidEntity(reqid string) (string, bool) {
	name, tid, ok := strings.Cut(reqid, ":")
	if !ok || tid == "" {
		return "", false
	}
	parts := strings.Split(name, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", false
	}
	return parts[0] + "." + parts[1], true
}

// hobjectName returns the object name of an hobject_t printed as
// <pool>:<hash>:<namespace>:<key>:<name>:<snap>. Names with an escaped
// separator are not supported; the probe objects have plain names.
func hobjectName(hobject string) (string, bool) {
	parts := strings.Split(hobject, ":")
	if len(parts) != 6 || parts[4] == "" {
		return "", false
	}
	return parts[4], true
}

// errCopyGetIncomplete marks a history that does not hold the copy's entries
// yet: a later read of the same history may still show them. Every other
// attribution error is final.
var errCopyGetIncomplete = errors.New("the copy's history is incomplete")

// copyGetEvidence attributes copy-get chunk reads to one copy-from: the only
// copy-from on the destination object, and the distinct copy-get requests
// that an OSD issued on the source object while that copy-from ran.
type copyGetEvidence struct {
	Source      string          `json:"source"`
	Destination string          `json:"destination"`
	Chunk       int             `json:"chunk"`
	Want        int             `json:"want"`
	History     historicOpsDump `json:"history"`
	CopyFrom    *historicOp     `json:"copy_from,omitempty"`
	Chunks      []historicOp    `json:"chunks,omitempty"`
	// Relevant is every entry that names either probe object or carries a
	// copy-from or copy-get, in history order, for the diagnosis of a miss.
	Relevant []historicOp `json:"relevant,omitempty"`
}

// attributeCopyGets builds the evidence of one chunked copy from a parsed
// history. It fails, with the evidence so far, when the copy-from is not in
// the history (missing or late), when it is there more than once, when a
// copy-get of the copy
// reads another chunk size, or when fewer than want distinct chunks belong
// to it. Copy-gets of other objects, from clients, outside the copy-from's
// lifetime or repeated under one request id do not count, and neither do
// undecoded entries, which the shortfall error reports.
func attributeCopyGets(dump historicOpsDump, source, destination string, chunk, want int) (copyGetEvidence, error) {
	evidence := copyGetEvidence{Source: source, Destination: destination, Chunk: chunk, Want: want, History: dump}
	copyFroms := map[string]historicOp{}
	for _, op := range dump.Entries {
		if op.Undecoded || op.Object == source || op.Object == destination || slices.ContainsFunc(op.Ops, isCopyOp) {
			evidence.Relevant = append(evidence.Relevant, op)
		}
		if op.Decoded && op.Object == destination && slices.ContainsFunc(op.Ops, isCopyFrom) {
			if _, seen := copyFroms[op.ReqID]; !seen {
				copyFroms[op.ReqID] = op
			}
		}
	}
	switch len(copyFroms) {
	case 0:
		return evidence, fmt.Errorf("%w: the history has no copy-from on %s; the copy's entries are missing or not recorded yet", errCopyGetIncomplete, destination)
	case 1:
	default:
		return evidence, fmt.Errorf("the history has %d copy-from requests on %s; the copy is not identifiable", len(copyFroms), destination)
	}
	for _, op := range copyFroms {
		evidence.CopyFrom = &op
	}
	start := evidence.CopyFrom.Initiated
	end := start.Add(time.Duration(evidence.CopyFrom.Duration * float64(time.Second)))
	within := func(op historicOp) bool { return !op.Initiated.Before(start) && !op.Initiated.After(end) }
	seen, undecoded := map[string]bool{}, 0
	for _, op := range dump.Entries {
		if !within(op) {
			continue
		}
		if op.Undecoded {
			undecoded++
			continue
		}
		if !op.Decoded || op.Object != source || !strings.HasPrefix(op.Entity, "osd.") {
			continue
		}
		i := slices.IndexFunc(op.Ops, func(o string) bool { return strings.HasPrefix(o, "copy-get ") })
		if i < 0 {
			continue
		}
		if op.Ops[i] != fmt.Sprintf("copy-get max %d", chunk) {
			return evidence, fmt.Errorf("a copy-get of the copy reads %q, not chunks of %d bytes", op.Ops[i], chunk)
		}
		if !seen[op.ReqID] {
			seen[op.ReqID] = true
			evidence.Chunks = append(evidence.Chunks, op)
		}
	}
	if len(evidence.Chunks) < want {
		return evidence, fmt.Errorf("%w: the copy-from on %s has %d copy-get chunks of %d bytes on %s, want at least %d (%d undecoded entries during it)",
			errCopyGetIncomplete, destination, len(evidence.Chunks), chunk, source, want, undecoded)
	}
	return evidence, nil
}

func isCopyFrom(op string) bool { return strings.HasPrefix(op, "copy-from") }

func isCopyOp(op string) bool { return isCopyFrom(op) || strings.HasPrefix(op, "copy-get") }

// String renders the evidence as one JSON line, relevant entries included.
func (e copyGetEvidence) String() string {
	data, _ := json.Marshal(e)
	return string(data)
}
