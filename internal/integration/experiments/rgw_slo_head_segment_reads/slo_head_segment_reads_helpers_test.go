//go:build all || integration

package rgw_slo_head_segment_reads_test

import (
	"cmp"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// osdOp is one client operation of an OSD's op history: the request ID, the
// pool and the object it addressed, and its sub-operations as the OSD prints
// them, such as "getxattrs" or "read 0~3145728". An operation on a PG, such
// as an object listing, has no object. Unread is empty for an operation
// whose object and sub-operations were read, and otherwise says why they
// were not.
type osdOp struct {
	ReqID  string
	Pool   int64
	Object string
	Subs   []string
	Unread string
}

// opHistory is one dump_historic_ops report: the number of entries the
// history keeps (Size), the number it holds (Total) and its client
// operations.
type opHistory struct {
	Size  int
	Total int
	Ops   []osdOp
}

// parseOpHistory reads one dump_historic_ops report. The report must carry
// its size and its op list, which may be empty but not absent or null, and
// every entry a description. An entry that is not a client operation
// (osd_op) is counted and skipped. A client operation must name its request
// and its pool; one whose object or sub-operations cannot be read is kept
// as unread, so windowOps can refuse it in the pool it observes and an
// operation the history could not describe never reads as one that did not
// happen.
func parseOpHistory(report []byte) (opHistory, error) {
	var raw struct {
		Size *int `json:"size"`
		Ops  *[]struct {
			Description *string `json:"description"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(report, &raw); err != nil {
		return opHistory{}, fmt.Errorf("op history: %w", err)
	}
	if raw.Size == nil || raw.Ops == nil {
		return opHistory{}, fmt.Errorf("op history: the size or the op list is missing")
	}
	history := opHistory{Size: *raw.Size, Total: len(*raw.Ops)}
	for i, entry := range *raw.Ops {
		if entry.Description == nil || *entry.Description == "" {
			return opHistory{}, fmt.Errorf("op history: entry %d has no description", i)
		}
		if !strings.HasPrefix(*entry.Description, "osd_op(") {
			continue
		}
		op, err := parseOSDOp(*entry.Description)
		if err != nil {
			return opHistory{}, fmt.Errorf("op history: entry %d: %w", i, err)
		}
		history.Ops = append(history.Ops, op)
	}
	return history, nil
}

// parseOSDOp reads the description of a client operation:
//
//	osd_op(<reqid> <pool>.<pg> <pool>:<hash>:<namespace>:<key>:<name>:<snap> [<sub>,...] snapc ...)
//
// The name is empty for an operation on the PG. A description without a
// request ID or a pool is an error. One whose object or sub-operations do
// not have this form, such as an operation the OSD recorded before decoding
// it, is returned as unread.
func parseOSDOp(description string) (osdOp, error) {
	body, ok := strings.CutPrefix(description, "osd_op(")
	if !ok || !strings.HasSuffix(body, ")") {
		return osdOp{}, fmt.Errorf("client operation %q: not an osd_op(...)", description)
	}
	reqid, rest, _ := strings.Cut(body, " ")
	pgid, rest, _ := strings.Cut(strings.TrimSuffix(rest, ")"), " ")
	poolText, _, ok := strings.Cut(pgid, ".")
	pool, err := strconv.ParseInt(poolText, 10, 64)
	if reqid == "" || !ok || err != nil {
		return osdOp{}, fmt.Errorf("client operation %q: no request ID or no PG of a pool", description)
	}
	op := osdOp{ReqID: reqid, Pool: pool}
	unread := func(reason string) (osdOp, error) {
		op.Unread = fmt.Sprintf("%s: %s", reason, description)
		return op, nil
	}
	open := strings.Index(rest, " [")
	if open < 0 {
		return unread("no sub-operations")
	}
	closing := strings.Index(rest[open:], "] snapc ")
	if closing < 0 {
		return unread("the sub-operations do not end")
	}
	object := strings.Split(rest[:open], ":")
	if len(object) != 6 || object[0] != poolText {
		return unread("the object is not <pool>:<hash>:<namespace>:<key>:<name>:<snap> of the PG's pool")
	}
	op.Object, op.Subs = object[4], strings.Split(rest[open+2:open+closing], ",")
	for i, sub := range op.Subs {
		op.Subs[i] = strings.TrimSpace(sub)
		if op.Subs[i] == "" {
			op.Object, op.Subs = "", nil
			return unread("an empty sub-operation")
		}
	}
	return op, nil
}

// extent is the byte range one read sub-operation asks for. A length of
// zero asks for everything from the offset to the end of the object.
type extent struct{ Off, Len int64 }

// within returns the bytes [start, end) of an object of size bytes that the
// read returns: the OSD reads to the end of the object for a length of
// zero, and cuts a read off at the end of the object. A read that starts at
// or beyond the end returns nothing.
func (e extent) within(size int64) (start, end int64) {
	if e.Off >= size {
		return size, size
	}
	if e.Len == 0 || e.Off+e.Len > size {
		return e.Off, size
	}
	return e.Off, e.Off + e.Len
}

// metadataCall is the one object class method the case knows to return no
// object data: version.read, which reads the object's version from one of
// its attributes. Any other method may read the object's data inside the
// OSD without returning it, so it is not classified.
const metadataCall = "call version.read"

// sizeSuffix matches the size of the input a sub-operation carries.
var sizeSuffix = regexp.MustCompile(`^in=[0-9]+b$`)

// parseSub classifies one sub-operation by its whole text, as the OSD
// prints it for an operation it has not executed yet. "read
// <offset>~<length>" is a data read of that extent. "getxattrs", "stat" and
// "call version.read", the last with the size of its input, read attributes
// and the object's size and return no extent. Everything else is returned
// as other, because the case cannot say what it read of the object: a call
// of another method or of no named method, a read in another form or with
// anything after its extent, and every other sub-operation.
func parseSub(sub string) (read *extent, other bool) {
	fields := strings.Fields(sub)
	switch {
	case sub == "getxattrs" || sub == "stat":
		return nil, false
	case strings.Join(fields[:min(2, len(fields))], " ") == metadataCall:
		return nil, len(fields) > 3 || (len(fields) == 3 && !sizeSuffix.MatchString(fields[2]))
	case len(fields) != 2 || fields[0] != "read":
		return nil, true
	}
	offText, lenText, ok := strings.Cut(fields[1], "~")
	off, errOff := strconv.ParseUint(offText, 10, 63)
	length, errLen := strconv.ParseUint(lenText, 10, 63)
	if !ok || errOff != nil || errLen != nil {
		return nil, true
	}
	return &extent{Off: int64(off), Len: int64(length)}, false
}

// objectOps is what one window recorded on one object of the data pool: the
// number of client operations, the extents of their data reads and the
// sub-operations that are neither a data read nor a metadata read.
type objectOps struct {
	Requests int
	Reads    []extent
	Other    []string
}

// readBytes is the number of bytes the reads return of an object of size
// bytes, counting a byte once per read that returns it.
func (o objectOps) readBytes(size int64) int64 {
	var sum int64
	for _, read := range o.Reads {
		start, end := read.within(size)
		sum += end - start
	}
	return sum
}

// covers reports whether the reads together return every byte of an object
// of size bytes.
func (o objectOps) covers(size int64) bool {
	type span struct{ start, end int64 }
	spans := make([]span, len(o.Reads))
	for i, read := range o.Reads {
		spans[i].start, spans[i].end = read.within(size)
	}
	slices.SortFunc(spans, func(a, b span) int { return cmp.Compare(a.start, b.start) })
	var end int64
	for _, s := range spans {
		if s.start > end {
			break
		}
		end = max(end, s.end)
	}
	return size > 0 && end >= size
}

// windowOps returns, per object of the data pool, the client operations of
// history that seen does not hold yet, and adds every operation of history
// to seen. The window is closed by the marker: an operation on the object
// marker of the data pool, which the test sends after the response to its
// request, must be among the new operations exactly once, and is left out
// of the result. An operation on a PG of the data pool is returned under
// the empty object name. A history that holds as many entries as it keeps
// may have dropped one, a window without its marker has not caught up with
// the request, and a new operation of the data pool that was not read
// could be anything, so all three are errors.
func windowOps(history opHistory, seen map[string]bool, pool int64, marker string) (map[string]objectOps, error) {
	if history.Total >= history.Size {
		return nil, fmt.Errorf("window not observed: the op history holds %d entries of the %d it keeps, so it may have dropped one", history.Total, history.Size)
	}
	window, markers := map[string]objectOps{}, 0
	for _, op := range history.Ops {
		if seen[op.ReqID] {
			continue
		}
		seen[op.ReqID] = true
		if op.Pool != pool {
			continue
		}
		if op.Unread != "" {
			return nil, fmt.Errorf("window not observed: an operation of the data pool was not read: %s", op.Unread)
		}
		if op.Object == marker {
			markers++
			continue
		}
		object := window[op.Object]
		object.Requests++
		for _, sub := range op.Subs {
			read, other := parseSub(sub)
			switch {
			case other:
				object.Other = append(object.Other, sub)
			case read != nil:
				object.Reads = append(object.Reads, *read)
			}
		}
		window[op.Object] = object
	}
	if markers != 1 {
		return nil, fmt.Errorf("window not observed: the op history has %d new operations on the marker %s, want 1", markers, marker)
	}
	return window, nil
}

// requireMetadataOnly requires a window of a HEAD that reads no object data:
// at least one operation on the head object, so the request reached the
// OSD, no operation on any other object, and no data read or unclassified
// sub-operation.
func requireMetadataOnly(window map[string]objectOps, head string) error {
	if window[head].Requests == 0 {
		return fmt.Errorf("condition not established: the window has no operation on the head object %s: %v", head, window)
	}
	for name, object := range window {
		if name != head {
			return fmt.Errorf("window not observed: an operation on %s, which is not the head object %s: %v", name, head, window)
		}
		if len(object.Other) > 0 {
			return fmt.Errorf("window not observed: unclassified sub-operations %q on %s", object.Other, name)
		}
		if len(object.Reads) > 0 {
			return fmt.Errorf("control not established: the HEAD read data of %s: %v", name, object.Reads)
		}
	}
	return nil
}

// segment is one segment of the static large object: its object in the data
// pool and its size.
type segment struct {
	Object string
	Size   int64
}

// sloWindow is what the OSD recorded during one request for the static large
// object: the operations on the objects of the data pool, and the growth of
// the counter of bytes the OSD returned to its clients for reads.
type sloWindow struct {
	Ops      map[string]objectOps
	OutBytes int64
}

// judgeSLOHead decides what the OSD shows for a HEAD of the static large
// object whose manifest is the object manifest and whose payload is
// segments, given what it shows for a full GET of the same object.
//
// The GET is the positive control of the observation: its window must hold
// data reads that cover every segment, and the OSD must have returned at
// least the payload. The HEAD must have reached the OSD: its window must
// hold an operation on the manifest. Both windows may hold operations on the
// manifest and the segments only, and no sub-operation the case cannot
// classify. Then the symptoms are returned when the HEAD window's data reads
// cover every segment and the OSD returned at least the payload, and neither
// symptoms nor an error when the HEAD window has no data read on any
// segment and the OSD returned less than the smallest segment. Every other
// observation is an error: reads of some segments or of parts of them, and
// windows whose operations and byte counter disagree.
func judgeSLOHead(manifest string, segments []segment, get, head sloWindow) ([]string, error) {
	if manifest == "" || len(segments) == 0 {
		return nil, fmt.Errorf("outcome not observed: no manifest object or no segment")
	}
	var payload int64
	smallest := segments[0].Size
	for _, s := range segments {
		if s.Object == "" || s.Size <= 0 {
			return nil, fmt.Errorf("outcome not observed: segment %+v has no object or no size", s)
		}
		payload += s.Size
		smallest = min(smallest, s.Size)
	}
	for i, window := range []sloWindow{get, head} {
		name := []string{"GET", "HEAD"}[i]
		for object, ops := range window.Ops {
			if object != manifest && !slices.ContainsFunc(segments, func(s segment) bool { return s.Object == object }) {
				return nil, fmt.Errorf("window not observed: the %s window has an operation on %s, which is neither the manifest nor a segment: %v", name, object, window.Ops)
			}
			if len(ops.Other) > 0 {
				return nil, fmt.Errorf("window not observed: the %s window has unclassified sub-operations %q on %s", name, ops.Other, object)
			}
		}
		if window.Ops[manifest].Requests == 0 {
			return nil, fmt.Errorf("condition not established: the %s window has no operation on the manifest %s, so the request did not reach the OSD: %v", name, manifest, window.Ops)
		}
	}
	for _, s := range segments {
		if !get.Ops[s.Object].covers(s.Size) {
			return nil, fmt.Errorf("control not established: the full GET shows no data reads that cover the %d bytes of %s: %v", s.Size, s.Object, get.Ops[s.Object].Reads)
		}
	}
	if get.OutBytes < payload {
		return nil, fmt.Errorf("control not established: the OSD returned %d bytes during the full GET, less than the payload of %d", get.OutBytes, payload)
	}
	var covered, untouched []string
	for _, s := range segments {
		ops := head.Ops[s.Object]
		switch {
		case ops.covers(s.Size):
			covered = append(covered, fmt.Sprintf("the HEAD read all %d bytes of segment %s: %v", s.Size, s.Object, ops.Reads))
		case len(ops.Reads) == 0:
			untouched = append(untouched, s.Object)
		default:
			return nil, fmt.Errorf("another state: the HEAD read %d bytes of the %d of segment %s, neither none nor all of it: %v", ops.readBytes(s.Size), s.Size, s.Object, ops.Reads)
		}
	}
	switch {
	case len(covered) == len(segments):
		if head.OutBytes < payload {
			return nil, fmt.Errorf("outcome not observed: the HEAD window's reads cover every segment, but the OSD returned only %d bytes, less than the payload of %d", head.OutBytes, payload)
		}
		return append(covered, fmt.Sprintf("the OSD returned %d bytes during the HEAD, the payload is %d and the full GET returned %d", head.OutBytes, payload, get.OutBytes)), nil
	case len(untouched) == len(segments):
		if head.OutBytes >= smallest {
			return nil, fmt.Errorf("outcome not observed: the HEAD window has no data read on a segment, but the OSD returned %d bytes, at least the smallest segment of %d", head.OutBytes, smallest)
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("another state: the HEAD read %d of the %d segments in full and none of %v", len(covered), len(segments), untouched)
	}
}

// parseReadOutBytes reads the counter of bytes the OSD returned to its
// clients for reads from a "perf dump osd" report. Zero is a count; a
// missing or null counter is not.
func parseReadOutBytes(report []byte) (int64, error) {
	var raw struct {
		OSD *struct {
			OutBytes *int64 `json:"op_r_out_bytes"`
		} `json:"osd"`
	}
	if err := json.Unmarshal(report, &raw); err != nil {
		return 0, fmt.Errorf("perf dump: %w", err)
	}
	if raw.OSD == nil || raw.OSD.OutBytes == nil || *raw.OSD.OutBytes < 0 {
		return 0, fmt.Errorf("perf dump: no osd.op_r_out_bytes counter")
	}
	return *raw.OSD.OutBytes, nil
}

// parseBucketMarker reads the marker of a bucket, the prefix of its objects
// in the data pool, from a "bucket stats" report.
func parseBucketMarker(report []byte) (string, error) {
	var raw struct {
		Marker *string `json:"marker"`
	}
	if err := json.Unmarshal(report, &raw); err != nil {
		return "", fmt.Errorf("bucket stats: %w", err)
	}
	if raw.Marker == nil || *raw.Marker == "" {
		return "", fmt.Errorf("bucket stats: no marker")
	}
	return *raw.Marker, nil
}
