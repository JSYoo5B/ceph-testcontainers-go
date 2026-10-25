package capture_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/capture"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/pcapfile"
)

// captureTailKind names how a retrieved capture file ends, as the existing
// classic-pcap parser reports it. It is an observation, never a request: a
// capture whose helper is killed can end in any of the shapes below, and
// only captureTailOther breaks the prefix-valid expectation.
type captureTailKind string

const (
	// captureTailEmpty is a file without a byte.
	captureTailEmpty captureTailKind = "empty"
	// captureTailHeaderCut is a file that ends inside the 24-byte file header.
	captureTailHeaderCut captureTailKind = "file_header_cut"
	// captureTailHeaderOnly is a valid file header and no record.
	captureTailHeaderOnly captureTailKind = "header_only"
	// captureTailRecords is a valid file of one or more whole records that
	// ends on a record boundary.
	captureTailRecords captureTailKind = "records_clean"
	// captureTailRecordHeaderCut ends inside a record header.
	captureTailRecordHeaderCut captureTailKind = "record_header_cut"
	// captureTailRecordDataCut ends inside a record's data.
	captureTailRecordDataCut captureTailKind = "record_data_cut"
	// captureTailOther is anything else: not a prefix of a valid classic
	// pcap file, or a verdict that is not about the file's end.
	captureTailOther captureTailKind = "not_prefix_valid"
)

var (
	recordHeaderCutReason = regexp.MustCompile(`\Arecord ([0-9]+) header cut off after ([0-9]+) of 16 bytes\z`)
	recordDataCutReason   = regexp.MustCompile(`\Arecord ([0-9]+) data cut off after ([0-9]+) of ([0-9]+) bytes\z`)
	fileHeaderCutReason   = regexp.MustCompile(`\Afile header cut off after [0-9]+ of 24 bytes\z`)
	fileTooShortReason    = regexp.MustCompile(`\Afile has [0-9]+ bytes, too short for a pcap magic number\z`)
)

// captureTail is the independent verdict of the parser on retrieved bytes.
type captureTail struct {
	Kind   captureTailKind
	Report pcapfile.Report
}

// classifyCaptureTail runs the existing classic-pcap parser over exactly the
// retrieved bytes, with the bounds of the capture request, and names the
// end of the file from its verdict. A cut is accepted only when the parser
// says it happened at the record that follows the whole records it counted.
func classifyCaptureTail(data []byte, limits pcapfile.Limits) (captureTail, error) {
	report, err := pcapfile.Validate(bytes.NewReader(data), limits)
	if err != nil {
		return captureTail{}, fmt.Errorf("check the retrieved bytes: %w", err)
	}
	return captureTail{Kind: captureTailKindOf(len(data), report), Report: report}, nil
}

// captureTailKindOf names the end of n bytes from the parser's verdict.
func captureTailKindOf(n int, report pcapfile.Report) captureTailKind {
	switch {
	case n == 0:
		return captureTailEmpty
	case report.Status == pcapfile.Valid && report.Complete && report.Packets == 0:
		return captureTailHeaderOnly
	case report.Status == pcapfile.Valid && report.Complete:
		return captureTailRecords
	case report.Status == pcapfile.Malformed && (fileHeaderCutReason.MatchString(report.Reason) || fileTooShortReason.MatchString(report.Reason)):
		return captureTailHeaderCut
	case report.Status == pcapfile.Malformed:
		if m := recordHeaderCutReason.FindStringSubmatch(report.Reason); m != nil && m[1] == strconv.Itoa(report.Packets+1) {
			return captureTailRecordHeaderCut
		}
		if m := recordDataCutReason.FindStringSubmatch(report.Reason); m != nil && m[1] == strconv.Itoa(report.Packets+1) {
			return captureTailRecordDataCut
		}
	}
	return captureTailOther
}

// captureStatistic renders a tcpdump counter: an absent counter is
// "unknown", never "0".
func captureStatistic(value *uint64) string {
	if value == nil {
		return "unknown"
	}
	return strconv.FormatUint(*value, 10)
}

// captureKillInput is everything the observation of one killed capture is
// built from. Snapshot is the helper's file as read while the capture still
// ran, before the kill.
type captureKillInput struct {
	Report capture.Report
	// Retrieved is the content of Report.Output as read back from disk.
	Retrieved []byte
	Snapshot  []byte
	// HelperRunningBeforeKill and HelperRunningAfterKill are the engine's
	// state of the one helper joined to the target around the kill.
	HelperRunningBeforeKill, HelperRunningAfterKill bool
	// HelperExitCode is the exit status the engine recorded for the helper
	// after the kill, taken before Stop removed it.
	HelperExitCode int
	// RequestsBefore and RequestsAfter are the client's completed
	// requests when the kill was sent and after the kill had taken effect.
	RequestsBefore, RequestsAfter int
	// TargetUnchanged is the caller's comparison of the target before the
	// capture with the target after cleanup.
	TargetUnchanged bool
	Limits          pcapfile.Limits
}

// captureKillObservation is the recorded outcome of one killed capture. It
// holds counts, sizes and digests only, never packet content.
type captureKillObservation struct {
	SnapshotBytes   int    `json:"snapshot_bytes"`
	SnapshotRecords int    `json:"snapshot_records"`
	RequestsBefore  int    `json:"requests_before_kill"`
	RequestsAfter   int    `json:"requests_after_kill"`
	HelperExitCode  int    `json:"helper_exit_code"`
	End             string `json:"capture_end"`
	ReportExitCode  int    `json:"capture_exit_code"`
	Target          string `json:"target"`
	HelperRemoved   bool   `json:"helper_removed"`

	Tail            captureTailKind `json:"tail"`
	TailStatus      string          `json:"parser_status"`
	TailReason      string          `json:"parser_reason,omitempty"`
	Records         int             `json:"records"`
	RetrievedBytes  int64           `json:"retrieved_bytes"`
	RetrievedSHA256 string          `json:"retrieved_sha256"`
	// SnapshotIsPrefix says that the pre-kill snapshot is a byte prefix of
	// the retrieved file.
	SnapshotIsPrefix bool `json:"snapshot_is_prefix"`

	Captured           string `json:"stat_captured"`
	ReceivedByFilter   string `json:"stat_received_by_filter"`
	DroppedByKernel    string `json:"stat_dropped_by_kernel"`
	DroppedByInterface string `json:"stat_dropped_by_interface"`

	// Violations lists the invariants that the observation breaks; the
	// outcome itself (End, Tail) is never a violation.
	Violations []string `json:"violations,omitempty"`
}

// observeCaptureKill records the outcome and checks only invariants. Any
// tail shape the parser reports is a valid result, with the one exception of
// bytes that are not a prefix of a classic pcap file.
func observeCaptureKill(in captureKillInput) (captureKillObservation, error) {
	r := in.Report
	tail, err := classifyCaptureTail(in.Retrieved, in.Limits)
	if err != nil {
		return captureKillObservation{}, err
	}
	snapshot, err := classifyCaptureTail(in.Snapshot, in.Limits)
	if err != nil {
		return captureKillObservation{}, err
	}
	sum := sha256.Sum256(in.Retrieved)
	o := captureKillObservation{
		SnapshotBytes: len(in.Snapshot), SnapshotRecords: snapshot.Report.Packets,
		RequestsBefore: in.RequestsBefore, RequestsAfter: in.RequestsAfter,
		HelperExitCode: in.HelperExitCode, End: string(r.End), ReportExitCode: r.ExitCode,
		Target: string(r.Target), HelperRemoved: r.HelperRemoved,
		Tail: tail.Kind, TailStatus: string(tail.Report.Status), TailReason: tail.Report.Reason, Records: tail.Report.Packets,
		RetrievedBytes: int64(len(in.Retrieved)), RetrievedSHA256: hex.EncodeToString(sum[:]),
		SnapshotIsPrefix: bytes.HasPrefix(in.Retrieved, in.Snapshot),
		Captured:         captureStatistic(r.Statistics.Captured), ReceivedByFilter: captureStatistic(r.Statistics.ReceivedByFilter),
		DroppedByKernel: captureStatistic(r.Statistics.DroppedByKernel), DroppedByInterface: captureStatistic(r.Statistics.DroppedByInterface),
	}
	violate := func(format string, args ...any) { o.Violations = append(o.Violations, fmt.Sprintf(format, args...)) }

	// The kill: a running helper before, a stopped one after, with a
	// nonzero status (a process that a signal ended never exits with 0 here).
	if !in.HelperRunningBeforeKill {
		violate("the helper was not running when the kill was sent")
	}
	if in.HelperRunningAfterKill {
		violate("the helper still ran after the kill")
	}
	if in.HelperExitCode == 0 {
		violate("the killed helper recorded exit status 0")
	}
	// Traffic was in flight: the client kept completing requests past the kill.
	if in.RequestsAfter <= in.RequestsBefore {
		violate("the client completed no request after the kill (%d, then %d)", in.RequestsBefore, in.RequestsAfter)
	}
	// Records before the kill: the snapshot is whole records the parser read.
	if o.SnapshotRecords == 0 {
		violate("the pre-kill snapshot holds no complete record")
	}
	if !o.SnapshotIsPrefix {
		violate("the pre-kill snapshot is not a prefix of the retrieved file")
	}
	// The retrieved bytes.
	if tail.Kind == captureTailOther {
		violate("the retrieved bytes are not a prefix-valid classic pcap: %s %q", tail.Report.Status, tail.Report.Reason)
	}
	if tail.Report.Packets < snapshot.Report.Packets {
		violate("fewer records after the kill (%d) than before it (%d)", tail.Report.Packets, snapshot.Report.Packets)
	}
	// The report must describe those same bytes.
	if !r.Retrieved || r.RetrievedBytes != int64(len(in.Retrieved)) {
		violate("report retrieved %t/%d bytes, the delivered file has %d", r.Retrieved, r.RetrievedBytes, len(in.Retrieved))
	}
	if r.RetrievedSHA256 != o.RetrievedSHA256 {
		violate("report digest %q, delivered file digest %q", r.RetrievedSHA256, o.RetrievedSHA256)
	}
	if want := tail.Report; string(r.File.Status) != string(want.Status) || r.File.Reason != want.Reason || r.File.Packets != want.Packets ||
		r.File.TruncatedPackets != want.TruncatedPackets || r.File.BytesRead != want.BytesRead || r.File.SHA256 != want.SHA256 || r.File.Complete != want.Complete {
		violate("report file check %+v differs from the independent check %+v", r.File, want)
	}
	// The helper's end was recorded as a failure with the engine's status.
	if r.End == capture.EndStopped || r.End == capture.EndPacketLimit {
		violate("a killed helper was reported as %s", r.End)
	}
	if r.ExitCode != in.HelperExitCode {
		violate("report exit status %d, engine recorded %d", r.ExitCode, in.HelperExitCode)
	}
	// Ownership and cleanup.
	if !r.HelperRemoved {
		violate("the helper was not removed")
	}
	if r.Target != capture.TargetUnchanged || !in.TargetUnchanged {
		violate("the caller's target changed (report %s, caller check %t)", r.Target, in.TargetUnchanged)
	}
	return o, nil
}
