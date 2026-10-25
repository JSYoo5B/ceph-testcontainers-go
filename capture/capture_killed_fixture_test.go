package capture_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/capture"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/pcapfile"
)

const killedFixtureSnapLen = 96

var killedFixtureLimits = pcapfile.Limits{MaxFileBytes: 1 << 20, MaxRecordBytes: killedFixtureSnapLen}

// killedFixtureFile builds a little-endian microsecond classic pcap file with
// records of the given captured lengths.
func killedFixtureFile(records ...int) []byte {
	var out bytes.Buffer
	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header, 0xa1b2c3d4)
	binary.LittleEndian.PutUint16(header[4:], 2)
	binary.LittleEndian.PutUint16(header[6:], 4)
	binary.LittleEndian.PutUint32(header[16:], killedFixtureSnapLen)
	binary.LittleEndian.PutUint32(header[20:], 1)
	out.Write(header)
	for i, n := range records {
		record := make([]byte, 16)
		binary.LittleEndian.PutUint32(record, uint32(1000+i))
		binary.LittleEndian.PutUint32(record[8:], uint32(n))
		binary.LittleEndian.PutUint32(record[12:], uint32(n))
		out.Write(record)
		out.Write(bytes.Repeat([]byte{byte(i + 1)}, n))
	}
	return out.Bytes()
}

func TestCaptureTailClassification(t *testing.T) {
	whole := killedFixtureFile(60, 40, 96)
	pcapng := append([]byte{0x0a, 0x0d, 0x0d, 0x0a}, make([]byte, 40)...)
	oversized := killedFixtureFile(60)
	binary.LittleEndian.PutUint32(oversized[24+8:], killedFixtureSnapLen+1)
	for _, test := range []struct {
		name    string
		data    []byte
		kind    captureTailKind
		records int
	}{
		{"empty", nil, captureTailEmpty, 0},
		{"two bytes", whole[:2], captureTailHeaderCut, 0},
		{"inside the file header", whole[:10], captureTailHeaderCut, 0},
		{"header only", whole[:24], captureTailHeaderOnly, 0},
		{"one whole record", whole[:24+16+60], captureTailRecords, 1},
		{"all records", whole, captureTailRecords, 3},
		{"inside a record header", whole[:24+16+60+7], captureTailRecordHeaderCut, 1},
		{"record header without data", whole[:24+16+60+16], captureTailRecordDataCut, 1},
		{"inside record data", whole[:len(whole)-1], captureTailRecordDataCut, 2},
		{"first record cut", whole[:24+16+5], captureTailRecordDataCut, 0},
		// Controls: not a prefix of a valid classic pcap file.
		{"unknown magic", append([]byte{1, 2, 3, 4}, whole[4:]...), captureTailOther, 0},
		{"pcapng", pcapng, captureTailOther, 0},
		{"captured length above snaplen", oversized, captureTailOther, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			tail, err := classifyCaptureTail(test.data, killedFixtureLimits)
			if err != nil {
				t.Fatal(err)
			}
			if tail.Kind != test.kind || tail.Report.Packets != test.records {
				t.Fatalf("tail %s with %d records (%s %q), want %s with %d", tail.Kind, tail.Report.Packets,
					tail.Report.Status, tail.Report.Reason, test.kind, test.records)
			}
		})
	}
}

// A cut verdict counts only at the record after the whole records the parser
// counted, and only for a malformed verdict.
func TestCaptureTailKindRefusesCutAtAnotherRecord(t *testing.T) {
	cut := pcapfile.Report{Status: pcapfile.Malformed, Reason: "record 3 data cut off after 5 of 40 bytes", Packets: 2}
	if kind := captureTailKindOf(500, cut); kind != captureTailRecordDataCut {
		t.Fatalf("matching record: %s", kind)
	}
	for name, report := range map[string]pcapfile.Report{
		"another record": {Status: pcapfile.Malformed, Reason: cut.Reason, Packets: 5},
		"valid verdict":  {Status: pcapfile.Valid, Reason: cut.Reason, Packets: 2},
		"unsupported":    {Status: pcapfile.Unsupported, Reason: cut.Reason, Packets: 2},
		"limit":          {Status: pcapfile.LimitExceeded, Reason: cut.Reason, Packets: 2},
		"incomplete":     {Status: pcapfile.Valid, Packets: 2},
	} {
		if kind := captureTailKindOf(500, report); kind != captureTailOther {
			t.Errorf("%s: %s", name, kind)
		}
	}
}

func killedFixtureReport(data []byte) capture.Report {
	checked, err := pcapfile.Validate(bytes.NewReader(data), killedFixtureLimits)
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(data)
	return capture.Report{
		End: capture.EndFailed, ExitCode: 137, Target: capture.TargetUnchanged, HelperRemoved: true,
		Retrieved: true, RetrievedBytes: int64(len(data)), RetrievedSHA256: hex.EncodeToString(sum[:]),
		File: capture.FileReport{Status: capture.FileStatus(checked.Status), Reason: checked.Reason, Packets: checked.Packets,
			TruncatedPackets: checked.TruncatedPackets, BytesRead: checked.BytesRead, SHA256: checked.SHA256, Complete: checked.Complete},
	}
}

func killedFixtureInput(final []byte) captureKillInput {
	snapshot := final[:24+16+60]
	return captureKillInput{
		Report: killedFixtureReport(final), Retrieved: final, Snapshot: snapshot,
		HelperRunningBeforeKill: true, HelperRunningAfterKill: false, HelperExitCode: 137,
		RequestsAtStop: 10, RequestsLater: 13, TargetUnchanged: true, Limits: killedFixtureLimits,
	}
}

func TestObserveCaptureKillRecordsEveryTailWithoutViolation(t *testing.T) {
	whole := killedFixtureFile(60, 40, 96)
	for name, final := range map[string][]byte{
		"clean":             whole,
		"cut record data":   whole[:len(whole)-3],
		"cut record header": whole[:24+16+60+16+40+5],
	} {
		t.Run(name, func(t *testing.T) {
			o, err := observeCaptureKill(killedFixtureInput(final))
			if err != nil {
				t.Fatal(err)
			}
			if len(o.Violations) != 0 {
				t.Fatalf("violations %q for %s", o.Violations, o.Tail)
			}
			sum := sha256.Sum256(final)
			if o.RetrievedBytes != int64(len(final)) || o.RetrievedSHA256 != hex.EncodeToString(sum[:]) || !o.SnapshotIsPrefix || o.SnapshotRecords != 1 {
				t.Fatalf("observation %+v", o)
			}
		})
	}
}

func TestObserveCaptureKillKeepsUnknownStatisticsUnknown(t *testing.T) {
	input := killedFixtureInput(killedFixtureFile(60, 40))
	zero, three := uint64(0), uint64(3)
	input.Report.Statistics = capture.Statistics{Captured: &zero, DroppedByKernel: &three}
	o, err := observeCaptureKill(input)
	if err != nil {
		t.Fatal(err)
	}
	if o.Captured != "0" || o.DroppedByKernel != "3" || o.ReceivedByFilter != "unknown" || o.DroppedByInterface != "unknown" {
		t.Fatalf("statistics %+v", o)
	}
	input.Report.Statistics = capture.Statistics{}
	o, err = observeCaptureKill(input)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(o)
	for _, field := range []string{"stat_captured", "stat_received_by_filter", "stat_dropped_by_kernel", "stat_dropped_by_interface"} {
		if !strings.Contains(string(data), `"`+field+`":"unknown"`) {
			t.Fatalf("%s is not unknown in %s", field, data)
		}
	}
	if strings.Contains(string(data), `"0"`) {
		t.Fatalf("an unknown statistic became zero: %s", data)
	}
}

// Each control breaks one invariant and must be named; the others stay quiet.
func TestObserveCaptureKillNamesEveryBrokenInvariant(t *testing.T) {
	whole := killedFixtureFile(60, 40, 96)
	for _, test := range []struct {
		name   string
		break_ func(*captureKillInput)
		want   string
	}{
		{"helper not running at the kill", func(in *captureKillInput) { in.HelperRunningBeforeKill = false }, "not running when the kill"},
		{"helper still running", func(in *captureKillInput) { in.HelperRunningAfterKill = true }, "still ran after the kill"},
		{"exit status zero", func(in *captureKillInput) { in.HelperExitCode = 0; in.Report.ExitCode = 0 }, "exit status 0"},
		{"no traffic after the helper stopped", func(in *captureKillInput) { in.RequestsLater = in.RequestsAtStop }, "no request after the helper stopped"},
		{"unavailable baseline", func(in *captureKillInput) { in.RequestsAtStop = -1 }, "counter was unavailable"},
		{"unavailable later read", func(in *captureKillInput) { in.RequestsLater = -1 }, "counter was unavailable"},
		{"no record before the kill", func(in *captureKillInput) { in.Snapshot = in.Retrieved[:24] }, "no complete record"},
		{"snapshot not a prefix", func(in *captureKillInput) { in.Snapshot = slices.Clone(in.Snapshot); in.Snapshot[30] ^= 0xff }, "not a prefix of the retrieved"},
		{"not a pcap", func(in *captureKillInput) {
			in.Retrieved = append([]byte{9, 9, 9, 9}, in.Retrieved[4:]...)
			in.Snapshot = in.Retrieved[:24+16+60]
		}, "not a prefix-valid classic pcap"},
		{"fewer records after the kill", func(in *captureKillInput) { in.Snapshot = whole; in.Retrieved = whole[:24+16+60] }, "fewer records"},
		{"retrieved size", func(in *captureKillInput) { in.Report.RetrievedBytes++ }, "report retrieved"},
		{"digest", func(in *captureKillInput) { in.Report.RetrievedSHA256 = strings.Repeat("0", 64) }, "report digest"},
		{"file check", func(in *captureKillInput) { in.Report.File.Packets++ }, "differs from the independent check"},
		{"reported as stopped", func(in *captureKillInput) { in.Report.End = capture.EndStopped }, "reported as stopped"},
		{"reported as packet limit", func(in *captureKillInput) { in.Report.End = capture.EndPacketLimit }, "reported as packet_limit"},
		{"exit status mismatch", func(in *captureKillInput) { in.Report.ExitCode = 1 }, "report exit status"},
		{"helper kept", func(in *captureKillInput) { in.Report.HelperRemoved = false }, "not removed"},
		{"target changed in the report", func(in *captureKillInput) { in.Report.Target = capture.TargetChanged }, "target changed"},
		{"target changed for the caller", func(in *captureKillInput) { in.TargetUnchanged = false }, "target changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := killedFixtureInput(whole)
			test.break_(&input)
			o, err := observeCaptureKill(input)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(o.Violations, func(v string) bool { return strings.Contains(v, test.want) }) {
				t.Fatalf("violations %q do not name %q", o.Violations, test.want)
			}
		})
	}
}

func counterReader(values ...int) (read func() int, reads *int) {
	reads = new(int)
	return func() int {
		n := values[min(*reads, len(values)-1)]
		*reads++
		return n
	}, reads
}

func TestCaptureCounterBaselineRetriesEmptyReadsAndRefusesUnavailable(t *testing.T) {
	noWait := func() {}
	read, _ := counterReader(-1, -1, 7)
	if got := captureCounterBaseline(read, 5, noWait); got != 7 {
		t.Fatalf("baseline %d after transient empty reads", got)
	}
	read, reads := counterReader(-1)
	if got := captureCounterBaseline(read, 5, noWait); got != -1 || *reads != 5 {
		t.Fatalf("baseline %d after %d reads of an unavailable counter", got, *reads)
	}
}

func TestCaptureCounterAfterNeedsALaterIncrement(t *testing.T) {
	noWait := func() {}
	read, _ := counterReader(10, 10, -1, 11)
	if got := captureCounterAfter(read, 10, 6, noWait); got != 11 {
		t.Fatalf("later %d", got)
	}
	// Progress confined to the interval before the baseline: the counter
	// never rises above it, and the result is refused by the observation.
	read, reads := counterReader(10)
	later := captureCounterAfter(read, 10, 4, noWait)
	if later != 10 || *reads != 4 {
		t.Fatalf("later %d after %d reads", later, *reads)
	}
	input := killedFixtureInput(killedFixtureFile(60, 40, 96))
	input.RequestsAtStop, input.RequestsLater = 10, later
	o, err := observeCaptureKill(input)
	if err != nil || len(o.Violations) != 1 || !strings.Contains(o.Violations[0], "no request after the helper stopped") {
		t.Fatalf("%+v, %v", o.Violations, err)
	}
	// An unavailable baseline is never compared.
	read, reads = counterReader(50)
	if got := captureCounterAfter(read, -1, 4, noWait); got != -1 || *reads != 0 {
		t.Fatalf("later %d after %d reads against an unavailable baseline", got, *reads)
	}
}
