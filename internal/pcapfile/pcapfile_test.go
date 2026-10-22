package pcapfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
)

// capture builds a classic pcap file. Each record is (captured bytes,
// original length).
type capture struct {
	order    binary.ByteOrder
	magic    uint32
	major    uint16
	minor    uint16
	snaplen  uint32
	linkType uint32
	records  []record
}

type record struct {
	data      []byte
	original  uint32
	subsecond uint32
}

func newCapture(order binary.ByteOrder, magic uint32) *capture {
	return &capture{order: order, magic: magic, major: 2, minor: 4, snaplen: 65535, linkType: 1}
}

func (c *capture) add(data []byte, original uint32) *capture {
	c.records = append(c.records, record{data: data, original: original})
	return c
}

func (c *capture) bytes() []byte {
	var b bytes.Buffer
	put := func(value any) { _ = binary.Write(&b, c.order, value) }
	put(c.magic)
	put(c.major)
	put(c.minor)
	put(int32(0))
	put(uint32(0))
	put(c.snaplen)
	put(c.linkType)
	for i, r := range c.records {
		put(uint32(1_700_000_000 + i))
		put(r.subsecond)
		put(uint32(len(r.data)))
		put(r.original)
		b.Write(r.data)
	}
	return b.Bytes()
}

func check(t *testing.T, data []byte, limits Limits) Report {
	t.Helper()
	report, err := Validate(bytes.NewReader(data), limits)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestValidCapturesInEveryByteOrderAndTimestampUnit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		order binary.ByteOrder
		magic uint32
		want  [2]string
	}{
		{"little micro", binary.LittleEndian, magicMicro, [2]string{"little", "microsecond"}},
		{"big micro", binary.BigEndian, magicMicro, [2]string{"big", "microsecond"}},
		{"little nano", binary.LittleEndian, magicNano, [2]string{"little", "nanosecond"}},
		{"big nano", binary.BigEndian, magicNano, [2]string{"big", "nanosecond"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := newCapture(tc.order, tc.magic).add([]byte("syn"), 3).add(bytes.Repeat([]byte{7}, 60), 60).bytes()
			report := check(t, data, Limits{})
			if report.Status != Valid || report.Reason != "" || report.Packets != 2 || report.TruncatedPackets != 0 {
				t.Fatalf("report %+v", report)
			}
			if [2]string{report.ByteOrder, report.TimestampUnit} != tc.want || report.SnapLen != 65535 || report.LinkType != 1 ||
				report.VersionMajor != 2 || report.VersionMinor != 4 {
				t.Fatalf("format %+v", report)
			}
			if report.BytesRead != int64(len(data)) || report.SHA256 != digest(data) {
				t.Fatalf("identity %d %s", report.BytesRead, report.SHA256)
			}
		})
	}
}

// A well-formed header without records is a valid, empty capture: not a
// failed capture and not evidence that traffic was absent.
func TestZeroPacketCaptureIsValid(t *testing.T) {
	data := newCapture(binary.LittleEndian, magicMicro).bytes()
	if report := check(t, data, Limits{}); report.Status != Valid || report.Packets != 0 || report.BytesRead != fileHeaderBytes {
		t.Fatalf("report %+v", report)
	}
}

// A record captured shorter than its wire length is snaplen truncation of
// that packet, which the report counts; the file stays valid.
func TestSnaplenTruncatedPacketsAreCountedNotMalformed(t *testing.T) {
	c := newCapture(binary.BigEndian, magicNano)
	c.snaplen = 64
	data := c.add(bytes.Repeat([]byte{1}, 64), 1500).add([]byte("ack"), 3).bytes()
	if report := check(t, data, Limits{}); report.Status != Valid || report.Packets != 2 || report.TruncatedPackets != 1 {
		t.Fatalf("report %+v", report)
	}
}

func TestMalformedAndUnsupportedInputsAreDistinguished(t *testing.T) {
	valid := newCapture(binary.LittleEndian, magicMicro).add([]byte("hello"), 5).add([]byte("world!"), 6).bytes()
	pcapng := []byte{0x0a, 0x0d, 0x0d, 0x0a, 28, 0, 0, 0, 0x4d, 0x3c, 0x2b, 0x1a}
	version := newCapture(binary.LittleEndian, magicMicro)
	version.major, version.minor = 2, 3
	overSnap := newCapture(binary.LittleEndian, magicMicro)
	overSnap.snaplen = 4
	overSnap.add([]byte("hello"), 5)
	badFraction := newCapture(binary.LittleEndian, magicMicro).add([]byte("x"), 1)
	badFraction.records[0].subsecond = 1_000_000
	for _, tc := range []struct {
		name   string
		data   []byte
		status Status
		reason string
		read   int
	}{
		{"empty", nil, Malformed, "too short for a pcap magic number", 0},
		{"not pcap", []byte("GET / HTTP/1.1\r\n\r\nmore text"), Malformed, "unknown magic number", 24},
		{"pcapng", pcapng, Unsupported, "pcapng", len(pcapng)},
		{"other version", version.bytes(), Unsupported, "pcap version 2.3", fileHeaderBytes},
		{"cut file header", valid[:20], Malformed, "file header cut off after 20 of 24", 20},
		{"cut record header", valid[:fileHeaderBytes+recordHeaderBytes+5+9], Malformed, "record 2 header cut off after 9 of 16", fileHeaderBytes + recordHeaderBytes + 5 + 9},
		{"cut record data", valid[:len(valid)-2], Malformed, "record 2 data cut off after 4 of 6", len(valid) - 2},
		{"captured over snaplen", overSnap.bytes(), Malformed, "record 1 captured length 5 exceeds snaplen 4", fileHeaderBytes + recordHeaderBytes},
		{"captured over original", newCapture(binary.LittleEndian, magicMicro).add([]byte("hello"), 4).bytes(), Malformed, "exceeds its original length 4", fileHeaderBytes + recordHeaderBytes},
		{"timestamp fraction", badFraction.bytes(), Malformed, "timestamp fraction 1000000 is not below 1000000", fileHeaderBytes + recordHeaderBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := check(t, tc.data, Limits{})
			if report.Status != tc.status || !strings.Contains(report.Reason, tc.reason) || report.BytesRead != int64(tc.read) {
				t.Fatalf("report %+v", report)
			}
			if report.Packets != 0 && tc.name != "cut record header" && tc.name != "cut record data" {
				t.Fatalf("packets counted before the verdict: %+v", report)
			}
		})
	}
	if report := check(t, valid[:len(valid)-2], Limits{}); report.Packets != 1 {
		t.Fatalf("records before a cut-off record: %+v", report)
	}
}

// A declared captured length is checked against a bound before anything is
// read, so a huge length can neither allocate nor read the declared size.
func TestRecordBoundRefusesOversizedDeclaredLength(t *testing.T) {
	c := newCapture(binary.LittleEndian, magicMicro)
	c.snaplen = 0
	data := c.bytes()
	data = binary.LittleEndian.AppendUint32(data, 1)
	data = binary.LittleEndian.AppendUint32(data, 0)
	data = binary.LittleEndian.AppendUint32(data, 0xfffffff0)
	data = binary.LittleEndian.AppendUint32(data, 0xfffffff0)
	report := check(t, data, Limits{MaxRecordBytes: 1024})
	if report.Status != Malformed || !strings.Contains(report.Reason, "exceeds the 1024-byte record bound") || report.BytesRead != int64(len(data)) {
		t.Fatalf("report %+v", report)
	}
}

// The file bound stops reading; the part read is reported with its digest,
// and an early end caused by the bound is not called a cut-off file.
func TestFileBoundStopsWithoutCallingTheFileMalformed(t *testing.T) {
	data := newCapture(binary.LittleEndian, magicMicro).add(bytes.Repeat([]byte{2}, 100), 100).add([]byte("next"), 4).bytes()
	for _, bound := range []int64{10, 30, fileHeaderBytes + recordHeaderBytes + 50, fileHeaderBytes + recordHeaderBytes + 100, int64(len(data)) - 1} {
		report := check(t, data, Limits{MaxFileBytes: bound})
		if report.Status != LimitExceeded || report.BytesRead != bound || report.SHA256 != digest(data[:bound]) {
			t.Fatalf("bound %d: %+v", bound, report)
		}
	}
	if report := check(t, data, Limits{MaxFileBytes: int64(len(data))}); report.Status != Valid || report.Packets != 2 {
		t.Fatalf("exact bound: %+v", report)
	}
}

func TestReadErrorsAreReturnedNotJudged(t *testing.T) {
	data := newCapture(binary.LittleEndian, magicMicro).add([]byte("hello"), 5).bytes()
	failure := errors.New("synthetic read failure")
	for _, at := range []int{0, 10, fileHeaderBytes + 4, len(data) - 1} {
		reader := iotest.TimeoutReader(bytes.NewReader(data))
		if at > 0 {
			reader = &failingReader{data: data[:at], err: failure}
		}
		report, err := Validate(reader, Limits{})
		if err == nil || report.Status == Valid {
			t.Fatalf("read failure at %d judged: %+v %v", at, report, err)
		}
	}
}

type failingReader struct {
	data []byte
	err  error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestValidateFileReadsFromDisk(t *testing.T) {
	data := newCapture(binary.LittleEndian, magicMicro).add([]byte("hello"), 5).bytes()
	path := filepath.Join(t.TempDir(), "capture.pcap")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if report, err := ValidateFile(path, Limits{}); err != nil || report.Status != Valid || report.SHA256 != digest(data) {
		t.Fatalf("file %+v %v", report, err)
	}
	if _, err := ValidateFile(filepath.Join(t.TempDir(), "missing.pcap"), Limits{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}

// Any input yields a verdict within its bound without panicking.
func FuzzValidate(f *testing.F) {
	f.Add(newCapture(binary.LittleEndian, magicMicro).add([]byte("hello"), 5).bytes())
	f.Add(newCapture(binary.BigEndian, magicNano).bytes())
	f.Add([]byte{0x0a, 0x0d, 0x0d, 0x0a})
	f.Fuzz(func(t *testing.T, data []byte) {
		report, err := Validate(bytes.NewReader(data), Limits{MaxFileBytes: 4096, MaxRecordBytes: 2048})
		if err != nil {
			t.Fatal(err)
		}
		switch report.Status {
		case Valid, Malformed, Unsupported, LimitExceeded:
		default:
			t.Fatalf("no verdict: %+v", report)
		}
		if report.BytesRead > 4096 || report.BytesRead > int64(len(data)) || (report.Status != Valid) == (report.Reason == "") ||
			report.Complete && report.BytesRead != int64(len(data)) || report.Status == Valid && !report.Complete {
			t.Fatalf("inconsistent report %+v for %d bytes", report, len(data))
		}
	})
}

// The largest representable bound needs no lookahead and must not wrap into
// a negative read limit.
func TestLargestFileBoundDoesNotOverflow(t *testing.T) {
	data := newCapture(binary.LittleEndian, magicMicro).bytes()
	report := check(t, data, Limits{MaxFileBytes: math.MaxInt64})
	if report.Status != Valid || report.BytesRead != fileHeaderBytes || !report.Complete || report.SHA256 != digest(data) {
		t.Fatalf("report %+v", report)
	}
}

// The byte past the bound only shows that the input is larger. Whatever it
// holds, the verdict on the same reported prefix is the same.
func TestLookaheadByteIsNeverDecoded(t *testing.T) {
	c := newCapture(binary.LittleEndian, magicMicro)
	header := c.bytes()
	// A record that claims more captured than original bytes is malformed
	// once its header is read whole.
	record := binary.LittleEndian.AppendUint32(nil, 1)
	record = binary.LittleEndian.AppendUint32(record, 0)
	record = binary.LittleEndian.AppendUint32(record, 1)
	record = binary.LittleEndian.AppendUint32(record, 0)
	bad := append(append(bytes.Clone(header), record...), 'x')
	for _, tc := range []struct {
		name  string
		data  []byte
		bound int64
	}{
		{"file header", header, fileHeaderBytes - 1},
		{"record header", bad, fileHeaderBytes + recordHeaderBytes - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, last := range []byte{0x00, 0xff} {
				data := bytes.Clone(tc.data)
				data[tc.bound] = last
				report := check(t, data, Limits{MaxFileBytes: tc.bound})
				if report.Status != LimitExceeded || report.BytesRead != tc.bound || report.Complete || report.SHA256 != digest(data[:tc.bound]) {
					t.Fatalf("lookahead %#x: %+v", last, report)
				}
			}
		})
	}
	if report := check(t, bad, Limits{MaxFileBytes: fileHeaderBytes + recordHeaderBytes}); report.Status != Malformed {
		t.Fatalf("the same record inside the bound: %+v", report)
	}
}

// Only reading to the end of the input makes the digest a whole-file
// identity; an early verdict, a read error or the bound leave a prefix digest.
func TestCompleteMarksWholeFileDigests(t *testing.T) {
	valid := newCapture(binary.LittleEndian, magicMicro).add([]byte("hello"), 5).bytes()
	notPCAP := bytes.Repeat([]byte("not a capture "), 20)
	for _, tc := range []struct {
		name     string
		data     []byte
		limits   Limits
		complete bool
		read     int
	}{
		{"valid", valid, Limits{}, true, len(valid)},
		{"cut off at the end", valid[:len(valid)-1], Limits{}, true, len(valid) - 1},
		{"early unknown magic", notPCAP, Limits{}, false, fileHeaderBytes},
		{"bound", valid, Limits{MaxFileBytes: 30}, false, 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := check(t, tc.data, tc.limits)
			if report.Complete != tc.complete || report.BytesRead != int64(tc.read) || report.SHA256 != digest(tc.data[:tc.read]) {
				t.Fatalf("report %+v", report)
			}
		})
	}
	report, err := Validate(&failingReader{data: valid[:10], err: errors.New("synthetic")}, Limits{})
	if err == nil || report.Complete {
		t.Fatalf("read error: %+v %v", report, err)
	}
}
