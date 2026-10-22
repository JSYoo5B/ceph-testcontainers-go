// Package pcapfile checks the structure of a classic pcap capture file with
// bounded reads. It never interprets the packets: a structurally valid file
// is not evidence that the intended traffic was captured, that nothing was
// dropped, or that any protocol message in it is valid.
package pcapfile

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
)

// Status is the structural verdict on one file.
type Status string

const (
	// Valid means every record header and its declared data fit the file.
	// A valid file can hold zero packets.
	Valid Status = "valid"
	// Unsupported means a recognized format or variant that this package
	// does not check, such as pcapng or another classic pcap version.
	Unsupported Status = "unsupported"
	// Malformed means the input is not a classic pcap file or breaks its
	// structure, including a final record that was cut off.
	Malformed Status = "malformed"
	// LimitExceeded means the file is larger than the requested bound; the
	// part that was read is reported, the rest was not checked.
	LimitExceeded Status = "limit_exceeded"
)

// Limits bound one check. Zero fields take the defaults.
type Limits struct {
	// MaxFileBytes bounds the bytes read from the file.
	MaxFileBytes int64
	// MaxRecordBytes bounds one record's captured length, independent of
	// the snaplen that the file declares.
	MaxRecordBytes uint32
}

// DefaultLimits read up to 1 GiB, with records up to 256 KiB: libpcap's
// largest default snaplen.
var DefaultLimits = Limits{MaxFileBytes: 1 << 30, MaxRecordBytes: 262144}

// Report is what one check found. Counts cover the records that were read
// completely before the verdict.
type Report struct {
	Status Status `json:"status"`
	// Reason explains any status other than Valid.
	Reason string `json:"reason,omitempty"`
	// ByteOrder is "little" or "big"; TimestampUnit is "microsecond" or
	// "nanosecond". Both are empty when the header is not classic pcap.
	ByteOrder     string `json:"byte_order,omitempty"`
	TimestampUnit string `json:"timestamp_unit,omitempty"`
	VersionMajor  uint16 `json:"version_major,omitempty"`
	VersionMinor  uint16 `json:"version_minor,omitempty"`
	SnapLen       uint32 `json:"snaplen,omitempty"`
	// LinkType is the header's link-layer field as written.
	LinkType uint32 `json:"link_type,omitempty"`
	Packets  int    `json:"packets"`
	// TruncatedPackets counts records whose captured length is smaller than
	// their original length: snaplen truncation, not a malformed file.
	TruncatedPackets int `json:"truncated_packets"`
	// BytesRead is how much of the file was read; SHA256 is of those bytes,
	// so it identifies the whole file only when Status is not LimitExceeded.
	BytesRead int64  `json:"bytes_read"`
	SHA256    string `json:"sha256"`
}

const (
	fileHeaderBytes   = 24
	recordHeaderBytes = 16
	magicMicro        = 0xa1b2c3d4
	magicNano         = 0xa1b23c4d
	magicPCAPNG       = 0x0a0d0d0a
)

// ValidateFile checks the file at path. The error reports only a failure to
// open or read it; structural findings are in the report.
func ValidateFile(path string, limits Limits) (Report, error) {
	f, err := os.Open(path)
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = f.Close() }()
	return Validate(f, limits)
}

// Validate checks a classic pcap stream. It reads at most
// limits.MaxFileBytes+1 bytes and never allocates a record's declared size.
// The error reports only a failure of r; structural findings, including
// input that ends early, are in the report.
func Validate(r io.Reader, limits Limits) (Report, error) {
	if limits.MaxFileBytes <= 0 {
		limits.MaxFileBytes = DefaultLimits.MaxFileBytes
	}
	if limits.MaxRecordBytes == 0 {
		limits.MaxRecordBytes = DefaultLimits.MaxRecordBytes
	}
	v := &validator{sum: sha256.New(), limit: limits.MaxFileBytes}
	// One byte past the bound shows that the file is larger; it is read
	// but not hashed, so SHA256 covers exactly BytesRead.
	v.in = io.TeeReader(io.LimitReader(r, limits.MaxFileBytes+1), &boundedWriter{w: v.sum, left: limits.MaxFileBytes})
	report, err := v.run(limits)
	report.BytesRead = min(v.read, limits.MaxFileBytes)
	report.SHA256 = hex.EncodeToString(v.sum.Sum(nil))
	if report.Status == "" && err == nil {
		// Only the bound stops a check without a verdict.
		report.Status = LimitExceeded
		report.Reason = fmt.Sprintf("file is larger than %d bytes", limits.MaxFileBytes)
	}
	return report, err
}

type validator struct {
	in    io.Reader
	sum   hash.Hash
	read  int64
	limit int64
}

// boundedWriter passes on at most left bytes and drops the rest.
type boundedWriter struct {
	w    io.Writer
	left int64
}

func (b *boundedWriter) Write(p []byte) (int, error) {
	n := len(p)
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	b.left -= int64(len(p))
	if _, err := b.w.Write(p); err != nil {
		return 0, err
	}
	return n, nil
}

// errShort marks input that ended inside a structure.
var errShort = errors.New("input ended")

// fill reads exactly len(buf) bytes. It returns errShort, with the count
// read, when the input ends first, and a read error as is.
func (v *validator) fill(buf []byte) (int, error) {
	n, err := io.ReadFull(v.in, buf)
	v.read += int64(n)
	switch {
	case err == nil:
		return n, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return n, errShort
	default:
		return n, err
	}
}

// capped reports whether the read reached past the file bound, so that an
// early end is the bound's, not the file's.
func (v *validator) capped() bool { return v.read > v.limit }

// skip consumes n bytes, through the hash, without holding them.
func (v *validator) skip(n int64) (int64, error) {
	copied, err := io.CopyN(io.Discard, v.in, n)
	v.read += copied
	if errors.Is(err, io.EOF) {
		return copied, errShort
	}
	return copied, err
}

func (v *validator) run(limits Limits) (Report, error) {
	report := Report{}
	header := make([]byte, fileHeaderBytes)
	n, err := v.fill(header)
	if err != nil && !errors.Is(err, errShort) {
		return report, err
	}
	if n >= 4 && binary.LittleEndian.Uint32(header) == magicPCAPNG {
		return verdict(report, Unsupported, "pcapng is not classic pcap"), nil
	}
	if errors.Is(err, errShort) && v.capped() {
		return report, nil
	}
	if errors.Is(err, errShort) {
		if n < 4 {
			return verdict(report, Malformed, fmt.Sprintf("file has %d bytes, too short for a pcap magic number", n)), nil
		}
	}
	var order binary.ByteOrder
	switch {
	case binary.LittleEndian.Uint32(header) == magicMicro:
		order, report.ByteOrder, report.TimestampUnit = binary.LittleEndian, "little", "microsecond"
	case binary.BigEndian.Uint32(header) == magicMicro:
		order, report.ByteOrder, report.TimestampUnit = binary.BigEndian, "big", "microsecond"
	case binary.LittleEndian.Uint32(header) == magicNano:
		order, report.ByteOrder, report.TimestampUnit = binary.LittleEndian, "little", "nanosecond"
	case binary.BigEndian.Uint32(header) == magicNano:
		order, report.ByteOrder, report.TimestampUnit = binary.BigEndian, "big", "nanosecond"
	default:
		return verdict(report, Malformed, fmt.Sprintf("unknown magic number %#08x", binary.BigEndian.Uint32(header))), nil
	}
	if errors.Is(err, errShort) {
		return verdict(report, Malformed, fmt.Sprintf("file header cut off after %d of %d bytes", n, fileHeaderBytes)), nil
	}
	report.VersionMajor, report.VersionMinor = order.Uint16(header[4:]), order.Uint16(header[6:])
	report.SnapLen, report.LinkType = order.Uint32(header[16:]), order.Uint32(header[20:])
	if report.VersionMajor != 2 || report.VersionMinor != 4 {
		return verdict(report, Unsupported, fmt.Sprintf("pcap version %d.%d is not 2.4", report.VersionMajor, report.VersionMinor)), nil
	}
	fraction := uint32(1_000_000)
	if report.TimestampUnit == "nanosecond" {
		fraction = 1_000_000_000
	}
	record := make([]byte, recordHeaderBytes)
	for {
		n, err := v.fill(record)
		if errors.Is(err, errShort) {
			if v.capped() {
				return report, nil
			}
			if n == 0 {
				return verdict(report, Valid, ""), nil
			}
			return verdict(report, Malformed, fmt.Sprintf("record %d header cut off after %d of %d bytes", report.Packets+1, n, recordHeaderBytes)), nil
		}
		if err != nil {
			return report, err
		}
		index := report.Packets + 1
		subsecond, captured, original := order.Uint32(record[4:]), order.Uint32(record[8:]), order.Uint32(record[12:])
		switch {
		case subsecond >= fraction:
			return verdict(report, Malformed, fmt.Sprintf("record %d timestamp fraction %d is not below %d", index, subsecond, fraction)), nil
		case captured > limits.MaxRecordBytes:
			return verdict(report, Malformed, fmt.Sprintf("record %d captured length %d exceeds the %d-byte record bound", index, captured, limits.MaxRecordBytes)), nil
		case report.SnapLen != 0 && captured > report.SnapLen:
			return verdict(report, Malformed, fmt.Sprintf("record %d captured length %d exceeds snaplen %d", index, captured, report.SnapLen)), nil
		case captured > original:
			return verdict(report, Malformed, fmt.Sprintf("record %d captured length %d exceeds its original length %d", index, captured, original)), nil
		}
		copied, err := v.skip(int64(captured))
		if errors.Is(err, errShort) {
			if v.capped() {
				return report, nil
			}
			return verdict(report, Malformed, fmt.Sprintf("record %d data cut off after %d of %d bytes", index, copied, captured)), nil
		}
		if err != nil {
			return report, err
		}
		if v.capped() {
			// The record extends past the bound: it was not read whole.
			return report, nil
		}
		report.Packets++
		if captured < original {
			report.TruncatedPackets++
		}
	}
}

func verdict(report Report, status Status, reason string) Report {
	report.Status, report.Reason = status, reason
	return report
}
