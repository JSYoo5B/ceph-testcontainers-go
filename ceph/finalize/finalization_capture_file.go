package finalize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CaptureFileSource is a settled capture with a read-only local file. CaptureFile
// returns a reader, the complete retrieved byte count and its SHA256. The
// descriptor must remain the settlement's frozen identity even if the file is
// later edited. It must not start or settle a capture, change or delete the
// caller's file, or expose a host path in errors. Opening and reading must honor
// ctx; Finalization waits for operations to return rather than abandoning them.
type CaptureFileSource interface {
	CaptureSettler
	CaptureFile(ctx context.Context) (io.ReadCloser, int64, string, error)
}

const (
	captureArchiveBytes = 64 << 20
	captureArchiveFiles = 16
)

// CaptureWithFile registers a capture with explicit consent to copy its raw,
// unsanitized packet payloads and endpoint identifiers into the failure archive.
// Use Capture for metadata alone. maxBytes must be positive and at most 64 MiB;
// at most 16 files and 64 MiB together are admitted per finalization. There is no
// implicit truncation. Settlement, copying, archive retention and later upload
// are separate outcomes. A passing run settles without opening the source.
// Local filesystem calls may finish after a deadline (for example on a stalled
// filesystem); no background copying is abandoned underneath cleanup.
//
// Each copy shares ArchiveTimeout, checks it between reads, verifies the complete retrieved size/SHA256
// and publishes atomically. Invalid/partial pcap structure does not prevent
// preserving its exact retrieved bytes. The caller retains its original file
// and must keep it and its directory available until finalization returns. If
// cleanup produces the first failure, copying runs after cleanup and reports
// an unavailable source rather than recovering bytes that were removed.
//
//purpose:evidence
func (f *Finalization) CaptureWithFile(name string, session CaptureFileSource, maxBytes int64) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if isNilSession(session) || f.started || maxBytes <= 0 || maxBytes > captureArchiveBytes {
		f.t.Errorf("ceph finalization: CaptureWithFile %q has an invalid source, limit or registration phase", name)
		return
	}
	f.captures = append(f.captures, finalizationCapture{name: name, session: session, fileLimit: maxBytes})
}

func captureFileSource(c finalizationCapture) CaptureFileSource {
	if c.fileLimit == 0 {
		return nil
	}
	source, _ := c.session.(CaptureFileSource)
	return source
}

// finalizationCaptureFile separates raw-copy identity/outcome from structural
// validation, metadata settlement and archive retention.
type finalizationCaptureFile struct {
	Registration   int    `json:"registration"`
	Name           string `json:"name"`
	Limit          int64  `json:"limit"`
	ExpectedBytes  int64  `json:"expected_bytes"`
	ExpectedSHA256 string `json:"expected_sha256,omitempty"`
	Bytes          int64  `json:"bytes"`
	SHA256         string `json:"sha256,omitempty"`
	Copied         bool   `json:"copied"`
	Error          string `json:"error,omitempty"`
}

func (f *Finalization) archiveCaptureFiles(ctx context.Context, directory string, reports []finalizationCaptureReport, m *finalizationManifest) {
	var used int64
	count := 0
	for _, report := range reports {
		if report.source == nil {
			continue
		}
		outcome := finalizationCaptureFile{Registration: report.sequence, Name: fmt.Sprintf("capture-%d-%s.pcap", report.sequence, artifactName(report.name)), Limit: report.fileLimit}
		count++
		switch {
		case count > captureArchiveFiles:
			outcome.Error = "capture file count limit exceeded"
		case used >= captureArchiveBytes:
			outcome.Error = "capture archive byte limit exhausted"
		default:
			outcome = copyCaptureFile(ctx, directory, report.source, outcome, captureArchiveBytes-used)
			used += outcome.Bytes
		}
		m.CaptureFiles = append(m.CaptureFiles, outcome)
		m.Files = append(m.Files, finalizationFile{Name: outcome.Name, Bytes: int(outcome.Bytes), Error: outcome.Error})
	}
}

func copyCaptureFile(ctx context.Context, directory string, source CaptureFileSource, out finalizationCaptureFile, remaining int64) (result finalizationCaptureFile) {
	result = out
	// Providers are application code. A panic must not skip the remaining
	// captures or finalization cleanup; arbitrary provider errors are not kept.
	defer func() {
		if recover() != nil {
			result.Error = "capture source panicked"
		}
	}()
	if ctx.Err() != nil {
		result.Error = "capture archive deadline expired"
		return
	}
	reader, size, digest, err := source.CaptureFile(ctx)
	if err != nil {
		if reader != nil {
			_ = reader.Close()
		}
		result.Error = "capture source unavailable"
		return
	}
	if reader == nil {
		result.Error = "capture source has no reader"
		return
	}
	readerClosed := false
	defer func() {
		if !readerClosed && reader.Close() != nil && result.Error == "" {
			result.Error = "capture source close failed"
		}
	}()
	if len(digest) != sha256.Size*2 {
		result.Error = "capture source identity invalid"
		return
	}
	decoded, hashErr := hex.DecodeString(digest)
	if size < 0 || hashErr != nil || len(decoded) != sha256.Size {
		result.Error = "capture source identity invalid"
		return
	}
	result.ExpectedBytes, result.ExpectedSHA256 = size, hex.EncodeToString(decoded)
	if size > out.Limit || size > remaining {
		result.Error = "capture file byte limit exceeded"
		return
	}
	temp, err := os.CreateTemp(directory, ".capture-*")
	if err != nil {
		result.Error = "capture destination unavailable"
		return
	}
	defer func() { _ = temp.Close(); _ = os.Remove(temp.Name()) }()
	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	limited := io.LimitReader(reader, size+1)
	for {
		if ctx.Err() != nil {
			result.Error = "capture archive deadline expired"
			return
		}
		n, readErr := limited.Read(buffer)
		if n > 0 {
			if result.Bytes+int64(n) > size {
				result.Error = "capture source size changed"
				return
			}
			written, writeErr := io.MultiWriter(temp, hash).Write(buffer[:n])
			result.Bytes += int64(written)
			if writeErr != nil {
				result.Error = "capture destination write failed"
				return
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			result.Error = "capture source read failed"
			return
		}
		if n == 0 {
			result.Error = "capture source made no progress"
			return
		}
	}
	result.SHA256 = fmt.Sprintf("%x", hash.Sum(nil))
	if result.Bytes != size || result.SHA256 != result.ExpectedSHA256 {
		result.Error = "capture source size or digest changed"
		return
	}
	if ctx.Err() != nil {
		result.Error = "capture archive deadline expired"
		return
	}
	readerClosed = true
	if reader.Close() != nil {
		result.Error = "capture source close failed"
		return
	}
	if temp.Close() != nil {
		result.Error = "capture destination close failed"
		return
	}
	// Names are unique registration identities inside a new execution archive.
	if os.Rename(temp.Name(), filepath.Join(directory, result.Name)) != nil {
		result.Error = "capture destination publish failed"
		return
	}
	result.Copied = true
	return
}
