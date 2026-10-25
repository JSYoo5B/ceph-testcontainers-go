package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCaptureFileUsesWholeRetrievedIdentityAndPreservesSource(t *testing.T) {
	data := append(pcapFile(64), []byte("cut record")...)
	path := filepath.Join(t.TempDir(), "capture.pcap")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	s := &Session{finished: true, request: Request{Output: path}, report: Report{Retrieved: true, RetrievedBytes: int64(len(data)), RetrievedSHA256: digest, File: FileReport{Status: FileMalformed, Complete: false, SHA256: "prefix-only"}}}
	reader, size, hash, err := s.CaptureFile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(got, data) || size != int64(len(data)) || hash != digest {
		t.Fatalf("identity %d %s %v", size, hash, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("caller file removed", err)
	}
	if err := os.Rename(path, path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path+".original", path); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.CaptureFile(context.Background()); err == nil {
		t.Fatal("symlink admitted")
	}
}

func TestCaptureFileRefusesUnsettledAndUnavailableIdentity(t *testing.T) {
	for _, s := range []*Session{nil, {}, {finished: true, report: Report{Retrieved: true}}} {
		if _, _, _, err := s.CaptureFile(context.Background()); err == nil {
			t.Fatal("unidentified source admitted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := (&Session{}).CaptureFile(ctx); err == nil {
		t.Fatal("expired source opened")
	}
}

func TestRetrievalHashCoversUnvalidatedPayloadRatherThanValidatorPrefix(t *testing.T) {
	backend := newFakeBackend()
	backend.file = pcapFile(128, 129)
	session, err := startFake(t, backend, request(t))
	if err != nil {
		t.Fatal(err)
	}
	report, err := session.Stop(context.Background())
	if err == nil || !report.Retrieved || report.File.Status != FileMalformed || report.File.Complete {
		t.Fatalf("partial report %+v %v", report, err)
	}
	whole := fmt.Sprintf("%x", sha256.Sum256(backend.file))
	if report.RetrievedSHA256 != whole || report.File.SHA256 == whole || report.File.BytesRead >= report.RetrievedBytes {
		t.Fatalf("whole vs prefix %+v", report)
	}
	reader, size, digest, err := session.CaptureFile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || size != int64(len(backend.file)) || digest != whole || !bytes.Equal(data, backend.file) {
		t.Fatalf("partial identity %d %s %v", size, digest, err)
	}
}

func TestCaptureFileDoesNotWaitForConcurrentSettlement(t *testing.T) {
	s := &Session{}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, _, _, err := s.CaptureFile(context.Background()); err == nil {
		t.Fatal("busy source admitted")
	}
}
