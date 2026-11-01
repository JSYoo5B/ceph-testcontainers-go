package finalize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeCaptureFile struct {
	*fakeSettler
	data    []byte
	size    int64
	digest  string
	opens   int
	closed  bool
	openErr error
	reader  io.ReadCloser
}

func (s *fakeCaptureFile) CaptureFile(context.Context) (io.ReadCloser, int64, string, error) {
	s.opens++
	if s.openErr != nil {
		return nil, 0, "", s.openErr
	}
	if s.reader != nil {
		return s.reader, s.size, s.digest, nil
	}
	return &trackedCaptureReader{Reader: bytes.NewReader(s.data), closed: &s.closed}, s.size, s.digest, nil
}

type trackedCaptureReader struct {
	io.Reader
	closed *bool
}

func (r *trackedCaptureReader) Close() error { *r.closed = true; return nil }
func captureFileFixture(fx *finalizationFixture, data []byte) *fakeCaptureFile {
	return &fakeCaptureFile{fakeSettler: fx.settler("raw", map[string]string{"status": "malformed", "output": "partial.pcap"}, nil), data: data, size: int64(len(data)), digest: fmt.Sprintf("%x", sha256.Sum256(data))}
}

func TestFinalizationCaptureFileConsentIdentityAndOrdering(t *testing.T) {
	for _, consent := range []bool{false, true} {
		t.Run(fmt.Sprint(consent), func(t *testing.T) {
			fx := newFinalizationFixture(t, Config{Archive: true})
			raw := captureFileFixture(fx, []byte("partial unsanitized packet bytes"))
			if consent {
				fx.CaptureWithFile("traffic", raw, 128)
			} else {
				fx.Capture("traffic", raw)
			}
			fx.Cleanup("caller source", func(context.Context) error {
				if consent && !raw.closed {
					t.Error("source not closed before cleanup")
				}
				return nil
			})
			fx.tb.Errorf("original workload failure")
			fx.run()
			dir := fx.archives(t)[0]
			m := readManifest(t, dir)
			if !m.Final || !m.Trigger.TestFailed || !m.Archive.Retained {
				t.Fatalf("manifest %+v", m)
			}
			path := filepath.Join(dir, "capture-1-traffic.pcap")
			if !consent {
				if raw.opens != 0 || len(m.CaptureFiles) != 0 {
					t.Fatal("metadata-only source opened")
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("raw file retained")
				}
				return
			}
			data, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(data, raw.data) {
				t.Fatalf("raw file %q %v", data, err)
			}
			out := m.CaptureFiles[0]
			if !out.Copied || out.Error != "" || out.Bytes != raw.size || out.SHA256 != raw.digest || out.ExpectedSHA256 != raw.digest || raw.opens != 1 || !raw.closed {
				t.Fatalf("copy %+v, source %+v", out, raw)
			}
			if !strings.Contains(strings.Join(fx.tb.errors, " "), "original workload failure") {
				t.Fatal("original failure lost")
			}
		})
	}
}

func TestFinalizationPassingCaptureFileNeverOpens(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	raw := captureFileFixture(fx, []byte("bytes"))
	fx.CaptureWithFile("raw", raw, 128)
	fx.run()
	if raw.calls != 1 || raw.opens != 0 || len(fx.archives(t)) != 0 {
		t.Fatalf("settle %d open %d", raw.calls, raw.opens)
	}
}

func TestFinalizationCaptureFileRefusalsPreserveCleanupAndBoundedEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*fakeCaptureFile)
		limit  int64
	}{
		{"oversize", func(s *fakeCaptureFile) {}, 2},
		{"changed digest", func(s *fakeCaptureFile) { s.data = []byte("other") }, 128},
		{"short", func(s *fakeCaptureFile) { s.size++ }, 128},
		{"long", func(s *fakeCaptureFile) { s.size-- }, 128},
		{"invalid identity", func(s *fakeCaptureFile) { s.digest = "private arbitrary value" }, 128},
		{"unavailable", func(s *fakeCaptureFile) { s.openErr = errors.New("private source path and secret") }, 128},
		{"reader error", func(s *fakeCaptureFile) { s.reader = io.NopCloser(errorCaptureReader{}) }, 128},
	} {
		t.Run(test.name, func(t *testing.T) {
			fx := newFinalizationFixture(t, Config{Archive: true})
			raw := captureFileFixture(fx, []byte("bytes"))
			test.mutate(raw)
			fx.CaptureWithFile("raw", raw, test.limit)
			fx.tb.Errorf("workload failure")
			fx.run()
			dir := fx.archives(t)[0]
			m := readManifest(t, dir)
			out := m.CaptureFiles[0]
			if out.Copied || out.Error == "" || m.Archive.Complete || !m.Final || m.Termination == nil {
				t.Fatalf("outcome %+v manifest %+v", out, m)
			}
			if strings.Contains(out.Error, "private") || len(out.Error) > 80 {
				t.Fatalf("unsafe error %q", out.Error)
			}
			files, _ := filepath.Glob(filepath.Join(dir, "*.pcap"))
			temps, _ := filepath.Glob(filepath.Join(dir, ".capture-*"))
			if len(files)+len(temps) != 0 {
				t.Fatalf("unverified output retained %v %v", files, temps)
			}
		})
	}
}

type errorCaptureReader struct{}

func (errorCaptureReader) Read([]byte) (int, error) { return 0, errors.New("private read path") }

func TestFinalizationCaptureFileLimitsAndRegistrationIdentity(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	sources := make([]*fakeCaptureFile, 17)
	for i := range sources {
		sources[i] = captureFileFixture(fx, []byte("bytes"))
		fx.CaptureWithFile("same", sources[i], 128)
	}
	fx.tb.Errorf("workload failure")
	fx.run()
	m := readManifest(t, fx.archives(t)[0])
	if len(m.CaptureFiles) != 17 {
		t.Fatal(len(m.CaptureFiles))
	}
	for i, out := range m.CaptureFiles {
		if out.Registration != i+1 {
			t.Fatal(out)
		}
		if i < 16 && !out.Copied {
			t.Fatal(out)
		}
	}
	if sources[16].opens != 0 || m.CaptureFiles[16].Error != "capture file count limit exceeded" {
		t.Fatal(m.CaptureFiles[16])
	}
}

func TestCaptureFileCopyExpiredBudgetDoesNotOpen(t *testing.T) {
	fx := newFinalizationFixture(t, Config{})
	raw := captureFileFixture(fx, []byte("bytes"))
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	out := copyCaptureFile(ctx, t.TempDir(), raw, finalizationCaptureFile{Name: "raw.pcap", Limit: 128}, 128)
	if raw.opens != 0 || out.Error != "capture archive deadline expired" {
		t.Fatal(out)
	}
	// The shared byte budget refuses a file before reading, even when its
	// independent selected limit would allow it.
	out = copyCaptureFile(context.Background(), t.TempDir(), raw, finalizationCaptureFile{Name: "raw.pcap", Limit: 128}, 2)
	if out.Error != "capture file byte limit exceeded" || !raw.closed {
		t.Fatal(out)
	}
}

type cancelCaptureReader struct {
	cancel context.CancelFunc
	closed bool
}

func (r *cancelCaptureReader) Read(b []byte) (int, error) {
	copy(b, "first")
	r.cancel()
	return 5, nil
}
func (r *cancelCaptureReader) Close() error { r.closed = true; return nil }

func TestCaptureFileDeadlineRemovesPartialCopy(t *testing.T) {
	fx := newFinalizationFixture(t, Config{})
	raw := captureFileFixture(fx, []byte("first second"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &cancelCaptureReader{cancel: cancel}
	raw.reader = reader
	dir := t.TempDir()
	out := copyCaptureFile(ctx, dir, raw, finalizationCaptureFile{Name: "raw.pcap", Limit: 128}, 128)
	if out.Copied || out.Bytes != 5 || out.Error != "capture archive deadline expired" || !reader.closed {
		t.Fatal(out)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("partial copy %v %v", files, err)
	}
}

func TestCaptureFilePublishFailureKeepsDestinationAndRemovesTemp(t *testing.T) {
	fx := newFinalizationFixture(t, Config{})
	raw := captureFileFixture(fx, []byte("bytes"))
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "raw.pcap"), 0o700); err != nil {
		t.Fatal(err)
	}
	out := copyCaptureFile(context.Background(), dir, raw, finalizationCaptureFile{Name: "raw.pcap", Limit: 128}, 128)
	if out.Copied || out.Error != "capture destination publish failed" || !raw.closed {
		t.Fatal(out)
	}
	temps, _ := filepath.Glob(filepath.Join(dir, ".capture-*"))
	if len(temps) != 0 {
		t.Fatal(temps)
	}
}

func TestFinalizationLateCleanupFailureReportsUnavailableSource(t *testing.T) {
	fx := newFinalizationFixture(t, Config{Archive: true})
	raw := captureFileFixture(fx, []byte("bytes"))
	fx.CaptureWithFile("raw", raw, 128)
	fx.Cleanup("caller removed original", func(context.Context) error {
		raw.openErr = errors.New("original no longer available")
		return errors.New("original cleanup failure")
	})
	fx.run()
	m := readManifest(t, fx.archives(t)[0])
	if m.Phase != "after-cleanup" || !m.Trigger.CleanupFailed || !m.Final || !m.Captures[0].Reported || m.CaptureFiles[0].Error != "capture source unavailable" || m.Termination == nil {
		t.Fatalf("late archive %+v", m)
	}
}
