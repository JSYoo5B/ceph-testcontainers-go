package capture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestCaptureFileUsesWholeRetrievedIdentityAndPreservesSource(t *testing.T) {
	data := append(pcapFile(64), []byte("cut record")...)
	path := filepath.Join(t.TempDir(), "capture.pcap")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	s := &Session{finished: true, outputInfo: info, request: Request{Output: path}, report: Report{Retrieved: true, RetrievedBytes: int64(len(data)), RetrievedSHA256: digest, File: FileReport{Status: FileMalformed, Complete: false, SHA256: "prefix-only"}}}
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

func TestStopPreservesReplacementAtReservedOutput(t *testing.T) {
	for _, name := range []string{"regular", "symlink"} {
		t.Run(name, func(t *testing.T) {
			req := request(t)
			s, err := startFake(t, newFakeBackend(), req)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = s.Stop(context.Background()) })
			if err := os.Rename(req.Output, req.Output+".owned"); err != nil {
				t.Fatal(err)
			}
			foreign := req.Output
			if name == "symlink" {
				foreign += ".foreign"
				if err := os.Symlink(foreign, req.Output); err != nil {
					t.Fatal(err)
				}
			}
			const marker = "belongs to the caller"
			if err := os.WriteFile(foreign, []byte(marker), 0o600); err != nil {
				t.Fatal(err)
			}
			report, err := s.Stop(t.Context())
			if err == nil || report.Retrieved || !report.HelperRemoved || s.output != nil {
				t.Fatalf("Stop: %+v, %v", report, err)
			}
			if _, _, _, err := s.CaptureFile(t.Context()); err == nil {
				t.Fatal("replacement admitted for archiving")
			}
			if got, err := os.ReadFile(foreign); err != nil || string(got) != marker {
				t.Fatalf("foreign file %q, %v", got, err)
			}
		})
	}
}

// interleavedProbe lets another writer claim the real output name after the
// initial path check and before startup settles.
type interleavedProbe struct {
	*fakeBackend
	duringProbe func()
}

func (b interleavedProbe) probe(ctx context.Context, image, network string, argv []string) (probeResult, string, error) {
	b.duringProbe()
	return b.fakeBackend.probe(ctx, image, network, argv)
}

func TestConcurrentWriterOwnsOutputAfterStartupRefusal(t *testing.T) {
	probeFailure := errors.New("injected probe failure")
	removalFailure := errors.New("injected helper removal failure")
	for _, tc := range []struct {
		name                 string
		probeErr, removalErr error
	}{
		{"probe refuses", probeFailure, nil},
		{"ready helper loses final exclusive create", nil, nil},
		{"retry helper removal with foreign file still present", nil, removalFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := request(t)
			writerPath := req.Output + ".foreign"
			const sentinel = "foreign writer's bytes"
			if err := os.WriteFile(writerPath, []byte(sentinel), 0600); err != nil {
				t.Fatal(err)
			}

			checked := make(chan struct{})
			claimed := make(chan error, 1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go func() {
				select {
				case <-checked:
					claimed <- os.Rename(writerPath, req.Output)
				case <-ctx.Done():
				}
			}()
			f := newFakeBackend()
			f.probeErr = tc.probeErr
			if tc.removalErr != nil {
				f.removeErrs = []error{tc.removalErr}
			}
			b := interleavedProbe{f, func() {
				if exists(req.Output) {
					t.Error("output was reserved before startup could succeed")
				}
				close(checked)
				if err := <-claimed; err != nil {
					t.Fatal(err)
				}
			}}
			s, err := start(ctx, b, fakeTarget{}, req)
			want := tc.probeErr
			if want == nil {
				want = os.ErrExist
			}
			if !errors.Is(err, want) || (s != nil) != (tc.removalErr != nil) {
				t.Fatalf("Start returned %p, %v", s, err)
			}
			if s != nil {
				if !errors.Is(err, tc.removalErr) || s.output != nil {
					t.Fatalf("partial startup retained an output or lost its cleanup error: %v", err)
				}
				for range 2 {
					report, stopErr := s.Stop(ctx)
					if stopErr != nil || !report.HelperRemoved || report.Retrieved {
						t.Fatalf("Stop: %+v, %v", report, stopErr)
					}
				}
			}
			if tc.probeErr == nil && !slices.Equal(f.removed, []string{helperID}) {
				t.Fatalf("helper removals %v", f.removed)
			}
			got, readErr := os.ReadFile(req.Output)
			if readErr != nil || string(got) != sentinel {
				t.Fatalf("foreign file after cleanup: %q, %v", got, readErr)
			}
		})
	}
}
