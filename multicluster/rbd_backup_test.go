package multicluster

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

type archiveClientWithoutTransport struct{ testcontainers.Container }

type failingArchiveReader struct{ read bool }

func (r *failingArchiveReader) Read(p []byte) (int, error) {
	if !r.read {
		r.read = true
		return copy(p, []byte("partial archive")), nil
	}
	return 0, io.ErrUnexpectedEOF
}

func TestRestoreCanceledArchiveDoesNotReadOrSpool(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("TMPDIR", directory)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader := &failingArchiveReader{}
	err := RestoreRBDBackup(ctx, &archiveClientWithoutTransport{}, "rbd/restore", reader)
	if !errors.Is(err, context.Canceled) || reader.read {
		t.Fatalf("canceled archive was consumed: error=%v read=%v", err, reader.read)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled archive left a spool: %v %v", entries, err)
	}
}

func TestRestoreArchiveReaderFailureLeavesNoSpoolOrContainerMutation(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("TMPDIR", directory)
	// Its embedded transport is nil: touching Docker would panic. A failed
	// reader must be rejected before either copying or importing an archive.
	client := &archiveClientWithoutTransport{}
	err := RestoreRBDBackup(t.Context(), client, "rbd/restore", &failingArchiveReader{})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("archive reader error lost: %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed archive left temporary files: %v", entries)
	}
}
