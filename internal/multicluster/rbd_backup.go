package multicluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// ExportRBDBackup writes a CLI-generated format-2 archive to destination. The
// archive includes image metadata and snapshots. No mirror daemon is required.
// The caller owns the writer and decides where to retain the backup.
func ExportRBDBackup(ctx context.Context, client testcontainers.Container, image string, destination io.Writer) error {
	return exportRBDArchive(ctx, client, image, "", destination)
}

// ExportRBDIncremental writes the CLI diff from fromSnapshot to image (which
// can include @snapshot). Restore requires the matching baseline snapshot.
func ExportRBDIncremental(ctx context.Context, client testcontainers.Container, image, fromSnapshot string, destination io.Writer) error {
	if fromSnapshot == "" || strings.ContainsAny(fromSnapshot, "/@\x00\r\n") || strings.HasPrefix(fromSnapshot, "-") {
		return errors.New("incremental backup requires a valid baseline snapshot name")
	}
	return exportRBDArchive(ctx, client, image, fromSnapshot, destination)
}

// RestoreRBDBackup imports a format-2 archive into a new image on the client's
// own cluster. Pool creation and archive retention belong to the caller.
func RestoreRBDBackup(ctx context.Context, client testcontainers.Container, image string, archive io.Reader) error {
	return restoreRBDArchive(ctx, client, image, archive, false)
}

// RestoreRBDIncremental applies a CLI diff to an existing image. Ceph checks
// that its baseline snapshot exists; this function does not invent a baseline.
func RestoreRBDIncremental(ctx context.Context, client testcontainers.Container, image string, archive io.Reader) error {
	return restoreRBDArchive(ctx, client, image, archive, true)
}

func validateRBDArchiveClient(client testcontainers.Container, image string) error {
	if client == nil {
		return errors.New("RBD archive operation requires a client container")
	}
	if image == "" || strings.HasPrefix(image, "-") || strings.ContainsAny(image, "\x00\r\n") {
		return errors.New("RBD archive operation requires a valid image specification")
	}
	return nil
}

func exportRBDArchive(ctx context.Context, client testcontainers.Container, image, fromSnapshot string, destination io.Writer) (result error) {
	if err := validateRBDArchiveClient(client, image); err != nil {
		return err
	}
	if destination == nil {
		return errors.New("RBD archive destination writer is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := "/tmp/tc-rbd-archive-" + uuid.NewString()
	defer func() { result = errors.Join(result, removeRBDArchive(client, path)) }()
	command := []string{"rbd", "export", image, path, "--export-format", "2", "--no-progress"}
	if fromSnapshot != "" {
		command = []string{"rbd", "export-diff", "--from-snap", fromSnapshot, image, path, "--no-progress"}
	}
	if _, err := exec(ctx, client, command...); err != nil {
		return fmt.Errorf("export RBD archive: %w", err)
	}
	reader, err := client.CopyFileFromContainer(ctx, path)
	if err != nil {
		return fmt.Errorf("read RBD archive: %w", err)
	}
	_, copyErr := io.Copy(destination, contextArchiveReader{ctx: ctx, reader: reader})
	return errors.Join(copyErr, reader.Close())
}

func restoreRBDArchive(ctx context.Context, client testcontainers.Container, image string, archive io.Reader, incremental bool) (result error) {
	if err := validateRBDArchiveClient(client, image); err != nil {
		return err
	}
	if archive == nil {
		return errors.New("RBD archive reader is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Spool to a private host temporary file instead of retaining a potentially
	// large archive in memory. Container copy and Ceph I/O remain native to the
	// Linux container; the Go host only transports bytes.
	file, err := os.CreateTemp("", "tc-rbd-restore-*")
	if err != nil {
		return fmt.Errorf("create RBD archive spool: %w", err)
	}
	defer func() {
		if err := os.Remove(file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("remove RBD archive spool: %w", err))
		}
	}()
	_, copyErr := io.Copy(file, contextArchiveReader{ctx: ctx, reader: archive})
	if err := errors.Join(copyErr, file.Close()); err != nil {
		return fmt.Errorf("spool RBD archive: %w", err)
	}
	path := "/tmp/tc-rbd-archive-" + uuid.NewString()
	defer func() { result = errors.Join(result, removeRBDArchive(client, path)) }()
	if err := client.CopyFileToContainer(ctx, file.Name(), path, 0o600); err != nil {
		return fmt.Errorf("transfer RBD restore archive: %w", err)
	}
	command := []string{"rbd", "import", path, image, "--export-format", "2", "--no-progress"}
	if incremental {
		command = []string{"rbd", "import-diff", path, image, "--no-progress"}
	}
	if _, err := exec(ctx, client, command...); err != nil {
		return fmt.Errorf("restore RBD archive: %w", err)
	}
	return nil
}

// Cancellation is checked between reads. A caller-supplied reader or writer
// that can block indefinitely must provide its own way to cancel blocked I/O.
type contextArchiveReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextArchiveReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func removeRBDArchive(client testcontainers.Container, path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := exec(ctx, client, "rm", "-f", path); err != nil {
		return fmt.Errorf("remove RBD temporary archive: %w", err)
	}
	return nil
}
