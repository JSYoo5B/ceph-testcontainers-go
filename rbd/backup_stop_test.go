package rbd

import (
	"context"
	"io"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// backupClient is a client container whose every command succeeds silently.
type backupClient struct{ testcontainers.Container }

func (c backupClient) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	return clustertest.Supervise(ctx, args, opts, func(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
		return clustertest.Reply(args, 0, nil)
	})
}

// ExportBackup runs rbd export supervised in the caller's client, which no
// cluster owns: a canceled call stops the command before it returns, or
// returns a NativeCommandUnstoppedError whose Stop stops it.
func TestRBDExportBackupStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	hang := clustertest.NewHangingCommand(backupClient{}, func(argv []string) bool {
		return len(argv) > 2 && slices.Equal(argv[:3], []string{"rbd", "export", "volumes/disk"})
	})
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		return ExportBackup(ctx, hang, "volumes/disk", io.Discard)
	})
}
