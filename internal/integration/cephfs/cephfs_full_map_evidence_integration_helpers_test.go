//go:build all || integration

package cephfs_test

import (
	"context"
	"fmt"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// retainedMapSources reads the retained client through its admin socket
// path in the client container and each owned OSD through the cluster's
// admin socket API. The OSD list is taken when the sources are built.
func retainedMapSources(t testing.TB, cluster *ceph.Container, client testcontainers.Container, asok string) harness.MapEvidenceSources {
	src := harness.MapEvidenceSources{
		ClientAdmin: func(ctx context.Context, args ...string) ([]byte, error) {
			result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
				Argv: append([]string{"ceph", "--admin-daemon", asok}, args...), Timeout: harness.MapEvidenceCallBound,
				MaxStdout: 256 << 10, MaxStderr: 16 << 10, Label: "client map evidence",
			})
			if err != nil || result.ExitCode != 0 || !result.Complete() {
				return nil, fmt.Errorf("exit=%d complete=%t err=%v stderr=%.256s", result.ExitCode, result.Complete(), err, result.Stderr)
			}
			return result.Stdout, nil
		},
		OSDAdmin: func(ctx context.Context, osd int, args ...string) ([]byte, error) {
			handle, err := harness.FindOSD(ctx, cluster, osd)
			if err != nil {
				return nil, err
			}
			return handle.AdminSocket(ctx, args...)
		},
		MonCommand: func(ctx context.Context, args ...string) ([]byte, error) { return cluster.Ceph(ctx, args...) },
	}
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		src.OSDs = append(src.OSDs, osd.ID)
	}
	return src
}
