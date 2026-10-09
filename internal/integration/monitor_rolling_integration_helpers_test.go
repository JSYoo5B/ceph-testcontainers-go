//go:build all || (integration && topology)

package integration_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

type monitorRollingNode struct {
	container   testcontainers.Container
	config      []byte
	keyringPath string
	keyring     []byte
}

func monitorRollingFile(t *testing.T, ctx context.Context, ctr testcontainers.Container, path string) []byte {
	t.Helper()
	reader, err := ctr.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatalf("read owned container file %s: %v", path, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		t.Fatalf("read bounded owned container file %s: error=%v bytes=%d", path, err, len(data))
	}
	return data
}

func monitorRollingBootstrap(config []byte) string {
	var lines []string
	for _, line := range strings.Split(string(config), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "mon host = ") {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	return strings.Join(lines, "\n")
}

func monitorRollingWithoutBootstrap(config []byte) []byte {
	var lines []string
	for _, line := range strings.Split(string(config), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "mon host = ") {
			lines = append(lines, line)
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

func monitorRollingQuorum(t *testing.T, ctx context.Context, cluster *ceph.Container, count int) {
	t.Helper()
	if err := cluster.WaitForQuorum(ctx); err != nil {
		t.Fatal(err)
	}
	topologyWait(t, ctx, func() bool {
		quorum, err := cluster.QuorumStatus(ctx)
		return err == nil && len(quorum.QuorumNames) == count && len(quorum.MonMap.Mons) == count && len(cluster.Monitors()) == count
	})
}

func monitorRollingRADOS(t *testing.T, ctx context.Context, control testcontainers.Container, payload []byte) {
	t.Helper()
	topologyExecOutput(t, ctx, control, "rados", "-p", "tc-mon-rolling", "get", "retained", "/tmp/tc-mon-rolling-output")
	if !bytes.Equal(payload, monitorRollingFile(t, ctx, control, "/tmp/tc-mon-rolling-output")) {
		t.Fatal("retained RADOS bytes changed during MON replacement")
	}
}

func monitorRollingS3(t *testing.T, ctx context.Context, client s3HTTPClient, path string, payload []byte) {
	t.Helper()
	if !bytes.Equal(payload, client.request(t, ctx, http.MethodGet, path, nil, http.StatusOK)) {
		t.Fatal("retained signed S3 bytes changed during MON replacement")
	}
}
