//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// rbd task list is served by the required rbd_support MGR module. A fresh
// cluster has no tasks; require its actual JSON list after active promotion.
// Bound the CLI process itself while the module finishes initialization.
func managerLifecycleRBDReady(t *testing.T, parent context.Context, cluster *ceph.Container) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	for {
		code, reader, err := cluster.ControlContainer().Exec(ctx,
			topologyCommandWithTimeout(40*time.Second, "ceph", "--connect-timeout", "5", "rbd", "task", "list", "--format", "json"), tcexec.Multiplexed())
		var output []byte
		if err == nil {
			output, err = io.ReadAll(reader)
		}
		var tasks []json.RawMessage
		if err == nil && code == 0 && json.Unmarshal(output, &tasks) == nil && tasks != nil && len(tasks) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native rbd_support task command unavailable or fresh queue not empty: error=%v exit=%d output=%s", err, code, output)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
