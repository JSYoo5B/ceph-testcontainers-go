//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_client_setup))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Client operations remain in the consumer's native librbd. The public module
// supplies only the replicated pool, isolated namespace, credentials and network.
func TestRBDClientFeatures(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		t.Run(network, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, admin := newServiceCluster(t, opts...)
			const pool, namespace = "tc-rbd-client-features", "consumer"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.InitRBDPool(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.CreateRBDNamespace(ctx, pool, namespace); err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.CreateRBDNamespace(ctx, pool, "foreign"); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			status, err := cluster.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			state, err := cluster.PoolStatus(ctx, pool)
			if err != nil || state.ID <= 0 || state.Type != "replicated" || state.Size != 1 || state.MinSize != 1 {
				t.Fatalf("native RBD metadata pool: %+v error=%v", state, err)
			}
			identity, err := cluster.CreateClient(ctx, "rbd-features", ceph.ClientCaps{
				Mon: "profile rbd",
				MGR: "profile rbd pool=" + pool + " namespace=" + namespace,
				OSD: "profile rbd pool=" + pool + " namespace=" + namespace,
			})
			if err != nil {
				t.Fatal(err)
			}
			image, _ := integrationImages(t)
			if custom := os.Getenv("CEPH_TEST_RBD_CLIENT_IMAGE"); custom != "" {
				image = custom
			}
			client, err := testcontainers.Run(ctx, image, cluster.WithClientIdentity(identity),
				testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
				testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados, rbd"})))
			if client != nil {
				testcontainers.CleanupContainer(t, client)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native RBD client image=%s; encryption-rekey requires cryptsetup in this image", image)
			// Namespace-wide trash purge is safe here because the namespace is a
			// fresh fixture. Expired foreign/default trash must survive the purge.
			execCommand(t, ctx, admin, "python3", "-c", rbdClientFeatureSentinelScript, pool, "seed")
			for _, phase := range []string{
				"layering-flatten", "trash-restore-purge", "migration-commit", "migration-abort",
				"group-snapshot", "exclusive-lock", "encryption-format-load", "encryption-rekey",
			} {
				if !t.Run(phase, func(t *testing.T) {
					args := []string{"python3", "-c", `import subprocess, sys
phase = sys.argv[2]
try:
    result = subprocess.run([sys.executable, "-c", sys.argv[1], *sys.argv[2:]], timeout=180, check=False, stderr=subprocess.PIPE)
except subprocess.TimeoutExpired:
    print('native phase timeout: phase=' + phase + ' exit=124', file=sys.stderr)
    sys.exit(124)
if result.returncode != 0:
    print('native phase failed: phase=' + phase + ' exit=' + str(result.returncode), file=sys.stderr)
    if result.stderr:
        # Expected lock/key denials also log on native stderr. Only failed
        # phases forward a bounded tail; successful stdout stays exact JSON.
        sys.stderr.write(result.stderr[-8192:].decode('utf-8', errors='replace'))
sys.exit(result.returncode)`,
						rbdClientFeatureScript, phase, pool, namespace, identity.Name(), identity.KeyringPath(), status.FSID, fmt.Sprint(state.ID)}
					code, reader, err := client.Exec(ctx, args, tcexec.Multiplexed())
					if err != nil {
						t.Fatal(err)
					}
					output, err := io.ReadAll(reader)
					if err != nil || code != 0 {
						t.Fatalf("native %s exit=%d error=%v:\n%s", phase, code, err, output)
					}
					var result struct {
						Phase  string `json:"phase"`
						FSID   string `json:"fsid"`
						PoolID int64  `json:"pool_id"`
					}
					if err := json.Unmarshal(output, &result); err != nil || result.Phase != phase || result.FSID != status.FSID || result.PoolID != state.ID {
						t.Fatalf("unexpected native proof: %s error=%v", output, err)
					}
					t.Logf("native proof: %s", output)
				}) {
					// A failed native phase may leave owned partial images or trash.
					// Preserve its first error and let this fixture's cleanup remove
					// the cluster, instead of cascading unrelated empty-list failures.
					return
				}
			}
			execCommand(t, ctx, admin, "python3", "-c", rbdClientFeatureSentinelScript, pool, "verify-and-remove")
			after, err := cluster.PoolStatus(ctx, pool)
			if err != nil || after.ID != state.ID {
				t.Fatalf("native pool identity changed: before=%+v after=%+v error=%v", state, after, err)
			}
		})
	}
}
