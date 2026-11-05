//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_basic))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client

package rbd_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// Client operations remain in the consumer's native librbd. The public module
// supplies only the replicated pool, isolated namespace, credentials and
// network, and archives an encrypted image as its raw LUKS header and
// ciphertext.
func TestRBDClientFeatures(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
			defer cancel()
			var opts []testcontainers.ContainerCustomizer
			if mode.Host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			f := harness.NewRBDClientFeatures(t, ctx, opts...)
			for _, phase := range []string{
				"layering-flatten", "trash-restore-purge", "migration-commit", "migration-abort",
				"group-snapshot", "exclusive-lock", "encryption-format-load", "encryption-rekey",
				"encryption-backup-source", "encryption-backup-restore",
			} {
				var after []func(*testing.T)
				if phase == "encryption-backup-source" {
					after = append(after, func(t *testing.T) { f.Backup(t, ctx, "encrypted-source", "encrypted-backup-restored") })
				}
				if !f.Phase(t, ctx, rbdClientFeatureScript, phase, after...) {
					return
				}
			}
			f.Finish(t, ctx)
		})
	}
}
