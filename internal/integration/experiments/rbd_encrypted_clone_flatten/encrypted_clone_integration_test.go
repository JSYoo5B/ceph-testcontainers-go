//go:build all || (integration && features && (!ci || (ci_topology && (!ci_batch || ci_batch_rbd_encrypted_clone_flatten))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package rbd_encrypted_clone_flatten_test

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// An unformatted clone of an encrypted parent opens with the parent secret.
// A clone formatted with its own secret opens only with the whole chain
// loaded, flattens while it is loaded, then opens only with its own secret and
// survives the parent's removal. The fixture's backup of the flattened clone
// restores as raw LUKS header and ciphertext that opens with the child secret.
func TestRBDEncryptedCloneFlatten(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	f := harness.NewRBDClientFeatures(t, ctx)
	backup := func(t *testing.T) { f.Backup(t, ctx, "encrypted-child", "encrypted-restored") }
	if !f.Phase(t, ctx, rbdEncryptedCloneScript, "encryption-clone-flatten", backup) {
		return
	}
	if !f.Phase(t, ctx, rbdEncryptedCloneScript, "encryption-clone-restore") {
		return
	}
	f.Finish(t, ctx)
}
