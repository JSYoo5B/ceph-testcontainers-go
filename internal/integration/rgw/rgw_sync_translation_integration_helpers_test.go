//go:build all || (integration && multicluster && features)

package rgw_test

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

func testRGWSyncTranslationFiltering(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	f := harness.NewRGWSyncTranslationFixture(t, ctx, opts...)
	rgwSyncTagOwnerClass(t, f)
	rgwSyncTenantSystemUserIsolation(t, f)
}
