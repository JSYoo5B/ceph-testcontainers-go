//go:build (all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_translation))))) && !native_regression

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestMultiClusterRGWSyncTranslationFiltering(t *testing.T) {
	testRGWSyncTranslationFiltering(t)
}

func TestHostNetworkMultiClusterRGWSyncTranslationFiltering(t *testing.T) {
	testRGWSyncTranslationFiltering(t, ceph.WithHostNetwork())
}
