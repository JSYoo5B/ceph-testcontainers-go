//go:build (all || (integration && multicluster && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_native_rgw_translation)))

//ci: timeout=60m job-timeout=75 image-roles=essential,client,rgw

package rgw_test

import (
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestMultiClusterRGWSyncTranslationFiltering(t *testing.T) {
	testRGWSyncTranslationFiltering(t)
}

func TestHostNetworkMultiClusterRGWSyncTranslationFiltering(t *testing.T) {
	testRGWSyncTranslationFiltering(t, ceph.WithHostNetwork())
}
