//go:build (all || (integration && multicluster && features && ci)) && !native_regression

package integration_test

import (
	"testing"
)

func runRGWSyncTranslationCases(t *testing.T, f *rgwSyncTranslationFixture) {
	f.tag_owner_class(t)
	f.tenant_system_user_isolation(t)
}
