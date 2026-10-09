//go:build (integration && multicluster && features && !all && !ci) || ((all || (integration && multicluster && features)) && native_regression && !ci)

package integration_test

import (
	"testing"
)

func runRGWSyncTranslationCases(t *testing.T, f *rgwSyncTranslationFixture) {
	f.priority_tags_owner_class(t)
	f.tag_owner_class(t)
	f.ordinary_user_denial_grant(t)
	f.tenant_system_user_isolation(t)
}
