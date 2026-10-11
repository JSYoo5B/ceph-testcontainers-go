//go:build (all || (integration && multicluster && features)) && ci && native_regression

package integration_test

import (
	"testing"
)

func runRGWSyncTranslationCases(t *testing.T, f *rgwSyncTranslationFixture) {
	f.priority_tags_owner_class(t)
	f.ordinary_user_denial_grant(t)
}
