package pkg_test

import (
	"testing"

	"example.com/images/harness"
	"example.com/images/pkg"
)

func TestOld(t *testing.T) { t.Log(harness.Old) }

func TestRelease(t *testing.T) { t.Log(harness.Official("20.2.4"), harness.Squid) }

func TestLibrary(t *testing.T) { t.Log(harness.Netem(), pkg.Vault(), harness.Placeholders()) }

func TestRoleFallback(t *testing.T) { t.Log(harness.IntegrationImage()) }

func TestExplicitImage(t *testing.T) { t.Log(harness.ExplicitImage()) }

func TestOtherIntegrationImage(t *testing.T) { t.Log(pkg.IntegrationImage()) }

func TestFallbackThenExplicit(t *testing.T) {
	t.Log(harness.IntegrationImage(), harness.ExplicitImage())
}

func TestExplicitThenFallback(t *testing.T) {
	t.Log(harness.ExplicitImage(), harness.IntegrationImage())
}
