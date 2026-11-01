package pkg_test

import (
	"testing"

	"example.com/images/harness"
	"example.com/images/pkg"
)

func TestOld(t *testing.T) { t.Log(harness.Old) }

func TestRelease(t *testing.T) { t.Log(harness.Official("20.2.4"), harness.Pins()) }

func TestLibrary(t *testing.T) { t.Log(harness.Netem(), pkg.Vault(), harness.Placeholders()) }
