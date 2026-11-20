//go:build all || (integration && features)

package integration_test

import (
	"fmt"
	"slices"
)

func servicePackageExpected(kind string, phase int) []string {
	prefix := "image"
	if kind == "cephfs" {
		prefix = "file"
	}
	names := make([]string, phase)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%d", prefix, i+1)
	}
	return names
}

type servicePackageReadback struct {
	Listed  []string `json:"listed"`
	Checked []string `json:"checked"`
}

func requireServicePackageReadback(expected []string, receipt servicePackageReadback) error {
	for _, name := range expected {
		if !slices.Contains(receipt.Listed, name) {
			return fmt.Errorf("owned object %s was not listed: %v", name, receipt.Listed)
		}
	}
	if !slices.Equal(receipt.Checked, expected) {
		return fmt.Errorf("byte checks for %v, want %v", receipt.Checked, expected)
	}
	return nil
}
