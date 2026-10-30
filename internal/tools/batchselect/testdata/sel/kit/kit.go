//go:build all || ci

// Package kit is a non-test package that the tests of pkg share, like the
// integration harness.
package kit

func Upper() string { return "E" }

func Two() int { return 2 }
