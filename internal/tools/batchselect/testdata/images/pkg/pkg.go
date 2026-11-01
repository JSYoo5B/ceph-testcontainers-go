// Package pkg has its own and external test files, like capture.
package pkg

// Vault is an image of the package itself that an external test reaches.
func Vault() string { return "hashicorp/vault:1.21.4" }
