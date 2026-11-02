// Package pkg has its own and external test files, like capture.
package pkg

import "example.com/images/lib"

// Vault is an image of the package itself that an external test reaches.
func Vault() string { return "hashicorp/vault:1.21.4" }

// The same function name outside the known harness file is explicit use.
func IntegrationImage() string { return lib.Published }
