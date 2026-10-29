package ceph

import "github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"

// harnessHooks is the core side of the contract that internal/harnesshook
// types. Its methods are exported only so that internal/harnesshook can call
// them through an interface; the type itself is unexported and never appears
// in a public package. It currently carries the error contract only; the
// backend entry points join it with the package move
// (docs/design/shared-harness-boundary.md).
type harnessHooks struct{}

func init() {
	hookreg.Harness = harnessHooks{}
}

// Invalid marks err as a refused request with ErrInvalidRequest, with the one
// construction that invalidRequest uses.
func (harnessHooks) Invalid(err error) error { return newInvalid(err) }
