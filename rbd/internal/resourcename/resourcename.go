// Package resourcename validates the pool, namespace and image names accepted
// by standalone RBD operations.
package resourcename

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

// Check reports whether name is acceptable as an RBD resource of the given
// kind ("pool", "namespace" or "image"). The error carries only the message;
// the caller wraps it as an invalid request.
func Check(kind, name string) error {
	if len(name) > 128 || !naming.PoolResource.MatchString(name) {
		return fmt.Errorf("RBD %s name must use letters, digits, underscores, dots or dashes and cannot start with a dot or dash", kind)
	}
	return nil
}
