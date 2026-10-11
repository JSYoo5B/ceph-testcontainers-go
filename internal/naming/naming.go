// Package naming holds the name patterns the cluster and service packages
// accept for Ceph resources before passing them to a CLI.
package naming

import "regexp"

// PoolResource matches pool, RBD namespace and image names: letters, digits,
// underscores, dots or dashes, not starting with a dot or dash.
var PoolResource = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)
