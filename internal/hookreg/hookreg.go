// Package hookreg holds the hooks that the cluster implementation registers
// for the rbd, cephfs and rgw packages. It imports nothing, so the cluster
// implementation can register its hooks without an import cycle; the
// internal/servicehook package gives them their typed form.
package hookreg

// Value is set once, during package initialization, by the cluster
// implementation.
var Value any
