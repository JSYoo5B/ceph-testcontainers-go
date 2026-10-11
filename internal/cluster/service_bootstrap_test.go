package cluster

import (
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// testExtension is per-cluster service state that lists secrets and
// configuration targets, as a service package's registry does.
type testExtension struct {
	secrets []string
	targets map[string]testcontainers.Container
	modules []string
}

func (e testExtension) Secrets() []string { return e.secrets }

func (e testExtension) ConfigTargets() map[string]testcontainers.Container { return e.targets }

func (e testExtension) RequiresMGRModule(module string) bool {
	return slices.Contains(e.modules, module)
}
