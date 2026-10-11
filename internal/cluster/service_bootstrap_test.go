package cluster

import "github.com/testcontainers/testcontainers-go"

// Test views of the normalized service configuration a Run would provision.

func testFilesystems(settings options) []CephFSConfig {
	for _, service := range settings.services {
		if b, ok := service.(*cephFSBootstrap); ok {
			return b.configs
		}
	}
	return nil
}

// testExtension is per-cluster service state that lists secrets and
// configuration targets, as a service package's registry does.
type testExtension struct {
	secrets []string
	targets map[string]testcontainers.Container
}

func (e testExtension) Secrets() []string { return e.secrets }

func (e testExtension) ConfigTargets() map[string]testcontainers.Container { return e.targets }
