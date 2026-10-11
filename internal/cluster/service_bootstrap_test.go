package cluster

// Test views of the normalized service configuration a Run would provision.

func testFilesystems(settings options) []CephFSConfig {
	for _, service := range settings.services {
		if b, ok := service.(*cephFSBootstrap); ok {
			return b.configs
		}
	}
	return nil
}

func testGateways(settings options) []RGWConfig {
	for _, service := range settings.services {
		if b, ok := service.(*rgwBootstrap); ok {
			return b.configs
		}
	}
	return nil
}
