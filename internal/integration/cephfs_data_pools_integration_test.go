//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_data_layout))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"testing"
)

func TestCephFSDynamicDataPools(t *testing.T) {
	parallelWhenEnabled(t)
	for _, mode := range networkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { testCephFSDynamicDataPools(t, host) })
	}
}
