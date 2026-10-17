//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"testing"
)

func TestClientFencing(t *testing.T) {
	parallelWhenEnabled(t)
	for _, mode := range networkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { testClientFencing(t, host) })
	}
}
