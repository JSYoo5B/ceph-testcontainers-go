//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rgw_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,rgw

package integration_test

import (
	"testing"
)

func TestRGWProtocolBackends(t *testing.T) {
	parallelWhenEnabled(t)
	for _, mode := range networkModes(t, "bridge", "host") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) { rgwProtocolBackends(t, host) })
	}
}
