//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_empty_bootstrap))))

//ci: timeout=80m job-timeout=90

package integration_test

import (
	"fmt"
	"testing"
)

// Small logical devices use an explicit preparation profile that skips mClock's
// startup I/O benchmark. The scheduler is unchanged. Separate 512 MiB controls
// retain the native benchmark; each case owns a fresh serial bridge cluster.
func TestSmallOSDBlockSizeTopology(t *testing.T) {
	for _, mib := range []int64{64, 128, 256, 512} {
		for _, ram := range []bool{false, true} {
			name := "disk"
			if ram {
				name = "ram"
			}
			t.Run(fmt.Sprintf("%s-%dMiB-skip-benchmark", name, mib), func(t *testing.T) {
				testSmallOSDBlockSize(t, mib<<20, ram, true)
			})
		}
	}
	for _, ram := range []bool{false, true} {
		name := "disk"
		if ram {
			name = "ram"
		}
		t.Run(name+"-512MiB-native-benchmark", func(t *testing.T) {
			testSmallOSDBlockSize(t, 512<<20, ram, false)
		})
	}
}
