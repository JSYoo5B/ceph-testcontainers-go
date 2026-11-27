package preflight_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/preflight"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault/blockio"
)

// Observe the block I/O prerequisites of the selected engine before any
// cluster starts, print the summary, keep the same snapshot as JSON and
// decide from the typed verdicts.
func ExampleObserve() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report, err := preflight.Observe(ctx, preflight.Request{
		Capabilities: []preflight.Capability{preflight.BlockIODelay, preflight.BlockIOEIO},
		Level:        preflight.Helper,
		HelperImage:  blockio.HelperImage,
	})
	if report != nil {
		fmt.Print(report)
		data, _ := json.MarshalIndent(report, "", "  ")
		_ = os.WriteFile("preflight.json", data, 0o644)
	}
	if err != nil {
		fmt.Println("preflight incomplete:", err)
	}
	switch report.Verdict(preflight.BlockIODelay) {
	case preflight.Observed:
		// Start the backend and the cluster, then apply the delay.
	case preflight.Unavailable:
		// The engine lacks a prerequisite: skip the delay case on this engine.
	default:
		// Unknown: nothing proves the prerequisite missing; run the case and
		// let blockio report what fails.
	}
}
