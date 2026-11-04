package expect_test

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
)

// A health expectation compares check codes, severities, counts and mute
// state, never the messages, and names each check that differs.
func ExampleHealth_Evaluate() {
	observed := ceph.HealthSnapshot{
		FSID:   "44444444-4444-4444-8444-444444444444",
		Status: "HEALTH_WARN",
		Checks: map[string]ceph.HealthCheck{
			"OSD_DOWN": {Severity: "HEALTH_WARN", Summary: "1 osds down", Count: 1, Details: []string{"osd.1 is down"}},
			"SLOW_OPS": {Severity: "HEALTH_WARN", Summary: "2 slow ops", Count: 2},
		},
		Mutes: []ceph.HealthMute{},
	}
	want := expect.Health{
		Require: []expect.HealthCheck{{Code: "OSD_DOWN", Severity: "HEALTH_WARN", Muted: expect.HealthUnmuted, Count: 1}},
		Forbid:  []string{"SLOW_OPS"},
		NoMutes: []string{"OSD_DOWN"},
	}
	result, err := want.Evaluate(observed)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	fmt.Println(result)
	// Output:
	// false
	// health want require=[OSD_DOWN] forbid=[SLOW_OPS] nomutes=[OSD_DOWN] in HEALTH_WARN with 2 checks, 0 mutes: SLOW_OPS present: HEALTH_WARN count=2 unmuted
}
