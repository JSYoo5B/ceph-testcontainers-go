//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

// TestReleaseCapabilityTable checks the shared expectation table without a
// cluster: every row is either detected or lists each supported release, an
// unlisted patch is classified by its major release, and an unknown release,
// an unparseable version and a mixed-release cluster are explicit errors.
func TestReleaseCapabilityTable(t *testing.T) {
	for id, row := range releaseCapabilities {
		if row.id != id || id == "" {
			t.Fatalf("row %q has id %q", id, row.id)
		}
		if (row.detect == nil) == (row.releases == nil) {
			t.Fatalf("capability %q must be either detected or classified by release", id)
		}
		for _, major := range []int{19, 20} {
			if _, listed := row.releases[major]; row.releases != nil && !listed {
				t.Fatalf("capability %q has no expectation for supported Ceph %d", id, major)
			}
		}
	}
	pipe := releaseCapabilities[capabilityRGWAccountUserModePipe]
	for version, want := range map[string]bool{"19.2.5": false, "19.2.9": false, "20.2.4": true, "20.2.0": true} {
		if got, err := classifyRelease(pipe, version); err != nil || got != want {
			t.Fatalf("classifyRelease(%q) = %v, %v; want %v", version, got, err, want)
		}
	}
	for version, want := range map[string]string{
		"21.2.0": "no expectation for Ceph 21.2.0", "18.2.7": "no expectation for Ceph 18.2.7",
		"": "unrecognized Ceph release", "20.2": "unrecognized Ceph release", "v20.2.4": "unrecognized Ceph release",
	} {
		if got, err := classifyRelease(pipe, version); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("classifyRelease(%q) = %v, %v; want error %q", version, got, err, want)
		}
	}
	if _, err := classifyRelease(releaseCapabilities[capabilityMessengerDump], "20.2.4"); err == nil {
		t.Fatal("a detected capability was classified by release")
	}

	const tentacle = "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle (stable - RelWithDebInfo)"
	const squid = "ceph version 19.2.5 (aa4a4a4a) squid (stable)"
	uniform := `{"mon":{"` + tentacle + `":1},"overall":{"` + tentacle + `":6}}`
	if err := uniformRelease([]byte(uniform), "20.2.4"); err != nil {
		t.Fatal(err)
	}
	for data, want := range map[string]string{
		`{"overall":{"` + tentacle + `":5,"` + squid + `":1}}`: "mixed-release cluster",
		uniform:                         "mixed-release cluster", // against a 19.2.5 control image below
		`{"overall":{}}`:                "no daemon releases",
		`not json`:                      "no daemon releases",
		`{"overall":{"ceph 20.2.4":1}}`: "unrecognized daemon release",
	} {
		control := "20.2.4"
		if data == uniform {
			control = "19.2.5"
		}
		if err := uniformRelease([]byte(data), control); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("uniformRelease(%s, %s) = %v, want %q", data, control, err, want)
		}
	}
}
