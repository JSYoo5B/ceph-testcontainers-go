package main

import (
	"slices"
	"testing"
	"time"

	container "github.com/moby/moby/api/types/container"
)

func TestWorkingSet(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		stats         map[string]uint64
		want          uint64
	}{
		{"v2-both-keys", "2", map[string]uint64{"inactive_file": 30, "total_inactive_file": 90}, 70},
		{"v1-both-keys", "1", map[string]uint64{"inactive_file": 30, "total_inactive_file": 90}, 10},
		{"v1", "1", map[string]uint64{"total_inactive_file": 40}, 60},
		{"v2-missing", "2", nil, 100},
		{"v1-missing", "1", nil, 100},
		{"v2-no-fallback", "2", map[string]uint64{"total_inactive_file": 90}, 100},
		{"v1-no-fallback", "1", map[string]uint64{"inactive_file": 30}, 100},
		{"v2-underflow", "2", map[string]uint64{"inactive_file": 101}, 0},
		{"v1-underflow", "1", map[string]uint64{"total_inactive_file": 101}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := workingSet(container.MemoryStats{Usage: 100, Stats: tc.stats}, tc.version); err != nil || got != tc.want {
				t.Fatalf("got %d, want %d, err %v", got, tc.want, err)
			}
		})
	}
	for _, version := range []string{"", "3", "unknown"} {
		if _, err := workingSet(container.MemoryStats{Usage: 100}, version); err == nil {
			t.Fatalf("accepted unsupported cgroup version %q", version)
		}
	}
}

func statsFixture() phase {
	start := time.Unix(100, 0)
	return phase{Snapshots: [][]observation{
		{{ID: "a", ImageID: "image", Read: start, CPU: 10, Memory: 100, WorkingSet: 50},
			{ID: "b", ImageID: "other", Read: start.Add(time.Second), CPU: 20, Memory: 200, WorkingSet: 75}},
		{{ID: "a", ImageID: "image", Read: start.Add(2 * time.Second), CPU: 1_000_000_010, Memory: 300, WorkingSet: 100},
			{ID: "b", ImageID: "other", Read: start.Add(5 * time.Second), CPU: 1_000_000_020, Memory: 400, WorkingSet: 125}},
	}}
}

func TestSummarizeUsesPerContainerTimeAndSumsMemory(t *testing.T) {
	p, err := summarize(statsFixture())
	if err != nil || p.CPUPercent != 75 || p.MeanMemoryBytes != 500 || p.MeanWorkingSet != 175 {
		t.Fatalf("summary: %+v, %v", p, err)
	}
}

func TestSummarizeRefusesInvalidEvidence(t *testing.T) {
	for _, name := range []string{"missing", "empty", "removed", "replaced", "image-changed", "reset", "clock"} {
		t.Run(name, func(t *testing.T) {
			p := statsFixture()
			switch name {
			case "missing":
				p.Snapshots = p.Snapshots[:1]
			case "empty":
				p.Snapshots = [][]observation{{}, {}}
			case "removed":
				p.Snapshots[1] = p.Snapshots[1][:1]
			case "replaced":
				p.Snapshots[1][0].ID = "replacement"
			case "image-changed":
				p.Snapshots[1][0].ImageID = "different"
			case "reset":
				p.Snapshots[1][0].CPU = 0
			case "clock":
				p.Snapshots[1][0].Read = p.Snapshots[0][0].Read
			}
			if _, err := summarize(p); err == nil {
				t.Fatal("accepted incomplete/invalid statistics")
			}
		})
	}
}

func TestSummarizeValidatesEveryAdjacentSample(t *testing.T) {
	for _, name := range []string{"valid", "idle-equal-cpu", "middle-reset", "middle-duplicate-clock", "middle-backward-clock", "middle-later-than-end", "middle-cpu-above-end"} {
		t.Run(name, func(t *testing.T) {
			p := statsFixture()
			middle := slices.Clone(p.Snapshots[0])
			for i := range middle {
				middle[i].Read = middle[i].Read.Add(time.Second)
				if name != "idle-equal-cpu" {
					middle[i].CPU++
				}
			}
			switch name {
			case "middle-reset":
				middle[0].CPU = 0
			case "middle-duplicate-clock":
				middle[0].Read = p.Snapshots[0][0].Read
			case "middle-backward-clock":
				middle[0].Read = p.Snapshots[0][0].Read.Add(-time.Second)
			case "middle-later-than-end":
				middle[0].Read = p.Snapshots[1][0].Read.Add(time.Second)
			case "middle-cpu-above-end":
				middle[0].CPU = p.Snapshots[1][0].CPU + 1
			}
			p.Snapshots = [][]observation{p.Snapshots[0], middle, p.Snapshots[1]}
			got, err := summarize(p)
			valid := name == "valid" || name == "idle-equal-cpu"
			if valid {
				if err != nil || got.CPUPercent != 75 {
					t.Fatalf("valid three-snapshot result: %v, CPU %v", err, got.CPUPercent)
				}
			} else if err == nil {
				t.Fatal("accepted an intermediate discontinuity despite valid endpoints")
			}
		})
	}
}

func TestCompositionRejectsFallbackAndUnexpectedToolkit(t *testing.T) {
	core := []observation{{ImageID: "core"}, {ImageID: "core"}, {ImageID: "core"}}
	for _, tc := range []struct {
		name      string
		items     []observation
		client    string
		wantError bool
	}{
		{"core-only", core, "", false},
		{"split", append(slices.Clone(core), observation{ImageID: "client"}), "client", false},
		{"missing-toolkit", core, "client", true},
		{"unexpected-toolkit", append(slices.Clone(core), observation{ImageID: "client"}), "", true},
		{"fallback", append(slices.Clone(core), observation{ImageID: "core"}), "client", true},
		{"wrong-image", append(slices.Clone(core), observation{ImageID: "other"}), "client", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := composition(phase{Snapshots: [][]observation{tc.items}}, "core", tc.client)
			if (err != nil) != tc.wantError {
				t.Fatalf("unexpected composition: %v", err)
			}
		})
	}
}
