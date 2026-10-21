package main

import (
	"slices"
	"testing"
	"time"

	container "github.com/moby/moby/api/types/container"
)

func TestWorkingSet(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stats map[string]uint64
		want  uint64
	}{
		{"v2", map[string]uint64{"inactive_file": 30, "total_inactive_file": 90}, 70},
		{"v1", map[string]uint64{"total_inactive_file": 40}, 60},
		{"missing", nil, 100},
		{"underflow", map[string]uint64{"inactive_file": 101}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := workingSet(container.MemoryStats{Usage: 100, Stats: tc.stats}); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
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
