package multicluster

import (
	"fmt"
	"testing"
)

func TestInitialMirrorDaemonNameSequence(t *testing.T) {
	for index, want := range map[int]string{0: "a", 1: "b", 25: "z", 26: "node-27", 27: "node-28", 99: "node-100"} {
		if got := InitialMirrorDaemonName(index); got != want {
			t.Errorf("index %d: got %q want %q", index, got, want)
		}
	}
	seen := make(map[string]int)
	for index := range 200 {
		name := InitialMirrorDaemonName(index)
		if previous, ok := seen[name]; ok {
			t.Fatalf("name %q repeated at %d and %d", name, previous, index)
		}
		seen[name] = index
		// The pre-extraction RBD and CephFS loops produced exactly this sequence.
		legacy := fmt.Sprintf("node-%d", index+1)
		if index < 26 {
			legacy = string(rune('a' + index))
		}
		if name != legacy {
			t.Fatalf("index %d diverged from the published sequence: %q vs %q", index, name, legacy)
		}
	}
}
