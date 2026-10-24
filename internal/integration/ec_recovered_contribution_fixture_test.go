//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"testing"
)

// Docker-free controls of the exact-K plan and of the readback comparison.
// They are synthetic: they check the witness design, not what a cluster does.

func TestECExactKPlan(t *testing.T) {
	good, err := planECExactK([]int{7, 3, 5}, 7, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if good.SecondOSD != 5 || good.SecondIndex != 2 || !slices.Equal(good.Remaining, []int{0, 1}) || !slices.Equal(good.WantActing, []int{7, 3, ecFragmentNone}) {
		t.Fatalf("plan: %+v", good)
	}
	// A primary that is not at position 0: indices are positions, not IDs.
	shifted, err := planECExactK([]int{3, 7, 5}, 7, 2, 2)
	if err != nil || shifted.SecondOSD != 3 || shifted.SecondIndex != 0 || !slices.Equal(shifted.Remaining, []int{1, 2}) || !slices.Equal(shifted.WantActing, []int{ecFragmentNone, 7, 5}) {
		t.Fatalf("shifted plan: %+v, %v", shifted, err)
	}
	for name, run := range map[string]func() (ecExactKPlan, error){
		"hole in the vector":      func() (ecExactKPlan, error) { return planECExactK([]int{7, ecFragmentNone, 5}, 7, 2, 2) },
		"repeated OSD":            func() (ecExactKPlan, error) { return planECExactK([]int{7, 3, 3}, 7, 1, 2) },
		"primary absent":          func() (ecExactKPlan, error) { return planECExactK([]int{7, 3, 5}, 9, 1, 2) },
		"restored is the primary": func() (ecExactKPlan, error) { return planECExactK([]int{7, 3, 5}, 7, 0, 2) },
		"restored out of range":   func() (ecExactKPlan, error) { return planECExactK([]int{7, 3, 5}, 7, 3, 2) },
		"negative restored":       func() (ecExactKPlan, error) { return planECExactK([]int{7, 3, 5}, 7, -1, 2) },
		"wrong width":             func() (ecExactKPlan, error) { return planECExactK([]int{7, 3, 5, 6}, 7, 1, 2) },
		"zero k":                  func() (ecExactKPlan, error) { return planECExactK([]int{7, 3, 5}, 7, 1, 0) },
	} {
		if plan, err := run(); err == nil {
			t.Errorf("%s: accepted %+v", name, plan)
		}
	}
}

func TestECRecoveredReadCheck(t *testing.T) {
	want := ecEligibleReceipt{Phase: "overwrite", Writer: 10, Reader: 11, Snap: 3, HeadSHA256: "aa", HeadVersion: 9, SnapSHA256: "bb", SnapVersion: 5}
	good := ecEligibleReceipt{Phase: "read", Writer: 20, Reader: 21, Snap: 3, HeadSHA256: "aa", HeadVersion: 9, SnapSHA256: "bb", SnapVersion: 5}
	earlier := map[uint64]bool{10: true, 11: true, 12: true}
	if err := checkECRecoveredRead(want, good, earlier); err != nil {
		t.Fatal(err)
	}
	mutate := func(edit func(*ecEligibleReceipt)) ecEligibleReceipt {
		copy := good
		edit(&copy)
		return copy
	}
	for name, got := range map[string]ecEligibleReceipt{
		"not a read":          mutate(func(r *ecEligibleReceipt) { r.Phase = "verify" }),
		"reused reader":       mutate(func(r *ecEligibleReceipt) { r.Reader = 11 }),
		"reused writer":       mutate(func(r *ecEligibleReceipt) { r.Writer = 12 }),
		"same instance twice": mutate(func(r *ecEligibleReceipt) { r.Reader = r.Writer }),
		"missing instance":    mutate(func(r *ecEligibleReceipt) { r.Reader = 0 }),
		"other snapshot":      mutate(func(r *ecEligibleReceipt) { r.Snap = 4 }),
		"other head bytes":    mutate(func(r *ecEligibleReceipt) { r.HeadSHA256 = "cc" }),
		"stale head version":  mutate(func(r *ecEligibleReceipt) { r.HeadVersion = 8 }),
		"other clone bytes":   mutate(func(r *ecEligibleReceipt) { r.SnapSHA256 = "cc" }),
		"other clone version": mutate(func(r *ecEligibleReceipt) { r.SnapVersion = 6 }),
	} {
		if err := checkECRecoveredRead(want, got, earlier); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// An acknowledged receipt that cannot tell the head from the snapshot
	// proves nothing, even when the readback repeats it exactly.
	for name, ack := range map[string]ecEligibleReceipt{
		"empty receipt":     {},
		"head equals clone": {Phase: "overwrite", Snap: 3, HeadSHA256: "aa", HeadVersion: 9, SnapSHA256: "aa", SnapVersion: 5},
		"no snapshot hash":  {Phase: "overwrite", Snap: 3, HeadSHA256: "aa", HeadVersion: 9, SnapVersion: 5},
	} {
		got := good
		got.HeadSHA256, got.HeadVersion, got.SnapSHA256, got.SnapVersion, got.Snap = ack.HeadSHA256, ack.HeadVersion, ack.SnapSHA256, ack.SnapVersion, ack.Snap
		if err := checkECRecoveredRead(ack, got, earlier); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestECShardCoverage(t *testing.T) {
	// 20000 bytes, stripe width 8192 (4096-byte chunks): two full stripes and a
	// partial one of 3616 bytes, so index 1 stores 2*4096 bytes and no more.
	for index, want := range map[int]uint64{0: 8192 + 3616, 1: 8192, 2: 8192 + 3616} {
		if got, err := ecShardBytes(20000, 8192, 2, index); err != nil || got != want {
			t.Errorf("index %d: %d bytes, %v; want %d", index, got, err, want)
		}
	}
	if err := requireECShardCoverage([]uint64{20000, 20000}, 8192, 2, []int{0, 1}); err != nil {
		t.Error(err)
	}
	if err := requireECShardCoverage([]uint64{20000}, 8192, 2, []int{1, 2}); err != nil {
		t.Error(err)
	}
	// A large chunk: 65536-byte stripes put all 20000 bytes on data index 0, so
	// data index 1 holds nothing and cannot show a contribution.
	if got, err := ecShardBytes(20000, 65536, 2, 1); err != nil || got != 0 {
		t.Errorf("large-chunk index 1: %d, %v", got, err)
	}
	for name, run := range map[string]func() error{
		"large chunk, data index":     func() error { return requireECShardCoverage([]uint64{20000}, 65536, 2, []int{0, 1}) },
		"object of exactly one chunk": func() error { return requireECShardCoverage([]uint64{4096}, 8192, 2, []int{0, 1}) },
		"empty object":                func() error { return requireECShardCoverage([]uint64{0}, 8192, 2, []int{0}) },
		"one small size among two":    func() error { return requireECShardCoverage([]uint64{20000, 100}, 8192, 2, []int{1}) },
		"zero stripe width":           func() error { return requireECShardCoverage([]uint64{20000}, 0, 2, []int{0}) },
		"stripe not a multiple":       func() error { return requireECShardCoverage([]uint64{20000}, 8193, 2, []int{0}) },
		"no indices":                  func() error { return requireECShardCoverage([]uint64{20000}, 8192, 2, nil) },
		"no sizes":                    func() error { return requireECShardCoverage(nil, 8192, 2, []int{0}) },
	} {
		if run() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The parity index keeps index 0's size even when index 1 is empty.
	if err := requireECShardCoverage([]uint64{20000}, 65536, 2, []int{0, 2}); err != nil {
		t.Error(err)
	}
}
