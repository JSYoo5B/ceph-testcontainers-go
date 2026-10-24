//go:build all || integration

package integration_test

import (
	"fmt"
	"slices"
)

// ecExactKPlan is the second outage of the recovered-contribution witness:
// after the first fragment holder has recovered, a different non-primary
// holder is stopped so that exactly K fragment indices remain.
type ecExactKPlan struct {
	SecondOSD   int
	SecondIndex int
	// Remaining are the fragment indices that stay reachable: the primary's
	// and the restored holder's, and nothing else.
	Remaining []int
	// WantActing is the original acting vector with the stopped index replaced
	// by the hole marker. It is neither compacted nor reordered.
	WantActing []int
}

// planECExactK chooses the second holder from the complete original acting
// vector of a K+1 wide pool. It refuses a vector that already has a hole or a
// repeated OSD, a primary that is not at its own position, a restored index
// that is the primary's or out of range, and any vector that does not leave
// exactly K indices once the second holder is gone. Fragment indices are
// positions in the vector, never OSD IDs.
func planECExactK(acting []int, primary, restoredIndex, k int) (ecExactKPlan, error) {
	if k < 1 || len(acting) != k+1 {
		return ecExactKPlan{}, fmt.Errorf("acting vector %v is not K+1 wide for k=%d", acting, k)
	}
	seen := map[int]bool{}
	for index, osd := range acting {
		if osd == ecFragmentNone || osd < 0 {
			return ecExactKPlan{}, fmt.Errorf("acting vector %v has no OSD at fragment %d", acting, index)
		}
		if seen[osd] {
			return ecExactKPlan{}, fmt.Errorf("acting vector %v repeats osd.%d", acting, osd)
		}
		seen[osd] = true
	}
	primaryIndex := slices.Index(acting, primary)
	if primaryIndex < 0 {
		return ecExactKPlan{}, fmt.Errorf("primary osd.%d is not in acting vector %v", primary, acting)
	}
	if restoredIndex < 0 || restoredIndex >= len(acting) || restoredIndex == primaryIndex {
		return ecExactKPlan{}, fmt.Errorf("restored fragment %d is not a non-primary position of %v", restoredIndex, acting)
	}
	plan := ecExactKPlan{SecondIndex: -1}
	for index, osd := range acting {
		if index != primaryIndex && index != restoredIndex {
			plan.SecondOSD, plan.SecondIndex = osd, index
			break
		}
	}
	if plan.SecondIndex < 0 {
		return ecExactKPlan{}, fmt.Errorf("no second non-primary holder in %v", acting)
	}
	plan.WantActing = slices.Clone(acting)
	plan.WantActing[plan.SecondIndex] = ecFragmentNone
	for index := range acting {
		if index != plan.SecondIndex {
			plan.Remaining = append(plan.Remaining, index)
		}
	}
	if len(plan.Remaining) != k || !slices.Contains(plan.Remaining, restoredIndex) || !slices.Contains(plan.Remaining, primaryIndex) {
		return ecExactKPlan{}, fmt.Errorf("remaining indices %v are not exactly K=%d including the primary's and the restored one", plan.Remaining, k)
	}
	return plan, nil
}

// checkECRecoveredRead requires the exact-K readback to repeat the
// acknowledged receipt (snapshot ID, bytes and versions of the head and the
// snapshot) through client instances that no earlier phase used.
func checkECRecoveredRead(want, got ecEligibleReceipt, earlier map[uint64]bool) error {
	if got.Phase != "read" {
		return fmt.Errorf("receipt is of phase %q, not a read", got.Phase)
	}
	if got.Writer == 0 || got.Reader == 0 || got.Writer == got.Reader {
		return fmt.Errorf("client instances %d and %d are not two distinct clients", got.Writer, got.Reader)
	}
	if earlier[got.Writer] || earlier[got.Reader] {
		return fmt.Errorf("the readback reused client instance %d or %d of an earlier phase", got.Writer, got.Reader)
	}
	if got.Snap != want.Snap || got.HeadSHA256 != want.HeadSHA256 || got.HeadVersion != want.HeadVersion ||
		got.SnapSHA256 != want.SnapSHA256 || got.SnapVersion != want.SnapVersion {
		return fmt.Errorf("readback %+v differs from the acknowledged receipt %+v", got, want)
	}
	if want.HeadSHA256 == "" || want.SnapSHA256 == "" || want.HeadVersion == 0 || want.SnapVersion == 0 || want.HeadSHA256 == want.SnapSHA256 {
		return fmt.Errorf("the acknowledged receipt %+v does not tell the head from the snapshot", want)
	}
	return nil
}
