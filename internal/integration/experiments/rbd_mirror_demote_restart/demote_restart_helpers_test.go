//go:build all || integration

package rbd_mirror_demote_restart_test

import (
	"bytes"
	"fmt"
)

// rbdDemoteRestartObject is the object size of the mirrored images.
const rbdDemoteRestartObject = 256 << 10

// judgeDemotionResume judges whether the restarted receiver resumed the
// demotion copy it was killed in. A copy that completes is the absence of
// the orphan classification. A copy that stays incomplete until the bound,
// with the same snapshot still copying the same primary snapshot, is the
// documented symptom. A probe that saw the copy disappear or change its
// primary snapshot is another fault: no verdict.
func judgeDemotionResume(completed, timedOut bool, probe error, snapshot uint64, last string) (symptom bool, detail string, err error) {
	switch {
	case completed:
		return false, fmt.Sprintf("demotion copy %d completed after the restart", snapshot), nil
	case timedOut:
		return true, fmt.Sprintf("the restarted receiver did not complete demotion copy %d (%s)", snapshot, last), nil
	default:
		return false, "", fmt.Errorf("another fault: demotion copy %d: %v", snapshot, probe)
	}
}

// judgeResumedBytes judges what the destination exports after the resumed
// demotion copy reported complete. The source's new bytes mean the symptom
// is absent. The documented symptom is a complete copy in which whole
// objects keep the previous snapshot's bytes; progress is the
// last_copied_object_number recorded at the kill. An export of another size
// or an object that holds neither the old nor the new bytes is another
// fault: no verdict.
func judgeResumedBytes(data, before, after []byte, progress uint64) (symptom bool, detail string, err error) {
	if bytes.Equal(data, after) {
		return false, "the destination exports the demotion snapshot's bytes", nil
	}
	if len(data) != len(after) || len(before) != len(after) {
		return false, "", fmt.Errorf("another fault: the destination exports %d bytes, want %d", len(data), len(after))
	}
	var fresh, stale, other []int
	for index := 0; index*rbdDemoteRestartObject < len(after); index++ {
		low, high := index*rbdDemoteRestartObject, min((index+1)*rbdDemoteRestartObject, len(after))
		switch chunk := data[low:high]; {
		case bytes.Equal(chunk, after[low:high]):
			fresh = append(fresh, index)
		case bytes.Equal(chunk, before[low:high]):
			stale = append(stale, index)
		default:
			other = append(other, index)
		}
	}
	if len(other) != 0 {
		return false, "", fmt.Errorf("another fault: objects new=%d old=%v neither=%v", len(fresh), stale, other)
	}
	return true, fmt.Sprintf("the resumed copy reports complete, but objects %v keep the previous snapshot's bytes (new=%d, last_copied_object_number at the kill %d)", stale, len(fresh), progress), nil
}
