//go:build all || integration

package rgw_sync_policy_translation_test

import (
	"fmt"
	"net/http"
)

// priorityReplica is what the destination shows of one object that the
// overlapping pipes select: whether it arrived with its bytes, and the
// storage class its listing gives.
type priorityReplica struct {
	Arrived error
	Class   string
	Listed  bool
}

// judgePriority judges the priority between the overlapping pipes. blue is
// selected by both pipes and is the control: it must arrive. red is selected
// only by the higher-priority pipe. The documented symptom is a resolution
// that is not by priority: red never arrives, or a replica is listed in the
// lower-priority pipe's class. Both arrived in STANDARD_IA means the symptom
// is absent. An arrived replica the listing omits is another fault.
func judgePriority(blue, red priorityReplica) (symptom bool, detail string, err error) {
	switch {
	case blue.Arrived != nil:
		return false, "", fmt.Errorf("control failed: the blue object, which both pipes select, did not replicate: %v", blue.Arrived)
	case red.Arrived != nil:
		return true, fmt.Sprintf("the red object, which only the higher-priority pipe selects, never arrived: %v", red.Arrived), nil
	case !blue.Listed || !red.Listed:
		return false, "", fmt.Errorf("another fault: the destination listing omits an arrived replica (blue listed %t, red listed %t)", blue.Listed, red.Listed)
	case blue.Class != "STANDARD_IA" || red.Class != "STANDARD_IA":
		return true, fmt.Sprintf("the replicas are listed blue=%s red=%s, want the higher-priority pipe's STANDARD_IA", blue.Class, red.Class), nil
	}
	return false, "blue and red replicated through the higher-priority pipe (STANDARD_IA)", nil
}

// judgeDenial judges the object written while the source denied the
// user-mode pipe's principal the read. held says whether the destination
// answered NoSuchKey throughout; otherwise status, code and failure are the
// first other answer. The documented symptom is the object being served
// (200) before the grant. Any other answer is another fault.
func judgeDenial(held bool, window string, status int, code string, failure error) (symptom bool, detail string, err error) {
	switch {
	case held:
		return false, "the denied object stayed NoSuchKey " + window, nil
	case failure == nil && status == http.StatusOK:
		return true, "the denied object replicated before the grant: the destination served it " + window, nil
	}
	return false, "", fmt.Errorf("another fault: the destination answered the denied object %s with status=%d code=%s error=%v", window, status, code, failure)
}
