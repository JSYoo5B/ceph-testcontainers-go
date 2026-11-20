//go:build all || integration

package rbd_mirror_resume_checkpoint_test

import (
	"bytes"
	"fmt"
)

// checkpointObjectSize is the object size of the mirrored image. The
// checkpoint, last_copied_object_number, counts objects of this size.
const checkpointObjectSize = 256 << 10

// objectClasses classifies every object of data as "new" when it holds the
// snapshot's bytes (after), "old" when it holds the previous snapshot's bytes
// (before), and "other" otherwise. All three must have the same whole number
// of objects.
func objectClasses(data, before, after []byte) ([]string, error) {
	if len(before) != len(after) || len(after)%checkpointObjectSize != 0 {
		return nil, fmt.Errorf("payloads of %d and %d bytes are not the same whole number of %d-byte objects", len(before), len(after), checkpointObjectSize)
	}
	if len(data) != len(after) {
		return nil, fmt.Errorf("read %d bytes, want %d", len(data), len(after))
	}
	classes := make([]string, len(after)/checkpointObjectSize)
	for index := range classes {
		start, end := index*checkpointObjectSize, (index+1)*checkpointObjectSize
		switch {
		case bytes.Equal(data[start:end], after[start:end]):
			classes[index] = "new"
		case bytes.Equal(data[start:end], before[start:end]):
			classes[index] = "old"
		default:
			classes[index] = "other"
		}
	}
	return classes, nil
}

// classSummary names the objects of every class but "new", which it counts.
func classSummary(classes []string) string {
	var fresh int
	var old, other []int
	for index, class := range classes {
		switch class {
		case "new":
			fresh++
		case "old":
			old = append(old, index)
		default:
			other = append(other, index)
		}
	}
	return fmt.Sprintf("new=%d old=%v other=%v", fresh, old, other)
}

// checkKillWindow is the when step's prerequisite, judged on the destination
// image read while the receiver is down. The persisted checkpoint must be
// positive and inside the image, every object below it must hold the new
// bytes (the checkpoint acknowledges them), and the object at the checkpoint
// must still hold the old bytes, so that its copy was in flight or not yet
// started at the kill. Only then does the restart decide whether the copy
// resumes at the checkpoint object or after it. A nil error means the
// prerequisite holds.
func checkKillWindow(checkpoint uint64, atKill, before, after []byte) error {
	classes, err := objectClasses(atKill, before, after)
	if err != nil {
		return err
	}
	if checkpoint == 0 {
		return fmt.Errorf("checkpoint 0 at the kill: a restart copies from object 0; %s", classSummary(classes))
	}
	if checkpoint >= uint64(len(classes)) {
		return fmt.Errorf("checkpoint %d at the kill covers all %d objects; %s", checkpoint, len(classes), classSummary(classes))
	}
	for index := range checkpoint {
		if classes[index] != "new" {
			return fmt.Errorf("object %d below checkpoint %d is %s at the kill; %s", index, checkpoint, classes[index], classSummary(classes))
		}
	}
	if classes[checkpoint] != "old" {
		return fmt.Errorf("object %d at checkpoint %d is already %s at the kill: the interruption missed its copy; %s", checkpoint, checkpoint, classes[checkpoint], classSummary(classes))
	}
	return nil
}

// judgeResumedCopy recognizes a skipped checkpoint object in a completed
// copy. Every other object must hold the new bytes.
func judgeResumedCopy(checkpoint uint64, final, before, after []byte) ([]string, error) {
	classes, err := objectClasses(final, before, after)
	if err != nil {
		return nil, err
	}
	if checkpoint >= uint64(len(classes)) {
		return nil, fmt.Errorf("checkpoint %d outside the %d objects", checkpoint, len(classes))
	}
	index := int(checkpoint)
	for object, class := range classes {
		if class == "new" || (object == index && class == "old") {
			continue
		}
		return nil, fmt.Errorf("another fault: object %d is %s after the resumed copy at checkpoint %d; %s", object, class, checkpoint, classSummary(classes))
	}
	if classes[index] == "old" {
		return []string{fmt.Sprintf("object %d, at checkpoint last_copied_object_number=%d, kept the previous snapshot's bytes after the resumed copy completed; %s", checkpoint, checkpoint, classSummary(classes))}, nil
	}
	return nil, nil
}
