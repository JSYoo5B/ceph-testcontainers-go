//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_mirror_resume_checkpoint_test

import (
	"bytes"
	"strings"
	"testing"
)

// checkpointPayloads returns the old and new bytes of an image of n objects;
// every object of each differs from the other's.
func checkpointPayloads(n int) (before, after []byte) {
	return bytes.Repeat([]byte{0xb0}, n*checkpointObjectSize), bytes.Repeat([]byte{0xa0}, n*checkpointObjectSize)
}

// checkpointImage returns an image whose object i is old, new or other as
// classes[i] says.
func checkpointImage(classes string) []byte {
	before, after := checkpointPayloads(len(classes))
	data := make([]byte, 0, len(after))
	for index, class := range classes {
		start, end := index*checkpointObjectSize, (index+1)*checkpointObjectSize
		switch class {
		case 'n':
			data = append(data, after[start:end]...)
		case 'o':
			data = append(data, before[start:end]...)
		default:
			data = append(data, bytes.Repeat([]byte{0x11}, checkpointObjectSize)...)
		}
	}
	return data
}

// These cases feed synthetic images to the prerequisite and the judgement.
// They are not native evidence of what a receiver copies.
func TestCheckKillWindowRequiresTheCheckpointObjectInFlight(t *testing.T) {
	before, after := checkpointPayloads(6)
	for _, tc := range []struct {
		name       string
		checkpoint uint64
		atKill     string
		want       string
	}{
		{name: "checkpoint object old, later objects mixed", checkpoint: 2, atKill: "nnonon"},
		{name: "checkpoint object old, nothing later copied", checkpoint: 3, atKill: "nnnooo"},
		{name: "zero checkpoint", checkpoint: 0, atKill: "oonooo", want: "checkpoint 0 at the kill"},
		{name: "checkpoint past the last object", checkpoint: 6, atKill: "nnnnnn", want: "covers all 6 objects"},
		{name: "checkpoint object already copied", checkpoint: 2, atKill: "nnnooo", want: "missed its copy"},
		{name: "checkpoint object holds other bytes", checkpoint: 2, atKill: "nnxooo", want: "already other"},
		{name: "object below the checkpoint not copied", checkpoint: 3, atKill: "nonooo", want: "object 1 below checkpoint 3 is old"},
		{name: "short read", checkpoint: 2, atKill: "nno", want: "read 786432 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkKillWindow(tc.checkpoint, checkpointImage(tc.atKill), before, after)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("prerequisite refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func TestJudgeResumedCopyReportsOnlyTheCheckpointObject(t *testing.T) {
	before, after := checkpointPayloads(6)
	for _, tc := range []struct {
		name       string
		checkpoint uint64
		final      string
		symptom    string
		err        string
	}{
		{name: "checkpoint object skipped", checkpoint: 2, final: "nnonnn", symptom: "object 2, at checkpoint last_copied_object_number=2"},
		{name: "checkpoint object and another skipped", checkpoint: 2, final: "nnonon", symptom: "old=[2 4]"},
		{name: "exact bytes", checkpoint: 2, final: "nnnnnn"},
		{name: "another object old", checkpoint: 2, final: "nnnnon", err: "another fault"},
		{name: "other bytes", checkpoint: 2, final: "nnnxnn", err: "another fault"},
		{name: "checkpoint outside the image", checkpoint: 6, final: "nnnnnn", err: "outside the 6 objects"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			symptoms, err := judgeResumedCopy(tc.checkpoint, checkpointImage(tc.final), before, after)
			switch {
			case tc.err != "":
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %v %q, want an error containing %q", err, symptoms, tc.err)
				}
			case err != nil:
				t.Fatalf("judgement failed: %v", err)
			case tc.symptom == "" && len(symptoms) != 0:
				t.Fatalf("got symptoms %q, want none", symptoms)
			case tc.symptom != "" && (len(symptoms) != 1 || !strings.Contains(symptoms[0], tc.symptom)):
				t.Fatalf("got symptoms %q, want one containing %q", symptoms, tc.symptom)
			}
		})
	}
}
