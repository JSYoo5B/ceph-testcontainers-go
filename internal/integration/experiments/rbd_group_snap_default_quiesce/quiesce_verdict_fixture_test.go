//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rbd_group_snap_default_quiesce_test

import (
	"os/exec"
	"strings"
	"testing"
)

func TestQuiesceVerdictRequiresCallbacks(t *testing.T) {
	const observations = `
for image, group in [(0, 0), (1, 0), (0, 1)]:
    counts = {"image-required": image, "group-required": group, "image-skip": 0, "group-skip": 0}
    try:
        result = quiesce_verdict(counts)
    except ValueError as error:
        if error.args[0][0] != "quiesce control counts":
            raise
    else:
        raise RuntimeError(("accepted missing required callbacks", counts, result))
counts = {"image-required": 1, "group-required": 1, "image-skip": 0, "group-skip": 0}
print(quiesce_verdict(counts))
counts["group-skip"] = 1
print(quiesce_verdict(counts))
`
	for _, level := range []string{"", "-O"} {
		var args []string
		if level != "" {
			args = append(args, level)
		}
		args = append(args, "-c", quiesceVerdict+observations)
		out, err := exec.CommandContext(t.Context(), "python3", args...).CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "NOT_REPRODUCED|[]\nREPRODUCED|[group-skip notified watcher]" {
			t.Fatalf("python3 %q: %v\n%s", level, err, out)
		}
	}
}
