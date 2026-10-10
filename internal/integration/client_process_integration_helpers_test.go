//go:build all || integration

package integration_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Helpers for client processes that tests start in the background and that
// run in separate CI batches.

// waitClientFile waits for a file the client process writes and reports the
// process log if it never appears.
func waitClientFile(t *testing.T, ctx context.Context, client testcontainers.Container, path, log string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		code, out, err := sessionExec(ctx, client, "cat", path)
		if err == nil && code == 0 {
			return strings.TrimSpace(out)
		}
		if time.Now().After(deadline) {
			_, output, _ := sessionExec(ctx, client, "cat", log)
			t.Fatalf("%s did not appear; client log: %s", path, output)
		}
		time.Sleep(time.Second)
	}
}

func sessionExec(ctx context.Context, ctr testcontainers.Container, args ...string) (int, string, error) {
	execCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	code, r, err := ctr.Exec(execCtx, args, tcexec.Multiplexed())
	if err != nil {
		return code, "", err
	}
	out, err := io.ReadAll(r)
	return code, string(out), err
}

// lastLine returns the final line a client script printed, after any log noise.
func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	return lines[len(lines)-1]
}

// lastJSONLine returns the last line that holds a JSON object. Native client
// libraries log to stderr, which the combined output interleaves before and
// after the result, for example while shutting down.
func lastJSONLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		if line := strings.TrimSpace(lines[index]); strings.HasPrefix(line, "{") {
			return line
		}
	}
	return ""
}
