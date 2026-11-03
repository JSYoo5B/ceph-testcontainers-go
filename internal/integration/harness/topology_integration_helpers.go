//go:build all || integration

package harness

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Every native process and CLI probe has a container-side watchdog as well as
// the Docker request deadline. Cancelling Exec alone does not kill a C call.
func TopologyExecOutput(t *testing.T, parent context.Context, ctr testcontainers.Container, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 50*time.Second)
	defer cancel()
	command := TopologyCommandWithTimeout(45*time.Second, args...)
	code, reader, err := ctr.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("bounded topology probe exited %d: %v: %s", code, err, output)
	}
	return output
}

func TopologyCommandWithTimeout(timeout time.Duration, args ...string) []string {
	return append([]string{"python3", "-c", topologyCommandTimeoutScript, strconv.FormatFloat(timeout.Seconds(), 'f', -1, 64)}, args...)
}

// subprocess.run kills and waits for the direct native command on timeout.
// Preserve its output and exit status without a shell or an external watchdog.
const topologyCommandTimeoutScript = `import subprocess, sys
try:
    result = subprocess.run(sys.argv[2:], timeout=float(sys.argv[1]), check=False)
except subprocess.TimeoutExpired:
    print('native command exceeded ' + sys.argv[1] + '-second watchdog', file=sys.stderr)
    sys.exit(124)
except OSError as error:
    print('native command launch failed: ' + type(error).__name__, file=sys.stderr)
    sys.exit(127)
sys.exit(result.returncode if result.returncode >= 0 else 128 - result.returncode)
`
