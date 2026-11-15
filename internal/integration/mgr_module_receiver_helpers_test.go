//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// moduleReceiverDir holds a receiver's control and record files inside its
// container.
const moduleReceiverDir = "/tmp/receiver"

// moduleReceiverPrelude is the common part of a receiver script. The script
// defines open_listener and close_listener and then calls serve_receiver,
// which opens the listener while moduleReceiverDir/down is absent and closes
// it while the file exists. It publishes the current state in
// moduleReceiverDir/state and prints RECEIVER UP or RECEIVER DOWN after each
// change, so a test can make the receiver unavailable without replacing its
// container or its address. record appends one JSON line to
// moduleReceiverDir/records.jsonl, which exists, empty, before the first
// RECEIVER UP, so a missing file is a failed read rather than an empty
// history.
const moduleReceiverPrelude = `import json, os, socket, sys, threading, time
BASE = '/tmp/receiver'
os.makedirs(BASE, exist_ok=True)
open(BASE + '/records.jsonl', 'a').close()
_lock = threading.Lock()
def record(entry):
    entry['time'] = time.time()
    with _lock:
        with open(BASE + '/records.jsonl', 'a') as f:
            f.write(json.dumps(entry) + '\n')
def serve_receiver():
    state = None
    while True:
        want = 'down' if os.path.exists(BASE + '/down') else 'up'
        if want != state:
            if want == 'down':
                close_listener()
            else:
                open_listener()
            state = want
            with open(BASE + '/state.tmp', 'w') as f:
                f.write(state)
            os.replace(BASE + '/state.tmp', BASE + '/state')
            print('RECEIVER ' + state.upper(), flush=True)
        time.sleep(0.1)
`

// moduleReceiver is a container the test owns on the cluster's public
// network, which plays the receiver of an MGR module, or an independent
// client of one, for the SUT. It has no Ceph configuration or keyring.
type moduleReceiver struct {
	testcontainers.Container
	// Alias resolves to the container on the cluster's public network.
	Alias string
	// history holds the record lines of the longest earlier read, which
	// every later read must extend.
	history []string
}

// runModuleReceiver starts a container from the client role image on the
// cluster's public network with alias. With a script it runs
// moduleReceiverPrelude and script under Python with args and waits for the
// receiver to listen; without one it idles for Exec. The container is
// terminated in cleanup, after every cleanup registered later, so the MGR
// module that sends to it is restored first.
func runModuleReceiver(t *testing.T, ctx context.Context, cluster *ceph.Container, alias, script string, args ...string) *moduleReceiver {
	t.Helper()
	facts := harness.StaticFacts(cluster)
	opts := []testcontainers.ContainerCustomizer{network.WithNetworkName([]string{alias}, facts.NetworkName)}
	if script == "" {
		opts = append(opts, ceph.WithIdleEntrypoint())
	} else {
		opts = append(opts,
			testcontainers.WithEntrypoint(append([]string{"python3", "-u", "-I", "-c", moduleReceiverPrelude + script}, args...)...),
			testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
				enabled := true
				host.Init = &enabled
			}),
			testcontainers.WithWaitStrategy(wait.ForLog("RECEIVER UP").WithStartupTimeout(time.Minute)))
	}
	ctr, err := testcontainers.Run(ctx, harness.NativeClientImage(t, facts.Images.Client), opts...)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("receiver %s: %v", alias, err)
	}
	return &moduleReceiver{Container: ctr, Alias: alias}
}

// SetAvailable opens or closes the receiver's listener and waits until the
// receiver reports the new state.
func (r *moduleReceiver) SetAvailable(t *testing.T, ctx context.Context, available bool) {
	t.Helper()
	command, want := "touch "+moduleReceiverDir+"/down", "down"
	if available {
		command, want = "rm -f "+moduleReceiverDir+"/down", "up"
	}
	harness.ExecCommand(t, ctx, r, "sh", "-c", command)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if harness.ExecOutput(t, ctx, r, "cat", moduleReceiverDir+"/state") == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("receiver %s did not become %s", r.Alias, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// moduleReceiverRecords returns the receiver's records in arrival order,
// decoded into T. A failed read and a history that does not extend the one an
// earlier read returned fail the test, so an absence or quiet assertion never
// sees a read failure as an empty history and an index taken from an earlier
// read stays valid.
func moduleReceiverRecords[T any](t *testing.T, ctx context.Context, r *moduleReceiver) []T {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	code, reader, err := r.Exec(execCtx, []string{"cat", moduleReceiverDir + "/records.jsonl"}, tcexec.Multiplexed())
	var out []byte
	if err == nil {
		out, err = io.ReadAll(reader)
	}
	lines, err := receiverHistory(code, out, err, r.history)
	if err != nil {
		t.Fatalf("receiver %s: %v", r.Alias, err)
	}
	r.history = lines
	records := make([]T, 0, len(lines))
	for _, line := range lines {
		var record T
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("receiver %s record %q: %v", r.Alias, line, err)
		}
		records = append(records, record)
	}
	return records
}

// receiverHistory returns the complete record lines of one read of a
// receiver's records file, which exited with code and printed out, or why the
// read is not a history. A trailing line without its newline is a record
// still being written and is left for a later read. The lines must extend
// earlier, the lines of the longest earlier read: the file is append-only, so
// a shorter or different history is a failed read, not fewer records.
func receiverHistory(code int, out []byte, err error, earlier []string) ([]string, error) {
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("read records: cat exited %d: %s", code, bytes.TrimSpace(out))
	}
	text := string(out)
	text = text[:strings.LastIndexByte(text, '\n')+1]
	lines := strings.SplitAfter(text, "\n")
	lines = lines[:len(lines)-1]
	for i := range lines {
		lines[i] = strings.TrimSuffix(lines[i], "\n")
	}
	if len(lines) < len(earlier) {
		return nil, fmt.Errorf("history shrank from %d records to %d", len(earlier), len(lines))
	}
	for i, line := range earlier {
		if lines[i] != line {
			return nil, fmt.Errorf("record %d changed from %q to %q", i, line, lines[i])
		}
	}
	return lines, nil
}
