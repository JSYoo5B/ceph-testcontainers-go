//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
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
// moduleReceiverDir/records.jsonl.
const moduleReceiverPrelude = `import json, os, socket, sys, threading, time
BASE = '/tmp/receiver'
os.makedirs(BASE, exist_ok=True)
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

// Records returns the receiver's records in arrival order, decoded into T.
func moduleReceiverRecords[T any](t *testing.T, ctx context.Context, r *moduleReceiver) []T {
	t.Helper()
	out := harness.ExecOutput(t, ctx, r, "sh", "-c", "cat "+moduleReceiverDir+"/records.jsonl 2>/dev/null || true")
	var records []T
	for line := range strings.SplitSeq(out, "\n") {
		if line == "" {
			continue
		}
		var record T
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("receiver %s record %q: %v", r.Alias, line, err)
		}
		records = append(records, record)
	}
	return records
}
