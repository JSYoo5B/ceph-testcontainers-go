package fault

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	enableCommand = "tell osd.0 config set bluestore_debug_inject_read_err true"
	injectCommand = "tell osd.0 injectdataerr fixture victim"
	mapCommand    = "osd map fixture victim --format json"
)

func injectFixture() *clustertest.CannedCommandContainer {
	return &clustertest.CannedCommandContainer{Output: map[string]string{
		"osd pool ls detail --format json": `[{"pool_id":7,"pool_name":"fixture","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":3}]`,
		mapCommand:                         `{"epoch":14,"pool":"fixture","pool_id":7,"objname":"victim","pgid":"7.0","up":[1,0],"up_primary":1,"acting":[1,0],"acting_primary":1}`,
	}}
}

func injectCluster(ctr *clustertest.CannedCommandContainer, config testhelper.TestConfig) *ceph.Container {
	config.StartupTimeout = time.Second
	config.OSDs = max(config.OSDs, 2)
	return testhelper.TestCluster(ctr, config)
}

func tellCommands(ctr *clustertest.CannedCommandContainer) []string {
	var result []string
	for _, args := range ctr.Calls {
		if slices.Contains(args, "tell") || slices.Contains(args, "deep-scrub") || slices.Contains(args, "repair") {
			result = append(result, strings.Join(args, " "))
		}
	}
	return result
}

func requireNoRegistryEntry(t *testing.T, c *ceph.Container) {
	t.Helper()
	for _, kind := range []faulthook.Kind{faulthook.NetworkDelay, faulthook.NetworkInterruption, faulthook.ContainerPause, faulthook.Blocklist, faulthook.FullRatios} {
		if faults := testhelper.TestFaults(c, kind); len(faults) != 0 {
			t.Fatalf("a one-shot injection registered a %s fault: %v", kind, faults)
		}
	}
}

func TestInjectObjectDataErrorValidatesTargetBeforeNativeCommands(t *testing.T) {
	for name, call := range map[string]func(*ceph.Container) error{
		"pool":        func(c *ceph.Container) error { return InjectObjectDataError(t.Context(), c, "--pool", "victim", 0) },
		"object dash": func(c *ceph.Container) error { return InjectObjectDataError(t.Context(), c, "fixture", "-victim", 0) },
		"object NUL": func(c *ceph.Container) error {
			return InjectObjectDataError(t.Context(), c, "fixture", "vic\x00tim", 0)
		},
		"object slash": func(c *ceph.Container) error { return InjectObjectDataError(t.Context(), c, "fixture", "ns/victim", 0) },
		"unowned OSD":  func(c *ceph.Container) error { return InjectObjectDataError(t.Context(), c, "fixture", "victim", 9) },
	} {
		ctr := injectFixture()
		if err := call(injectCluster(ctr, testhelper.TestConfig{})); err == nil || len(ctr.Calls) != 0 {
			t.Fatalf("%s: invalid target reached native CLI: %v %v", name, err, ctr.Calls)
		}
	}
	ctr := injectFixture()
	ctr.Output["osd pool ls detail --format json"] = strings.Replace(ctr.Output["osd pool ls detail --format json"], `"type":1`, `"type":3`, 1)
	if err := InjectObjectDataError(t.Context(), injectCluster(ctr, testhelper.TestConfig{}), "fixture", "victim", 0); err == nil || len(tellCommands(ctr)) != 0 {
		t.Fatal("EC pool accepted a replica data error")
	}
	ctr = injectFixture()
	ctr.Output[mapCommand] = strings.Replace(ctr.Output[mapCommand], `"acting":[1,0]`, `"acting":[1]`, 1)
	if err := InjectObjectDataError(t.Context(), injectCluster(ctr, testhelper.TestConfig{}), "fixture", "victim", 0); err == nil || len(tellCommands(ctr)) != 0 {
		t.Fatal("OSD outside the acting set received an injection")
	}
	ctr = injectFixture()
	ctr.Output[mapCommand] = strings.Replace(ctr.Output[mapCommand], `"pool_id":7`, `"pool_id":8`, 1)
	if err := InjectObjectDataError(t.Context(), injectCluster(ctr, testhelper.TestConfig{}), "fixture", "victim", 0); err == nil || len(tellCommands(ctr)) != 0 {
		t.Fatal("mapping of another pool was accepted")
	}
}

func TestInjectObjectDataErrorEnablesOnlyTheTargetDaemon(t *testing.T) {
	ctr := injectFixture()
	c := injectCluster(ctr, testhelper.TestConfig{})
	if err := InjectObjectDataError(t.Context(), c, "fixture", "victim", 0); err != nil {
		t.Fatal(err)
	}
	if want := []string{enableCommand, injectCommand}; !slices.Equal(tellCommands(ctr), want) {
		t.Fatalf("unexpected injection commands: %v", tellCommands(ctr))
	}
	requireNoRegistryEntry(t, c)
}

func TestInjectObjectDataErrorNeedsBlueStore(t *testing.T) {
	ctr := injectFixture()
	if err := InjectObjectDataError(t.Context(), injectCluster(ctr, testhelper.TestConfig{MemStore: true}), "fixture", "victim", 0); err == nil || len(ctr.Calls) != 0 {
		t.Fatalf("BlueStore read-error injection ran on MemStore: %v %v", err, ctr.Calls)
	}
}

func TestInjectObjectDataErrorRefusesSharedAndClosedClusters(t *testing.T) {
	ctr := injectFixture()
	err := InjectObjectDataError(t.Context(), injectCluster(ctr, testhelper.TestConfig{Shared: true}), "fixture", "victim", 0)
	var op *ceph.OperationError
	if !errors.Is(err, ceph.ErrSharedCluster) || !errors.As(err, &op) || op.Op != "fault.InjectObjectDataError" || len(ctr.Calls) != 0 {
		t.Fatalf("shared cluster bypassed admission: %v %v", err, ctr.Calls)
	}
	ctr = injectFixture()
	_, terminated := faulthook.Terminated()
	if err := InjectObjectDataError(t.Context(), injectCluster(ctr, testhelper.TestConfig{Closed: true}), "fixture", "victim", 0); !errors.Is(err, terminated) || len(ctr.Calls) != 0 {
		t.Fatalf("terminated cluster ran a native command: %v %v (want %v)", err, ctr.Calls, terminated)
	}
}

// The injection is serialized with other topology operations: a caller whose
// deadline ends while the topology lock is busy runs no native command.
func TestInjectObjectDataErrorWaitsForTopologyLock(t *testing.T) {
	ctr := injectFixture()
	c := injectCluster(ctr, testhelper.TestConfig{})
	clustertest.AssertContextLockQueued(t, testhelper.TestState(c).Mutex, func(ctx context.Context) error {
		return InjectObjectDataError(ctx, c, "fixture", "victim", 0)
	})
	if len(ctr.Calls) != 0 {
		t.Fatalf("a queued injection reached the native CLI: %v", ctr.Calls)
	}
}

// A failure keeps the already-issued prefix: nothing disarms the debug setting,
// repairs the replica, retries the command or registers a recovery.
func TestInjectObjectDataErrorPartialFailureIsNotRolledBack(t *testing.T) {
	ctr := injectFixture()
	ctr.Fail = enableCommand
	c := injectCluster(ctr, testhelper.TestConfig{})
	if err := InjectObjectDataError(t.Context(), c, "fixture", "victim", 0); err == nil || !strings.Contains(err.Error(), "enable data error injection on osd.0") {
		t.Fatalf("enable failure was not reported: %v", err)
	}
	if !slices.Equal(tellCommands(ctr), []string{enableCommand}) {
		t.Fatalf("a failed enable continued or rolled back: %v", tellCommands(ctr))
	}
	requireNoRegistryEntry(t, c)

	ctr = injectFixture()
	ctr.Fail = injectCommand
	c = injectCluster(ctr, testhelper.TestConfig{})
	if err := InjectObjectDataError(t.Context(), c, "fixture", "victim", 0); err == nil || !strings.Contains(err.Error(), "inject data error on osd.0") {
		t.Fatalf("injection failure was not reported: %v", err)
	}
	if want := []string{enableCommand, injectCommand}; !slices.Equal(tellCommands(ctr), want) {
		t.Fatalf("a failed injection was retried, disarmed or repaired: %v", tellCommands(ctr))
	}
	requireNoRegistryEntry(t, c)
}

// The pool identity is rechecked after the debug switch is enabled; a pool
// replaced meanwhile receives no injection, and the enabled switch stays.
func TestInjectObjectDataErrorRechecksPoolIdentityBeforeInjecting(t *testing.T) {
	ctr := &poolReplacingCLI{CannedCommandContainer: injectFixture()}
	c := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: 2})
	if err := InjectObjectDataError(t.Context(), c, "fixture", "victim", 0); err == nil || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("a replaced pool was not refused: %v", err)
	}
	if want := []string{enableCommand}; !slices.Equal(tellCommands(ctr.CannedCommandContainer), want) {
		t.Fatalf("a replaced pool received an injection: %v", tellCommands(ctr.CannedCommandContainer))
	}
	requireNoRegistryEntry(t, c)
}

// poolReplacingCLI replaces the pool right after the debug switch is enabled.
type poolReplacingCLI struct {
	*clustertest.CannedCommandContainer
}

func (ctr *poolReplacingCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, out, err := ctr.CannedCommandContainer.Exec(ctx, args, opts...)
	if last := ctr.Calls[len(ctr.Calls)-1]; strings.Join(last, " ") == enableCommand {
		ctr.Output["osd pool ls detail --format json"] = strings.Replace(ctr.Output["osd pool ls detail --format json"], `"pool_id":7`, `"pool_id":9`, 1)
	}
	return code, out, err
}
