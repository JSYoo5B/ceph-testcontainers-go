package multicluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestLimitLeavesAMarginBeforeTheDeadline(t *testing.T) {
	if limit := Limit(context.Background()); limit != 0 {
		t.Fatalf("no deadline: %d", limit)
	}
	for bound, want := range map[time.Duration][2]int{3 * time.Minute: {164, 165}, 20 * time.Second: {9, 10}, time.Second: {1, 1}} {
		ctx, cancel := context.WithTimeout(t.Context(), bound)
		limit := Limit(ctx)
		cancel()
		if limit < want[0] || limit > want[1] {
			t.Errorf("bound %s: limit %d", bound, limit)
		}
	}
}

// supervisorReply is a container whose supervisor answers every command with
// one report, or exits code without one, and records the argv it got.
type supervisorReply struct {
	testcontainers.Container
	report     []byte
	exit       int
	argv, envs []string
}

func (c *supervisorReply) Exec(_ context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if clustertest.Supervised(args) && args[2] == supervised.CancelScript {
		return 0, bytes.NewReader(nil), nil
	}
	c.argv, c.envs = args, append(c.envs, clustertest.ExecEnv(opts, "TC_SUPERVISED_DEADLINE_SIGNAL"))
	if c.report == nil {
		return c.exit, nil, nil
	}
	return 0, bytes.NewReader(c.report), nil
}

func TestExecClassifiesTheSupervisorReport(t *testing.T) {
	notFound := clustertest.QuorumEnvelope(nil, []byte("rbd: error opening image disk: (2) No such file or directory\n"), 2, false, false)
	for name, tc := range map[string]struct {
		report []byte
		exit   int
		check  func(error) bool
	}{
		"nonzero exit": {report: notFound, check: func(err error) bool {
			var failure *ceph.NativeFailure
			return errors.As(err, &failure) && failure.ExitCode == 2 && failure.Errno == "ENOENT"
		}},
		"stopped at its limit": {report: clustertest.QuorumEnvelope(nil, nil, -6, true, false), check: func(err error) bool {
			return errors.Is(err, ceph.ErrNativeCommandIncomplete)
		}},
		"too much output": {report: clustertest.QuorumEnvelope([]byte("{"), nil, 0, false, true), check: func(err error) bool {
			return errors.Is(err, ceph.ErrNativeCommandIncomplete)
		}},
		"missing program": {exit: supervised.LaunchFailedExit, check: func(err error) bool {
			return errors.Is(err, ceph.ErrImageCapability)
		}},
		"missing python": {exit: 127, check: func(err error) bool {
			return errors.Is(err, ceph.ErrImageCapability)
		}},
	} {
		ctr := &supervisorReply{report: tc.report, exit: tc.exit}
		if _, err := Exec(t.Context(), ctr, "rbd", "info", "disk"); !tc.check(err) {
			t.Errorf("%s: %v", name, err)
		}
	}
	ctr := &supervisorReply{report: clustertest.QuorumEnvelope([]byte("ok"), nil, 0, false, false)}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	out, err := Exec(ctx, ctr, "rbd", "info", "disk")
	if err != nil || string(out) != "ok" || !slices.Equal(ctr.argv[3:5], []string{"16777216", "-1"}) || (ctr.argv[5] != "44" && ctr.argv[5] != "45") ||
		!slices.Equal(ctr.argv[6:], []string{"rbd", "info", "disk"}) || ctr.envs[0] != "6" {
		t.Fatalf("a supervised command: %q, %v, limits and command %q, deadline signal %q", out, err, ctr.argv[3:], ctr.envs)
	}
	for _, refused := range []struct {
		ctr  testcontainers.Container
		args []string
	}{{ctr, nil}, {nil, []string{"rbd"}}, {(*supervisorReply)(nil), []string{"rbd"}}} {
		if _, err := Exec(t.Context(), refused.ctr, refused.args...); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("Exec(%v, %q) = %v", refused.ctr, refused.args, err)
		}
	}
}

// A setup client that a fixture recorded runs its commands for its cluster
// until the fixture terminated it; an unrecorded container for none.
func TestAddOwnedRecordsEveryHandleUntilTermination(t *testing.T) {
	owner := testhelper.TestCluster(nil, testhelper.TestConfig{})
	client, inner := &terminationCounter{}, &terminationCounter{}
	var owned Resources
	owned.AddOwned(client, owner, inner)
	if ownerOf(client) != owner || ownerOf(inner) != owner || ownerOf(&terminationCounter{}) != nil {
		t.Fatal("recorded handles were not owned by their cluster")
	}
	if err := owned.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ownerOf(client) != nil || ownerOf(inner) != nil || client.terminations != 1 {
		t.Fatalf("termination kept the handles owned or did not terminate the client: %d", client.terminations)
	}
}

type terminationCounter struct {
	testcontainers.Container
	terminations int
}

func (c *terminationCounter) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminations++
	return nil
}
