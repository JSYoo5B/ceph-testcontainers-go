//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type partialDetachForwardTarget struct {
	testcontainers.Container
	context     context.Context
	timeout     *time.Duration
	optionCount int
	err         error
}

func (c *partialDetachForwardTarget) Stop(ctx context.Context, timeout *time.Duration) error {
	c.context = ctx
	c.timeout = timeout
	return c.err
}
func (c *partialDetachForwardTarget) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	c.context = ctx
	c.optionCount = len(opts)
	return c.err
}

func TestPartialDetachRecorderPreservesRuntimeArgumentsAndErrors(t *testing.T) {
	sentinel := errors.New("runtime result")
	target := &partialDetachForwardTarget{err: sentinel}
	recorder := &partialDetachContainer{Container: target}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	timeout := time.Second
	if err := recorder.Stop(ctx, &timeout); err != sentinel || target.context != ctx || target.timeout != &timeout {
		t.Fatal("stop forwarding changed the runtime call", err)
	}
	if err := recorder.Terminate(ctx); err != sentinel || target.context != ctx || target.optionCount != 0 {
		t.Fatal("terminate forwarding changed the runtime call", err)
	}
	if recorder.stops.Load() != 1 || recorder.terminations.Load() != 1 {
		t.Fatal("forwarding counts differ")
	}
}

func TestPartialDetachReadyRenamePreservesIdentityAndRefusesOverwrite(t *testing.T) {
	store := filepath.Join(t.TempDir(), "ceph-3")
	if err := os.Mkdir(store, 0o700); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(store, "ready")
	if err := os.WriteFile(ready, []byte("synthetic ready marker\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	invoke := func(action string) (partialDetachMarker, error) {
		data, err := exec.CommandContext(t.Context(), "python3", "-c", partialDetachReadyScript, store, action).Output()
		if err != nil {
			return partialDetachMarker{}, err
		}
		var marker partialDetachMarker
		err = json.Unmarshal(data, &marker)
		return marker, err
	}
	first, err := invoke("hide")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(ready); !os.IsNotExist(err) {
		t.Fatal("ready is still published", err)
	}
	if _, err := invoke("hide"); err == nil {
		t.Fatal("second hide was admitted")
	}
	restored, err := invoke("restore")
	if err != nil || restored != first {
		t.Fatal("restore changed marker identity or bytes", first, restored, err)
	}
	if settled, err := invoke("settle"); err != nil || settled != first {
		t.Fatal("settling an already published marker changed it", settled, err)
	}
	if _, err := invoke("hide"); err != nil {
		t.Fatal(err)
	}
	if settled, err := invoke("settle"); err != nil || settled != first {
		t.Fatal("settling a held marker changed it", settled, err)
	}
	if err := os.Remove(ready); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing-ready", ready); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke("hide"); err == nil {
		t.Fatal("symlink marker admitted")
	}
}
