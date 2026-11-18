//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// actorCleanupReport collects cleanup failures for controlled blocked actors.
type actorCleanupReport struct{ errors []string }

func (*actorCleanupReport) Helper() {}
func (r *actorCleanupReport) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func TestIntegrationActorCleanup(t *testing.T) {
	for _, mode := range []string{"early exit", "released", "ignores release", "reader ignores cancellation"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release, finish := make(chan struct{}), make(chan struct{})
				a := launchIntegrationActor(context.Background(), func(ctx context.Context) error {
					switch mode {
					case "early exit":
						return fmt.Errorf("early exit")
					case "released":
						<-release
					case "ignores release":
						<-ctx.Done()
					case "reader ignores cancellation":
						<-finish
					}
					return nil
				})
				var finishOnce sync.Once
				unblock := func() { finishOnce.Do(func() { close(finish) }) }
				defer func() { unblock(); a.cancel(); _, _ = a.wait(context.Background()) }()
				synctest.Wait()
				report := new(actorCleanupReport)
				a.cleanup(report, func(context.Context) error { close(release); return nil })
				if strings.Contains(mode, "ignores") {
					if len(report.errors) != 1 || !strings.Contains(report.errors[0], "cleanup incomplete") {
						t.Fatalf("unresolved cleanup report: %v", report.errors)
					}
				} else if len(report.errors) != 0 {
					t.Fatal(report.errors)
				}
				unblock()
				synctest.Wait()
				for range 2 {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					_, err := a.wait(ctx)
					cancel()
					if err != nil {
						t.Fatal("completion must remain readable:", err)
					}
				}
			})
		})
	}
}

// actorLifecycleContainer substitutes only the retained actor and readiness
// files. It exercises the real start helpers and testing cleanup registration.
type actorLifecycleContainer struct {
	testcontainers.Container
	release chan struct{}
	joined  chan struct{}
	once    sync.Once
	mode    string
}

func (c *actorLifecycleContainer) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	switch args[0] {
	case "timeout":
		defer close(c.joined)
		if c.mode == "transport" {
			return 0, nil, fmt.Errorf("injected reader failure")
		}
		if c.mode == "early" {
			return 4, strings.NewReader("early actor exit"), nil
		}
		select {
		case <-c.release:
		case <-ctx.Done():
		}
		return 0, strings.NewReader(""), nil
	case "touch":
		if c.mode == "ignore" {
			return 0, strings.NewReader(""), nil
		}
		c.once.Do(func() { close(c.release) })
		return 0, strings.NewReader(""), nil
	case "cat":
		if c.mode == "early" || c.mode == "transport" {
			<-c.joined
			return 1, strings.NewReader(""), nil
		}
		return 0, strings.NewReader("malformed readiness"), nil
	}
	return 0, strings.NewReader(""), nil
}
func TestIntegrationActorStartFailurePaths(t *testing.T) {
	if mode := os.Getenv("CTG_ACTOR_FAILURE_CONTROL"); mode != "" {
		c := &actorLifecycleContainer{mode: mode, release: make(chan struct{}), joined: make(chan struct{})}
		t.Cleanup(func() {
			select {
			case <-c.joined:
				t.Log("ACTOR_READER_JOINED")
			default:
				t.Error("actor reader still running")
			}
		})
		switch mode {
		case "early", "malformed", "ignore", "transport":
			startDualStackSession(t, t.Context(), c, "script", "control")
		case "retained":
			r := startRetainedClient(t, t.Context(), c, "control", "pool", "fs")
			r.round(t, t.Context(), 1, nil)
		case "family":
			a := startFamilyActor(t, t.Context(), c, "control", "mon", "pool")
			a.call(t, t.Context(), map[string]any{"wait": 0})
		}
		t.Fatal("injected intermediate assertion")
	}
	for _, mode := range []string{"early", "malformed", "retained", "family", "ignore", "transport"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIntegrationActorStartFailurePaths$", "-test.v")
			cmd.Env = append(os.Environ(), "CTG_ACTOR_FAILURE_CONTROL="+mode)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "ACTOR_READER_JOINED") || strings.Contains(string(output), "cleanup incomplete") != (mode == "ignore") || strings.Contains(string(output), "command completion unconfirmed") != (mode == "transport") {
				t.Fatalf("failure-path cleanup: %v\n%s", err, output)
			}
		})
	}
}

func TestFixtureProcessFailureCleanup(t *testing.T) {
	if mode := os.Getenv("CTG_PROCESS_FAILURE_CONTROL"); mode != "" {
		cmd := exec.Command("python3", "-c", "import time; time.sleep(600)")
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				t.Error("fixture child was not reaped")
			} else {
				t.Log("CHILD_REAPED")
			}
		})
		a := startFixtureProcess(t, cmd)
		switch mode {
		case "second start":
			startFixtureProcess(t, exec.Command(filepath.Join(t.TempDir(), "absent-executable")))
		case "readiness":
			waitFixtureReady(t, a, filepath.Join(t.TempDir(), "never-ready"))
		case "intermediate":
			t.Fatal("injected wait failure in another actor")
		}
		return
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("needs python3")
	}
	for _, mode := range []string{"second start", "readiness", "intermediate"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFixtureProcessFailureCleanup$", "-test.v")
			cmd.Env = append(os.Environ(), "CTG_PROCESS_FAILURE_CONTROL="+mode)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "CHILD_REAPED") || strings.Contains(string(output), "cleanup incomplete") {
				t.Fatalf("fixture failure cleanup: %v\n%s", err, output)
			}
		})
	}
}
