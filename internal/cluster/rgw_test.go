package cluster

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

type rgwTestContainer struct {
	testcontainers.Container
	id           string
	terminateErr error
	port         string
	protocol     string
	execCalls    int
}

func (c *rgwTestContainer) GetContainerID() string { return c.id }

func (c *rgwTestContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return c.terminateErr
}

func (c *rgwTestContainer) PortEndpoint(_ context.Context, port, protocol string) (string, error) {
	c.port, c.protocol = port, protocol
	return "http://docker-host:" + strings.TrimSuffix(port, "/tcp"), nil
}

func (c *rgwTestContainer) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.execCalls++
	return 0, strings.NewReader(""), nil
}

func TestRGWEndpointUsesSelectedHostPortAndBridgeDefault(t *testing.T) {
	for _, test := range []struct {
		name          string
		port          int
		publicAddress string
		want          string
		providerPort  string
	}{
		{"default host address uses Docker provider", 42791, "127.0.0.1", "http://docker-host:42791", "42791/tcp"},
		{"bridge selected port", 7480, "", "http://docker-host:7480", "7480/tcp"},
		{"legacy multicluster bridge gateway", 0, "", "http://docker-host:7480", "7480/tcp"},
		{"explicit public NIC differs from Docker API host", 42791, "192.0.2.34", "http://192.0.2.34:42791", ""},
		{"custom loopback differs from localhost", 42791, "127.0.0.2", "http://127.0.0.2:42791", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctr := &rgwTestContainer{}
			gateway := &RGWContainer{Container: ctr, port: test.port, publicAddress: test.publicAddress}
			endpoint, err := gateway.S3Endpoint(t.Context())
			if err != nil || endpoint != test.want || ctr.port != test.providerPort || (test.providerPort != "" && ctr.protocol != "http") {
				t.Fatalf("endpoint ignored selected port or Docker host: endpoint=%q port=%q protocol=%q error=%v", endpoint, ctr.port, ctr.protocol, err)
			}
		})
	}
}

func TestDiscardRGWConflictRetainsFailedCleanupAndNewOwner(t *testing.T) {
	cleanupFailure := errors.New("temporary Docker removal failure")
	failed := &rgwTestContainer{id: "conflicted", terminateErr: cleanupFailure}
	c := &Container{services: map[string]testcontainers.Container{"rgw": failed}}
	if err := c.discardRGWAttempt(t.Context(), failed); !errors.Is(err, cleanupFailure) || c.services["rgw"] != failed {
		t.Fatal("failed cleanup lost the partial RGW service")
	}
	failed.terminateErr = nil
	if err := c.discardRGWAttempt(t.Context(), failed); err != nil {
		t.Fatal(err)
	}
	if _, exists := c.services["rgw"]; exists {
		t.Fatal("successfully removed conflict still blocks a new RGW attempt")
	}
	replacement := &rgwTestContainer{id: "replacement"}
	c.services["rgw"] = replacement
	if err := c.discardRGWAttempt(t.Context(), failed); err != nil || c.services["rgw"] != replacement {
		t.Fatal("old-attempt cleanup removed a different RGW owner")
	}
}

func TestDiscardRGWConflictAcceptsOnlyMissingResources(t *testing.T) {
	cleanupFailure := errors.New("Docker termination hook failed")
	for _, tc := range []struct {
		name string
		err  error
		safe bool
	}{
		{"already removed", errdefs.ErrNotFound, true},
		{"all resources already removed", errors.Join(errdefs.ErrNotFound, errdefs.ErrNotFound), true},
		{"missing container and failed cleanup", errors.Join(errdefs.ErrNotFound, cleanupFailure), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctr := &rgwTestContainer{id: "conflicted", terminateErr: tc.err}
			c := &Container{services: map[string]testcontainers.Container{"rgw": ctr}}
			err := c.discardRGWAttempt(t.Context(), ctr)
			_, retained := c.services["rgw"]
			if (err == nil) != tc.safe || retained == tc.safe {
				t.Fatalf("RGW removal lost ownership or rejected an absent resource: retained=%t error=%v", retained, err)
			}
			if !tc.safe && !errors.Is(err, cleanupFailure) {
				t.Fatal("RGW cleanup hid the real termination failure")
			}
		})
	}
}

type rgwReadinessTarget struct {
	wait.StrategyTarget
	state     container.State
	probeCode int
	probe     func()
}

func (target *rgwReadinessTarget) State(context.Context) (*container.State, error) {
	return &target.state, nil
}

func (target *rgwReadinessTarget) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	if target.probe != nil {
		target.probe()
	}
	return target.probeCode, strings.NewReader(""), nil
}

func TestRGWHostReadinessRequiresOwnedListener(t *testing.T) {
	controller := &rgwTestContainer{}
	c := &Container{Container: controller, settings: options{startupTimeout: time.Second}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	target := &rgwReadinessTarget{state: container.State{Running: true}, probeCode: 1, probe: cancel}
	err := c.hostRGWReadiness(42791, "127.0.0.1:42791").WaitUntilReady(ctx, target)
	if !errors.Is(err, context.Canceled) || controller.execCalls != 0 {
		t.Fatalf("an unrelated host listener could satisfy readiness: controller calls=%d error=%v", controller.execCalls, err)
	}
	target = &rgwReadinessTarget{state: container.State{Running: true}, probeCode: 0}
	if err := c.hostRGWReadiness(42791, "127.0.0.1:42791").WaitUntilReady(t.Context(), target); err != nil || controller.execCalls != 1 {
		t.Fatalf("owned listener was not checked through the control container: calls=%d error=%v", controller.execCalls, err)
	}
	target = &rgwReadinessTarget{state: container.State{Status: "exited", ExitCode: 1}}
	if err := c.hostRGWReadiness(42791, "127.0.0.1:42791").WaitUntilReady(t.Context(), target); err == nil || controller.execCalls != 1 {
		t.Fatal("stopped RGW did not fail readiness before the HTTP probe")
	}
}
