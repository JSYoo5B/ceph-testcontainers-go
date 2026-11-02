package multisite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// timingGateway supplies only version and startup arguments to verify the
// profile admission. It is not a model of RGW synchronization or timing.
type timingGateway struct {
	testcontainers.Container
	version, arguments string
	commands           []string
	terminations       int
}

func (c *timingGateway) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	words := args
	command := strings.Join(words, " ")
	c.commands = append(c.commands, command)
	var out string
	switch command {
	case "radosgw --version":
		out = c.version
	case "cat /proc/1/cmdline":
		out = c.arguments
	default:
		return 0, nil, fmt.Errorf("unexpected timing command %s", command)
	}
	return clustertest.Reply(args, 0, []byte(out))
}
func (c *timingGateway) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminations++
	return nil
}

func TestTimingProfileRequestsCannotBeSilentlyReplaced(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		profile := TimingProfile{IncrementalSync: enabled}
		req := &testcontainers.GenericContainerRequest{}
		for _, option := range profile.gatewayOptions(nil, nil) {
			if err := option.Customize(req); err != nil {
				t.Fatal(err)
			}
		}
		want := ""
		if enabled {
			want = "1"
		}
		if req.Env[incrementalTimingEnv] != want {
			t.Fatal("incorrect startup marker", req.Env)
		}
		opposite := "1"
		if enabled {
			opposite = ""
		}
		req = &testcontainers.GenericContainerRequest{}
		var err error
		for _, option := range profile.gatewayOptions(nil, []testcontainers.ContainerCustomizer{testcontainers.WithEnv(map[string]string{incrementalTimingEnv: opposite})}) {
			if err = option.Customize(req); err != nil {
				break
			}
		}
		if err == nil {
			t.Fatalf("enabled=%t accepted changed marker", enabled)
		}
	}
}

func TestTimingProfileVerifiesNativeVersionAndOwnedProcessInputs(t *testing.T) {
	good := []string{"radosgw", "-f", "--rgw-meta-sync-poll-interval", "1", "--rgw-data-sync-poll-interval", "1"}
	for _, test := range []struct {
		name, version string
		args          []string
		wantErr       bool
	}{
		{name: "qualified", version: "ceph version 20.2.4 (build) tentacle (stable)", args: good},
		{name: "unreadable", version: "ceph version not-a-release (build)", args: good, wantErr: true},
		{name: "unqualified", version: "ceph version 19.2.5 (build) squid (stable)", args: good, wantErr: true},
		{name: "missing", version: "ceph version 20.2.4 (build)", args: good[:4], wantErr: true},
		{name: "changed", version: "ceph version 20.2.4 (build)", args: []string{"radosgw", "--rgw-meta-sync-poll-interval", "1", "--rgw-data-sync-poll-interval", "20"}, wantErr: true},
		{name: "duplicate", version: "ceph version 20.2.4 (build)", args: append(slices.Clone(good), "--rgw-data-sync-poll-interval=20"), wantErr: true},
		{name: "underscore-conflict", version: "ceph version 20.2.4 (build)", args: append(slices.Clone(good), "--rgw_data_sync_poll_interval=8"), wantErr: true},
		{name: "terminator", version: "ceph version 20.2.4 (build)", args: append([]string{"radosgw", "--"}, good[2:]...), wantErr: true},
		{name: "positional", version: "ceph version 20.2.4 (build)", args: append([]string{"radosgw", "payload"}, good[2:]...), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctr := &timingGateway{version: test.version, arguments: strings.Join(test.args, "\x00") + "\x00"}
			err := (TimingProfile{IncrementalSync: true}).verifyGateway(t.Context(), &rgw.Gateway{Container: ctr})
			if (err != nil) != test.wantErr {
				t.Fatalf("verify: %v", err)
			}
			if test.name == "unqualified" {
				if errors.Is(err, ceph.ErrUnsupported) || !strings.Contains(err.Error(), "19.2.5") || !strings.Contains(err.Error(), "qualified only") {
					t.Fatalf("unqualified profile lost version or claimed native incapability: %v", err)
				}
			} else if errors.Is(err, ceph.ErrUnsupported) {
				t.Fatalf("non-version error classified unsupported: %v", err)
			}
		})
	}
	ctr := &timingGateway{}
	if err := (TimingProfile{}).verifyGateway(t.Context(), &rgw.Gateway{Container: ctr}); err != nil || len(ctr.commands) != 0 {
		t.Fatal("ordinary fixture queried timing", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (TimingProfile{IncrementalSync: true}).verifyGateway(ctx, &rgw.Gateway{Container: ctr}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation was lost", err)
	}
}

func TestTimingProfilePartialGatewayRemainsOwnedAfterVerificationFailure(t *testing.T) {
	f := &Fixture{realm: rgwRealmAccess{config: Config{Timing: TimingProfile{IncrementalSync: true}}}}
	ctr := &timingGateway{version: "ceph version 19.2.5 (build) squid (stable)"}
	gateway := &rgw.Gateway{Container: ctr}
	start := func(context.Context, rgw.Config, ...testcontainers.ContainerCustomizer) (*rgw.Gateway, error) {
		return gateway, nil
	}
	got, err := f.runGateway(t.Context(), "image", nil, nil, "gateway", "zone", start)
	if got != gateway || err == nil || !strings.Contains(err.Error(), "qualified only") || f.owned.Len() != 1 {
		t.Fatalf("partial gateway lost: %v %v", got, err)
	}
	if err := f.Terminate(t.Context()); err != nil || ctr.terminations != 1 {
		t.Fatalf("partial cleanup: %v terminations=%d", err, ctr.terminations)
	}
}

func TestTimingProfileScriptPreservesOrdinaryPollingAndLeasePeriod(t *testing.T) {
	// The explicit replacement prints startup argv only. It verifies shell
	// plumbing and neither simulates nor qualifies the native daemon.
	dir := t.TempDir()
	command := filepath.Join(dir, "radosgw")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mkdir"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{false, true} {
		cmd := exec.Command("sh", "../../ceph/internal/scripts/rgw.sh")
		cmd.Env = []string{"PATH=" + dir + ":" + os.Getenv("PATH"), "CEPH_RGW_REALM=realm"}
		if enabled {
			cmd.Env = append(cmd.Env, incrementalTimingEnv+"=1")
		}
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("script: %v %s", err, data)
		}
		args := strings.Fields(string(data))
		if slices.Contains(args, "--rgw-data-sync-poll-interval") != enabled || slices.Contains(args, "--rgw-meta-sync-poll-interval") != enabled {
			t.Fatalf("enabled=%t args=%q", enabled, args)
		}
		lease := slices.Index(args, "--rgw-sync-lease-period")
		if lease < 0 || args[lease+1] != "30" {
			t.Fatal("timing changed sync lease", args)
		}
	}
}
