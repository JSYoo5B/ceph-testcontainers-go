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
	"time"

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

func TestTimingProfileSyncLeasePeriodIsValidatedBeforeAnythingStarts(t *testing.T) {
	for _, test := range []struct {
		period  time.Duration
		wantErr bool
	}{
		{0, false}, {30 * time.Second, false}, {5 * time.Minute, false},
		{29 * time.Second, true}, {time.Second, true}, {-time.Minute, true}, {30*time.Second + time.Millisecond, true},
	} {
		err := (TimingProfile{SyncLeasePeriod: test.period}).validate()
		if (err != nil) != test.wantErr || err != nil && !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("period %s: %v", test.period, err)
		}
	}
	// runPair refuses the profile before it looks at a cluster.
	f, err := runPair(t.Context(), "image", Config{Timing: TimingProfile{SyncLeasePeriod: 90 * time.Millisecond}})
	if f != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "sync lease period") {
		t.Fatalf("fixture %v, error %v", f, err)
	}
}

func TestTimingProfileSyncLeaseMarkerCannotBeSilentlyReplaced(t *testing.T) {
	for _, test := range []struct {
		profile TimingProfile
		want    string
		replace string
	}{
		{TimingProfile{}, "", "300"},
		{TimingProfile{SyncLeasePeriod: 5 * time.Minute}, "300", "30"},
		{TimingProfile{SyncLeasePeriod: 5 * time.Minute}, "300", ""},
		{TimingProfile{IncrementalSync: true, SyncLeasePeriod: 45 * time.Second}, "45", "46"},
	} {
		req := &testcontainers.GenericContainerRequest{}
		for _, option := range test.profile.gatewayOptions(nil, nil) {
			if err := option.Customize(req); err != nil {
				t.Fatal(err)
			}
		}
		if req.Env[syncLeaseEnv] != test.want || (req.Env[incrementalTimingEnv] == "1") != test.profile.IncrementalSync {
			t.Fatalf("profile %+v: startup markers %v", test.profile, req.Env)
		}
		req = &testcontainers.GenericContainerRequest{}
		var err error
		for _, option := range test.profile.gatewayOptions(nil, []testcontainers.ContainerCustomizer{testcontainers.WithEnv(map[string]string{syncLeaseEnv: test.replace})}) {
			if err = option.Customize(req); err != nil {
				break
			}
		}
		if err == nil {
			t.Fatalf("profile %+v accepted the marker %q", test.profile, test.replace)
		}
	}
}

func TestTimingProfileVerifiesTheSyncLeaseOfTheOwnedProcess(t *testing.T) {
	lease := TimingProfile{SyncLeasePeriod: 5 * time.Minute}
	good := []string{"radosgw", "-f", "--rgw-realm", "realm", "--rgw-sync-lease-period", "300"}
	for _, test := range []struct {
		name    string
		profile TimingProfile
		args    []string
		wantErr bool
	}{
		{name: "selected", profile: lease, args: good},
		{name: "the fixture's default", profile: lease, args: []string{"radosgw", "-f", "--rgw-sync-lease-period", "30"}, wantErr: true},
		{name: "missing", profile: lease, args: good[:4], wantErr: true},
		{name: "twice", profile: lease, args: append(slices.Clone(good), "--rgw-sync-lease-period=300"), wantErr: true},
		{name: "underscores", profile: lease, args: []string{"radosgw", "--rgw_sync_lease_period=300"}, wantErr: true},
		{name: "with polling", profile: TimingProfile{IncrementalSync: true, SyncLeasePeriod: 5 * time.Minute},
			args: append(slices.Clone(good), "--rgw-meta-sync-poll-interval", "1", "--rgw-data-sync-poll-interval", "1")},
		{name: "with polling, lease not applied", profile: TimingProfile{IncrementalSync: true, SyncLeasePeriod: 5 * time.Minute},
			args: []string{"radosgw", "--rgw-sync-lease-period", "30", "--rgw-meta-sync-poll-interval", "1", "--rgw-data-sync-poll-interval", "1"}, wantErr: true},
		// The polling profile alone does not look at the lease.
		{name: "polling alone", profile: TimingProfile{IncrementalSync: true},
			args: []string{"radosgw", "--rgw-sync-lease-period", "30", "--rgw-meta-sync-poll-interval", "1", "--rgw-data-sync-poll-interval", "1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctr := &timingGateway{version: "ceph version 20.2.4 (build) tentacle (stable)", arguments: strings.Join(test.args, "\x00") + "\x00"}
			err := test.profile.verifyGateway(t.Context(), &rgw.Gateway{Container: ctr})
			if (err != nil) != test.wantErr {
				t.Fatalf("verify: %v", err)
			}
			// Only the polling profile is bound to a release.
			if asked := slices.Contains(ctr.commands, "radosgw --version"); asked != test.profile.IncrementalSync {
				t.Fatalf("version read: %t, commands %v", asked, ctr.commands)
			}
		})
	}
	// The lease alone is not bound to a release.
	ctr := &timingGateway{version: "ceph version 19.2.5 (build) squid (stable)", arguments: strings.Join(good, "\x00") + "\x00"}
	if err := lease.verifyGateway(t.Context(), &rgw.Gateway{Container: ctr}); err != nil {
		t.Fatal(err)
	}
}

func TestTimingProfileScriptAppliesTheSyncLeasePeriod(t *testing.T) {
	// As above, the replacement prints startup argv only.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "radosgw"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mkdir"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	run := func(env ...string) ([]string, error) {
		cmd := exec.Command("sh", "../../ceph/internal/scripts/rgw.sh")
		cmd.Env = append([]string{"PATH=" + dir + ":" + os.Getenv("PATH")}, env...)
		data, err := cmd.CombinedOutput()
		return strings.Fields(string(data)), err
	}
	for value, want := range map[string]string{"": "30", "300": "300", "45": "45"} {
		args, err := run("CEPH_RGW_REALM=realm", syncLeaseEnv+"="+value)
		lease := slices.Index(args, "--rgw-sync-lease-period")
		if err != nil || lease < 0 || args[lease+1] != want || strings.Count(strings.Join(args, " "), "--rgw-sync-lease-period") != 1 {
			t.Fatalf("marker %q: %v, args %q", value, err, args)
		}
	}
	for _, value := range []string{"0", "030", "-5", "1.5", "30s", "3 0"} {
		if args, err := run("CEPH_RGW_REALM=realm", syncLeaseEnv+"="+value); err == nil || slices.Contains(args, "radosgw") || slices.Contains(args, "--rgw-realm") {
			t.Fatalf("marker %q started the gateway: %q", value, args)
		}
	}
	// A gateway outside a realm has no sync and gets no lease period.
	if args, err := run(syncLeaseEnv + "=300"); err != nil || slices.Contains(args, "--rgw-sync-lease-period") {
		t.Fatalf("standalone gateway: %v, args %q", err, args)
	}
}
