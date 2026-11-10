package multisite

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// TimingProfile selects qualified startup conditions for this fixture's
// gateways. The zero value preserves native default polling.
type TimingProfile struct {
	// IncrementalSync selects one-second metadata and data incremental sync
	// polling on Ceph 20.2.4. It preserves sync leases and notifications, and
	// does not accelerate initial full synchronization or guarantee an elapsed
	// bound. It is fixed for the lifetime of each owned gateway, including added
	// zones: terminating the fixture ends the condition. It sets no persistent
	// configuration entry and changes no gateway outside the fixture. A native
	// version outside the qualified release returns a qualification error after
	// startup; retain the partial Fixture for termination even on that error.
	IncrementalSync bool
	// SyncLeasePeriod replaces the 30 seconds of rgw_sync_lease_period that
	// the fixture's gateways otherwise start with (the native default is
	// two minutes). A gateway renews each sync shard's lease every half
	// period and counts it as lost after one period without a renewal, and
	// it renews none while its reads of a source zone that does not answer
	// wait. Set it for a test that keeps a zone from answering (a zone link
	// cut, a paused gateway) for longer than half of 30 seconds, to more
	// than twice that time. A killed gateway's leases stay taken until
	// they expire, so sync after a gateway restart can stall for one
	// period. It must be a whole number of seconds, at least 30. It is
	// fixed for the lifetime of each owned gateway, including added zones,
	// on every release, and sets no persistent configuration entry.
	SyncLeasePeriod time.Duration
}

const (
	incrementalTimingEnv = "CEPH_RGW_INCREMENTAL_SYNC_TIMING"
	syncLeaseEnv         = "CEPH_RGW_SYNC_LEASE_PERIOD"
)

// defaultSyncLease is the sync lease period of a fixture gateway without a
// profile (ceph/internal/scripts/rgw.sh).
const defaultSyncLease = 30 * time.Second

// validate rejects a sync lease period the startup script cannot apply.
func (p TimingProfile) validate() error {
	if p.SyncLeasePeriod == 0 {
		return nil
	}
	if p.SyncLeasePeriod < defaultSyncLease || p.SyncLeasePeriod%time.Second != 0 {
		return multisiteInvalid(fmt.Errorf("RGW sync lease period must be a whole number of seconds, at least %s (got %s)", defaultSyncLease, p.SyncLeasePeriod))
	}
	return nil
}

// markers returns the startup script's environment for the profile; an
// unselected condition has an empty value.
func (p TimingProfile) markers() map[string]string {
	markers := map[string]string{incrementalTimingEnv: "", syncLeaseEnv: ""}
	if p.IncrementalSync {
		markers[incrementalTimingEnv] = "1"
	}
	if p.SyncLeasePeriod != 0 {
		markers[syncLeaseEnv] = strconv.FormatInt(int64(p.SyncLeasePeriod/time.Second), 10)
	}
	return markers
}

var timingVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z._-]+)?$`)

// gatewayOptions injects the profile before caller customizers, and checks it
// afterwards without executing any customizer twice. The startup script is
// the sole consumer of the internal marker; ordinary callers cannot silently
// override a selected profile or select one through this marker alone.
func (p TimingProfile) gatewayOptions(base, opts []testcontainers.ContainerCustomizer) []testcontainers.ContainerCustomizer {
	selected := map[string]string{}
	for name, value := range p.markers() {
		if value != "" {
			selected[name] = value
		}
	}
	if len(selected) > 0 {
		base = append(base, testcontainers.WithEnv(selected))
	}
	all := append(base, opts...)
	return append(all, testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
		for name, want := range p.markers() {
			if req.Env[name] != want {
				return errors.New("RGW timing profile startup marker must not be replaced by a customizer")
			}
		}
		return nil
	}))
}

// verifyGateway checks the actual running binary and startup arguments. The
// read of /proc/1/cmdline proves the input of the owned process, not a new
// process's view of MON database entries. Native qualification separately
// checks the live admin-socket values and real replication behavior.
func (p TimingProfile) verifyGateway(ctx context.Context, gateway *rgw.Gateway) error {
	if p == (TimingProfile{}) {
		return nil
	}
	if gateway == nil || gateway.Container == nil {
		return errors.New("RGW timing requires an owned gateway")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if p.IncrementalSync {
		version, err := servicehook.Command(ctx, gateway.Container, "radosgw", "--version")
		if err != nil {
			return fmt.Errorf("read RGW timing binary version: %w", err)
		}
		words := strings.Fields(string(version))
		if len(words) < 3 || words[0] != "ceph" || words[1] != "version" || !timingVersionPattern.MatchString(words[2]) {
			return errors.New("could not identify the RGW timing binary version")
		}
		if words[2] != "20.2.4" {
			return fmt.Errorf("incremental RGW timing is qualified only for Ceph 20.2.4 (observed %s)", words[2])
		}
	}
	data, err := servicehook.Command(ctx, gateway.Container, "cat", "/proc/1/cmdline")
	if err != nil {
		return fmt.Errorf("read RGW timing startup arguments: %w", err)
	}
	if !timingArguments(strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00"), p.startupArguments()) {
		return errors.New("owned RGW process did not receive the selected timing profile")
	}
	return nil
}

// startupArguments returns the native options the profile selects with the
// value each must have, once, among the gateway's startup arguments.
func (p TimingProfile) startupArguments() map[string]string {
	want := map[string]string{}
	if p.IncrementalSync {
		want["--rgw-meta-sync-poll-interval"], want["--rgw-data-sync-poll-interval"] = "1", "1"
	}
	if lease := p.markers()[syncLeaseEnv]; lease != "" {
		want["--rgw-sync-lease-period"] = lease
	}
	return want
}

// timingArguments reports whether args start radosgw with each option of
// want exactly once, spelled with dashes and with the wanted value.
func timingArguments(args []string, want map[string]string) bool {
	if len(args) == 0 || args[0] != "radosgw" {
		return false
	}
	counts := map[string]int{}
	for option := range want {
		counts[option] = 0
	}
	for index := 1; index < len(args); {
		arg := args[index]
		if arg == "--" || !strings.HasPrefix(arg, "-") {
			return false
		}
		key, value, inline := strings.Cut(arg, "=")
		canonical := strings.ReplaceAll(key, "_", "-")
		if _, target := counts[canonical]; target {
			if key != canonical {
				return false
			}
			if !inline {
				index++
				if index >= len(args) {
					return false
				}
				value = args[index]
			}
			if value != want[canonical] {
				return false
			}
			counts[canonical]++
			index++
			continue
		}
		if arg == "-f" {
			index++
			continue
		}
		// Other native options may have one following value. The selected daemon
		// has already started successfully; only complete option/value tokens are
		// accepted here, never a positional command or an option terminator.
		index++
		if !inline && index < len(args) && !strings.HasPrefix(args[index], "-") {
			index++
		}
	}
	for _, count := range counts {
		if count != 1 {
			return false
		}
	}
	return true
}
