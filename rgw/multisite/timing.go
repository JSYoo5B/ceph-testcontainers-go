package multisite

import (
	"context"
	"errors"
	"fmt"
	"regexp"
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
}

const incrementalTimingEnv = "CEPH_RGW_INCREMENTAL_SYNC_TIMING"

var timingVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z._-]+)?$`)

// gatewayOptions injects the profile before caller customizers, and checks it
// afterwards without executing any customizer twice. The startup script is
// the sole consumer of the internal marker; ordinary callers cannot silently
// override a selected profile or select one through this marker alone.
func (p TimingProfile) gatewayOptions(base, opts []testcontainers.ContainerCustomizer) []testcontainers.ContainerCustomizer {
	if p.IncrementalSync {
		base = append(base, testcontainers.WithEnv(map[string]string{incrementalTimingEnv: "1"}))
	}
	all := append(base, opts...)
	return append(all, testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
		want := ""
		if p.IncrementalSync {
			want = "1"
		}
		if req.Env[incrementalTimingEnv] != want {
			return errors.New("RGW timing profile startup marker must not be replaced by a customizer")
		}
		return nil
	}))
}

// verifyGateway checks the actual running binary and startup arguments. The
// read of /proc/1/cmdline proves the input of the owned process, not a new
// process's view of MON database entries. Native qualification separately
// checks the live admin-socket values and real replication behavior.
func (p TimingProfile) verifyGateway(ctx context.Context, gateway *rgw.Gateway) error {
	if !p.IncrementalSync {
		return nil
	}
	if gateway == nil || gateway.Container == nil {
		return errors.New("RGW timing requires an owned gateway")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
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
	data, err := servicehook.Command(ctx, gateway.Container, "cat", "/proc/1/cmdline")
	if err != nil {
		return fmt.Errorf("read RGW timing startup arguments: %w", err)
	}
	if !timingArguments(strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00")) {
		return errors.New("owned RGW process did not receive the selected timing profile")
	}
	return nil
}

func timingArguments(args []string) bool {
	if len(args) == 0 || args[0] != "radosgw" {
		return false
	}
	counts := map[string]int{"--rgw-meta-sync-poll-interval": 0, "--rgw-data-sync-poll-interval": 0}
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
			if value != "1" {
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
