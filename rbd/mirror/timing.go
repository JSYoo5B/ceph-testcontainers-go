package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// TimingProfile selects qualified startup conditions of owned mirror daemons.
// Its zero value preserves ordinary native timing. Pass it in Config through
// WithConfig; it does not change either cluster's configuration database.
type TimingProfile struct {
	// JournalPolling selects a one-second journal poll age on Ceph 20.2.4 in
	// journal mode. The ordinary age is five seconds. Each newly enrolled image
	// captures the startup value in its JournalPlayer; an existing replay cannot
	// be retimed by central config readback. Terminating the daemon ends all of
	// its captured intervals. A later AddDaemon uses this fixture's same profile.
	//
	// The flag has precedence over local and central configuration. Actual native
	// version and effective daemon readback are checked, so customizers that
	// replace required commands cannot silently undo the condition. Snapshot
	// schedules, replay delay, heartbeat/failover detection and commit age are
	// unchanged; this profile does not accelerate those scenario windows or I/O.
	JournalPolling bool
}

func (p TimingProfile) validate(mode Mode) error {
	if p.JournalPolling && mode != ModeJournal {
		return servicehook.Invalid(errors.New("journal polling timing requires journal mirroring mode"))
	}
	return nil
}

func (p TimingProfile) command(args ...string) []string {
	if p.JournalPolling {
		return append(args, "--rbd-mirror-journal-poll-age=1")
	}
	return args
}

func (p TimingProfile) verify(parent context.Context, daemon testcontainers.Container) error {
	if !p.JournalPolling {
		return nil
	}
	if daemon == nil {
		return errors.New("timing profile has no owned mirror daemon")
	}
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	command, err := multicluster.Exec(ctx, daemon, "cat", "/proc/1/cmdline")
	if err != nil {
		return err
	}
	if err := verifyTimingCommand(command); err != nil {
		return err
	}
	version, err := multicluster.Exec(ctx, daemon, "rbd-mirror", "--version")
	if err != nil {
		return err
	}
	if !strings.HasPrefix(strings.TrimSpace(string(version)), "ceph version 20.2.4 ") {
		return fmt.Errorf("journal polling timing is not qualified for this rbd-mirror release")
	}
	data, err := multicluster.Exec(ctx, daemon, "ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "config", "get", "rbd_mirror_journal_poll_age")
	if err != nil {
		return err
	}
	var effective map[string]string
	if err := json.Unmarshal(data, &effective); err != nil {
		return fmt.Errorf("decode journal poll age: %w", err)
	}
	age, err := strconv.ParseFloat(effective["rbd_mirror_journal_poll_age"], 64)
	if err != nil || age != 1 {
		return errors.New("owned rbd-mirror did not apply the one-second journal polling profile")
	}
	return nil
}

// verifyTimingCommand requires the startup flag that this fixture owns. A
// central value of one is insufficient evidence: removing our flag would let
// a later database update change the condition for future JournalPlayers.
func verifyTimingCommand(command []byte) error {
	count := 0
	exact := false
	for _, argument := range strings.Split(string(command), "\x00") {
		if argument == "--" {
			break
		}
		key, _, _ := strings.Cut(argument, "=")
		if strings.ReplaceAll(key, "_", "-") != "--rbd-mirror-journal-poll-age" {
			continue
		}
		count++
		exact = argument == "--rbd-mirror-journal-poll-age=1"
	}
	if count != 1 || !exact {
		return errors.New("owned rbd-mirror is missing the exact unique startup journal polling flag")
	}
	return nil
}
