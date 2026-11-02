package mirror

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// timingDaemon replies only to native version and effective option reads.
// Explicit inputs exercise refusal and precedence handling; this is not evidence
// that a real JournalPlayer consumes the profile.
type timingDaemon struct {
	testcontainers.Container
	version, effective string
	command            []byte
	calls              [][]string
	err                error
}

func (d *timingDaemon) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	d.calls = append(d.calls, append([]string(nil), args...))
	if d.err != nil {
		return 0, nil, d.err
	}
	output := d.effective
	if reflect.DeepEqual(args, []string{"cat", "/proc/1/cmdline"}) {
		data := d.command
		if data == nil {
			data = []byte("rbd-mirror\x00-f\x00--rbd-mirror-journal-poll-age=1\x00")
		}
		return clustertest.Reply(args, 0, data)
	}
	if reflect.DeepEqual(args, []string{"rbd-mirror", "--version"}) {
		output = d.version
	}
	return clustertest.Reply(args, 0, []byte(output))
}

func TestTimingProfileModeAndCommandScope(t *testing.T) {
	for _, test := range []struct {
		mode    Mode
		profile bool
		valid   bool
	}{
		{ModeJournal, true, true}, {ModeSnapshot, true, false}, {ModeSnapshot, false, true}, {ModeJournal, false, true},
	} {
		cfg, err := normalizeRBDMirrorConfig(Config{Pool: "images", Mode: test.mode, Timing: TimingProfile{JournalPolling: test.profile}})
		if (err == nil) != test.valid {
			t.Fatalf("mode=%s profile=%v config=%+v err=%v", test.mode, test.profile, cfg, err)
		}
	}
	args := []string{"-f", "--name", "client.owned"}
	if got := (TimingProfile{}).command(args...); !reflect.DeepEqual(got, args) {
		t.Fatal("zero profile changed native command", got)
	}
	want := append(append([]string(nil), args...), "--rbd-mirror-journal-poll-age=1")
	if got := (TimingProfile{JournalPolling: true}).command(args...); !reflect.DeepEqual(got, want) {
		t.Fatal("profile changed unrelated arguments", got)
	}
}
func TestTimingProfileRequiresActualQualifiedVersionAndEffectiveReadback(t *testing.T) {
	for _, test := range []struct {
		name, version, effective string
		valid                    bool
	}{
		{"qualified", "ceph version 20.2.4 (abcdef) tentacle (stable)", `{"rbd_mirror_journal_poll_age":"1"}`, true},
		{"unqualified", "ceph version 19.2.5 (abcdef) squid (stable)", `{"rbd_mirror_journal_poll_age":"1"}`, false},
		{"prefix", "ceph version 20.2.40 (abcdef) tentacle (stable)", `{"rbd_mirror_journal_poll_age":"1"}`, false},
		{"customizer override", "ceph version 20.2.4 (abcdef) tentacle (stable)", `{"rbd_mirror_journal_poll_age":"5"}`, false},
		{"malformed", "ceph version 20.2.4 (abcdef) tentacle (stable)", `{"other":"1"}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := &timingDaemon{version: test.version, effective: test.effective}
			err := (TimingProfile{JournalPolling: true}).verify(t.Context(), d)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v err=%v", test.valid, err)
			}
			if !strings.HasPrefix(test.version, "ceph version 20.2.4 ") && len(d.calls) != 2 {
				t.Fatal("unqualified version reached config readback", d.calls)
			}
		})
	}
	d := &timingDaemon{err: errors.New("native query failed")}
	if err := (TimingProfile{}).verify(t.Context(), d); err != nil || len(d.calls) != 0 {
		t.Fatal("zero profile queried native daemon", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (TimingProfile{JournalPolling: true}).verify(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatal("native verification lost cancellation", err)
	}
	if err := (TimingProfile{JournalPolling: true}).verify(t.Context(), nil); err == nil {
		t.Fatal("missing daemon accepted")
	}
}

func TestTimingProfileRejectsRemovedDuplicateAndAliasedStartupFlags(t *testing.T) {
	for _, command := range []string{
		"rbd-mirror\x00-f\x00",
		"rbd-mirror\x00--\x00--rbd-mirror-journal-poll-age=1\x00",
		"rbd-mirror\x00--rbd-mirror-journal-poll-age=5\x00",
		"rbd-mirror\x00--rbd-mirror-journal-poll-age=1\x00--rbd-mirror-journal-poll-age=1\x00",
		"rbd-mirror\x00--rbd-mirror-journal-poll-age=1\x00--rbd_mirror_journal_poll_age=5\x00",
		"rbd-mirror\x00--rbd-mirror-journal-poll-age\x001\x00",
	} {
		d := &timingDaemon{command: []byte(command), version: "ceph version 20.2.4 (abc) tentacle", effective: `{"rbd_mirror_journal_poll_age":"1"}`}
		if err := (TimingProfile{JournalPolling: true}).verify(t.Context(), d); err == nil {
			t.Fatalf("altered startup flag accepted despite central value1: %q", command)
		}
		if len(d.calls) != 1 {
			t.Fatal("invalid command reached later verification", d.calls)
		}
	}
}
