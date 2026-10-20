package fault

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const blocklistTestAddress = "192.0.2.7:0/1234"

func TestTemporaryBlocklistRestoresOnlyOwnedEntryAndSharesCopies(t *testing.T) {
	c, native := newBlocklistFixture()
	other := "192.0.2.7:0/5678"
	native.entries[other] = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	prior := native.entries[other]
	change, err := TemporaryBlocklist(t.Context(), c, "v2:"+blocklistTestAddress, time.Minute)
	if err != nil || change.Address() != blocklistTestAddress {
		t.Fatal(err)
	}
	if _, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute); err == nil {
		t.Fatal("overlap accepted")
	}
	copy := *change
	if err := copy.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(native.entries) != 1 || !native.entries[other].Equal(prior) || len(native.ranges) != 1 {
		t.Fatal("restoration changed another session or range")
	}
	newChange, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before := native.removals
	if err := change.Restore(t.Context()); err != nil || native.removals != before {
		t.Fatal("stale copy removed a later entry")
	}
	if err := newChange.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTemporaryBlocklistRefusesExistingAndOutsideRenewal(t *testing.T) {
	c, native := newBlocklistFixture()
	native.entries[blocklistTestAddress] = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	if change, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute); err == nil || change != nil || native.additions != 0 {
		t.Fatal("existing entry was adopted or renewed")
	}
	delete(native.entries, blocklistTestAddress)
	change, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	native.entries[blocklistTestAddress] = native.entries[blocklistTestAddress].Add(time.Hour)
	if err := change.Restore(t.Context()); err == nil || native.removals != 0 {
		t.Fatal("outside renewal was removed")
	}
	// Natural native expiration converges even after outside renewal.
	delete(native.entries, blocklistTestAddress)
	if err := change.Restore(t.Context()); err != nil || len(trackedBlocklist(c)) != 0 {
		t.Fatal("native absence not reconciled")
	}
}

func TestTemporaryBlocklistLostRepliesAndCanceledApplyAreRecoverable(t *testing.T) {
	c, native := newBlocklistFixture()
	ctx, cancel := context.WithCancel(t.Context())
	native.cancelAfterAdd, native.loseAdd = cancel, true
	change, err := TemporaryBlocklist(ctx, c, blocklistTestAddress, time.Minute)
	if err == nil || change == nil || !change.state.known {
		t.Fatal("canceled apply lost its fresh readback handle")
	}
	native.loseRemove = true
	if err := change.Restore(t.Context()); err == nil || len(native.entries) != 0 {
		t.Fatal("lost removal reply not preserved")
	}
	if err := change.Restore(t.Context()); err != nil || native.removals != 1 {
		t.Fatal("removal retry did not reconcile absence")
	}
}

func TestTemporaryBlocklistUnknownApplyCannotRemovePresentEntry(t *testing.T) {
	c, native := newBlocklistFixture()
	native.failReadAfterAdd = true
	change, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err == nil || change == nil || change.state.known {
		t.Fatal("unknown readback claimed confirmed creation")
	}
	if err := change.Restore(t.Context()); err == nil || native.removals != 0 {
		t.Fatal("unknown creation authorized removal")
	}
	delete(native.entries, blocklistTestAddress)
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type blocklistFixture struct {
	testcontainers.Container
	entries                                             map[string]time.Time
	ranges                                              []map[string]string
	additions, removals                                 int
	loseAdd, loseRemove, failReadAfterAdd, failNextRead bool
	cancelAfterAdd                                      context.CancelFunc
}

func newBlocklistFixture() (*ceph.Container, *blocklistFixture) {
	native := &blocklistFixture{entries: make(map[string]time.Time), ranges: []map[string]string{{"range": "198.51.100.0:0/24", "until": "2026-10-03T08:00:00.123456+0000"}}}
	return servicehook.TestCluster(native, servicehook.TestConfig{StartupTimeout: time.Second}), native
}

func (f *blocklistFixture) Exec(ctx context.Context, argv []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	args := argv[3:]
	output, code := "", 0
	switch {
	case slices.Equal(args, []string{"osd", "blocklist", "ls", "--format", "json"}):
		if f.failNextRead {
			f.failNextRead = false
			return 0, nil, errors.New("lost readback")
		}
		entries := make([]map[string]string, 0, len(f.entries))
		for address, until := range f.entries {
			entries = append(entries, map[string]string{"addr": address, "until": until.Format("2006-01-02T15:04:05.999999-0700")})
		}
		a, _ := json.Marshal(entries)
		b, _ := json.Marshal(f.ranges)
		output = string(a) + string(b)
	case len(args) == 5 && slices.Equal(args[:3], []string{"osd", "blocklist", "add"}):
		seconds, _ := strconv.ParseFloat(args[4], 64)
		f.entries[args[3]] = time.Now().UTC().Add(time.Duration(seconds * float64(time.Second))).Truncate(time.Microsecond)
		f.additions++
		f.failNextRead = f.failReadAfterAdd
		if f.cancelAfterAdd != nil {
			f.cancelAfterAdd()
		}
		if f.loseAdd {
			code = 1
		}
	case len(args) == 4 && slices.Equal(args[:3], []string{"osd", "blocklist", "rm"}):
		delete(f.entries, args[3])
		f.removals++
		if f.loseRemove {
			code = 1
		}
	default:
		return 0, nil, fmt.Errorf("unexpected blocklist command %v", args)
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return code, bytes.NewReader(append(header[:], []byte(output)...)), nil
}

func TestBlocklistRefusesUntrackedHandle(t *testing.T) {
	t.Run("blocklist", func(t *testing.T) {
		c, native := newBlocklistFixture()
		change, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		forged := &BlocklistOverride{owner: c, address: change.address, state: &blocklistOverrideState{until: change.state.until, known: true}}
		if err := forged.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "not tracked") || native.removals != 0 {
			t.Fatalf("an untracked override removed the entry: %v", err)
		}
	})
}

func trackedBlocklist(c *ceph.Container) (held map[string]any) {
	faulthook.WithTopologyNoContext(c, func() { held = faulthook.TestFaults(c, faulthook.Blocklist) })
	return held
}

func TestBlocklistFailedPreflightReleasesReservation(t *testing.T) {
	c, native := newBlocklistFixture()
	native.failNextRead = true
	if change, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute); err == nil || change != nil || len(trackedBlocklist(c)) != 0 || native.additions != 0 {
		t.Fatalf("failed preflight retained ownership or mutated: %v", err)
	}
	change, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute)
	if err != nil || trackedBlocklist(c)[blocklistTestAddress] != change {
		t.Fatalf("freed reservation was not reusable: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := change.Restore(ctx); !errors.Is(err, context.Canceled) || trackedBlocklist(c)[blocklistTestAddress] != change || native.removals != 0 {
		t.Fatalf("cancelled restore lost ownership or mutated: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(trackedBlocklist(c)) != 0 {
		t.Fatalf("fresh restore did not release ownership: %v", err)
	}
}

func TestBlocklistAdmissionDoesNotMutateUnavailableCluster(t *testing.T) {
	for _, input := range []struct {
		address  string
		duration time.Duration
	}{{"192.0.2.7", time.Minute}, {blocklistTestAddress, time.Millisecond}} {
		c, native := newBlocklistFixture()
		if change, err := TemporaryBlocklist(t.Context(), c, input.address, input.duration); err == nil || change != nil || native.additions != 0 || len(trackedBlocklist(c)) != 0 {
			t.Fatalf("invalid request admitted: %v", err)
		}
	}
	c, native := newBlocklistFixture()
	*servicehook.TestState(c).Closed = true
	if change, err := TemporaryBlocklist(t.Context(), c, blocklistTestAddress, time.Minute); err == nil || change != nil || native.additions != 0 || len(trackedBlocklist(c)) != 0 {
		t.Fatalf("closed cluster admitted: %v", err)
	}
	var missing *BlocklistOverride
	if err := missing.Restore(t.Context()); err == nil {
		t.Fatal("nil override restored")
	}
}
