package ceph

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
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const blocklistTestAddress = "192.0.2.7:0/1234"

func TestClientBlocklistExactSessionNormalization(t *testing.T) {
	for input, want := range map[string]string{
		blocklistTestAddress:                        blocklistTestAddress,
		"[v2:192.0.2.7:0/1234,v1:192.0.2.7:0/1234]": blocklistTestAddress,
		"v2:[2001:db8::1]:0/1234":                   "[2001:db8::1]:0/1234",
	} {
		got, err := normalizeBlocklistSession(input, true)
		if err != nil || got != want {
			t.Fatalf("%s: %q %v", input, got, err)
		}
	}
	for _, invalid := range []string{"192.0.2.7", "192.0.2.7:0", "192.0.2.7:0/0", "192.0.2.0/24", "0.0.0.0:0/1", "224.0.0.1:0/1", "192.0.2.7:70000/1", "192.0.2.7:0/4294967296", "[v2:192.0.2.7:0/1,v1:192.0.2.7:0/2]", "[v2:192.0.2.7:0/1"} {
		if _, err := normalizeBlocklistSession(invalid, true); err == nil {
			t.Fatal("unsafe fencing address accepted:", invalid)
		}
	}
}

func TestBlocklistDecodePreservesRangesAndRejectsMalformedState(t *testing.T) {
	data := []byte(`[{"addr":"192.0.2.7:0/1234","until":"2026-10-03T08:00:00.123456+0000"}][{"range":"198.51.100.0:0/24","until":"2026-10-03T08:00:00.123456+0000"}]`)
	entries, err := decodeBlocklistEntries(data)
	if err != nil || len(entries) != 2 || !entries[1].Range || entries[0].Until.Nanosecond() != 123456000 {
		t.Fatalf("native arrays: %+v %v", entries, err)
	}
	for _, malformed := range []string{"", "null", "[] [] []", "[] {}", `[{"addr":"192.0.2.7:0/1","until":"invalid"}]`, `[{"addr":"192.0.2.7:0/1","until":"2026-10-03T08:00:00Z"},{"addr":"v2:192.0.2.7:0/1","until":"2026-10-03T08:00:00Z"}]`} {
		if _, err := decodeBlocklistEntries([]byte(malformed)); err == nil {
			t.Fatal("malformed native blocklist accepted:", malformed)
		}
	}
}

func TestTemporaryBlocklistRestoresOnlyOwnedEntryAndSharesCopies(t *testing.T) {
	c, native := newBlocklistFixture()
	other := "192.0.2.7:0/5678"
	native.entries[other] = time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	prior := native.entries[other]
	change, err := c.TemporaryBlocklist(t.Context(), "v2:"+blocklistTestAddress, time.Minute)
	if err != nil || change.Address() != blocklistTestAddress {
		t.Fatal(err)
	}
	if _, err := c.TemporaryBlocklist(t.Context(), blocklistTestAddress, time.Minute); err == nil {
		t.Fatal("overlap accepted")
	}
	copy := *change
	if err := copy.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(native.entries) != 1 || !native.entries[other].Equal(prior) || len(native.ranges) != 1 {
		t.Fatal("restoration changed another session or range")
	}
	newChange, err := c.TemporaryBlocklist(t.Context(), blocklistTestAddress, time.Minute)
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
	if change, err := c.TemporaryBlocklist(t.Context(), blocklistTestAddress, time.Minute); err == nil || change != nil || native.additions != 0 {
		t.Fatal("existing entry was adopted or renewed")
	}
	delete(native.entries, blocklistTestAddress)
	change, err := c.TemporaryBlocklist(t.Context(), blocklistTestAddress, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	native.entries[blocklistTestAddress] = native.entries[blocklistTestAddress].Add(time.Hour)
	if err := change.Restore(t.Context()); err == nil || native.removals != 0 {
		t.Fatal("outside renewal was removed")
	}
	// Natural native expiration converges even after outside renewal.
	delete(native.entries, blocklistTestAddress)
	if err := change.Restore(t.Context()); err != nil || len(c.blocklistOverrides) != 0 {
		t.Fatal("native absence not reconciled")
	}
}

func TestTemporaryBlocklistLostRepliesAndCanceledApplyAreRecoverable(t *testing.T) {
	c, native := newBlocklistFixture()
	ctx, cancel := context.WithCancel(t.Context())
	native.cancelAfterAdd, native.loseAdd = cancel, true
	change, err := c.TemporaryBlocklist(ctx, blocklistTestAddress, time.Minute)
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
	change, err := c.TemporaryBlocklist(t.Context(), blocklistTestAddress, time.Minute)
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

func newBlocklistFixture() (*Container, *blocklistFixture) {
	native := &blocklistFixture{entries: make(map[string]time.Time), ranges: []map[string]string{{"range": "198.51.100.0:0/24", "until": "2026-10-03T08:00:00.123456+0000"}}}
	return &Container{Container: native, settings: options{startupTimeout: time.Second}}, native
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
