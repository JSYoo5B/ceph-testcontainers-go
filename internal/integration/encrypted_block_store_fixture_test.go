//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
)

func encIntPtr(v int) *int { return &v }

func encStrPtr(v string) *string { return &v }

func TestJudgeEncRefusalNeedsAFailureNoMappingAndTheSameHeader(t *testing.T) {
	recorded := encHeader{UUID: "u-1", SHA256: strings.Repeat("a", 64), Offset: 16 << 20}
	offset := int64(16 << 20)
	refused := func() encReply {
		return encReply{Action: "open", RC: encIntPtr(2), Message: "No key available with this passphrase.",
			LUKSUUID: encStrPtr("u-1"), DataOffset: &offset, HeaderSHA256: strings.Repeat("a", 64)}
	}
	if v, d := judgeEncRefusal(refused(), recorded); v != encRefused {
		t.Fatalf("refused attempt: %s %s", v, d)
	}
	for name, c := range map[string]struct {
		mutate func(*encReply)
		want   string
	}{
		"opened":            {func(r *encReply) { r.Mapped = true }, encOpened},
		"mapping existed":   {func(r *encReply) { r.Existed = true }, encOpened},
		"accepted":          {func(r *encReply) { r.RC = encIntPtr(0) }, encAccepted},
		"no exit status":    {func(r *encReply) { r.RC = nil }, encUnobserved},
		"header unreadable": {func(r *encReply) { r.HeaderSHA256 = "" }, encUnobserved},
		"header rewritten":  {func(r *encReply) { r.HeaderSHA256 = strings.Repeat("b", 64) }, encHeaderChanged},
		"reformatted":       {func(r *encReply) { r.LUKSUUID = encStrPtr("u-2") }, encHeaderChanged},
	} {
		r := refused()
		c.mutate(&r)
		if got, d := judgeEncRefusal(r, recorded); got != c.want {
			t.Errorf("%s: %s %s, want %s", name, got, d, c.want)
		}
	}
	if got, _ := judgeEncRefusal(refused(), encHeader{}); got != encUnobserved {
		t.Errorf("an unrecorded header: %s", got)
	}
}

func TestJudgeEncStackNeedsEachLayerOverTheOwnedOneBelow(t *testing.T) {
	stacked := func() encReply {
		return encReply{Action: "stack", NodeTable: "0 1048576 linear 252:1 0", NodeOpen: encIntPtr(1),
			CryptNumber: "252:1", CryptTable: "0 1048576 crypt aes-xts-plain64 <redacted> 0 7:0 32768", CryptOpen: encIntPtr(1),
			CryptHolders: []string{"dm-3"}, LoopNumber: "7:0", LoopBacking: encStrPtr("/tc-enc/f"), LoopHolders: []string{"dm-1"}, File: "/tc-enc/f"}
	}
	if v, d := judgeEncStack(stacked(), "252:3"); v != encStacked {
		t.Fatalf("stacked chain: %s %s", v, d)
	}
	for name, mutate := range map[string]func(*encReply){
		"node maps an error table":  func(r *encReply) { r.NodeTable = "0 1048576 error" },
		"node maps another device":  func(r *encReply) { r.NodeTable = "0 1048576 linear 252:9 0" },
		"node maps at an offset":    func(r *encReply) { r.NodeTable = "0 1048576 linear 252:1 8" },
		"crypt mapping is closed":   func(r *encReply) { r.CryptNumber, r.CryptTable = "", "" },
		"crypt maps another device": func(r *encReply) { r.CryptTable = "0 1048576 crypt aes-xts-plain64 <redacted> 0 7:1 32768" },
		"crypt key not redacted":    func(r *encReply) { r.CryptTable = "0 1048576 crypt aes-xts-plain64 00ff 0 7:0 32768" },
		"another cipher":            func(r *encReply) { r.CryptTable = "0 1048576 crypt aes-cbc-essiv:sha256 <redacted> 0 7:0 32768" },
		"loop backs another file":   func(r *encReply) { r.LoopBacking = encStrPtr("/elsewhere") },
		"loop backing unknown":      func(r *encReply) { r.LoopBacking = nil },
		"crypt held twice":          func(r *encReply) { r.CryptHolders = []string{"dm-3", "dm-4"} },
		"crypt held by another":     func(r *encReply) { r.CryptHolders = []string{"dm-4"} },
		"crypt opened twice":        func(r *encReply) { r.CryptOpen = encIntPtr(2) },
		"loop held by another":      func(r *encReply) { r.LoopHolders = []string{"dm-5"} },
		"loop unheld":               func(r *encReply) { r.LoopHolders = nil },
	} {
		r := stacked()
		mutate(&r)
		if got, d := judgeEncStack(r, "252:3"); got != encNotStacked {
			t.Errorf("%s: %s %s", name, got, d)
		}
	}
}

func TestJudgeEncCiphertextNeedsTheMarkerAboveAndNotBelow(t *testing.T) {
	scan := func(n int) encReply { return encReply{Action: "scan", Count: encIntPtr(n)} }
	cases := []struct {
		name               string
		decrypted, backing encReply
		want               string
	}{
		{"ciphertext", scan(64), scan(0), encCiphertext},
		{"plaintext below", scan(64), scan(3), encPlaintextBehind},
		{"marker never written", scan(0), scan(0), encMarkerUnseen},
		{"scan failed", encReply{Action: "scan"}, scan(0), encScanUnavailable},
	}
	for _, c := range cases {
		if got := judgeEncCiphertext(c.decrypted, c.backing); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

func TestJudgeEncIdentityNeedsTheOriginalOSDStoreAndDevice(t *testing.T) {
	original := encOSDIdentity{ID: 0, UUID: "u-1", StoreFSID: "u-1", Number: "252:3", Attachment: blockAttached}
	if ok, d := judgeEncIdentity(original, original); !ok {
		t.Fatalf("same identity: %s", d)
	}
	for name, mutate := range map[string]func(*encOSDIdentity){
		"another ID":         func(i *encOSDIdentity) { i.ID = 1 },
		"another UUID":       func(i *encOSDIdentity) { i.UUID = "u-2" },
		"another store":      func(i *encOSDIdentity) { i.StoreFSID = "u-2" },
		"another device":     func(i *encOSDIdentity) { i.Number = "252:4" },
		"daemon not on node": func(i *encOSDIdentity) { i.Attachment = blockNotAttached },
	} {
		after := original
		mutate(&after)
		if ok, d := judgeEncIdentity(original, after); ok {
			t.Errorf("%s accepted: %s", name, d)
		}
	}
	unproven := original
	unproven.StoreFSID = "u-2"
	if ok, _ := judgeEncIdentity(unproven, unproven); ok {
		t.Error("an original store that is not the OSD's own was accepted")
	}
}

func TestJudgeEncLockedStartNeedsANonzeroExitWithoutABoot(t *testing.T) {
	cases := []struct {
		name string
		s    encLockedStart
		want string
	}{
		{"refused", encLockedStart{ExitCode: 1}, encStartRefused},
		{"booted locked", encLockedStart{ExitCode: 1, CameUp: true}, encStartedLocked},
		{"still running", encLockedStart{Running: true}, encStartUndecided},
		{"clean exit", encLockedStart{}, encStartUndecided},
	}
	for _, c := range cases {
		if got := judgeEncLockedStart(c.s); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

func TestJudgeEncReadbackNeedsEveryAcknowledgedDigest(t *testing.T) {
	ledger := []encLedgerEntry{{Key: "a", SHA256: "1", Outcome: mutAcked}, {Key: "b", SHA256: "2", Outcome: "failed 5"}}
	if ok, d := judgeEncReadback(ledger, map[string]string{"a": "1", "b": "absent"}); !ok {
		t.Fatalf("intact: %s", d)
	}
	if ok, _ := judgeEncReadback(ledger, map[string]string{"a": "absent"}); ok {
		t.Error("a missing acknowledged object was accepted")
	}
	if ok, _ := judgeEncReadback(ledger, map[string]string{"a": "9"}); ok {
		t.Error("changed bytes were accepted")
	}
	if ok, _ := judgeEncReadback(ledger[1:], map[string]string{}); ok {
		t.Error("a ledger without an acknowledged write was accepted")
	}
}

func TestEncLowerReadsOnlyOneTargetTables(t *testing.T) {
	for table, want := range map[string]string{
		"0 1048576 linear 252:1 0": "252:1",
		"0 1048576 linear 252:1 8": "",
		"0 1048576 error":          "error",
		"0 1048576 error ":         "error",
		"0 1048576 crypt aes-xts-plain64 <redacted> 0 7:0 32768": "7:0",
		"0 8 linear 252:1 0\n8 8 linear 252:2 0":                 "",
		"0 1048576 zero":                                         "",
		"":                                                       "",
		"0 1048576 crypt aes-xts-plain64 <redacted> 0 7:0 32768 1 allow_discards": "7:0",
	} {
		if got := encLower(table); got != want {
			t.Errorf("encLower(%q) = %q, want %q", table, got, want)
		}
	}
}

func TestEncAcquisitionRecordsOnlyWhatTheStepCreated(t *testing.T) {
	loop := encLoopRecord{Loop: "/dev/loop3", Number: "7:3", BackDev: "0:51", Inode: 2}
	luks := "0e1c1f9a-1111-2222-3333-444455556666"
	uuid := encCryptDMUUID(luks, "n-crypt")
	if uuid != "CRYPT-LUKS2-0e1c1f9a111122223333444455556666-n-crypt" {
		t.Fatalf("crypt DM UUID %q", uuid)
	}
	opened := func() encReply {
		return encReply{Action: "open", RC: encIntPtr(0), Mapped: true, UUID: uuid, Number: "252:4",
			Table: "0 1048576 crypt aes-xts-plain64 <redacted> 0 7:3 32768"}
	}
	if rec, why := encCryptAcquired(opened(), luks, "n-crypt", loop); why != "" || rec != (encDMRecord{UUID: uuid, Number: "252:4"}) {
		t.Fatalf("own open: %+v %s", rec, why)
	}
	for name, mutate := range map[string]func(*encReply){
		"cryptsetup failed":              func(r *encReply) { r.RC = encIntPtr(5) },
		"no exit status":                 func(r *encReply) { r.RC = nil },
		"nothing mapped":                 func(r *encReply) { r.Mapped = false },
		"the name existed before":        func(r *encReply) { r.RC, r.Existed = encIntPtr(5), true },
		"existed, reported success":      func(r *encReply) { r.Existed = true },
		"another mapping has the name":   func(r *encReply) { r.UUID = "TC-ENC-COLLIDE-1" },
		"another header under that name": func(r *encReply) { r.UUID = encCryptDMUUID("99999999-1111-2222-3333-444455556666", "n-crypt") },
		"maps another loop":              func(r *encReply) { r.Table = "0 1048576 crypt aes-xts-plain64 <redacted> 0 7:4 32768" },
	} {
		r := opened()
		mutate(&r)
		if rec, why := encCryptAcquired(r, luks, "n-crypt", loop); rec != (encDMRecord{}) || why == "" {
			t.Errorf("%s: recorded %+v", name, rec)
		}
	}
	if rec, _ := encCryptAcquired(opened(), "", "n-crypt", loop); rec != (encDMRecord{}) {
		t.Errorf("recorded without the header's UUID: %+v", rec)
	}

	linear := encReply{Action: "linear", RC: encIntPtr(0), UUID: "TC-ENC-n-1", Number: "252:5"}
	if rec, why := encNodeAcquired(linear, "TC-ENC-n-1"); why != "" || rec != (encDMRecord{UUID: "TC-ENC-n-1", Number: "252:5"}) {
		t.Fatalf("own node: %+v %s", rec, why)
	}
	busy := encReply{Action: "linear", RC: encIntPtr(1), Error: "dmsetup create: Device or resource busy"}
	if rec, _ := encNodeAcquired(busy, "TC-ENC-n-1"); rec != (encDMRecord{}) {
		t.Errorf("a refused create was recorded: %+v", rec)
	}
	other := linear
	other.UUID = "TC-ENC-COLLIDE-1"
	if rec, _ := encNodeAcquired(other, "TC-ENC-n-1"); rec != (encDMRecord{}) {
		t.Errorf("another mapping's UUID was recorded: %+v", rec)
	}

	created := encReply{Action: "create", File: "/tc-enc/n", Loop: "/dev/loop3", LoopNumber: "7:3", BackDev: "0:51", Inode: 2}
	if rec, ok := encLoopAcquired(created); !ok || rec != loop {
		t.Fatalf("own loop: %+v", rec)
	}
	created.Error = "loop identity"
	if _, ok := encLoopAcquired(created); ok {
		t.Error("a loop without a verified identity was recorded")
	}
}

// encChainFixture is a fully built chain and the observe receipt of it.
func encChainFixture() (encChainRecord, encReply) {
	rec := encChainRecord{
		File:  "/tc-enc/n",
		Loop:  encLoopRecord{Loop: "/dev/loop3", Number: "7:3", BackDev: "0:51", Inode: 2},
		Crypt: encDMRecord{UUID: "CRYPT-LUKS2-aa-n-crypt", Number: "252:4"},
		Node:  encDMRecord{UUID: "TC-ENC-n-1", Number: "252:5"},
	}
	seen := encReply{Action: "observe",
		SeenNode: &encDMSeen{Exists: true, Name: "n", UUID: "TC-ENC-n-1", Number: "252:5", Open: encIntPtr(0),
			Table: "0 1048576 linear 252:4 0", NameUUID: "TC-ENC-n-1"},
		SeenCrypt: &encDMSeen{Exists: true, Name: "n-crypt", UUID: "CRYPT-LUKS2-aa-n-crypt", Number: "252:4", Open: encIntPtr(1),
			Table: "0 1048576 crypt aes-xts-plain64 <redacted> 0 7:3 32768", NameUUID: "CRYPT-LUKS2-aa-n-crypt"},
		SeenLoop: &encLoopSeen{Attached: true, Number: "7:3", BackDev: "0:51", Inode: 2, Holders: []string{"dm-4"}},
	}
	return rec, seen
}

func TestJudgeEncOwnershipRemovesOnlyRecordedIdentities(t *testing.T) {
	rec, seen := encChainFixture()
	own := judgeEncOwnership(rec, seen)
	if own.Node.Verdict != encLayerOwned || own.Crypt.Verdict != encLayerOwned || own.Loop.Verdict != encLayerOwned {
		t.Fatalf("own chain: %+v", own)
	}
	want := []string{"TC-ENC-n-1", "252:5", "CRYPT-LUKS2-aa-n-crypt", "252:4", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}
	if got := encRemoveArgs(rec, own); !slices.Equal(got, want) {
		t.Fatalf("remove args %v, want %v", got, want)
	}

	cases := []struct {
		name              string
		mutate            func(*encChainRecord, *encReply)
		node, crypt, loop string
		args              []string
	}{
		{"node name collision: never recorded, the name is another mapping's",
			func(r *encChainRecord, s *encReply) {
				r.Node = encDMRecord{}
				s.SeenNode = &encDMSeen{Exists: true, Name: "n", UUID: "TC-ENC-COLLIDE-1", Number: "252:6", Table: "0 1 error", NameUUID: "TC-ENC-COLLIDE-1"}
				s.SeenCrypt.Open = encIntPtr(0)
			},
			encLayerForeign, encLayerOwned, encLayerOwned,
			[]string{"-", "-", "CRYPT-LUKS2-aa-n-crypt", "252:4", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
		{"crypt name collision: never recorded, the name is another mapping's",
			func(r *encChainRecord, s *encReply) {
				r.Node, r.Crypt = encDMRecord{}, encDMRecord{}
				s.SeenNode = &encDMSeen{}
				s.SeenCrypt = &encDMSeen{Exists: true, Name: "n-crypt", UUID: "TC-ENC-COLLIDE-2", Number: "252:6", Table: "0 1 error", NameUUID: "TC-ENC-COLLIDE-2"}
				s.SeenLoop.Holders = nil
			},
			encLayerNone, encLayerForeign, encLayerOwned,
			[]string{"-", "-", "-", "-", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
		{"node created again under the recorded UUID",
			func(_ *encChainRecord, s *encReply) { s.SeenNode.Number = "252:7" },
			encLayerReplaced, encLayerOwned, encLayerOwned,
			[]string{"-", "-", "CRYPT-LUKS2-aa-n-crypt", "252:4", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
		{"node gone, its name reused by another mapping",
			func(_ *encChainRecord, s *encReply) {
				s.SeenNode = &encDMSeen{NameUUID: "TC-ENC-COLLIDE-3"}
			},
			encLayerGone, encLayerOwned, encLayerOwned,
			[]string{"-", "-", "CRYPT-LUKS2-aa-n-crypt", "252:4", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
		{"node maps a device the test did not put under it",
			func(_ *encChainRecord, s *encReply) { s.SeenNode.Table = "0 1048576 linear 252:9 0" },
			encLayerRetargeted, encLayerOwned, encLayerOwned,
			[]string{"-", "-", "CRYPT-LUKS2-aa-n-crypt", "252:4", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
		{"locked store: node on the error table, crypt closed",
			func(r *encChainRecord, s *encReply) {
				r.Crypt = encDMRecord{}
				s.SeenNode.Table = "0 1048576 error"
				s.SeenCrypt = &encDMSeen{}
				s.SeenLoop.Holders = nil
			},
			encLayerOwned, encLayerNone, encLayerOwned,
			[]string{"TC-ENC-n-1", "252:5", "-", "-", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
		{"node created, its receipt lost: identified by its UUID",
			func(r *encChainRecord, _ *encReply) { r.Node.Number = "" },
			encLayerOwned, encLayerOwned, encLayerOwned,
			want},
		{"crypt maps another loop",
			func(_ *encChainRecord, s *encReply) {
				s.SeenCrypt.Table = "0 1048576 crypt aes-xts-plain64 <redacted> 0 7:8 32768"
			},
			encLayerRetargeted, encLayerRetargeted, encLayerOwned,
			[]string{"-", "-", "-", "-", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
		{"loop now backs another file",
			func(_ *encChainRecord, s *encReply) { s.SeenLoop.Inode = 9 },
			encLayerOwned, encLayerOwned, encLayerReplaced,
			[]string{"TC-ENC-n-1", "252:5", "CRYPT-LUKS2-aa-n-crypt", "252:4", "-", "-", "-", "-", "/tc-enc/n"}},
		{"keeper could not read the node",
			func(_ *encChainRecord, s *encReply) { s.SeenNode = &encDMSeen{Error: "dmsetup info: timeout"} },
			encLayerUnknown, encLayerOwned, encLayerOwned,
			[]string{"-", "-", "CRYPT-LUKS2-aa-n-crypt", "252:4", "/dev/loop3", "7:3", "0:51", "2", "/tc-enc/n"}},
	}
	for _, c := range cases {
		rec, seen := encChainFixture()
		c.mutate(&rec, &seen)
		own := judgeEncOwnership(rec, seen)
		if own.Node.Verdict != c.node || own.Crypt.Verdict != c.crypt || own.Loop.Verdict != c.loop {
			t.Errorf("%s: %+v, want node %s crypt %s loop %s", c.name, own, c.node, c.crypt, c.loop)
			continue
		}
		if got := encRemoveArgs(rec, own); !slices.Equal(got, c.args) {
			t.Errorf("%s: remove args %v, want %v", c.name, got, c.args)
		}
	}
	if own := judgeEncOwnership(encChainRecord{}, encReply{Action: "observe"}); own.Node.Verdict != encLayerUnknown || own.Crypt.Verdict != encLayerUnknown || own.Loop.Verdict != encLayerNone {
		t.Errorf("an empty receipt: %+v", own)
	}
}

func TestEncReleasedKeepsEveryLayerThatWasNotRemoved(t *testing.T) {
	rec, seen := encChainFixture()
	own := judgeEncOwnership(rec, seen)
	removed := func(n, c, l, f string) encReply {
		return encReply{Action: "remove", ReleaseNode: &encOutcome{Outcome: n}, ReleaseCrypt: &encOutcome{Outcome: c},
			ReleaseLoop: &encOutcome{Outcome: l}, ReleaseFile: &encOutcome{Outcome: f}}
	}
	if left := encReleased(rec, own, removed("removed", "removed", "removed", "removed")); left.held() {
		t.Fatalf("a released chain is still held: %+v", left)
	}
	left := encReleased(rec, own, removed("held", "held", "held", "held"))
	if left != rec {
		t.Errorf("a held chain was forgotten: %+v", left)
	}
	left = encReleased(rec, own, removed("removed", "refused", "held", "held"))
	if left.Node != (encDMRecord{}) || left.Crypt != rec.Crypt || left.Loop != rec.Loop || left.File != rec.File {
		t.Errorf("a refused crypt mapping: %+v", left)
	}
	// A layer the judge did not find owned is never forgotten because the
	// keeper says it is gone: the keeper did not look for it.
	_, foreign := encChainFixture()
	foreign.SeenNode = &encDMSeen{Exists: true, Name: "n", UUID: "TC-ENC-COLLIDE-1", Number: "252:6", Table: "0 1 error"}
	replaced := judgeEncOwnership(rec, foreign)
	if left := encReleased(rec, replaced, removed("skipped", "held", "held", "held")); left.Node != rec.Node {
		t.Errorf("a replaced node was forgotten: %+v", left)
	}
}

// encFakeStream is an exec stream whose output and input are pipes that
// nobody else touches: a read never reaches EOF and a write never completes,
// until the stream is closed.
type encFakeStream struct {
	out     *io.PipeReader
	in      *io.PipeWriter
	closed  atomic.Int32
	writeUp bool
}

func newEncFakeStream(writeBlocks bool) *encFakeStream {
	outR, _ := io.Pipe()
	_, inW := io.Pipe()
	return &encFakeStream{out: outR, in: inW, writeUp: writeBlocks}
}

func (s *encFakeStream) Read(p []byte) (int, error) { return s.out.Read(p) }

func (s *encFakeStream) Write(p []byte) (int, error) {
	if s.writeUp {
		return s.in.Write(p)
	}
	return len(p), nil
}

func (s *encFakeStream) CloseWrite() error { return nil }

func (s *encFakeStream) Close() error {
	s.closed.Add(1)
	_ = s.out.Close()
	return s.in.Close()
}

func TestEncCopyEndsAStreamThatNeverReachesEOF(t *testing.T) {
	for name, writeBlocks := range map[string]bool{"output never ends": false, "input never read": true} {
		s := newEncFakeStream(writeBlocks)
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		done := make(chan error, 1)
		go func() {
			_, _, err := encCopy(ctx, s, []byte("key"))
			done <- err
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("%s: %v, want the deadline", name, err)
			}
			if s.closed.Load() == 0 {
				t.Errorf("%s: the stream was not closed", name)
			}
		case <-time.After(5 * time.Second):
			_ = s.Close()
			t.Errorf("%s: encCopy did not return after its context ended", name)
		}
		cancel()
	}

	var framed bytes.Buffer
	for _, f := range []struct {
		stream stdcopy.StdType
		data   string
	}{{stdcopy.Stdout, `{"action":"scan"}`}, {stdcopy.Stderr, "note"}} {
		framed.Write([]byte{byte(f.stream), 0, 0, 0})
		_ = binary.Write(&framed, binary.BigEndian, uint32(len(f.data)))
		framed.WriteString(f.data)
	}
	stdout, stderr, err := encCopy(t.Context(), &encEOFStream{Reader: &framed}, nil)
	if err != nil || string(stdout) != `{"action":"scan"}` || string(stderr) != "note" {
		t.Errorf("a stream that ends: %q %q %v", stdout, stderr, err)
	}
}

// encEOFStream is a stream whose output ends.
type encEOFStream struct{ io.Reader }

func (encEOFStream) Write(p []byte) (int, error) { return len(p), nil }
func (encEOFStream) CloseWrite() error           { return nil }
func (encEOFStream) Close() error                { return nil }

func TestEncUnsettledNamesARunningExec(t *testing.T) {
	err := error(&encUnsettled{ExecID: "e1", Err: context.DeadlineExceeded})
	var u *encUnsettled
	if !errors.As(err, &u) || u.ExecID != "e1" || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "may still be running") {
		t.Errorf("unsettled exec error: %v", err)
	}
	if ok, d := judgeEncSettled(nil); !ok {
		t.Errorf("no unsettled exec: %s", d)
	}
	if ok, d := judgeEncSettled([]encExecState{{ID: "e1"}, {ID: "e2"}}); !ok {
		t.Errorf("exited execs: %s", d)
	}
	for name, s := range map[string]encExecState{"running": {ID: "e1", Running: true}, "not inspected": {ID: "e1", Running: true, Err: "no such exec"}} {
		if ok, _ := judgeEncSettled([]encExecState{{ID: "e0"}, s}); ok {
			t.Errorf("%s exec allowed the release", name)
		}
	}
}

func TestEncNodesUnlinkOnlyTheRemovedMappingsStaleNode(t *testing.T) {
	rec, seen := encChainFixture()
	own := judgeEncOwnership(rec, seen)
	removed := encReply{Action: "remove", ReleaseNode: &encOutcome{Outcome: "removed"}, ReleaseCrypt: &encOutcome{Outcome: "removed"}}
	if got := encRemovedNodes(rec, own, removed, "n", "n-crypt"); !slices.Equal(got, []encNode{{"n", "252:5"}, {"n-crypt", "252:4"}}) {
		t.Fatalf("removed nodes %v", got)
	}
	held := removed
	held.ReleaseCrypt = &encOutcome{Outcome: "held"}
	if got := encRemovedNodes(rec, own, held, "n", "n-crypt"); !slices.Equal(got, []encNode{{"n", "252:5"}}) {
		t.Errorf("a held crypt mapping's node was offered: %v", got)
	}
	_, foreign := encChainFixture()
	foreign.SeenNode = &encDMSeen{Exists: true, Name: "n", UUID: "TC-ENC-COLLIDE-1", Number: "252:6", Table: "0 1 error"}
	never := rec
	never.Node = encDMRecord{}
	if got := encRemovedNodes(never, judgeEncOwnership(never, foreign), encReply{ReleaseNode: &encOutcome{Outcome: "skipped"}, ReleaseCrypt: &encOutcome{Outcome: "removed"}}, "n", "n-crypt"); !slices.Equal(got, []encNode{{"n-crypt", "252:4"}}) {
		t.Errorf("a foreign node was offered: %v", got)
	}

	stale := encNodeSeen{Name: "n", Kind: "block", Number: "252:5"}
	unlink, refused := judgeEncNodeRemoval([]encNode{{"n", "252:5"}, {"gone", "252:9"}}, []encNodeSeen{stale})
	if !slices.Equal(unlink, []encNode{{"n", "252:5"}}) || len(refused) != 0 {
		t.Fatalf("stale own node: unlink %v refused %v", unlink, refused)
	}
	for name, s := range map[string]encNodeSeen{
		"another number":        {Name: "n", Kind: "block", Number: "252:7"},
		"number live again":     {Name: "n", Kind: "block", Number: "252:5", Live: true, DMName: "other"},
		"a symlink of the name": {Name: "n", Kind: "symlink"},
		"a character device":    {Name: "n", Kind: "char"},
	} {
		if unlink, refused := judgeEncNodeRemoval([]encNode{{"n", "252:5"}}, []encNodeSeen{s}); len(unlink) != 0 || len(refused) != 1 {
			t.Errorf("%s: unlink %v refused %v", name, unlink, refused)
		}
	}

	host := []encNodeSeen{{Name: "control", Kind: "char"}, {Name: "vg-lv", Kind: "symlink"}}
	if ok, d := judgeEncNodesClean([]string{"n", "n-crypt"}, host); !ok {
		t.Fatalf("host entries only: %s", d)
	}
	if ok, d := judgeEncNodesClean([]string{"n", "n-crypt"}, append(slices.Clone(host), encNodeSeen{Name: "n", Kind: "block", Number: "252:6", Live: true, DMName: "n"})); !ok {
		t.Errorf("a live mapping that shares the name: %s", d)
	}
	for name, s := range map[string]encNodeSeen{
		"stale own node":            stale,
		"stale node of another run": {Name: "tcenc-old", Kind: "block", Number: "252:2"},
		"node of another device":    {Name: "n", Kind: "block", Number: "252:2", Live: true, DMName: "other"},
		"own name as a symlink":     {Name: "n-crypt", Kind: "symlink"},
	} {
		if ok, _ := judgeEncNodesClean([]string{"n", "n-crypt"}, append(slices.Clone(host), s)); ok {
			t.Errorf("%s accepted", name)
		}
	}
}
