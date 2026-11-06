//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
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
