//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"bytes"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestRBDExportDigestReportsEveryFailure runs rbdExportDigest in a local
// shell with substituted rbd, md5sum and rm commands: an export failure keeps
// its status without a digest, a failed removal of the exported file fails an
// otherwise successful run with its own diagnostic, and the temporary
// directory is gone whenever its removal succeeds. The commands are
// substitutes; this does not observe a Ceph client container.
func TestRBDExportDigestReportsEveryFailure(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	const digest = "0123456789abcdef0123456789abcdef"
	bin := t.TempDir()
	writeScript := func(name, body string) {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// rbd <args> export --no-progress <image> <path>: write the image, or
	// exit with $EXPORT_STATUS.
	writeScript("rbd", `test -z "$EXPORT_STATUS" || { echo "rbd: export failed" >&2; exit "$EXPORT_STATUS"; }
for last; do :; done
printf image > "$last"`)
	writeScript("md5sum", `cat > /dev/null; echo "`+digest+`  -"`)
	realRM, err := exec.LookPath("rm")
	if err != nil {
		t.Skip("no rm")
	}
	writeScript("rm", `test -z "$RM_STATUS" || { echo "rm: injected failure" >&2; exit "$RM_STATUS"; }
exec `+realRM+` "$@"`)

	for _, tc := range []struct {
		name                  string
		export, remove        string
		code                  int
		stderr                []string
		directoryRemains, sum bool
	}{
		{name: "success", sum: true},
		{name: "removal fails", remove: "73", code: 73, stderr: []string{"remove the temporary export directory exited 73"}, directoryRemains: true},
		{name: "export fails", export: "7", code: 7, stderr: []string{"rbd export exited 7"}},
		{name: "export and removal fail", export: "7", remove: "73", code: 7, stderr: []string{"rbd export exited 7", "remove the temporary export directory exited 73"}, directoryRemains: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			cmd := exec.CommandContext(t.Context(), sh, "-c", rbdExportDigest, "sh", "pool/image", "rbd", "--id", "admin")
			cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "TMPDIR="+tmp, "EXPORT_STATUS="+tc.export, "RM_STATUS="+tc.remove)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			code := 0
			if err := cmd.Run(); err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				code = exit.ExitCode()
			}
			got, err := rbdExportResult("pool/image", code, stdout.Bytes(), stderr.Bytes())
			if tc.sum {
				if err != nil || got != digest {
					t.Fatalf("a successful export returned %q, %v", got, err)
				}
			} else if err == nil || code != tc.code {
				t.Fatalf("exit %d with %q, %v; want exit %d and an error", code, got, err, tc.code)
			}
			for _, want := range tc.stderr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("the error %v lacks %q", err, want)
				}
			}
			left, _ := os.ReadDir(tmp)
			if remains := len(left) > 0; remains != tc.directoryRemains {
				t.Fatalf("temporary export directory remains: %v, want %v", remains, tc.directoryRemains)
			}
		})
	}
}

func TestRBDParentReadHistoryRejectsCacheBypass(t *testing.T) {
	objects := map[string]bool{"rbd_data.12.0000": true}
	read := func(out string) map[string]string {
		t.Helper()
		ops, err := harness.RBDDataReadsInHistory([]byte(out), objects)
		if err != nil {
			t.Fatal(err)
		}
		return ops
	}
	before := read(`{"ops":[
        {"description":"osd_op(client.1 ::rbd_data.12.0000:head [write 0~1])","initiated_at":"1"},
        {"description":"osd_op(client.1 ::rbd_data.12.00000:head [read 0~1])","initiated_at":"2"}
    ]}`)
	after := read(`{"ops":[{"description":"osd_op(client.1 ::rbd_data.12.0000:head [sparse-read 0~1])","initiated_at":"3"}]}`)
	if err := immutableCacheReadDelta(before, before, 0); err != nil {
		t.Fatal(err)
	}
	if err := immutableCacheReadDelta(before, after, 0); err == nil {
		t.Fatal("accepted a parent OSD read during a cache hit")
	}
	if err := immutableCacheReadDelta(before, after, 1); err != nil {
		t.Fatal(err)
	}
	if err := immutableCacheReadDelta(after, before, 0); err == nil {
		t.Fatal("accepted truncated history")
	}
	if _, err := harness.RBDDataReadsInHistory([]byte(`{}`), objects); err == nil {
		t.Fatal("accepted missing ops")
	}
}

// TestRBDReadServersAttributesByRequestEvidence covers the attribution of
// TestRBDZoneLocality with perf dump, op history and messenger log shapes
// recorded on Tentacle: a read is attributed to the OSD whose history or
// debug_ms 1 log names a request for the object, including the issue 650
// case of a reply without a history entry; read counters, which sum every
// read of the OSD, never attribute a read alone, even where an unrelated read
// of the same size moved them; and contradicting or malformed evidence is
// refused.
func TestRBDReadServersAttributesByRequestEvidence(t *testing.T) {
	const size = 4 << 20
	const object = "rbd_data.106465208873.0000000000000000"
	client := netip.MustParseAddr("172.18.0.9")
	counters := func(t *testing.T, out string) harness.OSDReadCounters {
		t.Helper()
		c, err := harness.ParseOSDReadCounters([]byte(out))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	// osd.0 around a localized read it served, and osd.1 around the same
	// read, which only read the image header.
	replicaBefore := counters(t, `{"osd":{"op":0,"op_r":0,"op_r_out_bytes":0,"replica_read":0,"replica_read_served":0}}`)
	replicaAfter := counters(t, `{"osd":{"op":1,"op_r":1,"op_r_out_bytes":4194328,"replica_read":1,"replica_read_served":1}}`)
	primaryBefore := counters(t, `{"osd":{"op_r":57,"op_r_out_bytes":4195560,"replica_read_served":0}}`)
	primaryAfter := counters(t, `{"osd":{"op_r":68,"op_r_out_bytes":4195799,"replica_read_served":0}}`)
	// osd.1 after the header reads and an unrelated read of another 4 MiB
	// object.
	primaryUnrelated := counters(t, `{"osd":{"op_r":69,"op_r_out_bytes":8390127,"replica_read_served":0}}`)
	// A release without the replica counter.
	unreported := counters(t, `{"osd":{"op_r":3,"op_r_out_bytes":12}}`)
	if unreported.ReplicaReadServed != nil {
		t.Fatal("reported an absent replica_read_served")
	}
	entry := []string{"2026-11-09T14:03:53.531961+0000 osd_op(client.4210.0:12 2.0 2:866fb5cd:::" + object + ":head [sparse-read 0~4194304 [fadvise_nocache]] snapc 0=[] ondisk+read+localize_reads+known_if_redirected+supports_pool_eio e18)"}

	replyLine := "2026-11-09T14:03:53.541+0000 7a9918570640  1 -- [v2:172.18.0.7:6800/4210582212,v1:172.18.0.7:6801/4210582212] --> 172.18.0.9:0/3430708390 -- osd_op_reply(12 " + object + " [sparse-read 0~4194304 [fadvise_nocache] out=4194328b] v0'0 uv12 ondisk = 0) -- 0x58e59cd72500 con 0x58e59ccffc00"
	window := strings.Join([]string{
		"2026-11-09T14:03:53.530+0000 7a9918570640  1 -- [v2:172.18.0.7:6800/4210582212,v1:172.18.0.7:6801/4210582212] <== client.4210 172.18.0.9:0/3430708390 1 ==== osd_op() ==== 237+0+0 (crc 0 0 0) 0x58e59cd72000 con 0x58e59ccffc00",
		replyLine,
		// Another object, a failed read, a write and another peer's read.
		strings.Replace(replyLine, object, "rbd_data.106465208873.0000000000000001", 1),
		strings.Replace(replyLine, "ondisk = 0)", "ondisk = -2)", 1),
		strings.Replace(replyLine, "sparse-read 0~4194304 [fadvise_nocache] out=4194328b", "write 0~4194304", 1),
		strings.Replace(replyLine, "--> 172.18.0.9:0/", "--> [v2:172.18.0.6:6800/", 1),
		// A reply with a protocol version, several operations and an
		// address vector.
		"2026-11-09T14:03:53.600+0000 7a9918570640  1 -- v2:172.18.0.7:6800/4210582212 --> [v2:172.18.0.9:0/3430708390] -- osd_op_reply(13 " + object + " [stat out=16b,read 0~8 [fadvise_nocache] out=8b] v0'0 uv12 ondisk = 0) v8 -- 0x58e59cd72500 con 0x58e59ccffc00",
	}, "\n")
	replies, err := harness.RBDDataReadReplies([]byte(window), object, client)
	if err != nil {
		t.Fatal(err)
	}
	want := []harness.RBDReadReply{{Peer: "172.18.0.9:0/3430708390", TID: "12", OutBytes: 4194328}, {Peer: "[v2:172.18.0.9:0/3430708390]", TID: "13", OutBytes: 8}}
	if !slices.Equal(replies, want) {
		t.Fatalf("replies %+v, want %+v", replies, want)
	}
	if _, err := harness.RBDDataReadReplies([]byte(strings.Replace(replyLine, "--> 172.18.0.9:0/", "--> host:0/", 1)), object, client); err == nil {
		t.Fatal("accepted an unparsable peer address")
	}
	reply := replies[:1]

	for _, tc := range []struct {
		name     string
		evidence map[int]harness.RBDReadEvidence
		servers  []int
		err      string
	}{
		{name: "history, reply and counters", servers: []int{0}, evidence: map[int]harness.RBDReadEvidence{
			0: {History: entry, Replies: reply, Before: replicaBefore, After: replicaAfter},
			1: {Before: primaryBefore, After: primaryAfter}}},
		{name: "reply without a history entry (issue 650)", servers: []int{0}, evidence: map[int]harness.RBDReadEvidence{
			0: {Replies: reply, Before: replicaBefore, After: replicaAfter},
			1: {Before: primaryBefore, After: primaryAfter}}},
		{name: "unrelated same-size read on another OSD", servers: []int{0}, evidence: map[int]harness.RBDReadEvidence{
			0: {History: entry, Before: replicaBefore, After: replicaAfter},
			1: {Before: primaryBefore, After: primaryUnrelated}}},
		{name: "two OSDs replied", servers: []int{0, 1}, evidence: map[int]harness.RBDReadEvidence{
			0: {Replies: reply, Before: replicaBefore, After: replicaAfter},
			1: {Replies: reply, Before: primaryBefore, After: primaryUnrelated}}},
		{name: "counters alone", err: harness.ErrRBDReadUnattributed.Error(), evidence: map[int]harness.RBDReadEvidence{
			0: {Before: replicaBefore, After: replicaAfter},
			1: {Before: primaryBefore, After: primaryUnrelated}}},
		{name: "counters alone without the replica counter", err: harness.ErrRBDReadUnattributed.Error(), evidence: map[int]harness.RBDReadEvidence{
			0: {Before: unreported, After: counters(t, `{"osd":{"op_r":4,"op_r_out_bytes":4194340}}`)}}},
		{name: "history on an OSD that returned too little", err: "less than", evidence: map[int]harness.RBDReadEvidence{
			1: {History: entry, Before: primaryBefore, After: primaryAfter}}},
		{name: "replies that returned too little", err: "less than", evidence: map[int]harness.RBDReadEvidence{
			0: {Replies: replies[1:], Before: replicaBefore, After: replicaAfter}}},
		{name: "replies beyond the counters", err: "but its counters", evidence: map[int]harness.RBDReadEvidence{
			1: {Replies: reply, Before: primaryBefore, After: primaryAfter}}},
		{name: "counters reset", err: "backwards", evidence: map[int]harness.RBDReadEvidence{
			1: {Before: primaryAfter, After: unreported}}},
		{name: "replica counter reset", err: "replica_read_served went backwards", evidence: map[int]harness.RBDReadEvidence{
			0: {History: entry, Before: counters(t, `{"osd":{"op_r":0,"op_r_out_bytes":0,"replica_read_served":2}}`), After: replicaAfter}}},
		{name: "unreported replica counter", servers: []int{0}, evidence: map[int]harness.RBDReadEvidence{
			0: {History: entry, Before: replicaBefore, After: replicaAfter},
			1: {Before: unreported, After: unreported}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			servers, err := harness.RBDReadServers(tc.evidence, size)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %v, %v, want an error with %q", servers, err, tc.err)
				}
				return
			}
			if err != nil || !slices.Equal(servers, tc.servers) {
				t.Fatalf("got %v, %v, want %v", servers, err, tc.servers)
			}
		})
	}

	e := harness.RBDReadEvidence{Replies: replies, Before: replicaBefore, After: replicaAfter}
	if served, ok := e.ReplicaServed(); !ok || served != 1 || e.OutBytes() != 4194328 || e.ReplyBytes() != 4194336 {
		t.Fatalf("replica evidence: %d %v, %d bytes, %d reply bytes", served, ok, e.OutBytes(), e.ReplyBytes())
	}
	if _, ok := (harness.RBDReadEvidence{Before: unreported, After: replicaAfter}).ReplicaServed(); ok {
		t.Fatal("reported a replica read count from one observation")
	}
	for _, out := range []string{`{}`, `{"osd":{"op_r":1}}`, `{"osd":{"op_r":1,"op_r_out_bytes":-1}}`, `{"osd":{"op_r":1.5,"op_r_out_bytes":1}}`,
		`{"osd":{"op_r":1,"op_r_out_bytes":1,"replica_read_served":"x"}}`, `not json`,
		// JSON null decodes to zero; a null counter is no starting sample.
		`{"osd":{"op_r":null,"op_r_out_bytes":0}}`, `{"osd":{"op_r":0,"op_r_out_bytes":null}}`,
		`{"osd":{"op_r":0,"op_r_out_bytes":0,"replica_read_served":null}}`, `{"osd":{"op_r": null ,"op_r_out_bytes":0}}`} {
		if c, err := harness.ParseOSDReadCounters([]byte(out)); err == nil {
			t.Errorf("accepted %s as %+v", out, c)
		}
	}
}
