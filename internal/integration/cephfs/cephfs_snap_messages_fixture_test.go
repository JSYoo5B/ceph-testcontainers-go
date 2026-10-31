//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"slices"
	"testing"
)

// client_snap lines a libcephfs client logs at debug_client 10: a split that
// creates the directory's own realm (inode 1099511627776), an update of an
// ancestor realm, and lines that must not count (a cap update, an unrelated
// message).
var snapMessageSample = []string{
	`2026-10-31T00:54:04.846+0000 ffff89fbf120 10 client.4233 handle_snap client_snap(update split=0x3 tracelen=78)`,
	`2026-10-31T00:54:04.846+0000 ffff89fbf120 10 client.4233 handle_snap client_snap(split split=0x10000000000 tracelen=148)`,
	`2026-10-31T00:54:04.846+0000 ffff89fbf120 10 client.4233 handle_snap client_snap(destroy split=0x3 tracelen=70)`,
	`2026-10-31T00:54:04.900+0000 ffff89fbf120 10 client.4233 handle_cap_grant on 0x1 client_caps(grant ino 0x10000000000)`,
	`2026-10-31T00:54:04.901+0000 ffff89fbf120  1 -- 172.19.0.7:0/1 <== mds.0 v2:1 5 ==== client_session(open) ==== 0+0+0`,
}

func TestParseSnapMessages(t *testing.T) {
	messages := parseSnapMessages(snapMessageSample)
	if len(messages) != 3 {
		t.Fatalf("messages = %+v", messages)
	}
	ops := []string{messages[0].Op, messages[1].Op, messages[2].Op}
	if !slices.Equal(ops, []string{"update", "split", "destroy"}) {
		t.Fatalf("ops = %v", ops)
	}
	if messages[0].Split != 0x3 || messages[1].Split != 0x10000000000 || messages[2].Split != 0x3 {
		t.Fatalf("split inodes = %d %d %d", messages[0].Split, messages[1].Split, messages[2].Split)
	}
	// The directory realm (1099511627776 = 0x10000000000) is split off by its
	// first snapshot.
	if !slices.ContainsFunc(messages, func(m snapMessage) bool { return m.Op == "split" && m.Split == 1099511627776 }) {
		t.Fatalf("no split of the directory realm: %+v", messages)
	}
	if n := len(parseSnapMessages(snapMessageSample[3:])); n != 0 {
		t.Fatalf("non-client_snap lines parsed %d messages", n)
	}
}
