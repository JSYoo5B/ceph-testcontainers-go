//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"
)

// Client and primary log lines of one PG-wide backoff, in the format that
// the messenger prints at debug_ms 1 and the Objecter at debug_objecter 1.
// The BLOCK, ACK_BLOCK, UNBLOCK and reply lines follow those that
// TestOSDBackoffReceipt logged on quay.io/ceph/ceph:v20.2.4, with other
// addresses, IDs and epochs; the rest are written in the same format:
// read tid 3 of backoff-read is blocked by osd.1 with id 1 at epoch 25,
// acknowledged at epoch 25, unblocked at epoch 27 and answered afterwards.
// Unrelated MON and OSD traffic stays in between.
var (
	backoffClientSample = []string{
		`2026-10-22T05:10:01.100+0000 7f1c0b7fe640  1 -- 172.18.0.2:0/2208442118 --> [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] -- osd_op(unknown.0.0:2 2.0 2:9a3f0c1e:::backoff-read:head [stat] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e24) -- 0x55d0c8e1a400 con 0x55d0c8e18000`,
		`2026-10-22T05:10:01.104+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== osd.1 v2:172.18.0.5:6800/1418922307 1 ==== osd_op_reply(2 backoff-read [stat] v0'0 uv1 ondisk = 0) ==== 160+0+16 (crc 0 0 0) 0x7f1bf4003a10 con 0x55d0c8e18000`,
		`2026-10-22T05:10:20.310+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== mon.0 v2:172.18.0.3:3300/0 9 ==== osd_map(25..25 src has 1..25) ==== 1432+0+0 (secure 0 0 0) 0x7f1bf4004f20 con 0x55d0c8dd9800`,
		`2026-10-22T05:10:21.002+0000 7f1c0b7fe640  1 -- 172.18.0.2:0/2208442118 --> [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] -- osd_op(unknown.0.0:3 2.0 2:9a3f0c1e:::backoff-read:head [read 0~16385] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e25) -- 0x55d0c8e1b800 con 0x55d0c8e18000`,
		`2026-10-22T05:10:21.006+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== osd.1 v2:172.18.0.5:6800/1418922307 2 ==== osd_backoff(2.0 block id 1 [2:00000000::::0,MAX) e25) ==== 115+0+0 (crc 0 0 0) 0x7f1bf4005b30 con 0x55d0c8e18000`,
		`2026-10-22T05:10:21.006+0000 7f1c0affd640 10 client.4321.objecter handle_osd_backoff osd_backoff(2.0 block id 1 [2:00000000::::0,MAX) e25)`,
		`2026-10-22T05:10:21.006+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 --> [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] -- osd_backoff(2.0 ack-block id 1 [2:00000000::::0,MAX) e25) -- 0x7f1bf4006c40 con 0x55d0c8e18000`,
		`2026-10-22T05:10:36.420+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== mon.0 v2:172.18.0.3:3300/0 10 ==== osd_map(26..27 src has 1..27) ==== 2210+0+0 (secure 0 0 0) 0x7f1bf4007d50 con 0x55d0c8dd9800`,
		`2026-10-22T05:10:37.512+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== osd.1 v2:172.18.0.5:6800/1418922307 3 ==== osd_backoff(2.0 unblock id 1 [2:00000000::::0,MAX) e27) ==== 115+0+0 (crc 0 0 0) 0x7f1bf4008e60 con 0x55d0c8e18000`,
		`2026-10-22T05:10:37.512+0000 7f1c0affd640 10 client.4321.objecter handle_osd_backoff unblock backoff 2.0 id 1 [2:00000000::::0,MAX)`,
		`2026-10-22T05:10:37.512+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 --> [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] -- osd_op(unknown.0.0:3 2.0 2:9a3f0c1e:::backoff-read:head [read 0~16385] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e27) -- 0x55d0c8e1c000 con 0x55d0c8e18000`,
		`2026-10-22T05:10:38.020+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== osd.1 v2:172.18.0.5:6800/1418922307 4 ==== osd_op_reply(3 backoff-read [read 0~16384 out=16384b] v0'0 uv1 ondisk = 0) ==== 160+0+16384 (crc 0 0 0) 0x7f1bf4009f70 con 0x55d0c8e18000`,
	}
	backoffPrimarySample = []string{
		`2026-10-22T05:10:01.102+0000 7f0e2d7fa640  1 -- [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] <== client.4321 172.18.0.2:0/2208442118 1 ==== osd_op(client.4321.0:2 2.0 2.783cfc59 (undecoded) ondisk+read+known_if_redirected+supports_pool_eio e24) ==== 196+0+0 (crc 0 0 0) 0x5612d0a3c000 con 0x5612d09f4800`,
		`2026-10-22T05:10:21.004+0000 7f0e2d7fa640  1 -- [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] <== client.4321 172.18.0.2:0/2208442118 2 ==== osd_op(client.4321.0:3 2.0 2.783cfc59 (undecoded) ondisk+read+known_if_redirected+supports_pool_eio e25) ==== 196+0+0 (crc 0 0 0) 0x5612d0a3d400 con 0x5612d09f4800`,
		`2026-10-22T05:10:21.005+0000 7f0e1effd640  1 -- [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] --> 172.18.0.2:0/2208442118 -- osd_backoff(2.0 block id 1 [2:00000000::::0,MAX) e25) -- 0x5612d0a3e800 con 0x5612d09f4800`,
		`2026-10-22T05:10:21.008+0000 7f0e2d7fa640  1 -- [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] <== client.4321 172.18.0.2:0/2208442118 3 ==== osd_backoff(2.0 ack-block id 1 [2:00000000::::0,MAX) e25) ==== 115+0+0 (crc 0 0 0) 0x5612d0a3fc00 con 0x5612d09f4800`,
		`2026-10-22T05:10:30.660+0000 7f0e2cff9640  1 -- [v2:172.18.0.5:6802/1418922307,v1:172.18.0.5:6803/1418922307] <== osd.0 v2:172.18.0.4:6802/88231045 12 ==== pg_notify2(2.0 2.0 (query:27 sent:27 ...) e27/27) ==== 1065+0+0 (crc 0 0 0) 0x5612d0a41000 con 0x5612d09f6c00`,
	}
	backoffSampleTarget = backoffTarget{Client: "client.4321", Primary: "osd.1", PGID: "2.0", Object: "backoff-read", Op: "read", Scope: "pg"}
)

// backoffLines returns lines with each of the replacements applied once.
func backoffLines(t *testing.T, lines []string, replacements ...string) []string {
	t.Helper()
	result := slices.Clone(lines)
	for index := 0; index < len(replacements); index += 2 {
		found := false
		for i, line := range result {
			if strings.Contains(line, replacements[index]) {
				result[i], found = strings.Replace(line, replacements[index], replacements[index+1], 1), true
				break
			}
		}
		if !found {
			t.Fatalf("no line holds %q", replacements[index])
		}
	}
	return result
}

// withoutLines drops the lines that contain any of the given texts.
func withoutLines(lines []string, texts ...string) []string {
	return slices.DeleteFunc(slices.Clone(lines), func(line string) bool {
		return slices.ContainsFunc(texts, func(text string) bool { return strings.Contains(line, text) })
	})
}

// bindBackoffSample binds the whole chain of the given logs.
func bindBackoffSample(client, primary []string) (backoffChain, error) {
	clientEvents := parseBackoffLog(client)
	chain, err := bindBackoffBlock(clientEvents, parseBackoffLog(primary), backoffSampleTarget)
	if err != nil {
		return chain, err
	}
	return chain, bindBackoffRelease(&chain, clientEvents, backoffSampleTarget)
}

func TestParseBackoffLog(t *testing.T) {
	events := parseBackoffLog(backoffClientSample)
	var kinds []string
	for _, e := range events {
		kinds = append(kinds, e.Dir+" "+e.Kind+" "+e.Op)
	}
	want := []string{"out osd_op ", "in osd_op_reply ", "out osd_op ", "in backoff block", "out backoff ack-block", "in backoff unblock", "out osd_op ", "in osd_op_reply "}
	if !slices.Equal(kinds, want) {
		t.Fatalf("events %q, want %q", kinds, want)
	}
	block := events[3]
	if block.Peer != "osd.1" || block.Addr != "172.18.0.5:6800/1418922307" || block.Local != "172.18.0.2:0/2208442118" || block.Seq != 2 ||
		block.PGID != "2.0" || block.ID != 1 || block.Begin != "2:00000000::::0" || block.End != "MAX" || block.Epoch != 25 {
		t.Fatalf("block = %+v", block)
	}
	if read := events[2]; read.Tid != 3 || read.Object != "backoff-read" || read.Ops != "read 0~16385" || read.Source != "unknown.0" {
		t.Fatalf("read = %+v", read)
	}
	if reply := events[7]; reply.Tid != 3 || reply.Object != "backoff-read" || reply.Result != 0 || reply.Ops != "read 0~16384 out=16384b" {
		t.Fatalf("reply = %+v", reply)
	}
	primary := parseBackoffLog(backoffPrimarySample)
	if len(primary) != 4 || primary[1].Source != "client.4321" || primary[1].Tid != 3 || primary[1].Object != "" ||
		primary[3].Op != "ack-block" || primary[3].Peer != "client.4321" || primary[3].Addr != "172.18.0.2:0/2208442118" {
		t.Fatalf("primary events %+v", primary)
	}
	errorReply := parseBackoffLog([]string{`2026-10-22T05:10:38.020+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== osd.1 v2:172.18.0.5:6800/1418922307 4 ==== osd_op_reply(3 backoff-read [read 0~16385] v0'0 uv0 ondisk = -2 ((2) No such file or directory)) ==== 160+0+0 (crc 0 0 0) 0x7f1bf4009f70 con 0x55d0c8e18000`})
	if len(errorReply) != 1 || errorReply[0].Result != -2 {
		t.Fatalf("error reply %+v", errorReply)
	}
}

func TestBindBackoffChain(t *testing.T) {
	chain, err := bindBackoffSample(backoffClientSample, backoffPrimarySample)
	if err != nil {
		t.Fatal(err)
	}
	if chain.ClientAddr != "172.18.0.2:0/2208442118" || chain.Request.Tid != 3 || chain.Block.ID != 1 || chain.Block.Epoch != 25 ||
		chain.AckSent == nil || chain.AckReceived.Epoch != 25 || chain.OpAtPrimary.Tid != 3 ||
		chain.Unblock.Epoch != 27 || chain.Reply.Result != 0 || len(chain.ExtraBlocks) != 0 || len(chain.Reconnects) != 0 {
		t.Fatalf("chain = %+v", chain)
	}
	// Before the replica returns, the chain binds up to ACK_BLOCK and is not
	// yet released.
	held := withoutLines(backoffClientSample, "unblock", "e27) --", "out=16384b")
	heldEvents := parseBackoffLog(held)
	if chain, err := bindBackoffBlock(heldEvents, parseBackoffLog(backoffPrimarySample), backoffSampleTarget); err != nil || chain.released(heldEvents) {
		t.Fatalf("held chain released=%t: %v", chain.released(heldEvents), err)
	}
	if chain, _ := bindBackoffBlock(parseBackoffLog(backoffClientSample), parseBackoffLog(backoffPrimarySample), backoffSampleTarget); !chain.released(parseBackoffLog(backoffClientSample)) {
		t.Fatal("a chain with its UNBLOCK was not released")
	}

	for name, test := range map[string]struct {
		client, primary []string
		want            string
	}{
		"no read":           {withoutLines(backoffClientSample, "[read 0~16385]"), backoffPrimarySample, "no read of backoff-read"},
		"missing BLOCK":     {withoutLines(backoffClientSample, "block id 1 [2:00000000::::0,MAX) e25) ===="), backoffPrimarySample, "no BLOCK"},
		"BLOCK of other PG": {backoffLines(t, backoffClientSample, "osd_backoff(2.0 block", "osd_backoff(2.1 block"), backoffPrimarySample, "no BLOCK"},
		"BLOCK from other OSD": {backoffLines(t, backoffClientSample, "<== osd.1 v2:172.18.0.5:6800/1418922307 2 ====", "<== osd.2 v2:172.18.0.6:6800/77 2 ===="),
			backoffPrimarySample, "no BLOCK"},
		"missing ACK_BLOCK":  {backoffClientSample, withoutLines(backoffPrimarySample, "ack-block"), "no ACK_BLOCK"},
		"ACK_BLOCK other id": {backoffClientSample, backoffLines(t, backoffPrimarySample, "ack-block id 1", "ack-block id 2"), "no ACK_BLOCK"},
		"ACK_BLOCK other client": {backoffClientSample, backoffLines(t, backoffPrimarySample, "<== client.4321 172.18.0.2:0/2208442118 3", "<== client.4400 172.18.0.2:0/2208442118 3"),
			"no ACK_BLOCK"},
		"ACK_BLOCK other epoch": {backoffClientSample, backoffLines(t, backoffPrimarySample, "ack-block id 1 [2:00000000::::0,MAX) e25", "ack-block id 1 [2:00000000::::0,MAX) e27"),
			"carries epoch 27"},
		"ACK_BLOCK on another connection": {backoffClientSample, withoutLines(backoffPrimarySample, "client.4321.0:3"), "connection that carried read tid 3"},
		"missing UNBLOCK":                 {withoutLines(backoffClientSample, "unblock id 1 [2:00000000::::0,MAX) e27) ===="), backoffPrimarySample, "no UNBLOCK"},
		"UNBLOCK other id":                {backoffLines(t, backoffClientSample, "unblock id 1", "unblock id 2"), backoffPrimarySample, "other UNBLOCK ids [2]"},
		"UNBLOCK other range":             {backoffLines(t, backoffClientSample, "unblock id 1 [2:00000000::::0,MAX)", "unblock id 1 [2:00000000::::0,2:80000000::::0)"), backoffPrimarySample, "no UNBLOCK"},
		"UNBLOCK older epoch":             {backoffLines(t, backoffClientSample, "unblock id 1 [2:00000000::::0,MAX) e27", "unblock id 1 [2:00000000::::0,MAX) e24"), backoffPrimarySample, "precedes BLOCK epoch"},
		"reply before UNBLOCK": {append(withoutLines(backoffClientSample, "unblock id 1 [2:00000000::::0,MAX) e27) ====", "out=16384b"),
			strings.Replace(backoffClientSample[11], " 4 ==== ", " 3 ==== ", 1), strings.Replace(backoffClientSample[8], " 3 ==== ", " 4 ==== ", 1)),
			backoffPrimarySample, "answered before the UNBLOCK"},
		"no reply":      {withoutLines(backoffClientSample, "out=16384b"), backoffPrimarySample, "no reply to read tid 3"},
		"failed reply":  {backoffLines(t, backoffClientSample, "ondisk = 0) ==== 160+0+16384", "ondisk = -5) ==== 160+0+16384"), backoffPrimarySample, "result -5"},
		"reset between": {slices.Insert(slices.Clone(backoffClientSample), 8, `2026-10-22T05:10:30.000+0000 7f1c0affd640  1 client.4321.objecter ms_handle_reset 0x55d0c8e18000 session 0x55d0c8e19000 osd.1`), backoffPrimarySample, "changed between BLOCK"},
		"reconnect between": {backoffLines(t, backoffClientSample, "<== osd.1 v2:172.18.0.5:6800/1418922307 3 ====", "<== osd.1 v2:172.18.0.5:6800/1418922307 1 ===="),
			backoffPrimarySample, "changed between BLOCK"},
	} {
		if _, err := bindBackoffSample(test.client, test.primary); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: %v, want an error containing %q", name, err, test.want)
		}
	}
}

// Reconnects and blocks that do not touch the chain are kept as noise and
// do not break it; a backoff ID of an older connection joins nothing.
func TestBackoffChainNoise(t *testing.T) {
	stale := []string{
		// An earlier connection of the same client to the primary carried
		// an ACK_BLOCK with the same ID, for another read.
		`2026-10-22T05:09:50.000+0000 7f0e2d7fa640  1 -- [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] <== client.4321 172.18.0.2:0/2208442118 1 ==== osd_op(client.4321.0:1 2.0 2.783cfc59 (undecoded) ondisk+read+known_if_redirected+supports_pool_eio e23) ==== 196+0+0 (crc 0 0 0) 0x5612d0a3c000 con 0x5612d09f2000`,
		`2026-10-22T05:09:50.004+0000 7f0e2d7fa640  1 -- [v2:172.18.0.5:6800/1418922307,v1:172.18.0.5:6801/1418922307] <== client.4321 172.18.0.2:0/2208442118 2 ==== osd_backoff(2.0 ack-block id 1 [2:00000000::::0,MAX) e25) ==== 115+0+0 (crc 0 0 0) 0x5612d0a3fc00 con 0x5612d09f2000`,
	}
	primary := append(slices.Clone(stale), backoffPrimarySample...)
	client := slices.Clone(backoffClientSample)
	// The MON session and the stopped replica's session reset, and a BLOCK of
	// another pool's PG arrives from another OSD.
	client = slices.Insert(client, 3,
		`2026-10-22T05:10:20.400+0000 7f1c0affd640  1 client.4321.objecter ms_handle_reset 0x55d0c8e30000 session 0x55d0c8e31000 osd.2`,
		`2026-10-22T05:10:20.500+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== mon.0 v2:172.18.0.3:3300/0 1 ==== mon_map magic: 0 v1 ==== 467+0+0 (secure 0 0 0) 0x7f1bf4004f20 con 0x55d0c8dd9c00`,
		`2026-10-22T05:10:20.600+0000 7f1c0affd640  1 -- 172.18.0.2:0/2208442118 <== osd.2 v2:172.18.0.6:6800/77 1 ==== osd_backoff(3.0 block id 1 [3:00000000::::0,MAX) e25) ==== 115+0+0 (crc 0 0 0) 0x7f1bf4005b30 con 0x55d0c8e30000`,
	)
	chain, err := bindBackoffSample(client, primary)
	if err != nil {
		t.Fatal(err)
	}
	if chain.AckReceived.Gen != 1 || chain.OpAtPrimary.Gen != 1 || len(chain.ExtraBlocks) != 1 || chain.ExtraBlocks[0].Peer != "osd.2" {
		t.Fatalf("chain = %+v", chain)
	}
	var reconnects []string
	for _, e := range chain.Reconnects {
		reconnects = append(reconnects, e.Kind+" "+e.Peer)
	}
	if !slices.Equal(reconnects, []string{"reset osd.2", "other mon.0", "osd_op client.4321"}) {
		t.Fatalf("reconnects %q", reconnects)
	}
	// Without the current connection's ACK_BLOCK, the stale one joins
	// nothing.
	if _, err := bindBackoffSample(client, withoutLines(primary, "05:10:21.008")); err == nil || !strings.Contains(err.Error(), "connection that carried read tid 3") {
		t.Fatalf("a stale ACK_BLOCK joined: %v", err)
	}
	if !peeredInactive("undersized+degraded+peered") || peeredInactive("active+undersized+degraded") || peeredInactive("peering") {
		t.Fatal("peeredInactive misclassified a state")
	}
}

// squidObjectBlock is the BLOCK that a client received in one Squid 19.2.5
// run of TestOSDObjectBackoffReceipt: a backoff of backoff-write alone.
const squidObjectBlock = `2026-10-30T23:53:59.078+0000 ffff7efdf120  1 -- 172.19.0.6:0/3579350518 <== osd.0 v2:172.19.0.4:6800/2973482461 2 ==== osd_backoff(2.0 block id 1 [2:756179b6:::backoff-write:head,2:756179b6:::backoff-write:head) e22) ==== 141+0+0 (crc 0 0 0) 0xffff80009bb0 con 0xaaaac677fe30`

func TestBackoffTargetCovers(t *testing.T) {
	events := parseBackoffLog([]string{squidObjectBlock, backoffClientSample[4]})
	if len(events) != 2 || events[0].Kind != "backoff" || events[1].Kind != "backoff" {
		t.Fatalf("events %+v", events)
	}
	objectBlock, pgBlock := events[0], events[1]
	object := backoffTarget{PGID: "2.0", Object: "backoff-write", Op: "writefull", Scope: "object"}
	pg := backoffTarget{PGID: "2.0", Object: "backoff-read", Op: "read", Scope: "pg"}
	for name, c := range map[string]struct {
		target backoffTarget
		event  backoffEvent
		want   bool
	}{
		"object block, object scope": {object, objectBlock, true},
		"PG block, PG scope":         {pg, pgBlock, true},
		"PG block, object scope":     {object, pgBlock, false},
		"object block, PG scope":     {pg, objectBlock, false},
		"another object":             {backoffTarget{PGID: "2.0", Object: "backoff", Scope: "object"}, objectBlock, false},
		"another pool":               {backoffTarget{PGID: "3.0", Object: "backoff-write", Scope: "object"}, objectBlock, false},
		"no scope":                   {backoffTarget{PGID: "2.0", Object: "backoff-write"}, objectBlock, false},
	} {
		if got := c.target.covers(c.event); got != c.want {
			t.Errorf("%s: covers %t, want %t", name, got, c.want)
		}
	}

	// A PG-wide BLOCK does not stand in for an object-scoped one.
	target := backoffSampleTarget
	target.Scope = "object"
	if _, err := bindBackoffBlock(parseBackoffLog(backoffClientSample), parseBackoffLog(backoffPrimarySample), target); err == nil || !strings.Contains(err.Error(), "object scope") {
		t.Fatalf("object scope bound a PG-wide BLOCK: %v", err)
	}
	// Nor does a held write stand in for the read.
	target = backoffSampleTarget
	target.Op = "writefull"
	if _, err := bindBackoffBlock(parseBackoffLog(backoffClientSample), parseBackoffLog(backoffPrimarySample), target); err == nil || !strings.Contains(err.Error(), "no writefull") {
		t.Fatalf("a read bound a write chain: %v", err)
	}
}
