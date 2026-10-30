//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Native output saved from TestMessengerCompressionReceipt and
// TestTargetedCommandRecipe, reduced to the fields that the parsers read.
const (
	// osd.0's cluster messenger on quay.io/ceph/ceph:v20.2.4 with
	// ms_cluster_mode=secure, ms_osd_compress_mode=force and
	// ms_compress_secure=true: the accepting side of a lossless session.
	compressedClusterDumpSample = `{"name":"cluster","messenger":{"my_name":{"type":"osd","num":0},"connections_count":1,"connections":[{"addrvec":[{"type":"v2","addr":"172.18.0.5:6801","nonce":414470695}],"async_connection":{"state":"STATE_CONNECTION_ESTABLISHED","status":{"connected":true,"loopback":false},"conn_id":8,"peer":{"entity_name":{"type":0,"type_str":"unknown","id":""},"type":"osd","id":-1,"global_id":0,"addr":{"addrvec":[{"type":"v2","addr":"172.18.0.5:6801","nonce":414470695}]}},"protocol":{"v2":{"state":"READY","con_mode":"unknown","rev1":true,"connect_seq":0,"peer_global_seq":1,"crypto":{"rx":"AES-128-GCM","tx":"AES-128-GCM"},"compression":{"rx":"snappy","tx":"snappy"}}}}}],"anon_conns":[]}}`
	// The same OSD's client messenger: a librados client that asked for
	// compression got an uncompressed CRC session.
	clientSessionDumpSample = `{"name":"client","messenger":{"my_name":{"type":"osd","num":0},"connections_count":6,"connections":[{"addrvec":[{"type":"any","addr":"172.18.0.2:0","nonce":1364117911}],"async_connection":{"state":"STATE_CONNECTION_ESTABLISHED","status":{"connected":true,"loopback":false},"conn_id":33,"peer":{"entity_name":{"type":8,"type_str":"client","id":"admin"},"type":"client","id":4402,"global_id":4402,"addr":{"addrvec":[{"type":"any","addr":"172.18.0.2:0","nonce":1364117911}]}},"protocol":{"v2":{"state":"READY","con_mode":"crc","rev1":true,"connect_seq":0,"peer_global_seq":3,"crypto":{"rx":"PLAIN","tx":"PLAIN"},"compression":{"rx":"UNCOMPRESSED","tx":"UNCOMPRESSED"}}}}}],"anon_conns":[]}}`
	messengerCatalogSample  = `{"status":{"messengers":["client","cluster","hb_back_client","hb_back_server","hb_front_client","hb_front_server","ms_objecter"]}}`
	monSessionsSample       = `[{"name":"osd.0","entity_name":"osd.0","con_type":"osd","con_features":4541880224203014143,"con_features_hex":"3f07fffffffdffff","con_features_release":"squid","open":true,"authenticated":true,"global_id":4133,"global_id_status":"new_ok","osd_epoch":25},{"name":"client.?","entity_name":"client.admin","con_type":"client","con_features":4541880224203014143,"con_features_hex":"3f07fffffffdffff","con_features_release":"squid","open":true,"authenticated":true,"global_id":4402,"global_id_status":"new_ok","osd_epoch":0}]`
	osdStatusReplySample    = `{"cluster_fsid":"a23e4e94-2926-4736-9f17-e4b631ba942c","osd_fsid":"7f6bdfb2-b656-4dfa-b461-41965aad4c39","whoami":1,"state":"active","oldest_map":1,"newest_map":24,"cluster_osdmap_trim_lower_bound":1,"num_pgs":13}`
	pgQueryReplySample      = `{"snap_trimq":"[]","snap_trimq_len":0,"state":"active+clean","epoch":24,"up":[0,1],"acting":[0,1],"acting_recovery_backfill":["0","1"],"info":{"pgid":"4.2","last_update":"0'0"}}`
)

// READY lines that quay.io/ceph/ceph:v19.2.5 OSDs logged at debug_ms 1.
var readyLogSample = []string{
	// The accepting side of a lossless CRC session reports mode "unknown".
	`2026-10-18T11:40:22.434+0000 74c3b5473640  1 --2- v2:172.18.0.5:6801/3239473298 >> v2:172.18.0.4:6801/4152328257 conn(0x5ed2721b3400 0x5ed271691080 unknown :-1 s=READY pgs=1 cs=0 l=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).ready entity=osd.0 client_cookie=99d2edcc66aacbf1 server_cookie=d94392446163ffc in_seq=0 out_seq=0`,
	// ms_osd_compress_mode=force: a compressed CRC heartbeat session.
	`2026-10-18T11:40:35.510+0000 78d2039ea640  1 --2- v2:172.18.0.4:6802/4152328257 >> 172.18.0.5:0/271245517 conn(0x55b570fd6c00 0x55b56f945600 crc :-1 s=READY pgs=1 cs=0 l=1 rev1=1 crypto rx=0 tx=0 comp rx=0x55b570fe6060 tx=0x55b570a85c80).ready entity=osd.1 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`,
	// The client that asked for compression.
	`2026-10-18T11:40:38.875+0000 77a1f6a50640  1 --2- v2:172.18.0.5:6800/271245517 >> 172.18.0.2:0/2069424066 conn(0x560053b89000 0x560054fa8b00 crc :-1 s=READY pgs=3 cs=0 l=1 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).ready entity=client.4258 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`,
	// ms_cluster_mode=secure without ms_compress_secure.
	`2026-10-18T11:40:46.839+0000 78d2019d4640  1 --2- v2:172.18.0.4:6802/4152328257 >> 172.18.0.5:0/1089999799 conn(0x55b57019cc00 0x55b5705c4100 secure :-1 s=READY pgs=1 cs=0 l=1 rev1=1 crypto rx=0x55b570acac90 tx=0x55b57028f860 comp rx=0 tx=0).ready entity=osd.1 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`,
	"2026-10-18T11:40:46.900+0000 78d2019d4640  1 osd.0 25 some other line",
}

func TestParseMessengerDump(t *testing.T) {
	names, err := harness.ParseMessengerCatalog([]byte(messengerCatalogSample))
	if err != nil || !slices.Contains(names, "cluster") || len(names) != 7 {
		t.Fatalf("catalog %v %v", names, err)
	}
	if _, err := harness.ParseMessengerCatalog([]byte(`{"status":{"messengers":[]}}`)); err == nil {
		t.Fatal("an empty catalog was accepted")
	}
	cluster, err := harness.ParseMessengerConnections([]byte(compressedClusterDumpSample))
	if err != nil || len(cluster) != 1 {
		t.Fatalf("cluster %+v %v", cluster, err)
	}
	c := cluster[0]
	if !c.Ready() || c.Messenger != "cluster" || c.PeerType != "osd" || !c.Revision1 || c.CompressionRx != "snappy" || c.CryptoTx != "AES-128-GCM" {
		t.Fatalf("cluster session %+v", c)
	}
	secureCompressed := compressionPhase{name: "secure-compressed", mode: "secure", compressed: true}
	if problems := checkDumpedSession("osd.0", c, secureCompressed); len(problems) != 0 {
		t.Fatal(problems)
	}
	if problems := checkDumpedSession("osd.0", c, compressionPhase{mode: "secure"}); len(problems) != 1 {
		t.Fatalf("an uncompressed expectation accepted snappy: %v", problems)
	}
	if problems := checkDumpedSession("osd.0", c, compressionPhase{mode: "crc", compressed: true}); len(problems) != 1 {
		t.Fatalf("a CRC expectation accepted AES-128-GCM: %v", problems)
	}
	clients, err := harness.ParseMessengerConnections([]byte(clientSessionDumpSample))
	if err != nil || len(clients) != 1 || clients[0].PeerType != "client" || clients[0].GlobalID != 4402 || clients[0].Mode != "crc" {
		t.Fatalf("client %+v %v", clients, err)
	}
	if problems := checkDumpedSession("osd.0", clients[0], secureCompressed); len(problems) != 0 {
		t.Fatal(problems)
	}
	compressedClient := clients[0]
	compressedClient.CompressionTx = "snappy"
	if problems := checkDumpedSession("osd.0", compressedClient, secureCompressed); len(problems) != 1 {
		t.Fatalf("a compressed client session was accepted: %v", problems)
	}
}

func TestParseReadyLogLines(t *testing.T) {
	sessions := harness.ParseReadyLogLines(readyLogSample)
	if len(sessions) != 4 {
		t.Fatalf("%+v", sessions)
	}
	want := []harness.ReadyLogConnection{
		{Mode: "unknown", PeerType: "osd", PeerID: "0"},
		{Mode: "crc", Compression: true, PeerType: "osd", PeerID: "1"},
		{Mode: "crc", PeerType: "client", PeerID: "4258"},
		{Mode: "secure", Crypto: true, PeerType: "osd", PeerID: "1"},
	}
	if !slices.Equal(sessions, want) {
		t.Fatalf("sessions %+v, want %+v", sessions, want)
	}
	force := compressionPhase{mode: "crc", compressed: true}
	secure := compressionPhase{mode: "secure"}
	for i, c := range []struct {
		phase compressionPhase
		ok    bool
	}{{compressionPhase{mode: "crc"}, true}, {force, true}, {force, true}, {secure, true}} {
		if problems := checkLoggedSession("osd.0", sessions[i], c.phase); (len(problems) == 0) != c.ok {
			t.Errorf("session %d under %+v: %v", i, c.phase, problems)
		}
	}
	// A secure expectation rejects the CRC sessions, a compressed one the
	// uncompressed OSD session.
	if problems := checkLoggedSession("osd.0", sessions[1], secure); len(problems) != 2 {
		t.Errorf("CRC compressed session under secure: %v", problems)
	}
	if problems := checkLoggedSession("osd.0", sessions[3], compressionPhase{mode: "secure", compressed: true}); len(problems) != 1 {
		t.Errorf("uncompressed secure session under secure-compressed: %v", problems)
	}
}

func TestParseFeatureSessions(t *testing.T) {
	sessions, err := parseFeatureSessions([]byte(monSessionsSample))
	if err != nil || len(sessions) != 2 {
		t.Fatalf("%+v %v", sessions, err)
	}
	if sessions[0].Entity != "osd.0" || sessions[0].Type != "osd" || sessions[0].Features != 0x3f07fffffffdffff ||
		sessions[1].Type != "client" || sessions[1].GlobalID != 4402 {
		t.Fatalf("%+v", sessions)
	}
	if missing := missingSessionFeatures(sessions[0].Features); len(missing) != 0 {
		t.Fatalf("Tentacle OSD session lacks %v", missing)
	}
	// Bit 21 without the incarnation 2 marker (bit 57) is the retired
	// MON_GV, not SERVER_LUMINOUS or RADOS_BACKOFF.
	missing := missingSessionFeatures(sessions[0].Features &^ (1 << 57))
	for _, name := range []string{"SERVER_LUMINOUS", "RADOS_BACKOFF", "SERVER_SQUID"} {
		if !slices.Contains(missing, name) {
			t.Errorf("without bit 57, %s is still reported present: missing %v", name, missing)
		}
	}
	if slices.Contains(missing, "MSG_ADDR2") {
		t.Errorf("MSG_ADDR2 (incarnation 1) depends on bit 57: %v", missing)
	}
}

func TestParseTargetedReplies(t *testing.T) {
	status, err := parseOSDStatusIdentity([]byte(osdStatusReplySample))
	if err != nil || status.WhoAmI != 1 || status.OSDFSID != "7f6bdfb2-b656-4dfa-b461-41965aad4c39" || status.NewestMap != 24 {
		t.Fatalf("%+v %v", status, err)
	}
	// Some releases print the epochs as strings.
	if quoted, err := parseOSDStatusIdentity([]byte(`{"whoami":0,"newest_map":"14"}`)); err != nil || quoted.NewestMap != 14 {
		t.Fatalf("%+v %v", quoted, err)
	}
	pg, err := parsePGQueryIdentity([]byte(pgQueryReplySample))
	if err != nil || pg.PGID != "4.2" || pg.Epoch != 24 || !slices.Equal(pg.Acting, []int{0, 1}) || pg.State != "active+clean" {
		t.Fatalf("%+v %v", pg, err)
	}
	if _, err := parsePGQueryIdentity([]byte(`null`)); err == nil {
		t.Fatal("an empty reply was accepted")
	}
}

// Client debug_ms 1 lines from TestTargetedCommandRecipe on
// quay.io/ceph/ceph:v20.2.4, one of each channel. The MGR's UnicodeDecodeError
// status ends in a newline, so its log entry spans two lines; the probe joins
// them, and the second line here is written in the same format.
var binaryClientLogSample = []string{
	`2026-10-20T06:19:32.603+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== osd.0 v2:172.18.0.4:6800/3407877150 1 ==== command_reply(tid 1: 0 ) ==== 8+0+233 (crc 0 0 0) 0x561a1b602c70 con 0x561a1b821950`,
	`2026-10-20T06:19:32.605+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== mgr.4106 v2:172.18.0.3:6800/2980602386 1 ==== command_reply(tid 0: -22 unknown command prefix crash ls) ==== 39+0+0 (secure 0 0 0) 0x561a1b5fb890 con 0x7515c003fe90`,
	`2026-10-20T06:19:32.612+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== mgr.4106 v2:172.18.0.3:6800/2980602386 4 ==== mgr_command_reply(tid 3: 0 ) ==== 8+0+98476 (secure 0 0 0) 0x561a1b5b6800 con 0x7515c003fe90`,
	"2026-10-20T06:19:32.613+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== mgr.4106 v2:172.18.0.3:6800/2980602386 5 ==== mgr_command_reply(tid 4: -22 UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff in position 0: invalid start byte\n) ==== 109+0+0 (secure 0 0 0) 0x561a1b5bf6c0 con 0x7515c003fe90",
	`2026-10-20T06:19:32.626+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== mon.0 v2:172.18.0.2:3300/0 2 ==== command_reply(tid 2: -22 unknown command prefix osd getcrushmap) ==== 46+0+0 (secure 0 0 0) 0x7515d801ccf0 con 0x561a1b860a40`,
	// MMonCommandAck prints the result twice for config-key set.
	`2026-10-20T06:19:32.630+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== mon.0 v2:172.18.0.2:3300/0 6 ==== mon_command_ack([{prefix=config-key set, key=tc/binary-io/4275}]=0 set tc/binary-io/4275 v6)=0 set tc/binary-io/4275 v6) ==== 111+0+0 (secure 0 0 0) 0x7515d801b870 con 0x561a1b6a7b90`,
	`2026-10-20T06:19:32.631+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== mon.0 v2:172.18.0.2:3300/0 7 ==== mon_command_ack([{"prefix": "config-key get", "key": "tc/binary-io/4275"}]=0  v6) ==== 90+0+65536 (secure 0 0 0) 0x7515d8002430 con 0x561a1b6a7b90`,
	`2026-10-20T06:19:33.648+0000 7515e57fa640  1 -- 172.18.0.2:0/83799995 <== mon.0 v2:172.18.0.2:3300/0 14 ==== mon_command_ack([{"prefix": "osd setcrushmap", "prior_version": 9}]=-22 Failed to parse crushmap: bad magic number: Malformed input [buffer:3] v25) ==== 153+0+0 (secure 0 0 0) 0x7515d8066110 con 0x561a1b6a7b90`,
}

// The cancellation probe's client log on quay.io/ceph/ceph:v20.2.4 with
// debug_objecter 10: tid 2 cancelled at the timeout, its late reply dropped.
var cancelClientLogSample = []string{
	`2026-10-20T06:31:57.047+0000 7927ece98640  1 -- 172.18.0.2:0/2473815101 <== osd.1 v2:172.18.0.4:6800/521295660 1 ==== command_reply(tid 1: 0 ) ==== 8+0+233 (crc 0 0 0) 0x7927e4027e50 con 0x7927c4064330`,
	`2026-10-20T06:32:03.211+0000 7927f1127640 10 client.4285.objecter command_op_cancel tid 2`,
	`2026-10-20T06:32:03.220+0000 7927ece98640  1 -- 172.18.0.2:0/2473815101 <== osd.1 v2:172.18.0.4:6800/521295660 2 ==== command_reply(tid 2: 0 ) ==== 8+0+233 (crc 0 0 0) 0x5cbb23da4fe0 con 0x7927c4064330`,
	`2026-10-20T06:32:03.220+0000 7927ece98640 10 client.4285.objecter handle_command_reply tid 2 not found`,
	`2026-10-20T06:32:04.342+0000 7927ece98640  1 -- 172.18.0.2:0/2473815101 <== osd.1 v2:172.18.0.4:6800/521295660 3 ==== command_reply(tid 3: 0 ) ==== 8+0+233 (crc 0 0 0) 0x5cbb23da2410 con 0x7927c4064330`,
}

// A MON and an OSD log the same client's requests; the MON names a client
// on its MON session "client.?", the OSD by its global id.
var binaryDaemonLogSample = []string{
	`2026-10-20T06:19:32.629+0000 7861e6f85640  1 -- [v2:172.18.0.2:3300/0,v1:172.18.0.2:6789/0] <== client.? 172.18.0.2:0/83799995 4 ==== mon_command([{prefix=config-key set, key=tc/binary-io/4275}] v 0) ==== 98+0+65536 (secure 0 0 0) 0x55a93e2e4900 con 0x55a93e2ae800`,
	`2026-10-20T06:19:32.602+0000 7a10c2bfe640  1 -- [v2:172.18.0.4:6800/3407877150,v1:172.18.0.4:6801/3407877150] <== client.4275 172.18.0.2:0/83799995 1 ==== command(tid 1: {"prefix": "status", "format": "json"}) ==== 62+0+1048576 (crc 0 0 0) 0x5a3c4e0 con 0x5a3c5a0`,
}

func TestParseCommandWireLines(t *testing.T) {
	lines := parseCommandWireLines(binaryClientLogSample)
	if len(lines) != len(binaryClientLogSample) {
		t.Fatalf("%d lines: %+v", len(lines), lines)
	}
	want := []commandWireLine{
		{From: "osd.0", Kind: "command_reply", Tid: 1, Data: 233, Mode: "crc"},
		{From: "mgr.4106", Kind: "command_reply", Tid: 0, Ret: -22, Mode: "secure"},
		{From: "mgr.4106", Kind: "mgr_command_reply", Tid: 3, Data: 98476, Mode: "secure"},
		{From: "mgr.4106", Kind: "mgr_command_reply", Tid: 4, Ret: -22, Mode: "secure"},
		{From: "mon.0", Kind: "command_reply", Tid: 2, Ret: -22, Mode: "secure"},
		{From: "mon.0", Kind: "mon_command_ack", Prefix: "config-key set", Mode: "secure"},
		{From: "mon.0", Kind: "mon_command_ack", Prefix: "config-key get", Data: 65536, Mode: "secure"},
		{From: "mon.0", Kind: "mon_command_ack", Ret: -22, Prefix: "osd setcrushmap", Mode: "secure"},
	}
	for i, line := range lines {
		if line.To != "172.18.0.2:0/83799995" {
			t.Errorf("line %d: local address %q", i, line.To)
		}
		line.To, line.Addr, line.Front = "", "", 0
		if line != want[i] {
			t.Errorf("line %d: %+v, want %+v", i, line, want[i])
		}
	}
	daemon := parseCommandWireLines(binaryDaemonLogSample)
	if len(daemon) != 2 || daemon[0].From != "client.?" || daemon[0].Addr != "172.18.0.2:0/83799995" ||
		daemon[0].Kind != "mon_command" || daemon[0].Prefix != "config-key set" || daemon[0].Data != 65536 ||
		daemon[1].Kind != "command" || daemon[1].Tid != 1 || daemon[1].Data != 1<<20 {
		t.Fatalf("%+v", daemon)
	}
}

func TestBindWireReplies(t *testing.T) {
	calls := func() []binaryCall {
		return []binaryCall{
			{Label: "osd-status", Channel: "osd", Bytes: 233},
			{Label: "mgr-tell", Channel: "mgr-tell", Ret: -22},
			{Label: "crash-info", Channel: "mgr", Bytes: 98476},
			{Label: "crash-post-not-utf8", Channel: "mgr", Ret: -22, InBytes: 98477},
			{Label: "mon-tell", Channel: "mon-tell", Ret: -22},
			{Label: "set", Channel: "mon", Prefix: "config-key set", InBytes: 65536},
			{Label: "get", Channel: "mon", Prefix: "config-key get", Bytes: 65536},
			{Label: "bad-magic", Channel: "mon", Prefix: "osd setcrushmap", Ret: -22, InBytes: 1030},
		}
	}
	lines := parseCommandWireLines(binaryClientLogSample)
	bound := calls()
	if err := bindWireReplies(bound, lines); err != nil {
		t.Fatal(err)
	}
	for _, call := range bound {
		if call.ReplyFrom == "" {
			t.Errorf("%s has no reply", call.Label)
		}
	}
	if bound[2].ReplyFrom != "mgr.4106" || bound[6].ReplyFrom != "mon.0" {
		t.Fatalf("%+v", bound)
	}
	short := calls()
	short[6].Bytes = 65535
	if err := bindWireReplies(short, lines); err == nil {
		t.Fatal("a reply one byte longer than the output buffer was accepted")
	}
	swapped := calls()
	swapped[5].Prefix, swapped[6].Prefix = swapped[6].Prefix, swapped[5].Prefix
	if err := bindWireReplies(swapped, lines); err == nil {
		t.Fatal("replies for other prefixes were accepted")
	}
	if err := bindWireReplies(calls()[:7], lines); err == nil {
		t.Fatal("a reply without a call was accepted")
	}

	// The daemons received the inputs from the client's address.
	daemon := parseCommandWireLines(binaryDaemonLogSample)
	if err := bindWireRequests("172.18.0.2:0/83799995", []binaryCall{
		{Label: "osd-status", Channel: "osd", InBytes: 1 << 20}, {Label: "set", Channel: "mon", Prefix: "config-key set", InBytes: 65536},
	}, daemon, map[string]int{"osd-status": 1}); err != nil {
		t.Fatal(err)
	}
	if err := bindWireRequests("172.18.0.2:0/83799995", []binaryCall{
		{Label: "set", Channel: "mon", Prefix: "config-key set", InBytes: 65536}, {Label: "again", Channel: "mon", Prefix: "config-key set", InBytes: 65536},
	}, daemon, nil); err == nil {
		t.Fatal("one request line answered for two calls")
	}
	if err := bindWireRequests("172.18.0.2:0/1", []binaryCall{{Label: "set", Channel: "mon", Prefix: "config-key set", InBytes: 65536}}, daemon, nil); err == nil {
		t.Fatal("another client's request was accepted")
	}
	if err := bindWireRequests("172.18.0.2:0/83799995", []binaryCall{{Label: "osd-status", Channel: "osd", InBytes: 1 << 20}}, daemon, map[string]int{"osd-status": 2}); err == nil {
		t.Fatal("a request with another tid was accepted")
	}
}

func TestCancelledTid(t *testing.T) {
	tid, late, err := cancelledTid(cancelClientLogSample)
	if err != nil || tid != 2 || late.From != "osd.1" || late.Data != 233 || late.Ret != 0 {
		t.Fatalf("tid %d late %+v: %v", tid, late, err)
	}
	if addr := clientAddr(t, cancelClientLogSample); addr != "172.18.0.2:0/2473815101" {
		t.Fatalf("client address %q", addr)
	}
	kept := slices.Delete(slices.Clone(cancelClientLogSample), 3, 4)
	if _, _, err := cancelledTid(kept); err == nil {
		t.Fatal("a late reply that the client did not drop was accepted")
	}
	early := []string{cancelClientLogSample[2], cancelClientLogSample[1], cancelClientLogSample[3]}
	if _, _, err := cancelledTid(early); err == nil {
		t.Fatal("a reply before the cancellation was taken as late")
	}
	if _, _, err := cancelledTid(cancelClientLogSample[:1]); err == nil {
		t.Fatal("a log without a cancellation was accepted")
	}
}
