//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import "testing"

// Lines in the format a debug_ms 20 log prints, one of each keepalive and
// ack frame kind on a ProtocolV2 session, plus lines that must not count: a
// plain READY state line, a v1 line, and the "appending keepalive" write
// event (which is not a sent frame).
var keepaliveSampleClient = []string{
	`2026-10-31T02:00:00.000+0000 ffff00 10 --2- 172.19.0.7:0/1 >> v2:172.19.0.2:3300/0 conn(0x1 0x2 secure :-1 s=READY pgs=1 gs=1 cs=0 l=1 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).write_event appending keepalive`,
	`2026-10-31T02:00:00.100+0000 ffff00 10 --2- 172.19.0.7:0/1 >> v2:172.19.0.2:3300/0 conn(0x1 0x2 secure :-1 s=READY ...).send_keepalive`,
	`2026-10-31T02:00:00.200+0000 ffff00 20 --2- 172.19.0.7:0/1 >> v2:172.19.0.2:3300/0 conn(0x1 0x2 secure :-1 s=READY ...).handle_keepalive2_ack got KEEPALIVE_ACK`,
	`2026-10-31T02:00:00.300+0000 ffff00 10 --2- 172.19.0.7:0/1 >> v2:172.19.0.6:6800/3 conn(0x3 0x4 secure :-1 s=READY ...).handle_message_ack got ack seq 7 >= 7 on 0x5 client_caps(...)`,
	`2026-10-31T02:00:00.400+0000 ffff00 10 --2- 172.19.0.7:0/1 >> v2:172.19.0.6:6800/3 conn(0x3 0x4 secure :-1 s=READY ...).write_event try send msg ack, acked 2 messages`,
	`2026-10-31T02:00:00.500+0000 ffff00 10 --1- 172.19.0.7:0/1 >> v1:172.19.0.2:6789/0 conn(0x9).send_keepalive`,
	`2026-10-31T02:00:00.600+0000 ffff00 10 --2- 172.19.0.7:0/1 >> v2:172.19.0.2:3300/0 conn(0x1 0x2 secure :-1 s=READY pgs=1).ready`,
}

var keepaliveSampleMon = []string{
	`2026-10-31T02:00:01.000+0000 ffff00 20 --2- [v2:172.19.0.2:3300/0,v1:172.19.0.2:6789/0] >> 172.19.0.7:0/1 conn(0x1 0x2 secure :-1 s=READY ...).handle_keepalive2 got KEEPALIVE2 2026-10-31T02:00:00.900+0000`,
	`2026-10-31T02:00:01.100+0000 ffff00 10 --2- [v2:172.19.0.2:3300/0,v1:172.19.0.2:6789/0] >> 172.19.0.7:0/1 conn(0x1 0x2 secure :-1 s=READY ...).write_event try send msg ack, acked 1 messages`,
}

func TestCountKeepaliveFrames(t *testing.T) {
	client := countKeepaliveFrames(keepaliveSampleClient, "")
	if client.SentKeepalive != 1 || client.GotKeepaliveAck != 1 || client.GotMessageAck != 1 || client.SentMessageAck != 1 || client.GotKeepalive != 0 {
		t.Fatalf("client frames = %+v", client)
	}
	mon := countKeepaliveFrames(keepaliveSampleMon, "")
	if mon.GotKeepalive != 1 || mon.SentMessageAck != 1 {
		t.Fatalf("mon frames = %+v", mon)
	}
	// A peer filter selects one connection: the client's MON session carries
	// the keepalive, its OSD/MDS session the message acks.
	if monSide := countKeepaliveFrames(keepaliveSampleClient, "v2:172.19.0.2:3300"); monSide.SentKeepalive != 1 || monSide.GotMessageAck != 0 {
		t.Fatalf("MON-session frames = %+v", monSide)
	}
	if dataSide := countKeepaliveFrames(keepaliveSampleClient, "v2:172.19.0.6:6800"); dataSide.GotMessageAck != 1 || dataSide.SentKeepalive != 0 {
		t.Fatalf("data-session frames = %+v", dataSide)
	}
}
