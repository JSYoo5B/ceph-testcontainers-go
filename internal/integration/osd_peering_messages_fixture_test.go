//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"
)

// The lines below are what one Squid 19.2.5 run of TestOSDPeeringMessages
// judged: the joined message lines and the entered peering and scrubber
// states of PG 2.0, created on osd.0 (primary) and osd.1 (replica) and deep
// scrubbed. Every other line of the daemons' logs is left out.

// squidPeeringMON holds the MON's pg_create2 sends lines about PG 2.0 from that run, in log order.
var squidPeeringMON = []string{
	`2026-10-30T23:44:53.722+0000 ffff873dfac0  1 -- [v2:172.19.0.2:3300/0,v1:172.19.0.2:6789/0] --> [v2:172.19.0.5:6800/2996635675,v1:172.19.0.5:6801/2996635675] -- pg_create2(e11 {2.0=11,2026-10-30T23:44:53.710052+0000}) -- 0xaaaaf12a52c0 con 0xaaaaf13f3c00`,
}

// squidPeeringPrimary holds the primary's (osd.0) lines about PG 2.0 from that run, in log order.
var squidPeeringPrimary = []string{
	`2026-10-30T23:44:53.722+0000 ffffb3ffd0c0  1 -- [v2:172.19.0.5:6800/2996635675,v1:172.19.0.5:6801/2996635675] <== mon.0 v2:172.19.0.2:3300/0 14 ==== pg_create2(e11 {2.0=11,2026-10-30T23:44:53.710052+0000}) ==== 207+0+0 (secure 0 0 0) 0xaaaacd6c92c0 con 0xaaaace6a3400`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  5 osd.0 pg_epoch: 11 pg[2.0(unlocked)] enter Initial`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0 10 osd.0 pg_epoch: 11 pg[2.0(unlocked)] scrubber<NotActive>: FSM: -- state -->> NotActive`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=0 crt=0'0 mlcod 0'0 unknown mbc={}] enter Reset`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Started`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Start`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Started/Primary`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 creating mbc={}] enter Started/Primary/Peering`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 creating+peering mbc={}] enter Started/Primary/Peering/GetInfo`,
	`2026-10-30T23:44:53.916+0000 ffff999940c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] --> [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] -- pg_query2(2.0 2.0 query(info 0'0 epoch_sent 11) e11/11) -- 0xaaaacfd36fc0 con 0xaaaace7f0800`,
	`2026-10-30T23:44:53.917+0000 ffffb3ffd0c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] <== osd.1 v2:172.19.0.4:6802/2152726140 5 ==== pg_notify2(2.0 2.0 (query:11 sent:11 2.0( DNE empty local-lis/les=0/0 n=0 ec=0/0 lis/c=0/0 les/c/f=0/0/0 sis=0) ([0,0] all_participants={} intervals=[])) e11/11) ==== 1062+0+0 (crc 0 0 0) 0xaaaacd896300 con 0xaaaace7f0800`,
	`2026-10-30T23:44:53.917+0000 ffff989740c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 creating+peering mbc={}] enter Started/Primary/Peering/GetLog`,
	`2026-10-30T23:44:53.917+0000 ffff989740c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 creating+peering mbc={}] enter Started/Primary/Peering/GetMissing`,
	`2026-10-30T23:44:53.917+0000 ffff989740c0  5 osd.0 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 creating+peering mbc={}] enter Started/Primary/Peering/WaitUpThru`,
	`2026-10-30T23:44:54.756+0000 ffff999940c0  5 osd.0 pg_epoch: 12 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 creating mbc={}] enter Started/Primary/Active`,
	`2026-10-30T23:44:54.756+0000 ffff999940c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] --> [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] -- PGlog(2.0 log log((0'0,0'0], crt=0'0) pi ([0,0] all_participants={} intervals=[]) pg_lease(ru 0.000000000s ub 23.787528992s int 16.000000000s) e12/11) -- 0xaaaacf921000 con 0xaaaace7f0800`,
	`2026-10-30T23:44:54.756+0000 ffff999940c0  5 osd.0 pg_epoch: 12 pg[2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 creating+activating mbc={}] enter Started/Primary/Active/Activating`,
	`2026-10-30T23:44:54.759+0000 ffffb3ffd0c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] <== osd.1 v2:172.19.0.4:6802/2152726140 41 ==== pg_info2(2.0 2.0 2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=11/0 les/c/f=12/0/0 sis=11) e12/12) ==== 1057+0+0 (crc 0 0 0) 0xaaaacd896a00 con 0xaaaace7f0800`,
	`2026-10-30T23:44:54.759+0000 ffff9a1a40c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] --> [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] -- pg_lease(2.0 pg_lease(ru 23.787528992s ub 23.790763855s int 16.000000000s) e12/12) -- 0xaaaaceda48c0 con 0xaaaace7f0800`,
	`2026-10-30T23:44:54.760+0000 ffff999940c0  5 osd.0 pg_epoch: 12 pg[2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=11/0 les/c/f=12/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 active mbc={}] enter Started/Primary/Active/Recovered`,
	`2026-10-30T23:44:54.760+0000 ffff999940c0  5 osd.0 pg_epoch: 12 pg[2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=11/0 les/c/f=12/0/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 active mbc={}] enter Started/Primary/Active/Clean`,
	`2026-10-30T23:44:54.760+0000 ffff999940c0 10 osd.0 pg_epoch: 12 pg[2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 active+clean] scrubber<PrimaryActive>: FSM: -- state -->> PrimaryActive`,
	`2026-10-30T23:44:54.760+0000 ffff999940c0 10 osd.0 pg_epoch: 12 pg[2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=0'0 mlcod 0'0 active+clean] scrubber<PrimaryActive/PrimaryIdle>: FSM: -- state -->> PrimaryActive/PrimaryIdle`,
	`2026-10-30T23:44:54.760+0000 ffffb3ffd0c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] <== osd.1 v2:172.19.0.4:6802/2152726140 43 ==== pg_lease_ack(2.0 pg_lease_ack(ruub 23.790763855s) e12/12) ==== 42+0+0 (crc 0 0 0) 0xaaaacf31ab60 con 0xaaaace7f0800`,
	`2026-10-30T23:44:58.975+0000 ffff991840c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<PrimaryActive/Session>: FSM: -- state -->> PrimaryActive/Session`,
	`2026-10-30T23:44:58.975+0000 ffff991840c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/ReservingReplicas>: FSM: -- state -->> ReservingReplicas`,
	`2026-10-30T23:44:58.975+0000 ffff991840c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/ActiveScrubbing>: FSM: -- state -->> ActiveScrubbing`,
	`2026-10-30T23:44:58.975+0000 ffff991840c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/PendingTimer>: FSM: -- state -->> Session/Act/PendingTimer`,
	`2026-10-30T23:44:58.978+0000 ffff989740c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/NewChunk>: FSM: -- state -->> Session/Act/NewChunk`,
	`2026-10-30T23:44:58.983+0000 ffff981640c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/WaitPushes>: FSM:  -- state -->> Session/Act/WaitPushes`,
	`2026-10-30T23:44:58.983+0000 ffff981640c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/WaitLastUpdate>: FSM:  -- state -->> Session/Act/WaitLastUpdate`,
	`2026-10-30T23:44:58.983+0000 ffff981640c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] --> [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] -- replica_scrub(pg: 2.0,from:0'0,to:14'1,epoch:14/11,start:2:00000000::::0,end:MAX,chunky:1,deep:1,version:9,allow_preemption:1,priority=5) -- 0xaaaacfd76000 con 0xaaaace7f0800`,
	`2026-10-30T23:44:58.983+0000 ffff981640c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/BuildMap>: FSM:  -- state -->> Session/Act/BuildMap`,
	`2026-10-30T23:44:58.986+0000 ffff999940c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/BuildMap>: FSM:  -- state -->> Session/Act/BuildMap`,
	`2026-10-30T23:44:58.986+0000 ffff999940c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/WaitReplicas>: FSM: -- state -->> Session/Act/WaitReplicas`,
	`2026-10-30T23:44:58.987+0000 ffffb3ffd0c0  1 -- [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] <== osd.1 v2:172.19.0.4:6802/2152726140 45 ==== rep_scrubmap(2.0 e11 from shard 1) ==== 40+0+498 (crc 0 0 0) 0xaaaacf8d4d20 con 0xaaaace7f0800`,
	`2026-10-30T23:44:58.989+0000 ffff991840c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean+scrubbing+deep [ 2.0:  REQ_SCRUB ]  MUST_DEEP_SCRUB MUST_SCRUB planned REQ_SCRUB] scrubber<Session/Act/WaitDigestUpdate>: FSM: -- state -->> Session/Act/WaitDigestUpdate`,
	`2026-10-30T23:44:58.993+0000 ffff991840c0 10 osd.0 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=0 lpr=11 crt=14'1 lcod 0'0 mlcod 0'0 active+clean] scrubber<PrimaryActive/PrimaryIdle>: FSM: -- state -->> PrimaryActive/PrimaryIdle`,
}

// squidPeeringReplica holds the replica's (osd.1) lines about PG 2.0 from that run, in log order.
var squidPeeringReplica = []string{
	`2026-10-30T23:44:53.916+0000 ffffaa3be0c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] <== osd.0 v2:172.19.0.5:6802/2996635675 4 ==== pg_query2(2.0 2.0 query(info 0'0 epoch_sent 11) e11/11) ==== 158+0+0 (unknown 0 0 0) 0xaaab24765d40 con 0xaaab243b2800`,
	`2026-10-30T23:44:53.916+0000 ffff91db60c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] --> [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] -- pg_notify2(2.0 2.0 (query:11 sent:11 2.0( DNE empty local-lis/les=0/0 n=0 ec=0/0 lis/c=0/0 les/c/f=0/0/0 sis=0) ([0,0] all_participants={} intervals=[])) e11/11) -- 0xaaab2290d100 con 0xaaab243b2800`,
	`2026-10-30T23:44:54.756+0000 ffffaa3be0c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] <== osd.0 v2:172.19.0.5:6802/2996635675 40 ==== PGlog(2.0 log log((0'0,0'0], crt=0'0) pi ([0,0] all_participants={} intervals=[]) pg_lease(ru 0.000000000s ub 23.787528992s int 16.000000000s) e12/11) ==== 1152+0+0 (unknown 0 0 0) 0xaaab24033800 con 0xaaab243b2800`,
	`2026-10-30T23:44:54.756+0000 ffff925c60c0  5 osd.1 pg_epoch: 11 pg[2.0(unlocked)] enter Initial`,
	`2026-10-30T23:44:54.756+0000 ffff925c60c0 10 osd.1 pg_epoch: 11 pg[2.0(unlocked)] scrubber<NotActive>: FSM: -- state -->> NotActive`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0  5 osd.1 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=0 crt=0'0 mlcod 0'0 unknown mbc={}] enter Reset`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0  5 osd.1 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Started`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0  5 osd.1 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Start`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0  5 osd.1 pg_epoch: 11 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Started/Stray`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0  5 osd.1 pg_epoch: 12 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Started/ReplicaActive`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0  5 osd.1 pg_epoch: 12 pg[2.0( empty local-lis/les=0/0 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] enter Started/ReplicaActive/RepNotRecovering`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0 10 osd.1 pg_epoch: 12 pg[2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] scrubber<ReplicaActive>: FSM: -- state -->> ReplicaActive`,
	`2026-10-30T23:44:54.757+0000 ffff925c60c0 10 osd.1 pg_epoch: 12 pg[2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=0/0 les/c/f=0/0/0 sis=11) [0,1] r=1 lpr=11 crt=0'0 mlcod 0'0 unknown mbc={}] scrubber<ReplicaActive/ReplicaIdle>: FSM: -- state -->> ReplicaActive/ReplicaIdle`,
	`2026-10-30T23:44:54.759+0000 ffff915a60c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] --> [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] -- pg_info2(2.0 2.0 2.0( empty local-lis/les=11/12 n=0 ec=11/11 lis/c=11/0 les/c/f=12/0/0 sis=11) e12/12) -- 0xaaab2290c300 con 0xaaab243b2800`,
	`2026-10-30T23:44:54.759+0000 ffffaa3be0c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] <== osd.0 v2:172.19.0.5:6802/2996635675 41 ==== pg_lease(2.0 pg_lease(ru 23.787528992s ub 23.790763855s int 16.000000000s) e12/12) ==== 58+0+0 (unknown 0 0 0) 0xaaab23dd8380 con 0xaaab243b2800`,
	`2026-10-30T23:44:54.760+0000 ffff91db60c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] --> [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] -- pg_lease_ack(2.0 pg_lease_ack(ruub 23.790763855s) e12/12) -- 0xaaab243736c0 con 0xaaab243b2800`,
	`2026-10-30T23:44:58.983+0000 ffffaa3be0c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] <== osd.0 v2:172.19.0.5:6802/2996635675 46 ==== replica_scrub(pg: 2.0,from:0'0,to:14'1,epoch:14/11,start:2:00000000::::0,end:MAX,chunky:1,deep:1,version:9,allow_preemption:1,priority=5) ==== 140+0+0 (unknown 0 0 0) 0xaaab24115b00 con 0xaaab243b2800`,
	`2026-10-30T23:44:58.983+0000 ffff915a60c0 10 osd.1 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=1 lpr=11 luod=0'0 lua=0'0 crt=14'1 lcod 0'0 mlcod 0'0 active mbc={}] scrubber<ReplicaActiveOp>: FSM: -- state -->> ReplicaActive/ReplicaActiveOp`,
	`2026-10-30T23:44:58.983+0000 ffff915a60c0 10 osd.1 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=1 lpr=11 luod=0'0 lua=0'0 crt=14'1 lcod 0'0 mlcod 0'0 active mbc={}] scrubber<ReplicaActive/ReplicaActiveOp/ReplicaWaitUpdates>: FSM: -- state -->> ReplicaActive/ReplicaActiveOp/ReplicaWaitUpdates`,
	`2026-10-30T23:44:58.983+0000 ffff915a60c0 10 osd.1 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=1 lpr=11 luod=0'0 lua=0'0 crt=14'1 lcod 0'0 mlcod 0'0 active mbc={}] scrubber<ReplicaActive/ReplicaActiveOp/ReplicaBuildingMap>: FSM: -- state -->> ReplicaActive/ReplicaActiveOp/ReplicaBuildingMap`,
	`2026-10-30T23:44:58.987+0000 ffff91db60c0  1 -- [v2:172.19.0.4:6802/2152726140,v1:172.19.0.4:6803/2152726140] --> [v2:172.19.0.5:6802/2996635675,v1:172.19.0.5:6803/2996635675] -- rep_scrubmap(2.0 e11 from shard 1) -- 0xaaab248623c0 con 0xaaab243b2800`,
	`2026-10-30T23:44:58.987+0000 ffff91db60c0 10 osd.1 pg_epoch: 14 pg[2.0( v 14'1 (0'0,14'1] local-lis/les=11/12 n=1 ec=11/11 lis/c=11/11 les/c/f=12/12/0 sis=11) [0,1] r=1 lpr=11 luod=0'0 lua=0'0 crt=14'1 lcod 0'0 mlcod 0'0 active mbc={}] scrubber<ReplicaActive/ReplicaIdle>: FSM: -- state -->> ReplicaActive/ReplicaIdle`,
}

// peeringDaemons parses saved lines into the MON, the primary and the
// replica, each with the v2 addresses of its messengers in that run.
func peeringDaemons(t *testing.T, mon, primary, replica []string) (peeringDaemon, peeringDaemon, peeringDaemon) {
	t.Helper()
	parse := func(name string, lines []string, addrs ...string) peeringDaemon {
		events, unusable := parsePeeringLog(lines)
		if len(unusable) != 0 {
			t.Fatalf("%s: unusable %q", name, unusable)
		}
		return peeringDaemon{Name: name, Addrs: addrs, Events: events}
	}
	return parse("mon.0", mon, "172.19.0.2:3300/0"),
		parse("osd.0", primary, "172.19.0.5:6800/2996635675", "172.19.0.5:6802/2996635675"),
		parse("osd.1", replica, "172.19.0.4:6800/2152726140", "172.19.0.4:6802/2152726140")
}

// edit returns a copy of lines with the first line containing marker
// replaced by what change makes of it, or removed when change returns "".
func edit(t *testing.T, lines []string, marker string, change func(string) string) []string {
	t.Helper()
	index := slices.IndexFunc(lines, func(line string) bool { return strings.Contains(line, marker) })
	if index < 0 {
		t.Fatalf("no line contains %q", marker)
	}
	result := slices.Clone(lines)
	if changed := change(result[index]); changed != "" {
		result[index] = changed
	} else {
		result = slices.Delete(result, index, index+1)
	}
	return result
}

// move returns a copy of lines with the first line containing marker moved
// to just after the first line containing after.
func move(t *testing.T, lines []string, marker, after string) []string {
	t.Helper()
	line := lines[slices.IndexFunc(lines, func(l string) bool { return strings.Contains(l, marker) })]
	result := edit(t, lines, marker, func(string) string { return "" })
	index := slices.IndexFunc(result, func(l string) bool { return strings.Contains(l, after) })
	if index < 0 {
		t.Fatalf("no line contains %q", after)
	}
	return slices.Insert(result, index+1, line)
}

func TestJudgePeeringSavedLines(t *testing.T) {
	mon, primary, replica := peeringDaemons(t, squidPeeringMON, squidPeeringPrimary, squidPeeringReplica)
	judgement := judgePeering(mon, primary, replica, "2.0")
	if len(judgement.Problems) != 0 {
		t.Fatalf("problems: %v", judgement.Problems)
	}
	for name, symbol := range peeringSymbols {
		pair, ok := judgement.Pairs[name]
		if !ok || pair.Symbol != symbol || judgement.PairCounts[name] != 1 {
			t.Errorf("%s: %+v, %d pairs", name, pair, judgement.PairCounts[name])
		}
	}
	if pair := judgement.Pairs["pg_create2"]; pair.From != "mon.0" || pair.To != "osd.0" {
		t.Errorf("pg_create2 went from %s to %s", pair.From, pair.To)
	}
	if pair := judgement.Pairs["rep_scrubmap"]; pair.From != "osd.1" || pair.To != "osd.0" || pair.Received.Seq != 45 {
		t.Errorf("rep_scrubmap: %+v", pair)
	}
	if !slices.Contains(judgement.PrimaryStates, "Started/Primary/Peering/GetMissing") || !slices.Contains(judgement.ReplicaStates, "Started/Stray") {
		t.Errorf("states: %v, %v", judgement.PrimaryStates, judgement.ReplicaStates)
	}
	if other := judgePeering(mon, primary, replica, "1.0"); len(other.Problems) == 0 {
		t.Error("the lines of PG 2.0 judged PG 1.0")
	}
}

func TestJudgePeeringRefusesBrokenChains(t *testing.T) {
	primaryAddr, replicaAddr := "v2:172.19.0.5:6802/2996635675", "v2:172.19.0.4:6802/2152726140"
	for name, c := range map[string]struct {
		mon, primary, replica []string
		want                  string
	}{
		"no MON send": {nil, squidPeeringPrimary, squidPeeringReplica, "no pg_create2"},
		"rep_scrubmap never sent": {squidPeeringMON, squidPeeringPrimary,
			edit(t, squidPeeringReplica, "--> ["+primaryAddr+",v1:172.19.0.5:6803/2996635675] -- rep_scrubmap(", func(string) string { return "" }), "no rep_scrubmap"},
		"pg_notify2 from another OSD": {squidPeeringMON,
			edit(t, squidPeeringPrimary, "<== osd.1 "+replicaAddr+" 5 ==== pg_notify2(", func(l string) string { return strings.Replace(l, "<== osd.1 ", "<== osd.2 ", 1) }),
			squidPeeringReplica, "no pg_notify2"},
		"PGlog altered in flight": {squidPeeringMON, squidPeeringPrimary,
			edit(t, squidPeeringReplica, "==== PGlog(", func(l string) string { return strings.Replace(l, "PGlog(2.0 log", "PGlog(2.0 log ", 1) }), "no PGlog"},
		"pg_lease to another messenger": {squidPeeringMON,
			edit(t, squidPeeringPrimary, "-- pg_lease(", func(l string) string {
				return strings.Replace(l, "--> ["+replicaAddr, "--> [v2:172.19.0.9:6802/1", 1)
			}), squidPeeringReplica, "no pg_lease about"},
		"pg_query2 after GetLog": {squidPeeringMON,
			move(t, squidPeeringPrimary, "-- pg_query2(", "enter Started/Primary/Peering/GetLog"), squidPeeringReplica, "the primary's pg_query2"},
		"PGlog before Active": {squidPeeringMON,
			move(t, squidPeeringPrimary, "-- PGlog(", "enter Started/Primary/Peering/WaitUpThru"), squidPeeringReplica, "the primary's PGlog"},
		"pg_info2 after Clean": {squidPeeringMON,
			move(t, squidPeeringPrimary, "==== pg_info2(", "enter Started/Primary/Active/Clean"), squidPeeringReplica, "the primary's receipt of pg_info2"},
		"never Clean": {squidPeeringMON,
			edit(t, squidPeeringPrimary, "enter Started/Primary/Active/Clean", func(string) string { return "" }), squidPeeringReplica, "never entered Started/Primary/Active/Clean"},
		"rep_scrubmap before the replica builds its map": {squidPeeringMON, squidPeeringPrimary,
			move(t, squidPeeringReplica, "-- rep_scrubmap(", "==== replica_scrub("), "the replica's rep_scrubmap"},
	} {
		t.Run(name, func(t *testing.T) {
			mon, primary, replica := peeringDaemons(t, c.mon, c.primary, c.replica)
			problems := judgePeering(mon, primary, replica, "2.0").Problems
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, c.want) }) {
				t.Fatalf("problems %v, want one with %q", problems, c.want)
			}
		})
	}
}

func TestParsePeeringLogUnusableLines(t *testing.T) {
	for name, line := range map[string]string{
		"no PG":   strings.Replace(squidPeeringPrimary[slices.IndexFunc(squidPeeringPrimary, func(l string) bool { return strings.Contains(l, "-- pg_query2(") })], "pg_query2(2.0 2.0 ", "pg_query2(", 1),
		"v1 peer": strings.Replace(squidPeeringReplica[slices.IndexFunc(squidPeeringReplica, func(l string) bool { return strings.Contains(l, "==== PGlog(") })], "<== osd.0 v2:", "<== osd.0 v1:", 1),
	} {
		if events, unusable := parsePeeringLog([]string{line}); len(events) != 0 || len(unusable) != 1 {
			t.Errorf("%s: events %+v, unusable %q", name, events, unusable)
		}
	}
	events, unusable := parsePeeringLog([]string{"", "2026-10-30T23:44:53.916+0000 ffff999940c0  1 -- [v2:172.19.0.5:6802/1] --> [v2:172.19.0.4:6802/2] -- osd_ping(ping e11 up_from 9) -- 0x1 con 0x2"})
	if len(events) != 0 || len(unusable) != 0 {
		t.Errorf("other messages: events %+v, unusable %q", events, unusable)
	}
}
