//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package rgw_test

import (
	"slices"
	"strings"
	"testing"
)

// gatewayAddress is the gateway container's address in the saved Squid
// 19.2.5 run below.
const gatewayAddress = "172.19.0.6"

// squidGatewaySessions is part of mon.a's sessions output from that run,
// taken after rgw.Run returned: the MGR's and an OSD's daemon sessions and
// the session of the radosgw messenger with nonce 965164259.
const squidGatewaySessions = `[{"name": "mgr.4106", "entity_name": "mgr.a", "addrs": {"addrvec": [{"type": "any", "addr": "172.19.0.3:0", "nonce": 528867515}]}, "socket_addr": {"type": "any", "addr": "172.19.0.3:0", "nonce": 528867515}, "con_type": "mgr", "global_id": 4106}, {"name": "osd.0", "entity_name": "osd.0", "addrs": {"addrvec": [{"type": "v2", "addr": "172.19.0.5:6800", "nonce": 3650383368}]}, "socket_addr": {"type": "v2", "addr": "172.19.0.5:6800", "nonce": 3650383368}, "con_type": "osd", "global_id": 4145}, {"name": "client.?", "entity_name": "client.admin", "addrs": {"addrvec": [{"type": "any", "addr": "172.19.0.6:0", "nonce": 965164259}]}, "socket_addr": {"type": "any", "addr": "172.19.0.6:0", "nonce": 965164259}, "con_type": "client", "global_id": 4167}]`

// READY lines that mon.a and osd.0 logged in that run with the radosgw
// messenger above and with the radosgw-admin process that created the S3
// user from the same container, client.admin as well.
const (
	squidRadosgwMonReady = `2026-10-30T22:35:39.852+0000 ffff87ae1ac0  1 --2- v2:172.19.0.2:3300/0 >> 172.19.0.6:0/965164259 conn(0xaaaabbf86c00 0xaaaabbfda580 secure :-1 s=READY pgs=1 cs=0 l=1 rev1=1 crypto rx=0xaaaabc3c5860 tx=0xaaaabc1f61e0 comp rx=0 tx=0).ready entity=client.? client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`
	squidRadosgwOSDReady = `2026-10-30T22:35:40.940+0000 ffffa47000c0  1 --2- v2:172.19.0.5:6800/3650383368 >> 172.19.0.6:0/965164259 conn(0xaaaafa224000 0xaaaafa3dbb80 crc :-1 s=READY pgs=4 cs=0 l=1 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).ready entity=client.4167 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`
	squidAdminMonReady   = `2026-10-30T22:35:48.347+0000 ffff882f1ac0  1 --2- v2:172.19.0.2:3300/0 >> 172.19.0.6:0/1586035655 conn(0xaaaabc0c4000 0xaaaabc49db80 secure :-1 s=READY pgs=1 cs=0 l=1 rev1=1 crypto rx=0xaaaabc3a4a20 tx=0xaaaabc3a7500 comp rx=0 tx=0).ready entity=client.? client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`
	squidAdminOSDReady   = `2026-10-30T22:35:48.349+0000 ffffa67610c0  1 --2- v2:172.19.0.5:6800/3650383368 >> 172.19.0.6:0/1586035655 conn(0xaaaafad83c00 0xaaaafacaf600 crc :-1 s=READY pgs=4 cs=0 l=1 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).ready entity=client.4176 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`
)

func TestParseGatewaySessions(t *testing.T) {
	sessions, err := parseGatewaySessions([]byte(squidGatewaySessions), gatewayAddress)
	if err != nil || !slices.Equal(sessions, []radosgwSession{{Nonce: 965164259, GlobalID: 4167}}) {
		t.Fatalf("sessions = %+v, %v", sessions, err)
	}
	if sessions, err := parseGatewaySessions([]byte(squidGatewaySessions), "172.19.0.9"); err != nil || len(sessions) != 0 {
		t.Fatalf("another address: %+v, %v", sessions, err)
	}
	for name, data := range map[string]string{
		"not JSON":     `sessions`,
		"no nonce":     strings.Replace(squidGatewaySessions, `"nonce": 965164259}, "con_type"`, `"nonce": 0}, "con_type"`, 1),
		"no global ID": strings.Replace(squidGatewaySessions, `"global_id": 4167`, `"global_id": 0`, 1),
	} {
		if sessions, err := parseGatewaySessions([]byte(data), gatewayAddress); err == nil {
			t.Errorf("%s: accepted %+v", name, sessions)
		}
	}
}

func TestSelectRadosgwReady(t *testing.T) {
	radosgw := []radosgwSession{{Nonce: 965164259, GlobalID: 4167}}
	lines := []string{"unrelated", squidRadosgwMonReady, squidAdminMonReady, squidRadosgwOSDReady, squidAdminOSDReady}
	selected, others, problems := selectRadosgwReady(lines, gatewayAddress, radosgw)
	if !slices.Equal(selected, []string{squidRadosgwMonReady, squidRadosgwOSDReady}) || others != 2 || len(problems) != 0 {
		t.Fatalf("selected %q, others %d, problems %v", selected, others, problems)
	}

	// radosgw-admin's sessions alone select nothing, however many there are.
	selected, others, problems = selectRadosgwReady([]string{squidAdminMonReady, squidAdminOSDReady}, gatewayAddress, radosgw)
	if len(selected) != 0 || others != 2 || len(problems) != 0 {
		t.Fatalf("admin only: selected %q, others %d, problems %v", selected, others, problems)
	}
	if selected, others, _ := selectRadosgwReady(lines, gatewayAddress, nil); len(selected) != 0 || others != 4 {
		t.Fatalf("no radosgw session: selected %q, others %d", selected, others)
	}
	if selected, others, _ := selectRadosgwReady(lines, "172.19.0.9", radosgw); len(selected) != 0 || others != 0 {
		t.Fatalf("another address: selected %q, others %d", selected, others)
	}

	// A session at radosgw's nonce whose authenticated entity is another
	// client's is not radosgw's.
	foreign := strings.Replace(squidRadosgwOSDReady, "entity=client.4167", "entity=client.4176", 1)
	selected, _, problems = selectRadosgwReady([]string{foreign}, gatewayAddress, radosgw)
	if len(selected) != 0 || len(problems) != 1 {
		t.Fatalf("foreign entity: selected %q, problems %v", selected, problems)
	}
	connecting := strings.Replace(squidRadosgwOSDReady, "s=READY", "s=AUTH_CONNECTING", 1)
	if selected, others, _ := selectRadosgwReady([]string{connecting}, gatewayAddress, radosgw); len(selected) != 0 || others != 0 {
		t.Fatalf("not READY: selected %q, others %d", selected, others)
	}
}
