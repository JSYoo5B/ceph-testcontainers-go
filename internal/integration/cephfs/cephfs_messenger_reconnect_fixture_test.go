//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_test

import (
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// READY lines of one libcephfs client that an MDS and an OSD logged at
// debug_ms 1 while the client ran with ms_inject_socket_failures 40 for 2 s.
// The first line of each daemon is from before the window.
var (
	// The default image, Ceph 20.2.4: client.4229.
	tentacleMDSReady = []string{
		`2026-10-30T17:03:04.758+0000 ffff8a2b6c80  1 --2- v2:172.19.0.6:6800/3944205725 >> 172.19.0.2:0/1038085452 conn(0xaaaafafb5000 0xaaaafaf3a100 secure :-1 s=READY pgs=2 gs=9 cs=0 l=0 c_cookie=12744356347057266623 s_cookie=9031935084660911428 reconnecting=0 rev1=1 crypto rx=0xaaaafaf0e8d0 tx=0xaaaafae61320 comp rx=0 tx=0).ready entity=client.4229 client_cookie=b0dd0c933d57dfbf server_cookie=7d57e11b3399ed44 in_seq=0 out_seq=0`,
		`2026-10-30T17:03:17.067+0000 ffff8b2d6c80  1 --2- v2:172.19.0.6:6800/3944205725 >> 172.19.0.2:0/1038085452 conn(0xaaaafafb5000 0xaaaafaf3a100 unknown :-1 s=READY pgs=8 gs=9 cs=1 l=0 c_cookie=12744356347057266623 s_cookie=9031935084660911428 reconnecting=0 rev1=1 crypto rx=0xaaaafaf915c0 tx=0xaaaafafa8a80 comp rx=0 tx=0).ready entity=client.4229 client_cookie=b0dd0c933d57dfbf server_cookie=7d57e11b3399ed44 in_seq=32 out_seq=19`,
		`2026-10-30T17:03:17.556+0000 ffff8a2b6c80  1 --2- v2:172.19.0.6:6800/3944205725 >> 172.19.0.2:0/1038085452 conn(0xaaaafafb5000 0xaaaafaf3a100 unknown :-1 s=READY pgs=16 gs=9 cs=2 l=0 c_cookie=12744356347057266623 s_cookie=9031935084660911428 reconnecting=0 rev1=1 crypto rx=0xaaaafaf0e8d0 tx=0xaaaafafa8ea0 comp rx=0 tx=0).ready entity=client.4229 client_cookie=b0dd0c933d57dfbf server_cookie=7d57e11b3399ed44 in_seq=46 out_seq=34`,
	}
	tentacleOSDReady = []string{
		`2026-10-30T17:03:04.765+0000 ffffbb518f00  1 --2- v2:172.19.0.5:6800/4093786475 >> 172.19.0.2:0/1038085452 conn(0xaaab1584bc00 0xaaab165bf180 secure :-1 s=READY pgs=3 gs=9 cs=0 l=1 c_cookie=18095772214125782595 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0xaaab15f7e4e0 tx=0xaaab1687c900 comp rx=0 tx=0).ready entity=client.4229 client_cookie=fb2118f40759e643 server_cookie=0 in_seq=0 out_seq=0`,
		`2026-10-30T17:03:17.041+0000 ffffbb518f00  1 --2- v2:172.19.0.5:6800/4093786475 >> 172.19.0.2:0/1038085452 conn(0xaaab1584b000 0xaaab157bb080 secure :-1 s=READY pgs=6 gs=12 cs=0 l=1 c_cookie=284145319855852337 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0xaaab160a8c90 tx=0xaaab1687c000 comp rx=0 tx=0).ready entity=client.4229 client_cookie=3f17ca9b6990b31 server_cookie=0 in_seq=0 out_seq=0`,
	}
	// quay.io/ceph/ceph:v19.2.5: client.4251. Squid prints no gs, cookies or
	// reconnecting field inside conn(); its cs skips values for attempts
	// that faulted before READY.
	squidMDSReady = []string{
		`2026-10-30T17:06:24.212+0000 ffff87344c40  1 --2- v2:172.19.0.6:6800/1721253874 >> 172.19.0.2:0/3051175094 conn(0xaaaac4448000 0xaaaac43f1600 secure :-1 s=READY pgs=2 cs=0 l=0 rev1=1 crypto rx=0xaaaac43feab0 tx=0xaaaac4261c80 comp rx=0 tx=0).ready entity=client.4251 client_cookie=c091747018a44e64 server_cookie=3672916d5d89cd4d in_seq=0 out_seq=0`,
		`2026-10-30T17:06:36.766+0000 ffff88b74c40  1 --2- v2:172.19.0.6:6800/1721253874 >> 172.19.0.2:0/3051175094 conn(0xaaaac4448000 0xaaaac43f1600 unknown :-1 s=READY pgs=11 cs=2 l=0 rev1=1 crypto rx=0xaaaac43fff80 tx=0xaaaac4313680 comp rx=0 tx=0).ready entity=client.4251 client_cookie=c091747018a44e64 server_cookie=3672916d5d89cd4d in_seq=33 out_seq=21`,
		`2026-10-30T17:06:37.064+0000 ffff87344c40  1 --2- v2:172.19.0.6:6800/1721253874 >> 172.19.0.2:0/3051175094 conn(0xaaaac4448000 0xaaaac43f1600 unknown :-1 s=READY pgs=16 cs=3 l=0 rev1=1 crypto rx=0xaaaac43feab0 tx=0xaaaac4313b00 comp rx=0 tx=0).ready entity=client.4251 client_cookie=c091747018a44e64 server_cookie=3672916d5d89cd4d in_seq=46 out_seq=34`,
	}
	squidOSDReady = []string{
		`2026-10-30T17:06:24.218+0000 ffff928ed0c0  1 --2- v2:172.19.0.5:6800/707943670 >> 172.19.0.2:0/3051175094 conn(0xaaaad05c6c00 0xaaaad0b7e680 secure :-1 s=READY pgs=3 cs=0 l=1 rev1=1 crypto rx=0xaaaad0794450 tx=0xaaaad0bdcf60 comp rx=0 tx=0).ready entity=client.4251 client_cookie=5be267eeeaa2bcef server_cookie=0 in_seq=0 out_seq=0`,
		`2026-10-30T17:06:36.510+0000 ffff928ed0c0  1 --2- v2:172.19.0.5:6800/707943670 >> 172.19.0.2:0/3051175094 conn(0xaaaad05c7000 0xaaaad068c580 secure :-1 s=READY pgs=6 cs=0 l=1 rev1=1 crypto rx=0xaaaacfb71a70 tx=0xaaaad0c18f00 comp rx=0 tx=0).ready entity=client.4251 client_cookie=3374af0195491303 server_cookie=0 in_seq=0 out_seq=0`,
	}
	// The MDS's own lossy session with the MON, as the MDS logged it: the
	// connecting side of a session without cookies.
	tentacleMonReady = `2026-10-30T17:03:01.547+0000 ffff8aac6c80  1 --2- v2:172.19.0.6:6800/3944205725 >> v2:172.19.0.2:3300/0 conn(0xaaaafa091c00 0xaaaafae3c000 secure :-1 s=READY pgs=104 gs=1 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0xaaaafa0ff9b0 tx=0xaaaafadb34a0 comp rx=0 tx=0).ready entity=mon.0 client_cookie=0 server_cookie=0 in_seq=0 out_seq=0`
)

// mutate replaces from, which must occur in line exactly once. Its results
// are synthetic, not observed Ceph output.
func mutate(t *testing.T, line, from, to string) string {
	t.Helper()
	if strings.Count(line, from) != 1 {
		t.Fatalf("line does not contain %q exactly once", from)
	}
	return strings.Replace(line, from, to, 1)
}

func readySessions(t *testing.T, lines ...string) []harness.ReadySession {
	t.Helper()
	sessions, unusable := harness.ParseReadySessions(lines)
	if len(unusable) != 0 || len(sessions) != len(lines) {
		t.Fatalf("%d sessions from %d lines, unusable %v", len(sessions), len(lines), unusable)
	}
	return sessions
}

func TestParseReadySessionsSavedLines(t *testing.T) {
	for _, c := range []struct {
		name  string
		lines []string
	}{{"tentacle mds", tentacleMDSReady}, {"tentacle osd", tentacleOSDReady}, {"squid mds", squidMDSReady}, {"squid osd", squidOSDReady}} {
		t.Run(c.name, func(t *testing.T) {
			for _, s := range readySessions(t, c.lines...) {
				if s.PeerType != "client" || s.PeerID == "" || !s.Revision1 || s.Line == "" {
					t.Errorf("%+v", s)
				}
			}
		})
	}
	last := readySessions(t, tentacleMDSReady[2])[0]
	want := harness.ReadySession{Line: tentacleMDSReady[2], Conn: "0xaaaafafb5000", Mode: "unknown", PeerGlobalSeq: 16, ConnectSeq: 2, Revision1: true,
		PeerType: "client", PeerID: "4229", ClientCookie: "b0dd0c933d57dfbf", ServerCookie: "7d57e11b3399ed44", InSeq: 46, OutSeq: 34}
	if last != want {
		t.Errorf("got %+v\nwant %+v", last, want)
	}
	if mon := readySessions(t, tentacleMonReady)[0]; !mon.Lossy || mon.PeerType != "mon" || mon.ClientCookie != "0" || mon.ServerCookie != "0" {
		t.Errorf("mon session %+v", mon)
	}
}

func TestParseReadySessionsRefusesIncompleteLines(t *testing.T) {
	line := squidMDSReady[1]
	for _, c := range []struct{ name, line string }{
		{"no cs (synthetic)", mutate(t, line, " cs=2", "")},
		{"repeated cs (synthetic)", mutate(t, line, " cs=2", " cs=2 cs=3")},
		{"policy 2 (synthetic)", mutate(t, line, " l=0", " l=2")},
		{"no rev1 (synthetic)", mutate(t, line, " rev1=1", "")},
		{"no server cookie (synthetic)", mutate(t, line, " server_cookie=3672916d5d89cd4d", "")},
		{"cookie not hex (synthetic)", mutate(t, line, "client_cookie=c091747018a44e64", "client_cookie=zz")},
		{"negative in_seq (synthetic)", mutate(t, line, "in_seq=33", "in_seq=-1")},
		{"no out_seq (synthetic)", mutate(t, line, " out_seq=21", "")},
	} {
		t.Run(c.name, func(t *testing.T) {
			sessions, unusable := harness.ParseReadySessions([]string{c.line})
			if len(sessions) != 0 || len(unusable) != 1 || !strings.HasSuffix(unusable[0], c.line) {
				t.Fatalf("sessions %+v, unusable %v", sessions, unusable)
			}
		})
	}
	if sessions, unusable := harness.ParseReadySessions([]string{"not a READY line", strings.Replace(line, "s=READY", "s=STANDBY", 1)}); len(sessions) != 0 || len(unusable) != 0 {
		t.Fatalf("other lines selected: %+v %v", sessions, unusable)
	}
}

func TestLosslessReconnectJudge(t *testing.T) {
	for _, c := range []struct {
		name   string
		lines  []string
		client uint64
	}{{"tentacle", tentacleMDSReady, 4229}, {"squid", squidMDSReady, 4251}} {
		t.Run(c.name+" saved", func(t *testing.T) {
			sessions := readySessions(t, c.lines...)
			if o := judgeLosslessReconnect(sessions[:1], sessions[1:], c.client); o.Verdict != reconnectObserved || len(o.Sessions) != 3 {
				t.Fatalf("%+v", o)
			}
			// The same lines do not show a reset of the lossy policy.
			if o := judgeLossyResets(sessions[:1], sessions[1:], c.client); o.Verdict != reconnectInconclusive {
				t.Fatalf("lossy judge on lossless lines: %+v", o)
			}
		})
	}
	base, first, second := tentacleMDSReady[0], tentacleMDSReady[1], tentacleMDSReady[2]
	for _, c := range []struct {
		name   string
		before []string
		window []string
		client uint64
		want   reconnectVerdict
	}{
		{"another client", []string{base}, []string{first, second}, 4230, reconnectInconclusive},
		{"no session before", nil, []string{first, second}, 4229, reconnectInconclusive},
		{"two sessions before (synthetic)", []string{base, mutate(t, base, "0xaaaafafb5000", "0xaaaafafb5400")}, []string{first}, 4229, reconnectInconclusive},
		{"reconnected before the window", []string{first}, []string{second}, 4229, reconnectInconclusive},
		{"no reconnect in the window", []string{base}, nil, 4229, reconnectInconclusive},
		{"nothing exchanged (synthetic)", []string{base}, []string{mutate(t, mutate(t, first, "in_seq=32", "in_seq=0"), "out_seq=19", "out_seq=0")}, 4229, reconnectInconclusive},
		{"new server cookie (synthetic)", []string{base}, []string{first, mutate(t, second, "server_cookie=7d57e11b3399ed44", "server_cookie=7d57e11b3399ed45")}, 4229, reconnectContradicted},
		{"new client cookie (synthetic)", []string{base}, []string{mutate(t, first, "client_cookie=b0dd0c933d57dfbf", "client_cookie=b0dd0c933d57dfbe")}, 4229, reconnectContradicted},
		{"another connection (synthetic)", []string{base}, []string{mutate(t, first, "conn(0xaaaafafb5000", "conn(0xaaaafafb5400")}, 4229, reconnectContradicted},
		{"cs repeated (synthetic)", []string{base}, []string{first, mutate(t, second, " cs=2", " cs=1")}, 4229, reconnectContradicted},
		{"pgs back (synthetic)", []string{base}, []string{first, mutate(t, second, "pgs=16", "pgs=7")}, 4229, reconnectContradicted},
		{"in_seq restarted (synthetic)", []string{base}, []string{first, mutate(t, second, "in_seq=46", "in_seq=3")}, 4229, reconnectContradicted},
		{"out_seq restarted (synthetic)", []string{base}, []string{first, mutate(t, second, "out_seq=34", "out_seq=0")}, 4229, reconnectContradicted},
		{"revision 0 (synthetic)", []string{base}, []string{mutate(t, first, " rev1=1", " rev1=0")}, 4229, reconnectContradicted},
		{"window order reversed", []string{base}, []string{second, first}, 4229, reconnectContradicted},
	} {
		t.Run(c.name, func(t *testing.T) {
			before, window := readySessions(t, c.before...), readySessions(t, c.window...)
			if o := judgeLosslessReconnect(before, window, c.client); o.Verdict != c.want || o.Reason == "" {
				t.Fatalf("got %+v, want %s", o, c.want)
			}
		})
	}
}

func TestLossyResetJudge(t *testing.T) {
	for _, c := range []struct {
		name   string
		lines  []string
		client uint64
	}{{"tentacle", tentacleOSDReady, 4229}, {"squid", squidOSDReady, 4251}} {
		t.Run(c.name+" saved", func(t *testing.T) {
			sessions := readySessions(t, c.lines...)
			if o := judgeLossyResets(sessions[:1], sessions[1:], c.client); o.Verdict != reconnectObserved || len(o.Sessions) != 2 {
				t.Fatalf("%+v", o)
			}
			if o := judgeLosslessReconnect(sessions[:1], sessions[1:], c.client); o.Verdict != reconnectInconclusive {
				t.Fatalf("lossless judge on lossy lines: %+v", o)
			}
		})
	}
	base, reset := tentacleOSDReady[0], tentacleOSDReady[1]
	for _, c := range []struct {
		name   string
		before []string
		window []string
		want   reconnectVerdict
	}{
		{"no session before", nil, []string{reset}, reconnectInconclusive},
		{"no reset in the window", []string{base}, nil, reconnectInconclusive},
		{"no client cookie (synthetic)", []string{base}, []string{mutate(t, reset, "client_cookie=3f17ca9b6990b31", "client_cookie=0")}, reconnectInconclusive},
		{"same client cookie (synthetic)", []string{base}, []string{mutate(t, reset, "client_cookie=3f17ca9b6990b31", "client_cookie=fb2118f40759e643")}, reconnectContradicted},
		{"cookie repeated in the window (synthetic)", []string{base}, []string{reset, reset}, reconnectContradicted},
		{"resumed cs (synthetic)", []string{base}, []string{mutate(t, reset, " cs=0", " cs=1")}, reconnectContradicted},
		{"server cookie (synthetic)", []string{base}, []string{mutate(t, reset, "server_cookie=0", "server_cookie=7d57e11b3399ed44")}, reconnectContradicted},
		{"carried in_seq (synthetic)", []string{base}, []string{mutate(t, reset, "in_seq=0", "in_seq=5")}, reconnectContradicted},
		{"revision 0 (synthetic)", []string{base}, []string{mutate(t, reset, " rev1=1", " rev1=0")}, reconnectContradicted},
	} {
		t.Run(c.name, func(t *testing.T) {
			if o := judgeLossyResets(readySessions(t, c.before...), readySessions(t, c.window...), 4229); o.Verdict != c.want || o.Reason == "" {
				t.Fatalf("got %+v, want %s", o, c.want)
			}
		})
	}
}

func TestDumpedReconnectJudge(t *testing.T) {
	last := readySessions(t, tentacleMDSReady[2])[0]
	connection := func(conn int64, nonce, cs, pgs uint64) harness.MessengerConnection {
		return harness.MessengerConnection{PeerType: "client", GlobalID: 4229, PeerNonce: nonce, ConnID: conn, State: "READY", Connected: true,
			Revision1: true, ConnectSeq: cs, PeerGlobalSeq: pgs}
	}
	before := []harness.MessengerConnection{connection(10, 1038085452, 0, 2), {PeerType: "client", GlobalID: 4235, ConnID: 12, State: "READY", Connected: true}}
	// The connection of the Tentacle run: conn_id 10, cs 0 then 2, pgs 16.
	if o := judgeDumpedReconnect(before, []harness.MessengerConnection{connection(10, 1038085452, 2, 16)}, 4229, last); o.Verdict != reconnectObserved {
		t.Fatalf("%+v", o)
	}
	closed := connection(10, 1038085452, 0, 2)
	closed.State, closed.Connected = "CLOSED", false
	notRev1 := connection(10, 1038085452, 2, 16)
	notRev1.Revision1 = false
	for _, c := range []struct {
		name   string
		before []harness.MessengerConnection
		after  []harness.MessengerConnection
		want   reconnectVerdict
	}{
		{"absent after", before, nil, reconnectInconclusive},
		{"closed before", []harness.MessengerConnection{closed}, []harness.MessengerConnection{connection(10, 1038085452, 2, 16)}, reconnectInconclusive},
		{"another connection", before, []harness.MessengerConnection{connection(23, 1038085452, 2, 16)}, reconnectContradicted},
		{"another nonce", before, []harness.MessengerConnection{connection(10, 1038085453, 2, 16)}, reconnectContradicted},
		{"cs behind the log", before, []harness.MessengerConnection{connection(10, 1038085452, 1, 16)}, reconnectContradicted},
		{"pgs behind the log", before, []harness.MessengerConnection{connection(10, 1038085452, 2, 8)}, reconnectContradicted},
		{"reconnected before", []harness.MessengerConnection{connection(10, 1038085452, 1, 8)}, []harness.MessengerConnection{connection(10, 1038085452, 2, 16)}, reconnectContradicted},
		{"not msgr2.1", before, []harness.MessengerConnection{notRev1}, reconnectContradicted},
	} {
		t.Run(c.name+" (synthetic)", func(t *testing.T) {
			if o := judgeDumpedReconnect(c.before, c.after, 4229, last); o.Verdict != c.want || o.Reason == "" {
				t.Fatalf("got %+v, want %s", o, c.want)
			}
		})
	}
}
