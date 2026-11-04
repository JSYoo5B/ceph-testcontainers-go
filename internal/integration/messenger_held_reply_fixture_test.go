//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"net/netip"
	"strings"
	"testing"
)

func TestParseSocketTable(t *testing.T) {
	tcp := strings.Join([]string{
		"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
		"   0: 0100007F:1A85 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 1 1 0 100 0 0 10 0",
		"   1: 0500A8C0:1A90 0700A8C0:D2F0 01 000B03D8:00000010 01:00000014 00000000   167        0 2 1 0 20 4 30 10 -1",
	}, "\n")
	tcp6 := strings.Join([]string{
		"  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
		"   0: 0000000000000000FFFF00000500A8C0:1A90 0000000000000000FFFF00000800A8C0:B000 01 00000000:00000000 00:00000000 00000000   167        0 3 1 0 20 4 30 10 -1",
		"   1: B80D0120000000000000000001000000:0050 B80D0120000000000000000002000000:C350 01 00000001:00000002 00:00000000 00000000     0        0 4 1 0 20 4 30 10 -1",
	}, "\n")
	rows, err := parseSocketTable(tcp + "\n" + tcp6)
	if err != nil {
		t.Fatal(err)
	}
	want := []socketRow{
		{Local: netip.MustParseAddrPort("127.0.0.1:6789"), Remote: netip.MustParseAddrPort("0.0.0.0:0"), State: 0x0a},
		{Local: netip.MustParseAddrPort("192.168.0.5:6800"), Remote: netip.MustParseAddrPort("192.168.0.7:54000"), State: 1, TX: 0x0b03d8, RX: 0x10},
		{Local: netip.MustParseAddrPort("192.168.0.5:6800"), Remote: netip.MustParseAddrPort("192.168.0.8:45056"), State: 1},
		{Local: netip.MustParseAddrPort("[2001:db8::1]:80"), Remote: netip.MustParseAddrPort("[2001:db8::2]:50000"), State: 1, TX: 1, RX: 2},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows %+v", rows)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Errorf("row %d: %+v, want %+v", i, rows[i], want[i])
		}
	}

	for _, bad := range []string{
		"   0: 0100007F 00000000:0000 01 00000000:00000000",
		"   0: 0100007F:1A85 00000000:0000 01 0000000000000000",
		"   0: 0100007:1A85 00000000:0000 01 00000000:00000000",
		"   0: 0100007F:1A85 00000000:0000 0Z 00000000:00000000",
		"   0: 0100007F:1A85",
	} {
		if _, err := parseSocketTable(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestPairSockets(t *testing.T) {
	osd := []socketRow{
		{Local: netip.MustParseAddrPort("10.0.0.5:6800"), Remote: netip.MustParseAddrPort("10.0.0.9:40000"), State: 1},
		{Local: netip.MustParseAddrPort("10.0.0.5:6800"), Remote: netip.MustParseAddrPort("10.0.0.2:41000"), State: 1},
		{Local: netip.MustParseAddrPort("10.0.0.5:6800"), Remote: netip.MustParseAddrPort("10.0.0.9:40001"), State: 6},
		{Local: netip.MustParseAddrPort("0.0.0.0:6800"), State: 0x0a},
	}
	client := []socketRow{
		{Local: netip.MustParseAddrPort("10.0.0.9:40000"), Remote: netip.MustParseAddrPort("10.0.0.5:6800"), State: 1},
		{Local: netip.MustParseAddrPort("10.0.0.9:40001"), Remote: netip.MustParseAddrPort("10.0.0.5:6800"), State: 6},
		{Local: netip.MustParseAddrPort("10.0.0.9:39000"), Remote: netip.MustParseAddrPort("10.0.0.3:3300"), State: 1},
	}
	pairs := pairSockets(osd, client)
	if len(pairs) != 1 || pairs[0].OSD != osd[0].Local || pairs[0].Client != client[0].Local {
		t.Fatalf("pairs %+v", pairs)
	}
	if row, ok := pairs[0].find(osd); !ok || row != osd[0] {
		t.Fatalf("find %+v %t", row, ok)
	}
	if row, ok := pairs[0].findClient(client); !ok || row != client[0] {
		t.Fatalf("findClient %+v %t", row, ok)
	}
	if _, ok := pairs[0].find(client); ok {
		t.Fatal("found the client's end in the OSD's orientation")
	}
	if _, ok := pairs[0].find(osd[1:]); ok {
		t.Fatal("found a closed tuple")
	}
}
