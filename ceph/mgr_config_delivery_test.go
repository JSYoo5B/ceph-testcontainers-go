package ceph

import "testing"

func TestMGRConfigDeliveryRequiresMONLayer(t *testing.T) {
	const key = "mon_lease_ack_timeout_factor"
	for _, tc := range []struct {
		data string
		want bool
	}{
		{`{"mon_lease_ack_timeout_factor":{"mon":17.25,"final":9.75}}`, true},
		{`{"diff":{"diff":{"mon_lease_ack_timeout_factor":{"mon":17.25}}}}`, true},
		{`{"mon_lease_ack_timeout_factor":{"mon":9.75,"final":17.25}}`, false},
		{`{"mon_lease_ack_timeout_factor":{"final":17.25}}`, false},
		{`{"mon_lease_ack_timeout_factor":{"mon":null,"final":17.25}}`, false},
		{`{"mon_lease_ack_timeout_factor":{"mon":"17.25"}}`, false},
		{`{"diff":null}`, false},
	} {
		if got := mgrConfigMONValue([]byte(tc.data), key, 17.25); got != tc.want {
			t.Errorf("MON marker from %s = %t, want %t", tc.data, got, tc.want)
		}
	}
}
