//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_messenger_client_profiles))))

//ci: timeout=50m job-timeout=60

package integration_test

import (
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The secure profile of a Messenger client suite, with its identities and
// daemon controls.
func TestMessengerClientProfileSecure(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerClientSecureProfile(t)
}

// One daemon type accepts only CRC and the other only secure.
func TestMessengerClientProfileModeRejection(t *testing.T) {
	parallelWhenEnabled(t)
	t.Run("monitors", func(t *testing.T) {
		testMessengerClientModeRejection(t, modeCRC, modeSecure, ceph.CipherAES256K)
	})
	t.Run("managers", func(t *testing.T) {
		testMessengerClientModeRejection(t, modeSecure, modeCRC, ceph.CipherAES)
	})
}

// Long tickets with fast MON timing.
func TestMessengerClientProfileIdleTickets(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerClientTicketProfile(t, 120*time.Second, 120*time.Second, `[global]
mon_subscribe_interval = 2
mon_tick_interval = 1
mon_session_timeout = 3
mon_client_ping_interval = 1
mon_client_hunt_interval = 1
`, map[string]float64{"mon_subscribe_interval": 2, "mon_tick_interval": 1, "mon_session_timeout": 3, "mon_client_ping_interval": 1, "mon_client_hunt_interval": 1}, "", 0)
}

// Three MONs without a MGR, then two MGRs started later.
func TestMessengerClientProfileDelayedManagers(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerClientDelayedManagers(t)
}
