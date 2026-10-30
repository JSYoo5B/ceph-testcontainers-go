//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_messenger_client_profiles))))

//ci: timeout=50m job-timeout=60 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The secure profile of a Messenger client suite, with its identities and
// daemon controls.
func TestMessengerClientProfileSecure(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMessengerClientSecureProfile(t)
}

// One daemon type accepts only CRC and the other only secure.
func TestMessengerClientProfileModeRejection(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	t.Run("monitors", func(t *testing.T) {
		testMessengerClientModeRejection(t, modeCRC, modeSecure, ceph.CipherAES256K)
	})
	t.Run("managers", func(t *testing.T) {
		testMessengerClientModeRejection(t, modeSecure, modeCRC, ceph.CipherAES)
	})
}

// Long tickets with fast MON timing.
func TestMessengerClientProfileIdleTickets(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMessengerClientTicketProfile(t, 120*time.Second, 120*time.Second, `[global]
mon_subscribe_interval = 2
mon_tick_interval = 1
mon_session_timeout = 3
mon_client_ping_interval = 1
mon_client_hunt_interval = 1
`, map[string]float64{"mon_subscribe_interval": 2, "mon_tick_interval": 1, "mon_session_timeout": 3, "mon_client_ping_interval": 1, "mon_client_hunt_interval": 1})
}

// Three MONs without a MGR, then two MGRs started later.
func TestMessengerClientProfileDelayedManagers(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMessengerClientDelayedManagers(t)
}

// A MON that never answers joins the monmap and leaves it again; new clients
// connect once the election that each change starts has settled.
func TestMessengerClientProfileUnreachableMonitor(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMessengerClientUnreachableMonitor(t)
}

// A MGR authenticates several MonClients; only its beacon client's global ID
// is the MgrMap GID.
func TestMessengerClientProfileManagerMonClients(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testManagerMonClients(t)
}

// The active MGR's process stops and the captured standby takes over with the
// same process; the old process starts again and returns as a standby with a
// new GID.
func TestMessengerClientProfileManagerTransition(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testManagerTransitionReceipt(t)
}

// A network interruption of a MON outlasts a restart of the MON until its
// owner restores it; then the MON gets its addresses back.
func TestMonitorAddressAcrossNetworkInterruption(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	testMonitorAddressAcrossNetworkInterruption(t)
}

// After one rotating-key wipe, a tested authentication that the MONs refuse
// is kept as recorded while a bounded bundle of MON, MGR and control
// observations is collected around it.
func TestAuthFirstFailureBundle(t *testing.T) {
	testAuthFirstFailureBundle(t)
}
