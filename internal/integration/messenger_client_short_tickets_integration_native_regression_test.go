//go:build (all || (integration && topology)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_messenger_client_short_tickets)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"testing"
	"time"
)

// MON tickets of 1.5 seconds and service tickets of 12 seconds, with a
// one-second MON tick. With MON tickets this short, Ceph can abort a MGR that
// renews late while it loads its modules (#23), so the profile runs only on
// request. It checks the bootstrap, the lifetimes the daemons and a client
// run with, and a MON and a MGR command; Ceph does not keep MGR commands
// working for long with these lifetimes (see the Messenger client fixtures
// guide).
func TestMessengerClientProfileShortTickets(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerClientTicketProfile(t, 1500*time.Millisecond, 12*time.Second, "[global]\nmon_tick_interval = 1\n",
		map[string]float64{"mon_tick_interval": 1})
}
