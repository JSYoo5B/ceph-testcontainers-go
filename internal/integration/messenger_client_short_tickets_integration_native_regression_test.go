//go:build (all || (integration && topology)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_messenger_client_short_tickets)))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"testing"
	"time"
)

// MON tickets of 1.5 seconds and service tickets of 12 seconds, with a
// one-second MON tick. One librados session outlives both lifetimes, and the
// MGRs must stay. With MON tickets this short, Ceph can abort a MGR that
// renews late while it loads its modules (#23), so the profile runs only on
// request.
func TestMessengerClientProfileShortTickets(t *testing.T) {
	parallelWhenEnabled(t)
	testMessengerClientTicketProfile(t, 1500*time.Millisecond, 12*time.Second, "[global]\nmon_tick_interval = 1\n",
		map[string]float64{"mon_tick_interval": 1}, 30)
}
