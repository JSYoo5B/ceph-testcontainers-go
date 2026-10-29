package ceph

import "context"

// portLeaseState is the host-port reservations of one cluster in host network
// mode: every allocator container that holds reserved ports for a MON, owned
// until Terminate releases it.
//
// The type has no mutex. Every
// reader and writer runs under Container.mu: track from the three places that
// publish a lease (reserveTrackedHostPortsWith and AddMonitor after
// lockTopology, trackHostPortLease with mu.Lock) and release from terminate,
// which holds it. A lease is published as soon as its allocator exists, even
// when the reservation failed or its context ended, so that Terminate cannot
// miss it. Each lease has its own mutex, taken by hostPortLease.Release, so a
// release that blocks on one lease does not need a lock of this type.
//
// The guards in port_lease_state_test.go enforce that nothing touches the
// field outside this type, that the methods call only their pinned
// collaborators, that every caller takes Container.mu first, that terminate
// releases the leases after the faults and native commands and before the
// services, and that the split adds no mutex.
type portLeaseState struct {
	leases []*hostPortLease
}

// track records a lease, or nothing for a nil lease. mu held.
func (s *portLeaseState) track(lease *hostPortLease) {
	if lease == nil {
		return
	}
	s.leases = append(s.leases, lease)
}

// count returns the number of recorded leases, released or not. mu held, or
// the cluster value is not shared.
func (s *portLeaseState) count() int { return len(s.leases) }

// has reports whether lease is recorded. mu held, or the cluster value is not
// shared.
func (s *portLeaseState) has(lease *hostPortLease) bool {
	for _, held := range s.leases {
		if held == lease {
			return true
		}
	}
	return false
}

// release releases every recorded lease and returns the failures. A lease
// stays recorded after a failure and after success, so a later call retries a
// failed one and finds a released one already done. mu held.
func (s *portLeaseState) release(ctx context.Context) []error {
	var errs []error
	for _, lease := range s.leases {
		if err := lease.Release(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
