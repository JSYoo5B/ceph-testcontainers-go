package ceph

import "sync"

// leaseDouble stands in for a shared-harness tenant lease in the Observer
// controls of this package, which cannot name the harness Tenant. It has the
// two behaviors that the tenant form of an Observer depends on, as
// ceph/harness.Tenant provides them: a target that resolves the leased cluster
// and refuses an expired lease, and the lease check itself. The harness side
// of the seam, with a real Tenant, is covered in ceph/harness
// (observe_tenant_test.go).
type leaseDouble struct {
	cluster *Container

	mu         sync.Mutex
	released   bool
	terminated bool
}

func newLeaseDouble(cluster *Container) *leaseDouble { return &leaseDouble{cluster: cluster} }

func (l *leaseDouble) check() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.terminated {
		return invalidRequest("tenant is released")
	}
	return nil
}

func (l *leaseDouble) container() (*Container, error) {
	if err := l.check(); err != nil {
		return nil, err
	}
	return l.cluster, nil
}

// release ends the lease while the cluster lives, as a tenant's cleanup does.
func (l *leaseDouble) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released = true
}

// terminate ends the cluster while the lease is held, as Close does.
func (l *leaseDouble) terminate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.terminated = true
}

// observer returns the tenant form of an Observer over the double.
func (l *leaseDouble) observer() *Observer { return newLeasedObserver(l.container, l.check) }
