package ceph

// ObserveTenant returns an Observer for tenant's shared cluster. A nil tenant
// returns an error and a nil Observer. The tenant is not validated or read
// here; every query validates it.
//
// This constructor belongs to the tenant side. It hands the Observer the
// tenant's cluster resolution and its admission check, and nothing else, so
// the Observer does not name Tenant.
func ObserveTenant(tenant *Tenant) (*Observer, error) {
	if tenant == nil {
		return nil, invalidRequest("observe: the tenant is nil")
	}
	return newLeasedObserver(tenant.container, tenant.check), nil
}
