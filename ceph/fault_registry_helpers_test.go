package ceph

import "context"

// faultsOf returns the keys of kind that the registry reserves or holds,
// each with its handle (nil while reserved).
func (c *Container) faultsOf(kind faultKind) map[string]any {
	faults := make(map[string]any)
	for id, entry := range c.faults.entries {
		if id.kind == kind {
			faults[id.key] = entry.handle
		}
	}
	return faults
}

// heldFault returns the handle that kind holds for key, or nil.
func (c *Container) heldFault(kind faultKind, key string) any {
	if entry := c.faults.entries[faultID{kind: kind, key: key}]; entry != nil {
		return entry.handle
	}
	return nil
}

// trackInterruption registers link as the cluster form of InterruptNetwork
// does after the disconnect.
func (c *Container) trackInterruption(key string, link *NetworkInterruption) {
	token, _, err := c.reserveFault(context.Background(), faultNetworkInterruption, key)
	if err != nil {
		panic(err)
	}
	c.settleFault(token, link.NetworkName, link, link.Restore, link.isComplete)
}
