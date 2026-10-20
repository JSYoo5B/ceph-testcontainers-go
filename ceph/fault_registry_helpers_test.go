package ceph

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
