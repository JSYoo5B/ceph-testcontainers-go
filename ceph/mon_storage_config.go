package ceph

// MonitorStorage is the MON storage group of the Ceph configuration: where every
// MON of the cluster keeps its store, including MONs added later. It is a plain
// value with chainable, value-returning methods, not a customizer; pass it to
// Run through its one grouped option function, WithMonitorStorage. The zero
// value changes nothing.
//
//	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithMonitorStorage(ceph.MonitorStorage{}.WithVolume()))
//
// Run refuses a second WithMonitorStorage that sets the volume again.
type MonitorStorage struct {
	volume bool
}

// WithVolume keeps each MON's store in a Docker volume of its own that the
// cluster owns, mounted at the MON data directory, instead of the container's
// writable layer. The volume outlives the MON's container, so
// MonitorContainer.Replace can start a new container, from another image if
// asked, on the same store and address. Terminate removes the volumes. It needs
// IPv4 addresses: the replacement binds the address the monmap pins. On a
// bridge that is the MON's IPv4 address on the cluster network; on the host
// network it is the public address and the MON's ports, which the replacement
// holds from the removal of the old container until the start of the new one.
//
//purpose:construction
func (s MonitorStorage) WithVolume() MonitorStorage {
	s.volume = true
	return s
}

// WithMonitorStorage applies storage to every MON of the cluster.
//
//purpose:construction
func WithMonitorStorage(storage MonitorStorage) Option {
	return func(o *options) error {
		if !storage.volume {
			return nil
		}
		if o.monitorVolume {
			return invalidRequest("MON storage volume set by more than one WithMonitorStorage")
		}
		o.monitorVolume = true
		return nil
	}
}
