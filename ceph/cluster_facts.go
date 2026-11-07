package ceph

import "errors"

// ClusterFacts is an independent snapshot of the nine static cluster facts. It
// holds only values: no Container, Docker handle, context, lock, pointer, func
// or channel, so nothing in it can control the cluster or reactivate it, and it
// owns no resource or cleanup lifetime. Later changes to the cluster do not
// update an obtained snapshot, and changing a snapshot does not change the
// cluster or another snapshot.
//
// The values describe the recorded configuration and identity of the cluster.
// They do not show that its resources still exist, are reachable or run, and
// they are not a native observation. Images and CephVersion describe the
// cluster's own settings and first MON; a daemon started with DaemonOptions.WithImage
// is not reflected (see the Image and Version methods of its handle).
type ClusterFacts struct {
	AddressFamily             AddressFamily
	Images                    ImageSet
	MessengerMode             MessengerMode
	NetworkName               string
	UsesHostNetwork           bool
	PublicAddress             string
	ClusterNetworkName        string
	HasSeparateClusterNetwork bool
	CephVersion               string
}

// errNilClusterFacts reports a capture request without a cluster. The capture
// then returns the zero snapshot, which is distinct from captured values.
var errNilClusterFacts = invalid(errors.New("cannot capture static facts of a nil cluster"))

// FactsOf returns an independent snapshot of the static facts of a non-nil
// cluster, including a terminated one. A nil cluster returns the zero
// ClusterFacts and an error that matches ErrInvalidRequest.
//
// The facts are written by Run before it returns and are never cleared by
// Terminate, so FactsOf works on a terminated cluster or while Terminate runs,
// without a lock, command or Docker call. A fact that was never recorded stays
// empty (for example the CephVersion of a cluster that Run returned with an
// error); an empty value does not distinguish not recorded from recorded as
// empty, and no value reports whether the resources still exist.
//
//purpose:observation
func FactsOf(c *Container) (ClusterFacts, error) {
	if c == nil {
		return ClusterFacts{}, errNilClusterFacts
	}
	return c.staticFacts(), nil
}

// staticFacts is the lock-free read behind FactsOf. Code inside the package
// uses it directly: it holds a non-nil cluster that may still be starting and
// has no use for the nil-cluster error. It reads the same recorded sources as
// FactsOf, so TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup
// guards both.
func (c *Container) staticFacts() ClusterFacts {
	return ClusterFacts{
		AddressFamily:             c.settings.addressFamily,
		Images:                    c.settings.effectiveImages(),
		MessengerMode:             c.settings.messengerMode,
		NetworkName:               c.settings.publicNetworkName(c.nets.public),
		UsesHostNetwork:           c.settings.hostNetwork,
		PublicAddress:             c.settings.advertisedPublicAddress(),
		ClusterNetworkName:        c.settings.clusterNetworkName(c.nets.public, c.nets.cluster),
		HasSeparateClusterNetwork: c.nets.cluster != nil,
		CephVersion:               c.bootstrap.rawVersion(),
	}
}
