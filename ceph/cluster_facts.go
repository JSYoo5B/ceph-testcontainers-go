package ceph

import "errors"

// ClusterFacts is an independent snapshot of the ten static cluster facts. It
// holds only values: no Container, Docker handle, context, lock, pointer, func
// or channel, so nothing in it can control the cluster or reactivate it, and it
// owns no resource or cleanup lifetime. Later changes to the cluster do not
// update an obtained snapshot, and changing a snapshot does not change the
// cluster or another snapshot.
//
// The values describe the recorded configuration and identity of the cluster.
// They do not show that its resources still exist, are reachable or run, and
// they are not a native observation. Images and CephVersion describe the
// cluster's own settings and first MON; a daemon started with WithDaemonImage
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
// It takes no lock, runs no command and makes no Docker call, and it never
// reads the closed flag: closing starts before teardown finishes,
// so the flag is no evidence about the state of the owned resources, and
// termination alone must not fail the capture. The recorded settings, version
// and network records are written before the cluster is returned to the caller
// and termination does not clear them, so reading them needs no
// synchronization with a concurrent Terminate. Retained and read are the run
// settings, the public and replication network records and the Ceph version
// recorded at startup. A fact never recorded, as in a cluster returned by Run
// with an error, stays empty and is not replaced by a default.
// TestClusterFactsRecordedSourcesAreNotRewrittenOutsideStartup guards that no
// later code path writes these sources.
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
