package ceph

import "github.com/testcontainers/testcontainers-go"

// The static cluster facts (AddressFamily, ControlImage, Images, MessengerMode,
// NetworkName, UsesHostNetwork, PublicAddress, ClusterNetworkName and
// HasSeparateClusterNetwork) are derived here from the immutable run settings
// and from the bridge networks that the caller passes in. These helpers take no
// lock, run no command and make no Docker call, so a fact stays readable after
// termination. They never see the Container: each exported accessor keeps its
// own nil handling and reads the fields it needs itself.

// hostNetworkName is the network name that host mode reports for both traffic
// planes.
const hostNetworkName = "host"

// effectiveImages returns the role images of the settings, including aggregate
// selections. The returned set is a copy.
func (o options) effectiveImages() ImageSet {
	if o.imageSet != nil {
		return *o.imageSet
	}
	return ImageSet{Essential: o.controlImage, Client: o.controlImage, CephFS: o.mdsImage, RBDAux: o.controlImage, RGW: o.rgwImage}
}

// publicNetworkName returns the bridge's name, or "host" in host mode. A nil
// bridge reports the empty name.
func (o options) publicNetworkName(bridge *testcontainers.DockerNetwork) string {
	if o.hostNetwork {
		return hostNetworkName
	}
	if bridge == nil {
		return ""
	}
	return bridge.Name
}

// clusterNetworkName returns the replication bridge's name. Without a separate
// bridge it returns the public network name.
func (o options) clusterNetworkName(public, cluster *testcontainers.DockerNetwork) string {
	if cluster != nil {
		return cluster.Name
	}
	return o.publicNetworkName(public)
}

// advertisedPublicAddress returns the host-mode address; empty in bridge mode.
func (o options) advertisedPublicAddress() string {
	if !o.hostNetwork {
		return ""
	}
	return o.publicAddress
}
