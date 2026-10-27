package ceph

import "github.com/testcontainers/testcontainers-go"

// The static cluster facts (AddressFamily, Images, MessengerMode,
// NetworkName, UsesHostNetwork, PublicAddress, ClusterNetworkName and
// HasSeparateClusterNetwork) are derived here from the immutable run settings
// and from the bridge networks that the caller passes in. These helpers take no
// lock, run no command and make no Docker call, so a fact stays readable after
// termination. They never see the Container: FactsOf and the in-package
// staticFacts read the fields they need and pass them in.

// hostNetworkName is the network name that host mode reports for both traffic
// planes.
const hostNetworkName = "host"

// effectiveImages returns the role images of the settings, including aggregate
// selections. The returned set is a copy.
func (o options) effectiveImages() ImageSet {
	if o.imageSet != nil {
		return *o.imageSet
	}
	return o.defaultImages()
}

// defaultImages is the aggregate selection of a Run without WithImages: the
// Run image supplies every role, except where a service image option replaced
// the cephfs or rgw role.
func (o options) defaultImages() ImageSet {
	images := ImageSet{Essential: o.essentialImage, Client: o.essentialImage, CephFS: o.essentialImage, RBDAux: o.essentialImage, RGW: o.essentialImage}
	if o.cephfsOverride != "" {
		images.CephFS = o.cephfsOverride
	}
	if o.rgwOverride != "" {
		images.RGW = o.rgwOverride
	}
	return images
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
