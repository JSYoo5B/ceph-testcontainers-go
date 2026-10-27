package ceph

import (
	"fmt"
	"maps"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/moby/moby/api/types/mount"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// osdLaunchRecipe captures values, not file Readers or a mutable Container.
// Customize can build independent requests; this does not authorize another
// container creation. startOSD consumes started once, including uncertain errors.
// Storage names are mounts only, not retained-store descriptors or leases.
type osdLaunchRecipe struct {
	image, config, adminKeyring, osdKeyring string
	id                                      int
	uuid                                    string
	placement                               OSDConfig
	hostNetwork                             bool
	publicAddress, publicNetwork            string
	clusterNetwork, runVolume, memoryVolume string
	dataVolume                              string
	owner                                   string
	timeout                                 time.Duration
	started                                 atomic.Bool
}

// The caller holds c.mu. Strings own snapshots of the source slices; each
// request receives fresh file Readers, maps, wait strategy and mount options.
func (c *Container) newOSDLaunchRecipe(osd *OSDContainer, keyring []byte) *osdLaunchRecipe {
	c.configMu.RLock()
	config, admin := string(c.config), string(c.keyring)
	c.configMu.RUnlock()
	image := osd.image
	if image == "" {
		image = c.settings.essentialImage
	}
	r := &osdLaunchRecipe{image: image, config: config, adminKeyring: admin, osdKeyring: string(keyring),
		id: osd.ID, uuid: osd.nativeUUID, placement: osd.placement,
		hostNetwork: c.settings.hostNetwork, publicAddress: c.settings.publicAddress, publicNetwork: c.NetworkName(),
		owner: c.owner, timeout: c.settings.startupTimeout}
	if c.nets.cluster != nil {
		r.clusterNetwork = c.nets.cluster.Name
	}
	if c.run != nil {
		r.runVolume = c.run.name
	}
	r.memoryVolume, r.dataVolume = c.stores.launchVolumes()
	return r
}

func (r *osdLaunchRecipe) Customize(req *testcontainers.GenericContainerRequest) error {
	config, err := clientContainerConfig([]byte(r.config), nil)
	if err != nil {
		return err
	}
	attach := network.WithNetworkName(nil, r.publicNetwork)
	if r.hostNetwork {
		attach = hostContainerCustomizer(r.publicAddress)
	}
	opts := []testcontainers.ContainerCustomizer{
		attach,
		testcontainers.WithFiles(textFile("/etc/ceph/ceph.conf", config, 0o644),
			textFile("/etc/ceph/ceph.client.admin.keyring", []byte(r.adminKeyring), 0o600)),
		testcontainers.WithEntrypoint("/bin/sh", "/tc/osd.sh"), testcontainers.WithCmd(),
		testcontainers.WithEnv(map[string]string{
			"CEPH_OSD_ID": strconv.Itoa(r.id), "CEPH_OSD_UUID": r.uuid, "CEPH_OSD_HOST": r.placement.Host,
			"CEPH_OSD_ROOT": r.placement.Root, "CEPH_OSD_RACK": r.placement.Rack, "CEPH_OSD_DEVICE_CLASS": r.placement.DeviceClass,
		}),
		testcontainers.WithFiles(scriptFile("osd"), textFile("/etc/ceph/osd.keyring", []byte(r.osdKeyring), 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", fmt.Sprintf("/var/run/ceph/ceph-osd.%d.asok", r.id)}).WithStartupTimeout(r.timeout)),
	}
	if r.memoryVolume != "" {
		opts = append(opts, osdLaunchVolume(r.memoryVolume, "/var/lib/ceph/osd", true))
	}
	if r.dataVolume != "" {
		opts = append(opts, osdLaunchVolume(r.dataVolume, "/var/lib/ceph/osd", true))
	}
	if r.runVolume != "" {
		opts = append(opts, osdLaunchVolume(r.runVolume, adminSocketDir, false))
	}
	if r.clusterNetwork != "" {
		opts = append(opts, network.WithNetworkName(nil, r.clusterNetwork))
	}
	opts = append(opts, testcontainers.WithLabels(map[string]string{clusterOwnerLabel: r.owner}))
	for _, opt := range opts {
		if err := opt.Customize(req); err != nil {
			return err
		}
	}
	return nil
}

func osdLaunchVolume(name, target string, noCopy bool) testcontainers.CustomizeRequestOption {
	return testcontainers.WithMounts(testcontainers.ContainerMount{
		Source: testcontainers.DockerVolumeMountSource{Name: name, VolumeOptions: &mount.VolumeOptions{
			NoCopy: noCopy, Labels: maps.Clone(testcontainers.GenericLabels()),
		}},
		Target: testcontainers.ContainerMountTarget(target),
	})
}
