package ceph

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// osdBlockDeviceContainerPath is where a mapped block device appears inside the
// OSD container. It is a fixed name so that the launch script and the recipe
// cannot disagree about the path BlueStore is told to use.
const osdBlockDeviceContainerPath = "/dev/tc-osd-block"

// osdBlockDeviceMinBytes matches the WithOSDBlockSize floor.
const osdBlockDeviceMinBytes = 64 << 20

// osdBlockDeviceHostPath admits only a stable device-mapper name. It is not a
// general host-device path: no partition, loop, by-id or symbolic alias.
var osdBlockDeviceHostPath = regexp.MustCompile(`^/dev/mapper/[A-Za-z0-9][A-Za-z0-9._+-]{0,126}$`)

// osdBlockDevice is the identity a caller verified for one host block device
// before an OSD is created on it: the device-mapper node, the major:minor
// number it observed, and the size it observed. It is a value; the launch
// copies it. This type verifies nothing itself. The caller owns the file, loop
// and device-mapper lease and must have observed these three facts from it.
// The OSD launch script compares the number and size it finds inside the
// container with these values before mkfs, so a substituted node is refused
// there. That is a check of the mapping Docker created, not of the lease.
//
// It is internal: no public option accepts a host device path.
type osdBlockDevice struct {
	hostPath     string
	major, minor uint32
	sizeBytes    int64
}

func (d osdBlockDevice) validate() error {
	switch {
	case !osdBlockDeviceHostPath.MatchString(d.hostPath) || d.hostPath == "/dev/mapper/control":
		return errors.New("OSD block device must be a /dev/mapper node name")
	case d.major == 0:
		return errors.New("OSD block device major number is missing")
	case d.sizeBytes < osdBlockDeviceMinBytes || d.sizeBytes%512 != 0:
		return fmt.Errorf("OSD block device size must be a multiple of 512 bytes and at least %d", osdBlockDeviceMinBytes)
	}
	return nil
}

func (d osdBlockDevice) number() string {
	return strconv.FormatUint(uint64(d.major), 10) + ":" + strconv.FormatUint(uint64(d.minor), 10)
}

// customizer maps the device read-write into the container and tells the launch
// script which device number and size to expect. It appends a device entry and
// does not replace the host-config modifiers or devices of earlier customizers.
func (d osdBlockDevice) customizer() testcontainers.ContainerCustomizer {
	return customizerFunc(func(req *testcontainers.GenericContainerRequest) error {
		for _, opt := range []testcontainers.ContainerCustomizer{
			testcontainers.WithEnv(map[string]string{
				"CEPH_OSD_BLOCK_DEVICE":       osdBlockDeviceContainerPath,
				"CEPH_OSD_BLOCK_DEVICE_ID":    d.number(),
				"CEPH_OSD_BLOCK_DEVICE_BYTES": strconv.FormatInt(d.sizeBytes, 10),
			}),
			testcontainers.WithHostConfigModifier(func(h *container.HostConfig) {
				h.Devices = append(h.Devices, container.DeviceMapping{
					PathOnHost: d.hostPath, PathInContainer: osdBlockDeviceContainerPath, CgroupPermissions: "rw",
				})
			}),
		} {
			if err := opt.Customize(req); err != nil {
				return err
			}
		}
		return nil
	})
}

type customizerFunc func(*testcontainers.GenericContainerRequest) error

func (f customizerFunc) Customize(req *testcontainers.GenericContainerRequest) error { return f(req) }

// attachBlockDevice records the device the recipe maps into its OSD container.
// It must run on the goroutine that built the recipe, before the recipe is
// handed to startOSD: Customize reads the pointer without a lock, and a
// recipe that was already started or shared is refused rather than changed.
// A recipe that mounts tmpfs or volume storage, or that creates a MemStore OSD,
// is refused: the retained-store admission requires a regular block file and a
// device behind a symbolic link must not widen it. The device is mapped into
// exactly this recipe; whether another OSD uses the same node is the caller's
// lease to decide, because no cluster-wide claim table exists yet.
func (r *osdLaunchRecipe) attachBlockDevice(d osdBlockDevice) error {
	if err := d.validate(); err != nil {
		return err
	}
	switch {
	case r.started.Load():
		return errors.New("OSD launch already started; its block device cannot change")
	case r.memstore:
		return errors.New("a block device needs the BlueStore object store")
	case r.memoryVolume != "" || r.dataVolume != "":
		return errors.New("a block device cannot be combined with OSD volume storage")
	}
	if !r.blockDevice.CompareAndSwap(nil, &d) {
		return errors.New("OSD launch already has a block device")
	}
	return nil
}
