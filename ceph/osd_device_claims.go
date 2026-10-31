package ceph

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
)

// osdDeviceLease admits the opaque token a caller names its device lease with.
// The table compares it and never interprets it.
var osdDeviceLease = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// osdDeviceClaims is the cluster-wide claim table of the host block devices
// that OSDs of one cluster are launched on: the table that the pre-mkfs launch
// seam (osd_block_device.go) left to its first production caller. It is a
// primitive in the shape of osdStorageState and monStoreState. Only the
// unexported wiring step registerBlockDeviceOSD (osd_device_wiring.go) calls
// claim, and its one production caller is addBlockDeviceOSD
// (osd_device_launch.go), which only internal/faulthook reaches. A cluster that
// never adds a device-backed OSD holds no claim, and the two Terminate stages
// and the removal hook that use the table return without effect.
//
// A claim is exclusion bookkeeping inside the Go cluster and nothing more. It
// is not the device lease: the caller still owns the file, loop and
// device-mapper chain, its identity checks, its serialized pre-start recipe and
// its release barrier. Dropping a claim says only that no container of this
// cluster can hold the device any more. It never says that the mapping, the
// loop association or the backing file may be released, and no code here
// touches them.
//
// What is unique: one claim per OSD ID, one per lease token, one per /dev/mapper
// node and one per major:minor number, each checked against every held claim, so
// that a second name for the same device or a second OSD on the same mapping is
// refused at admission. A refused claim changes nothing. A claim is never
// replaced or merged; repeating the same request is a conflict with itself.
//
// The type has no mutex. Container.mu guards the map, as it guards the OSD
// registry: claim runs where registerOSD runs, with the topology lock held and
// before the recipe can start, and release and settle run with it held from
// removeOSD and terminate. No method takes a lock or calls into Container.
//
// The record keeps the recipe's one-shot (osdLaunchRecipe.started) as its
// witness of whether a container creation was attempted. That atomic is the
// only state shared with startOSD, which runs without Container.mu. Releasing a
// claim whose creation was never attempted first seals the one-shot, so a
// recipe that outlives its claim can no longer start: startOSD reports the
// ordinary "do not replay" error and creates nothing. A claim whose creation
// was attempted stays until the container is confirmed gone, because a lost
// create reply can leave a running container that holds the device.
//
// Release order, with the existing Terminate order unchanged: an OSD's claim
// goes right after its own container terminated (or was never created), before
// the handle is forgotten. Whatever is left goes in settle, once the
// labeled-container removal succeeded, because only that removal reaches a
// container whose create reply was lost. A claim that survives Terminate is
// reported in its error, so the lease owner keeps the whole chain.
type osdDeviceClaims struct {
	records map[int]*osdDeviceClaim
}

// osdDeviceClaim is one held claim.
type osdDeviceClaim struct {
	osd      int
	lease    string
	device   osdBlockDevice
	creation *atomic.Bool
}

// osdDeviceClaimInfo is a copy of a claim for callers; it shares nothing.
type osdDeviceClaimInfo struct {
	osd       int
	lease     string
	device    osdBlockDevice
	attempted bool
}

// claim admits the device of osd under lease before the OSD's container is
// created. creation must be the one-shot of the recipe that will start the OSD,
// and must not be spent yet.
func (s *osdDeviceClaims) claim(osd int, lease string, device osdBlockDevice, creation *atomic.Bool) error {
	switch {
	case osd < 0:
		return errors.New("OSD device claim needs an OSD ID")
	case !osdDeviceLease.MatchString(lease):
		return errors.New("OSD device claim needs a lease token of letters, digits and . _ : -")
	case creation == nil:
		return errors.New("OSD device claim needs the creation one-shot of its launch")
	case creation.Load():
		return fmt.Errorf("osd.%d creation was already attempted; claim its device before the launch", osd)
	}
	if err := device.validate(); err != nil {
		return err
	}
	for _, id := range s.osds() {
		other := s.records[id]
		switch {
		case other.osd == osd:
			return fmt.Errorf("osd.%d already holds a device claim; a second one is never admitted", osd)
		case other.lease == lease:
			return fmt.Errorf("device lease %s is already claimed by osd.%d", lease, other.osd)
		case other.device.hostPath == device.hostPath:
			return fmt.Errorf("device %s is already claimed by osd.%d", device.hostPath, other.osd)
		case other.device.number() == device.number():
			return fmt.Errorf("device number %s is already claimed by osd.%d", device.number(), other.osd)
		}
	}
	if s.records == nil {
		s.records = map[int]*osdDeviceClaim{}
	}
	s.records[osd] = &osdDeviceClaim{osd: osd, lease: lease, device: device, creation: creation}
	return nil
}

// claimOf returns a copy of the claim of osd.
func (s *osdDeviceClaims) claimOf(osd int) (osdDeviceClaimInfo, bool) {
	r := s.records[osd]
	if r == nil {
		return osdDeviceClaimInfo{}, false
	}
	return osdDeviceClaimInfo{osd: r.osd, lease: r.lease, device: r.device, attempted: r.creation.Load()}, true
}

// count returns the number of held claims.
func (s *osdDeviceClaims) count() int { return len(s.records) }

// osds returns the OSD IDs that hold a claim, ascending.
func (s *osdDeviceClaims) osds() []int {
	return slices.Sorted(func(yield func(int) bool) {
		for id := range s.records {
			if !yield(id) {
				return
			}
		}
	})
}

// release drops the claim of osd and reports whether it was dropped. The caller
// passes containerConfirmed only when it observed the OSD's container removed
// (terminated, or reported missing). Without that, a claim whose creation was
// attempted stays; one whose creation was not is sealed against a later start
// and dropped. A missing claim is not an error and reports false.
func (s *osdDeviceClaims) release(osd int, containerConfirmed bool) bool {
	r := s.records[osd]
	if r == nil {
		return false
	}
	sealed := r.creation.CompareAndSwap(false, true)
	if !sealed && !containerConfirmed {
		return false
	}
	delete(s.records, osd)
	return true
}

// settle is the second Terminate stage. scanRemoved is true only when the
// labeled-container removal ran for an owned cluster and returned no error: it
// reaches a container whose create reply was lost, so every remaining claim is
// then dropped. Otherwise it drops only the claims that were never attempted,
// and returns an error naming each claim that stays.
func (s *osdDeviceClaims) settle(scanRemoved bool) error {
	var kept []string
	for _, id := range s.osds() {
		r := s.records[id]
		if s.release(id, scanRemoved) {
			continue
		}
		kept = append(kept, fmt.Sprintf("osd.%d lease %s device %s", id, r.lease, r.device.hostPath))
	}
	if len(kept) == 0 {
		return nil
	}
	return fmt.Errorf("OSD device claims retained because a container may still hold the device; keep the lease chain: %s", strings.Join(kept, "; "))
}
