package ceph

import (
	"context"
	"errors"
)

// seal spends the creation one-shot without creating anything, so that no
// holder of this recipe can start it any more. startOSD then reports the
// ordinary "do not replay" error. Sealing a recipe that was already started
// changes nothing; the table's release rules (osd_device_claims.go) cover a
// claim whose creation was attempted.
func (r *osdLaunchRecipe) seal() { r.started.CompareAndSwap(false, true) }

// registerBlockDeviceOSD is the one serialized wiring step of the block-device
// launch: it registers the next OSD, builds its launch recipe, maps the
// caller-verified device into that recipe and publishes the claim of the device
// under the caller's lease token, and hands the launch back for startOSD. It is
// the only caller of osdLaunchRecipe.attachBlockDevice and of
// osdDeviceClaims.claim. It is internal: no option and no exported name reach
// it, and its one production caller is addBlockDeviceOSD (osd_device_launch.go,
// osdDeviceWiringCallers), which only internal/faulthook reaches. The default
// OSD launch, AddOSD and the initial OSDs do not reach it, so their behavior is
// unchanged.
//
// Lock. The caller holds Container.mu, taken with lockTopology
// (ceph/topology.go:380, ceph/context_lock.go:11), and keeps it through
// startOSD, exactly as AddOSD does between ceph/ceph.go:548 (acquire,
// released by the defer at :551), registerOSD (:560) and startOSD (:564);
// startOSD itself runs without the lock (:668-669). This function takes no
// lock, starts no goroutine and adds no mutex, so the order is the existing one:
// Container.mu, then the leaf locks that registerOSD's collaborators take.
// Inside the step the order is fixed: validate the inputs (pure), registerOSD
// (which builds the recipe through newOSDLaunchRecipe), attachBlockDevice,
// claim. Nothing is published by an input check; the claim table is written
// last and only after the attach, so the table never holds a claim for a recipe
// that has no device.
//
// Lease token. The token is opaque: the table compares it and never interprets
// it, and this function passes it through unchanged. The caller owns the
// external lease (the file, loop and device-mapper chain, its identity checks
// and its release barrier) and keeps it through uncertain startup and owned
// cleanup. This function never releases, shortens or hands over that lease, and
// no result of it, success or failure, authorizes the caller to release the
// chain. Dropping the claim says only that no container of this cluster can
// hold the device any more.
//
// Failures, each with nothing published. An invalid device or token fails
// before registerOSD: no OSD is registered and the zero launch is returned. A
// failure of registerOSD is returned as it is, with no claim. A failure of the
// attach or of the claim leaves the OSD registered, as every later failure of
// registerOSD does, and Terminate owns it; the recipe is sealed with seal
// because its creation was never attempted, so it cannot start, and the
// returned launch carries the OSD and that sealed recipe. A refused claim
// changes nothing in the table. On success the claim is held with its creation
// not attempted; from then on the claim follows the table's own rules: released
// by osdDeviceClaims.release(osd, false), which seals the recipe, until
// startOSD attempts the creation, and kept after any attempted or uncertain
// creation until a confirmed container removal (removeOSD, terminate, settle).
// The caller must not drop a claim any other way.
func (f osdFleet) registerBlockDeviceOSD(ctx context.Context, config OSDConfig, image string, device osdBlockDevice, lease string) (osdLaunch, error) {
	if err := device.validate(); err != nil {
		return osdLaunch{}, err
	}
	if !osdDeviceLease.MatchString(lease) {
		return osdLaunch{}, errors.New("OSD device launch needs a lease token of letters, digits and . _ : -")
	}
	launch, err := f.registerOSD(ctx, config, image, nil)
	if err != nil {
		return launch, err
	}
	if err := launch.recipe.attachBlockDevice(device); err != nil {
		launch.recipe.seal()
		return launch, err
	}
	if err := f.deviceClaims.claim(launch.osd.ID, lease, device, &launch.recipe.started); err != nil {
		launch.recipe.seal()
		return launch, err
	}
	return launch, nil
}
