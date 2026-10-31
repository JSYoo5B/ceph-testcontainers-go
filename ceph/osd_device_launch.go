package ceph

import (
	"context"
	"fmt"
)

// addBlockDeviceOSD is the one production caller of registerBlockDeviceOSD. It
// adds an OSD whose BlueStore main device is the caller's device-mapper node,
// in the cluster's default placement and essential image, and waits for it as
// AddOSD does. It is internal: internal/faulthook reaches it for the
// block-device fault tests, and no public option or exported name leads to it.
//
// Lock. It takes the topology lock with lockTopology and keeps it through
// registerBlockDeviceOSD, startOSD and the up/in wait, exactly as AddOSD does.
//
// Lease. The caller owns the file, loop and device-mapper chain behind the
// device and keeps it until judgeDeviceChain says clear. An error here never
// releases that chain: an OSD returned with an error is tracked for cleanup,
// and its claim follows the claim table's rules (osd_device_claims.go).
func (c *Container) addBlockDeviceOSD(ctx context.Context, device osdBlockDevice, lease string) (*OSDContainer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan, err := composeOSDOptions(nil)
	if err != nil {
		return nil, err
	}
	config, err := normalizeOSDConfig(resolveOSDDefaults(c.settings, plan.placement))
	if err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	launch, err := c.osdFleet().registerBlockDeviceOSD(ctx, config, c.settings.essentialImage, device, lease)
	if err != nil {
		return launch.osd, err
	}
	if err := c.osdFleet().startOSD(ctx, launch); err != nil {
		return launch.osd, err
	}
	if err := (osdMap{c.conn()}).waitUp(ctx, []int{launch.osd.ID}); err != nil {
		return launch.osd, err
	}
	return launch.osd, osdMap{c.conn()}.assignDeviceClass(ctx, launch.osd)
}

// judgeDeviceChain is the one production caller of the release gate
// (osd_device_release_gate.go). It collects the lease owner's evidence first,
// without the lock, because the owner may block on the kernel; then it reads
// the claim table under the topology lock and joins the two. It works on a
// terminated cluster too: Terminate settles the claims and leaves the table
// readable. A lock that cannot be taken before ctx ends retains the chain.
func (c *Container) judgeDeviceChain(ctx context.Context, device osdBlockDevice, lease string, owner osdChainLeaseOwner) osdChainVerdict {
	ev, evErr := collectChainEvidence(ctx, owner, lease)
	if err := c.lockTopology(ctx); err != nil {
		return osdChainVerdict{outcome: osdChainRetainUnknown, detail: fmt.Sprintf("claim table not read: %v", err)}
	}
	ex := c.fleet.deviceClaims.exclusionOf(lease, device)
	c.mu.Unlock()
	return judgeChainRelease(ex, ev, evErr, lease, device)
}

// osdChainOwnerFunc adapts a function to osdChainLeaseOwner.
type osdChainOwnerFunc func(ctx context.Context, lease string) (osdChainEvidence, error)

func (f osdChainOwnerFunc) chainEvidence(ctx context.Context, lease string) (osdChainEvidence, error) {
	return f(ctx, lease)
}

// osdChainOutcomeNames are the names internal/faulthook reports.
var osdChainOutcomeNames = map[osdChainOutcome]string{
	osdChainRetainUnknown:  "retain-unknown",
	osdChainRetainClaim:    "retain-claim",
	osdChainRetainIdentity: "retain-identity",
	osdChainRetainBusy:     "retain-busy",
	osdChainClear:          "clear",
}
