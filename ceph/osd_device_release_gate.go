package ceph

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// This file types the two separate receipts that must both exist before a
// device-backed OSD experiment can say its device chain is no longer used, and
// the verdict that joins them. It is internal and has no production caller
// (osdChainReleaseCallers is empty). It releases, shortens or hands over
// nothing: the file, loop and device-mapper chain, its identity checks and its
// release barrier belong to the external lease owner, and no value in this file
// can be turned into an action on them.
//
// The two receipts:
//
//   - Cluster container exclusion (osdClusterExclusion): read from the claim
//     table (osd_device_claims.go). It says only that no OSD of this cluster
//     holds a claim naming the lease, the /dev/mapper node or the major:minor.
//     A claim is dropped only on container evidence, so absence means no
//     container of this cluster can hold the device.
//   - Chain evidence (osdChainEvidence): reported by the lease owner through
//     osdChainLeaseOwner. The cluster cannot see it: the daemon process ended,
//     observer borrows closed, and a fresh, complete opener scan found no user.
//
// Neither receipt implies the other. A dropped claim without owner evidence
// stays retained; owner evidence without a dropped claim stays retained.
//
// This is the lease-owner side of the device-backed capacity experiment (#63),
// distinct from the file-backed tmpfs oracle in internal/integration, which this
// file does not touch or reuse. It carries no capacity, allocation or ENOSPC
// notion: a finite device size is an identity field here and nothing else.
//
// Lock order. exclusionOf reads the claim table, so its caller holds
// Container.mu taken with lockTopology (ceph/topology.go:380,
// ceph/context_lock.go:11), as osdDeviceClaims.claimOf's callers do. The lease
// owner call (collectChainEvidence) may block on /proc and the kernel and must
// therefore run before or after that hold, never inside it. judgeChainRelease
// is pure and takes no lock. This file adds no mutex, goroutine, defer or
// import beyond context, errors, fmt and reflect.

// osdChainFact is a tri-state observation. The zero value is unknown, so a
// missing field never counts as evidence.
type osdChainFact uint8

const (
	osdChainUnknown osdChainFact = iota
	osdChainNo
	osdChainYes
)

// osdChainEvidence is a copy of what the lease owner observed about its chain
// for one lease. device is the identity the owner observed on the chain, not
// the one it was asked about.
type osdChainEvidence struct {
	lease         string
	device        osdBlockDevice
	daemonStopped osdChainFact // the OSD daemon process ended (start identity checked by the owner)
	borrowsClosed osdChainFact // every observer descriptor borrow was closed
	scanComplete  osdChainFact // the fresh opener scan covered every process and fd without error
	openers       int          // processes found holding the device; meaningful only when scanComplete is yes, never negative
}

// osdChainLeaseOwner is what the external lease owner provides. It has one
// read-only method on purpose: the interface carries no release, detach or
// destroy capability, so a holder of it cannot act on the chain.
type osdChainLeaseOwner interface {
	chainEvidence(ctx context.Context, lease string) (osdChainEvidence, error)
}

// collectChainEvidence asks owner for evidence. A nil owner (including a typed
// nil), an owner error and a result that arrives after ctx ended are errors:
// none is evidence. It takes no lock.
func collectChainEvidence(ctx context.Context, owner osdChainLeaseOwner, lease string) (osdChainEvidence, error) {
	if owner == nil {
		return osdChainEvidence{}, errors.New("OSD chain evidence needs a lease owner")
	}
	switch v := reflect.ValueOf(owner); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		if v.IsNil() {
			return osdChainEvidence{}, errors.New("OSD chain evidence needs a lease owner, got a typed nil")
		}
	}
	if err := ctx.Err(); err != nil {
		return osdChainEvidence{}, err
	}
	ev, err := owner.chainEvidence(ctx, lease)
	if err != nil {
		return osdChainEvidence{}, fmt.Errorf("lease owner evidence: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return osdChainEvidence{}, fmt.Errorf("lease owner evidence arrived after the deadline: %w", err)
	}
	return ev, nil
}

// osdClusterExclusion is a copy of what the claim table says about one lease
// and device at one moment. It is valid only at that moment.
type osdClusterExclusion struct {
	lease  string
	device osdBlockDevice
	held   bool // some claim names this lease, node or major:minor
	osd    int  // the holder, meaningful only when held
}

// exclusionOf reads the table; it changes nothing. The caller holds
// Container.mu, as for claimOf.
func (s *osdDeviceClaims) exclusionOf(lease string, device osdBlockDevice) osdClusterExclusion {
	ex := osdClusterExclusion{lease: lease, device: device}
	for _, id := range s.osds() {
		other := s.records[id]
		if other.lease == lease || other.device.hostPath == device.hostPath || other.device.number() == device.number() {
			ex.held, ex.osd = true, id
			return ex
		}
	}
	return ex
}

// osdChainOutcome is why the verdict is what it is. The zero value retains.
type osdChainOutcome uint8

const (
	// osdChainRetainUnknown: the evidence is missing, null, partial, late,
	// incomplete or from an owner that failed. Not busy and not clear.
	osdChainRetainUnknown osdChainOutcome = iota
	// osdChainRetainClaim: a claim of this cluster still names the device.
	osdChainRetainClaim
	// osdChainRetainIdentity: the evidence describes another lease or device.
	osdChainRetainIdentity
	// osdChainRetainBusy: the owner observed the daemon, a borrow or an opener.
	osdChainRetainBusy
	// osdChainClear: both receipts are present, agree on identity and raise no
	// objection. It is not authority to release anything.
	osdChainClear
)

// osdChainVerdict is the joined outcome. detail names the first reason.
type osdChainVerdict struct {
	outcome osdChainOutcome
	detail  string
}

// retained reports whether the whole chain must stay with its owner.
func (v osdChainVerdict) retained() bool { return v.outcome != osdChainClear }

// judgeChainRelease joins the cluster receipt and the owner's evidence for
// lease and device. It is pure. Every uncertainty retains, and the claim check
// runs first so a held claim is reported as such whatever the owner says.
func judgeChainRelease(ex osdClusterExclusion, ev osdChainEvidence, evErr error, lease string, device osdBlockDevice) osdChainVerdict {
	retain := func(o osdChainOutcome, format string, args ...any) osdChainVerdict {
		return osdChainVerdict{outcome: o, detail: fmt.Sprintf(format, args...)}
	}
	if !osdDeviceLease.MatchString(lease) {
		return retain(osdChainRetainUnknown, "lease token is not admissible")
	}
	if err := device.validate(); err != nil {
		return retain(osdChainRetainUnknown, "device is not admissible: %v", err)
	}
	if ex.lease != lease || ex.device != device {
		return retain(osdChainRetainUnknown, "cluster exclusion receipt is for another lease or device")
	}
	if ex.held {
		return retain(osdChainRetainClaim, "osd.%d still holds a claim", ex.osd)
	}
	if evErr != nil {
		return retain(osdChainRetainUnknown, "no owner evidence: %v", evErr)
	}
	if ev.lease != lease || ev.device != device {
		return retain(osdChainRetainIdentity, "owner evidence describes lease %q device %s %s", ev.lease, ev.device.hostPath, ev.device.number())
	}
	switch {
	case ev.daemonStopped == osdChainNo:
		return retain(osdChainRetainBusy, "the daemon has not stopped")
	case ev.borrowsClosed == osdChainNo:
		return retain(osdChainRetainBusy, "an observer borrow is still open")
	case ev.daemonStopped != osdChainYes:
		return retain(osdChainRetainUnknown, "daemon stop is not established")
	case ev.borrowsClosed != osdChainYes:
		return retain(osdChainRetainUnknown, "borrow closure is not established")
	case ev.scanComplete != osdChainYes:
		return retain(osdChainRetainUnknown, "the opener scan is not complete")
	case ev.openers < 0:
		return retain(osdChainRetainUnknown, "negative opener count")
	case ev.openers > 0:
		return retain(osdChainRetainBusy, "%d process(es) still hold the device", ev.openers)
	}
	return osdChainVerdict{outcome: osdChainClear, detail: "no objection from the cluster receipt or the owner evidence"}
}
