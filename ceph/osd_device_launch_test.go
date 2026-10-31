package ceph

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// judgeDeviceChain joins the live claim table with the owner's evidence. These
// cases use a cluster value with no container: the claim table and the lock
// are all the judge touches.
func TestJudgeDeviceChainReadsTheClaimTableAfterTheEvidence(t *testing.T) {
	device := osdBlockDevice{hostPath: "/dev/mapper/tc-judge", major: 253, minor: 7, sizeBytes: 128 << 20}
	const lease = "tc-judge"
	stopped := osdChainOwnerFunc(func(context.Context, string) (osdChainEvidence, error) {
		return osdChainEvidence{lease: lease, device: device, daemonStopped: osdChainYes, borrowsClosed: osdChainYes, scanComplete: osdChainYes}, nil
	})
	c := &Container{}
	var creation atomic.Bool
	if err := c.fleet.deviceClaims.claim(3, lease, device, &creation); err != nil {
		t.Fatal(err)
	}
	if v := c.judgeDeviceChain(t.Context(), device, lease, stopped); v.outcome != osdChainRetainClaim {
		t.Fatalf("a held claim must retain the chain: %+v", v)
	}
	// The evidence is collected before the lock: an owner that would block on
	// the lock is answered.
	locked := osdChainOwnerFunc(func(ctx context.Context, l string) (osdChainEvidence, error) {
		if !c.mu.TryLock() {
			return osdChainEvidence{}, errors.New("the judge holds the lock while the owner runs")
		}
		c.mu.Unlock()
		return stopped(ctx, l)
	})
	c.fleet.deviceClaims.release(3, true)
	if v := c.judgeDeviceChain(t.Context(), device, lease, locked); v.outcome != osdChainClear {
		t.Fatalf("no claim and complete evidence must be clear: %+v", v)
	}
	if v := c.judgeDeviceChain(t.Context(), device, lease, nil); v.outcome != osdChainRetainUnknown {
		t.Fatalf("no owner must retain: %+v", v)
	}
	busy := osdChainOwnerFunc(func(ctx context.Context, l string) (osdChainEvidence, error) {
		ev, err := stopped(ctx, l)
		ev.openers = 1
		return ev, err
	})
	if v := c.judgeDeviceChain(t.Context(), device, lease, busy); v.outcome != osdChainRetainBusy {
		t.Fatalf("an opener must retain: %+v", v)
	}
	c.mu.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	v := c.judgeDeviceChain(ctx, device, lease, stopped)
	c.mu.Unlock()
	if v.outcome != osdChainRetainUnknown {
		t.Fatalf("an unread claim table must retain: %+v", v)
	}
}

func TestAddBlockDeviceOSDRefusesAClosedClusterBeforeRegistering(t *testing.T) {
	device := osdBlockDevice{hostPath: "/dev/mapper/tc-closed", major: 253, minor: 8, sizeBytes: 128 << 20}
	c := &Container{closed: true}
	if osd, err := c.addBlockDeviceOSD(t.Context(), device, "tc-closed"); !errors.Is(err, errClusterTerminated) || osd != nil {
		t.Fatalf("closed cluster: %v %v", osd, err)
	}
	if c.fleet.deviceClaims.count() != 0 {
		t.Fatal("a refused launch published a claim")
	}
}

func TestOSDChainOutcomeNamesCoverEveryOutcome(t *testing.T) {
	for o := osdChainRetainUnknown; o <= osdChainClear; o++ {
		if osdChainOutcomeNames[o] == "" {
			t.Errorf("outcome %d has no name", o)
		}
	}
}
