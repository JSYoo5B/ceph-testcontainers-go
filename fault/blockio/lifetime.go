package blockio

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// Terminate restores the owned table, excludes cluster claims and all device
// openers/I/O, then releases mapping, loop, file and keeper in that order.
// Terminate the attached cluster first. A failure retains remaining resources
// and the same handle for retry; it never forces a busy or foreign mapping.
//
//purpose:condition,lifetime
func (b *Container) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	opts = append([]testcontainers.TerminateOption{testcontainers.StopTimeout(time.Second)}, opts...)
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if err := clusterutil.LockMutex(ctx, &b.mu); err != nil {
		return err
	}
	defer b.mu.Unlock()
	if b.released {
		return nil
	}
	b.closing = true
	if !b.backingReleased {
		if err := b.withdrawUnstarted(ctx); err != nil {
			return err
		}
	}
	if b.creationSent && !b.backingReleased {
		state, err := b.waitOwner(ctx, "inspect")
		if err != nil {
			return err
		}
		if state.Done == nil || !*state.Done {
			return errPending
		}
		if !b.ready && !b.mappingRemoved {
			// A failed creation still owns every successfully created component.
			file, err := b.owner(ctx, "file")
			if err != nil {
				return err
			}
			if file.Loop != "" {
				if err := b.acceptOwner(file); err != nil {
					return err
				}
			}
			view, err := b.observe(ctx)
			if err != nil {
				return err
			}
			if view.Exists {
				if !b.adoptLinear(view) {
					return failure(ceph.ErrNativeFailure, "partial creation has an unrecognized mapping; keeper retained")
				}
			} else {
				b.mappingRemoved = true
			}
		}
		if b.removeSent && !b.mappingRemoved {
			if err := b.settle(ctx); err != nil {
				if b.record.Unsettled == "" {
					b.removeSent = false
				}
				return err
			}
		}
		// settle withdraws a removal whose dispatch provably never ran.
		if b.removeSent && !b.mappingRemoved {
			view, err := b.observe(ctx)
			if err != nil {
				return err
			}
			if view.Exists {
				return failure(ceph.ErrNativeFailure, "mapping removal did not leave an absent node; retained")
			}
			b.mappingRemoved = true
		}
		if !b.mappingRemoved {
			if err := b.restore(ctx); err != nil {
				return err
			}
			if !b.excluded && b.cluster != nil {
				outcome, detail := faulthook.JudgeDeviceChain(ctx, b.cluster, b.device, b.name, func(ctx context.Context) (faulthook.ChainEvidence, error) { return b.chainEvidence(ctx) })
				if outcome != faulthook.ChainClear {
					return failure(ceph.ErrNativeFailure, "device chain retained (%s): %s", outcome, detail)
				}
				b.excluded = true
			}
			view, err := b.observe(ctx)
			if err != nil {
				return err
			}
			if verdict, detail := control.JudgeRelease(b.record, view); verdict != control.Releasable {
				return failure(ceph.ErrNativeFailure, "mapping retained: %s", detail)
			}
			b.removeSent = true
			if err := b.step(ctx, "remove", ""); err != nil {
				if b.record.Unsettled == "" {
					b.removeSent = false
				}
				return err
			}
			view, err = b.observe(ctx)
			if err != nil {
				return err
			}
			if view.Exists {
				return failure(ceph.ErrNativeFailure, "mapping removal is not confirmed")
			}
			b.mappingRemoved = true
		}
		if b.loop.Loop != "" && !b.loopReleased {
			if !b.detachSent {
				loop, err := b.control(ctx, "loopid", b.loop.Loop)
				if err != nil {
					return err
				}
				if ok, why := control.JudgeLoopRelease(b.loop, loop, nil); !ok {
					return failure(ceph.ErrNativeFailure, "loop retained: %s", why)
				}
				b.detachID = randomID()
				b.detachSent = true
				if _, err := b.owner(ctx, "start-detach"); err != nil {
					b.detachStopped = stopped(err)
					return err
				}
			}
			detach, err := b.waitOwner(ctx, "detached")
			if err != nil {
				return err
			}
			if detach.Done == nil || !*detach.Done {
				return errPending
			}
			if detach.Error != "" {
				// Completion is confirmed for this attempt. A later retry must
				// recheck loop identity/holders before allocating a fresh ID;
				// pending or lazy attempts never reach this branch.
				b.detachSent = false
				return failure(ceph.ErrNativeFailure, "loop detach: %s", detach.Error)
			}
			if !detach.Detached {
				return failure(ceph.ErrNativeFailure, "loop detach is still lazy; backing and keeper retained")
			}
			b.loopReleased = true
		}
		b.unlinkSent = true
		removed, err := b.owner(ctx, "unlink")
		if err != nil {
			return err
		}
		if !removed.FileRemoved {
			return failure(ceph.ErrNativeFailure, "backing file removal was not confirmed")
		}
		b.backingReleased = true
	}
	if b.Container != nil {
		if err := b.Container.Terminate(ctx, opts...); err != nil && !faulthook.OnlyMissingHostResource(err) {
			return err
		}
	} else if b.docker != nil {
		// No creation command is sent before keeper startup is acknowledged.
		// Recover only a lost helper-create reply, by this invocation's label.
		list, err := b.docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", ownerLabel+"="+b.name)})
		if err != nil {
			return err
		}
		for _, item := range list.Items {
			if item.Labels[ownerLabel] != b.name {
				return failure(ceph.ErrNativeFailure, "helper ownership differs")
			}
			if _, err := b.docker.ContainerRemove(ctx, item.ID, mobycl.ContainerRemoveOptions{Force: true}); err != nil && !faulthook.OnlyMissingHostResource(err) {
				return err
			}
		}
	}
	if b.registered {
		if err := faulthook.WithTopology(ctx, b.cluster, func() error { faulthook.Release(b.cluster, b.token); return nil }); err != nil {
			return err
		}
		b.registered = false
	}
	b.released = true
	if b.docker != nil {
		return b.docker.Close()
	}
	return nil
}
