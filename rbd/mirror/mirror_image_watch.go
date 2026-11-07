package mirror

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// imageWatchDefaultInterval is the default WatchConfig.Interval of
// WatchImage: each read checks the pool and namespace policies of both sites
// before and after reading, reads both images twice and the destination's
// replay status once, and attributes a replaying report to its receiver
// process, about a dozen container executions; rbd-mirror itself refreshes an
// image's replay status only every several seconds.
const imageWatchDefaultInterval = 5 * time.Second

const imageWatchOperation = "watch RBD mirror image"

// ImageWatchEvent is one event of an ImageWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state. Status.LastUpdate is the native
// text of that read's replay status, not a transition time.
//
// Status is the ImageStatus of that read. Every field except Description and
// LastUpdate is compared: the image identities and modes, primary ownership,
// both mirroring states, the native replay state, the reporting daemon and
// instance, and ReplayReady. Description and LastUpdate are kept as
// diagnostics of the read that produced the event; their changes alone produce
// no event. On a WatchGap Status is the partial report of the failed read,
// with ReplayReady false: evidence of what the read got before Err, not a
// successful observation. Two statuses separated by a gap, or simply by the
// polling interval, do not establish the transitions in between.
type ImageWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            ImageStatus
	Err               error
}

// ImageWatch is a running watch of one mirrored image pair. One goroutine
// reads the pair and sends events; Events closes after that goroutine has
// stopped using the fixture. Stop ends the watch and waits for it.
type ImageWatch struct {
	stream *watch.Stream[ImageWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchImage returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *ImageWatch) Events() <-chan ImageWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchImage
// context ended, a ceph.ErrWatchOverflow error, or the read error that ended
// it (including disabled source mirroring, a terminated mirror or cluster, or
// a replaced image or policy). Buffered
// events stay readable after Stop. Stop is safe to call more than once and
// from any goroutine; every call returns the same cause. Call it before the
// mirror's cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *ImageWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return rbdWatchNotStarted(imageWatchOperation)
	}
	return w.stream.Stop()
}

// WatchImage polls one image pair of this mirror's selected namespaces and
// reports changes of its replay state and receiver assignment, the same
// observation as ImageStatus. The default WatchConfig.Interval is 5 seconds.
//
// Before it returns, WatchImage makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing; a destination image
// that does not exist yet fails it too, because a failed query and an absent
// image look the same. That read is the baseline: the first event, already
// buffered, and the start of the identities the watch keeps, as
// WaitReplayReady does: the source image ID and global mirror ID, and the
// destination image ID from the first read that reports it. A destination
// that rbd-mirror created but has not enrolled yet is a successful read with
// DestinationMirrorState "disabled", not a gap. Receive the baseline before
// triggering the change to observe.
//
// Each later read takes the mirror's gate for one bounded observation, as
// ImageStatus does, and the clusters' topology locks only to check that they
// are not terminated; it holds none of them when the event is sent or during
// the wait for the next read, so the consumer may call any fixture operation
// while the watch runs. Enrollment, a changed native replay state, another
// owned daemon or native instance reporting the replay, and the loss of the
// reporting process are changes of Status, not errors. A read whose compared
// Status equals the previous successful read produces no event. A failed read
// produces a WatchGap and the watch continues; the next successful read is
// WatchResumed. These end the watch instead: ctx ending, Stop, a terminated
// mirror or cluster (ceph.ErrInvalidRequest), a full buffer
// (ceph.ErrWatchOverflow), a source or destination image replaced under the
// same name, disabled source mirroring, destination mirroring disabled after
// enrollment, a
// changed pool or policy identity, and a malformed native report. Those
// errors carry no native output.
//
// ReplayReady means that an owned receiving process replays the pair at that
// read. No event attests that a particular write or mirror snapshot arrived:
// verify the destination data or the application's checkpoint separately.
//
//purpose:observation
func (m *Container) WatchImage(ctx context.Context, cfg ceph.WatchConfig, imageName string) (*ImageWatch, error) {
	if err := validateRBDMirrorObservedImage(imageName); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, rbdImageGuardError("RBD mirror fixture is unavailable")
	}
	settings, err := rbdWatchSettings(imageWatchOperation, cfg, imageWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	return startImageWatch(ctx, settings, m.checkWatchOpen, func(ctx context.Context) (ImageStatus, error) {
		return m.ImageStatus(ctx, imageName)
	})
}

// WatchImage polls one image pair of this bound namespace pair, the same
// observation as ImageStatus. It behaves as Container.WatchImage in every other
// respect, and pins as Namespace.WaitReplayReady does the original owner's
// cohort at the baseline and the source image's mode when first observed: an
// added, removed or replaced owned daemon, a changed mode, a changed selected
// policy, and a Rebootstrap attempt or Terminate of the owner end it. A
// same-container restart of an original daemon is an event. The default
// WatchConfig.Interval is 5 seconds.
//
//purpose:observation
func (v *Namespace) WatchImage(ctx context.Context, cfg ceph.WatchConfig, imageName string) (*ImageWatch, error) {
	if err := validateRBDMirrorObservedImage(imageName); err != nil {
		return nil, scopedRBDImageError(ctx, err)
	}
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return nil, rbdImageGuardError("RBD namespace binding is unavailable")
	}
	settings, err := rbdWatchSettings(imageWatchOperation, cfg, imageWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	return startImageWatch(ctx, settings, v.owner.checkWatchOpen, v.imageReader(imageName, "while watching the image"))
}

// startImageWatch runs the one image watch lifecycle of both scopes. open is
// Container.checkWatchOpen of the owner in production.
func startImageWatch(ctx context.Context, settings watch.Settings, open func(context.Context, string) error, read func(context.Context) (ImageStatus, error)) (*ImageWatch, error) {
	var pins rbdImageContinuity
	stream, err := watch.Start(ctx, watch.Spec[ImageStatus, ImageWatchEvent]{
		Name:     imageWatchOperation,
		Settings: settings,
		Read: func(ctx context.Context) (ImageStatus, error) {
			if err := open(ctx, imageWatchOperation); err != nil {
				if errors.Is(err, errRBDWatchTerminated) {
					return ImageStatus{}, watch.Fatal(err)
				}
				return ImageStatus{}, err
			}
			status, err := read(ctx)
			if rbdImagePending(err) {
				err = nil // a created destination not enrolled yet is a state
			}
			if err != nil {
				status.ReplayReady = false
			}
			if refusal := pins.admit(status, "while watching the image"); refusal != nil {
				return status, watch.Fatal(fmt.Errorf("%s: %w", imageWatchOperation, refusal))
			}
			if err != nil && scopedRBDImagePermanent(err) {
				return status, watch.Fatal(fmt.Errorf("%s: %w", imageWatchOperation, err))
			}
			return status, err
		},
		Equal: func(a, b ImageStatus) bool {
			a.Description, a.LastUpdate, b.Description, b.LastUpdate = "", "", "", ""
			return a == b
		},
		Event: func(m watch.Meta, s ImageStatus, err error) ImageWatchEvent {
			e := ImageWatchEvent{Status: s, Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = rbdWatchMeta(m)
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &ImageWatch{stream: stream}, nil
}
