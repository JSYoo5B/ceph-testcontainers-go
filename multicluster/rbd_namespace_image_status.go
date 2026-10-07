package multicluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// ImageStatus observes an already mirrored image in this view's immutable
// namespace pair. Mode is derived from the source image and must match the
// destination; pool-scoped policies require journal mode. ReplayReady attributes
// an up+replaying report to one original owned live receiver, without requiring
// the whole cohort's election readiness or proving write/checkpoint delivery.
// It creates no resources or checkpoints. Admission and native/process reads
// take at most 30 seconds, bounded by ctx; a partial report can accompany an error.
func (v *RBDMirrorNamespace) ImageStatus(ctx context.Context, imageName string) (RBDMirrorImageStatus, error) {
	status, _, err := v.imageStatus(ctx, imageName, nil)
	return status, err
}

// WaitReplayReady waits up to four minutes, bounded by ctx, for this image's
// owned live receiver. It pins the original cohort at first admission, including
// zero daemons or an absent destination, and pins mode/local/global image IDs
// when first observed. Add/Remove or replacement requires a new wait; an original
// same-container restart may change its native instance. Polls release the owner
// and member gates. It does not wait for bytes or snapshot completion, rebind a
// stale bootstrap generation, or expose native private data through error causes.
func (v *RBDMirrorNamespace) WaitReplayReady(ctx context.Context, imageName string) (RBDMirrorImageStatus, error) {
	if err := validateRBDMirrorObservedImage(imageName); err != nil {
		return RBDMirrorImageStatus{}, scopedRBDImageError(ctx, err)
	}
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return RBDMirrorImageStatus{}, rbdImageGuardError("RBD namespace binding is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	return v.waitReplayReady(ctx, time.Second, imageName)
}

func (v *RBDMirrorNamespace) waitReplayReady(ctx context.Context, interval time.Duration, imageName string) (RBDMirrorImageStatus, error) {
	var witness *rbdReceiverWitness
	var mode RBDMirrorMode
	status, err := waitRBDMirrorReplay(ctx, interval, func(attempt context.Context) (RBDMirrorImageStatus, error) {
		current, captured, err := v.imageStatus(attempt, imageName, witness)
		if witness == nil && captured != nil {
			witness = captured
		}
		if current.SourceImageID != "" {
			if mode != "" && mode != current.Mode {
				current.ReplayReady = false
				return current, rbdImageGuardError("RBD source image mode changed while waiting for replay")
			}
			mode = current.Mode
		}
		return current, err
	})
	if err != nil {
		status.ReplayReady = false
		return status, scopedRBDImageError(ctx, err)
	}
	return status, nil
}

func (v *RBDMirrorNamespace) imageStatus(ctx context.Context, imageName string, previous *rbdReceiverWitness) (RBDMirrorImageStatus, *rbdReceiverWitness, error) {
	var result RBDMirrorImageStatus
	if err := validateRBDMirrorObservedImage(imageName); err != nil {
		return result, previous, scopedRBDImageError(ctx, err)
	}
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return result, previous, rbdImageGuardError("RBD namespace binding is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	m := v.owner
	if err := lockRGWSyncObservation(ctx, &m.mu); err != nil {
		return result, previous, scopedRBDImageError(ctx, err)
	}
	defer m.mu.Unlock()
	witness, err := m.receiverWitness(&v.scope, nil, previous)
	if err != nil {
		return result, previous, scopedRBDImageError(ctx, err)
	}
	result, err = m.observeRBDMirrorImageLocked(ctx, imageName, &v.scope, witness)
	if err != nil {
		result.ReplayReady = false
		return result, witness, scopedRBDImageError(ctx, err)
	}
	return result, witness, nil
}

// New scoped observations never retain arbitrary native stderr or a transport
// error in their cause chain. Preserve only canonical context causes while
// translating either observer family's permanent guards for the legacy loop.
func scopedRBDImageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	safe := rbdReceiverQuery(ctx, "observe RBD namespace image", err).(*rbdReceiverObservationError)
	permanent := scopedRBDImagePermanent(err)
	message := "observe RBD namespace image failed"
	if permanent {
		message = "RBD namespace image original authority or native identity is invalid"
	}
	return &rbdImageObservationError{message: message, cause: safe.cause, permanent: permanent}
}

func scopedRBDImagePermanent(err error) bool {
	if rbdReceiverPermanent(err) {
		return true
	}
	if classified, ok := err.(*rbdImageObservationError); ok && classified.permanent {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if scopedRBDImagePermanent(child) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return scopedRBDImagePermanent(wrapped.Unwrap())
	}
	return false
}

func readRBDMirrorImageInfoForScope(ctx context.Context, client testcontainers.Container, spec, name string, mode RBDMirrorMode, scoped bool) (rbdObservedInfo, error) {
	if !scoped {
		return readRBDMirrorObservedInfo(ctx, client, spec, name, mode)
	}
	data, err := rbdReceiverExec(ctx, client, "read RBD namespace image info", "rbd", "info", spec, "--format", "json")
	if err != nil {
		return rbdObservedInfo{}, err
	}
	if err := rbdReceiverJSON(data); err != nil {
		return rbdObservedInfo{}, err
	}
	return decodeRBDMirrorObservedInfo(data, name, mode)
}

func readRBDMirrorImageStatusForScope(ctx context.Context, client testcontainers.Container, spec, name, globalID string, scoped bool) (rbdObservedStatus, error) {
	if !scoped {
		return readRBDMirrorObservedStatus(ctx, client, spec, name, globalID)
	}
	data, err := rbdReceiverExec(ctx, client, "read RBD namespace image status", "rbd", "mirror", "image", "status", spec, "--format", "json")
	if err != nil {
		return rbdObservedStatus{}, err
	}
	if err := rbdReceiverJSON(data); err != nil {
		return rbdObservedStatus{}, err
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return rbdObservedStatus{}, rbdImageGuardError("decode RBD namespace image status")
	}
	if service, exists := fields["daemon_service"]; exists && string(service) == "null" {
		return rbdObservedStatus{}, rbdImageGuardError("decode RBD namespace image status: null daemon service")
	}
	return decodeRBDMirrorObservedStatus(data, name, globalID)
}
