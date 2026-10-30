package override

import (
	"context"
	"errors"
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
)

// MessengerPolicyHandle manages the configuration restoration handles of one
// SetMessengerPolicy call, including partial or uncertain application. Retain
// the handle returned with an error for explicit Restore; it does not promise
// successful application or automatic cleanup.
//
// SetMessengerPolicy returns a non-nil handle for every outcome. A handle of a
// call that was refused or never applied holds no option: its Restore returns
// nil. The zero value is unavailable, and its Restore returns an error.
type MessengerPolicyHandle struct {
	state *messengerPolicyState
}

type messengerPolicyState struct {
	// children are the options that were made, in order. A refused option is
	// not among them.
	children []*ConfigHandle
	// refused is why the call was not applied. A handle with it holds no
	// option.
	refused error
}

// notAppliedMessengerPolicy returns the handle of a call that changed nothing.
func notAppliedMessengerPolicy(cause error) (*MessengerPolicyHandle, error) {
	return &MessengerPolicyHandle{state: &messengerPolicyState{refused: cause}}, cause
}

// SetMessengerPolicy changes the connection modes that the running daemons
// accept from clients. Each non-nil field becomes a SetConfig override of the
// policy's option; nil fields stay as they are. The daemons apply the change
// to new connections without a restart, and established connections keep
// their mode. A handle returned with an error holds the overrides that were
// made and must be restored. A policy that selects no valid mode, and any
// policy with ceph.MessengerV2Secure, is refused with ceph.ErrInvalidRequest
// before any change.
//
// The handle is non-nil for every outcome. A refusal returns a not-applied
// handle whose Restore is a no-op. A nil cluster is refused with
// ceph.ErrInvalidRequest and Op "override.SetMessengerPolicy". When a
// later option is refused, the handle holds the options made before it.
//
// To change a single daemon, such as only osd.0, use SetConfig with the
// policy's option and the daemon name as Section; it takes precedence over the
// daemon type's entry.
func SetMessengerPolicy(ctx context.Context, c *ceph.Container, policy ceph.MessengerPolicy) (*MessengerPolicyHandle, error) {
	const op = "override.SetMessengerPolicy"
	if c == nil {
		return notAppliedMessengerPolicy(&ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the cluster is nil")})
	}

	settings, err := overridehook.MessengerPolicySettings(c, policy)
	if err != nil {
		return notAppliedMessengerPolicy(err)
	}
	result := &MessengerPolicyHandle{state: &messengerPolicyState{}}
	for _, setting := range settings {
		child, err := setConfig(ctx, c, setting)
		if child.state.refused == nil {
			result.state.children = append(result.state.children, child)
		}
		if err != nil {
			if len(result.state.children) == 0 {
				return notAppliedMessengerPolicy(err)
			}
			return result, err
		}
	}
	return result, nil
}

// Restore returns every option of the policy to its previous value, in
// reverse order. It is idempotent and can be retried after a failure. A
// handle of a call that was not applied returns nil.
func (policy *MessengerPolicyHandle) Restore(ctx context.Context) error {
	if policy == nil || policy.state == nil {
		return errors.New("messenger policy override is unavailable")
	}
	var errs []error
	for _, change := range slices.Backward(policy.state.children) {
		errs = append(errs, change.Restore(ctx))
	}
	return errors.Join(errs...)
}
