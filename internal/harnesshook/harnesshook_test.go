package harnesshook

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// reference is the construction that the rbd, cephfs, rgw and mirror packages
// each kept as their own invalid function before the shared contract.
func reference(err error) error {
	return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: err}
}

func TestInvalidIsTheFormerPerPackageConstruction(t *testing.T) {
	cause := errors.New("tenant is released")
	for name, err := range map[string]error{
		"plain":    cause,
		"wrapped":  errors.Join(cause, context.Canceled),
		"deadline": context.DeadlineExceeded,
		"nil":      nil,
	} {
		t.Run(name, func(t *testing.T) {
			got, want := Invalid(err), reference(err)
			if got == nil {
				t.Fatal("Invalid returned nil; the former construction never did, also for a nil cause")
			}
			if !reflect.DeepEqual(got, want) || got.Error() != want.Error() {
				t.Fatalf("Invalid = %#v (%q), want %#v (%q)", got, got.Error(), want, want.Error())
			}
			var op *ceph.OperationError
			if !errors.As(got, &op) || op.Kind != ceph.ErrInvalidRequest || op.Err != err {
				t.Fatalf("errors.As = %#v, want kind ErrInvalidRequest with the cause kept as it is", op)
			}
			if !errors.Is(got, ceph.ErrInvalidRequest) {
				t.Fatal("errors.Is(ErrInvalidRequest) is false")
			}
			if err != nil && !errors.Is(got, err) {
				t.Fatal("the cause is not reachable through errors.Is")
			}
		})
	}
	// The kind is the only category: other kinds are not matched.
	if errors.Is(Invalid(cause), ceph.ErrSharedCluster) {
		t.Fatal("an invalid request matched ErrSharedCluster")
	}
}

func TestInvalidPanicsNamingTheMissingRegistrant(t *testing.T) {
	saved := hookreg.Harness
	defer func() { hookreg.Harness = saved }()
	for name, value := range map[string]any{"unset": nil, "foreign": struct{}{}} {
		hookreg.Harness = value
		func() {
			defer func() {
				msg, _ := recover().(string)
				if msg == "" || !strings.Contains(msg, "package ceph did not register") {
					t.Errorf("%s slot: recovered %q, want a panic naming package ceph", name, msg)
				}
			}()
			_ = Invalid(errors.New("x"))
		}()
	}
}
