package dashboard

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// replacedValue is one stored value that the fixture replaced: whether it
// existed and its value before, the value the fixture installed, and the
// write that could still change it. Values compare without surrounding
// whitespace, whichever store holds them, so a value that only gained or lost
// a trailing newline is still the same value.
type replacedValue struct {
	name     string
	existed  bool
	previous []byte
	// installed is the value the fixture wrote, nil before it tried.
	installed []byte
	write     pendingMutation
}

// restore puts the previous value back with rewrite, or removes a value that
// did not exist before with remove, when the stored value is still the
// installed one. read returns the stored value and whether it exists. A value
// that is already back as it was needs nothing; a value that was removed or
// changed outside the fixture is refused with ceph.ErrInvalidRequest and left
// as it is. An install that never applied leaves nothing to restore.
func (v *replacedValue) restore(ctx context.Context, read func(context.Context) ([]byte, bool, error), remove func(context.Context) error, rewrite func(context.Context, []byte) error) error {
	if v.installed == nil {
		return nil
	}
	if err := v.write.settle(ctx); err != nil {
		return err
	}
	current, found, err := read(ctx)
	if err != nil {
		return err
	}
	switch {
	case !found && !v.existed:
		return nil
	case !found:
		return v.refuse("removed")
	case v.existed && sameValue(current, v.previous):
		return nil
	case !sameValue(current, v.installed):
		return v.refuse("changed")
	}
	if v.existed {
		err = v.write.track(rewrite(ctx, v.previous))
	} else {
		err = remove(ctx)
	}
	if err != nil {
		return fmt.Errorf("restore %s: %w", v.name, err)
	}
	return nil
}

func (v *replacedValue) refuse(how string) error {
	return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: "restore " + v.name, Err: errors.New(how + " outside the dashboard fixture; refusing restoration")}
}

func sameValue(a, b []byte) bool { return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b)) }
