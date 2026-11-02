package blockio

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

var errPending = failure(ceph.ErrNativeFailure, "owned kernel operation has not settled; backend retained")

func (b *Container) command(ctx context.Context, args ...string) ([]byte, error) {
	if b.nativeCommand != nil {
		raw, err := b.nativeCommand(ctx, args...)
		return raw, commandError(err)
	}
	result, err := supervised.Command(ctx, b.Container, "block I/O keeper", 5*time.Second, 64<<10, 8<<10, args...)
	if err != nil {
		return nil, commandError(err)
	}
	if result.ExitCode != 0 {
		return nil, failure(ceph.ErrNativeFailure, "keeper exited %d: %s", result.ExitCode, result.Stderr)
	}
	return result.Stdout, nil
}

func commandError(err error) error {
	var exit *supervised.ExitError
	if errors.As(err, &exit) && (exit.Code == 126 || exit.Code == 127) {
		return failure(ceph.ErrImageCapability, "keeper cannot execute the Python supervisor: %v", err)
	}
	return err
}

func (b *Container) control(ctx context.Context, action string, args ...string) (control.View, error) {
	// The instance check and UUID selector run in the same invocation as the
	// control, so a restarted keeper or reused mapper name is not adopted.
	argv := []string{"python3", "-I", "-c", control.Script, action, b.name}
	argv = append(argv, args...)
	wrapped := []string{"python3", "-I", ownerProgram, "exec", b.name, strconv.FormatInt(b.size, 10), b.instance}
	wrapped = append(wrapped, argv...)
	raw, err := b.command(ctx, wrapped...)
	if err != nil {
		return control.View{}, err
	}
	view, err := control.Parse(raw, action)
	if err == nil && view.Name != b.name {
		err = failure(ceph.ErrNativeFailure, "keeper observation belongs to another mapping")
	}
	return view, err
}

func (b *Container) observe(ctx context.Context) (control.View, error) {
	return b.control(ctx, "observe")
}

func (b *Container) settle(ctx context.Context) error {
	if b.record.Unsettled == "" {
		return nil
	}
	id, op := b.record.UnsettledID, b.record.Unsettled
	view, err := b.control(ctx, "settled", id)
	if err != nil {
		return err
	}
	if view.ID != id {
		return failure(ceph.ErrNativeFailure, "step completion belongs to another invocation")
	}
	b.record.Settle(view)
	if !view.Done {
		return errPending
	}
	if view.RC != 0 {
		return failure(ceph.ErrNativeFailure, "%s was refused: %s", op, view.Error)
	}
	return nil
}

func (b *Container) step(ctx context.Context, op, table string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.record.Unsettled != "" {
		return errPending
	}
	id := randomID()
	b.record.Begin(op, table, id)
	args := []string{op, "0.1", id}
	if op == "load" {
		args = append(args, table)
	}
	view, err := b.control(ctx, "step", args...)
	if err != nil {
		return err
	}
	if view.ID != id || view.Op != op {
		return failure(ceph.ErrNativeFailure, "step reply does not identify the dispatched operation")
	}
	b.record.Settle(view)
	if view.Done {
		if view.RC != 0 {
			return failure(ceph.ErrNativeFailure, "%s was refused: %s", op, view.Error)
		}
		return nil
	}
	// Poll only the recorded operation. Never resend an unconfirmed mutation.
	for {
		if err := b.settle(ctx); !errors.Is(err, errPending) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (b *Container) transition(ctx context.Context, want string) error {
	if err := b.settle(ctx); err != nil {
		return err
	}
	for range 4 {
		view, err := b.observe(ctx)
		if err != nil {
			return err
		}
		state := control.Classify(b.record, want, view)
		if state == control.Applied {
			b.record.Live = want
			return nil
		}
		op := control.Forward(state)
		if op == "" {
			return failure(ceph.ErrNativeFailure, "mapping is %s; no mutation dispatched", state)
		}
		if op == "load" {
			probe, err := b.owner(ctx, "probe")
			if err != nil {
				return err
			}
			if !contains(probe.Targets, strings.Fields(want)[2]) {
				return failure(ceph.ErrEnvironmentUnavailable, "requested device-mapper target is no longer registered")
			}
		}
		if err := b.step(ctx, op, want); err != nil {
			return err
		}
	}
	return failure(ceph.ErrNativeFailure, "mapping did not reach the requested active table")
}

func (b *Container) prepare(ctx context.Context) (bool, error) {
	if b.ready {
		return true, nil
	}
	state, err := b.owner(ctx, "inspect")
	if err != nil {
		return false, err
	}
	if state.Done == nil || !*state.Done {
		return false, nil
	}
	if state.Error != "" {
		return false, failure(ceph.ErrNativeFailure, "backing preparation: %s", state.Error)
	}
	if err := b.acceptOwner(state); err != nil {
		return false, err
	}
	view, err := b.observe(ctx)
	if err != nil {
		return false, err
	}
	want := fmt.Sprintf("0 %d linear %s 0", b.size/512, b.loop.Dev)
	if !view.Exists || view.UUID != "TC-BLK-"+b.name || view.Live == nil || control.Normalize(*view.Live) != want || view.Inactive != nil || view.State != "ACTIVE" {
		return false, failure(ceph.ErrNativeFailure, "created mapping identity or linear table was not confirmed")
	}
	b.record = control.Record{Name: b.name, UUID: view.UUID, Major: view.Major, Minor: view.Minor, Live: want}
	b.device.HostPath = "/dev/mapper/" + b.name
	b.device.Major, b.device.Minor, b.device.SizeBytes = view.Major, view.Minor, b.size
	b.linear, b.ready = want, true
	return true, nil
}
