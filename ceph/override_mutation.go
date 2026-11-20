package ceph

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/mutation"
	"github.com/testcontainers/testcontainers-go"
)

// overrideMutation retains a dispatched CLI until its reply, independently of
// caller cancellation. A transport failure keeps restoration fenced because
// neither a stopped local client nor a prior-value read settles a MON write.
func overrideMutation(ctx context.Context, ctr testcontainers.Container, bound time.Duration, args ...string) error {
	return mutation.Run(ctx, bound, func(runCtx context.Context) (bool, error) {
		_, err := command(runCtx, ctr, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
		var failure *NativeFailure
		complete := err == nil || (errors.As(err, &failure) && slices.Contains([]string{"EINVAL", "EPERM", "EACCES", "ENOENT"}, failure.Errno))
		if err != nil && len(args) > 0 && args[0] == "config" {
			err = quiet(errors.New("Ceph configuration command failed; native output is redacted"), err)
		}
		return complete, err
	})
}

func (c *Container) overrideCommand(ctx context.Context, args ...string) error {
	ctr, err := c.conn().controlContainer(ctx)
	if err != nil {
		return err
	}
	return overrideMutation(ctx, ctr, c.settings.startupTimeout, args...)
}
