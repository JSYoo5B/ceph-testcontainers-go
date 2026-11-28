package dashboard

import (
	"context"
	"errors"
	"time"
)

// retry runs attempt until it returns a nil error or reports stop, waiting
// interval between attempts. It returns the error of the last attempt, joined
// with ctx's error when ctx ends while it waits. The first attempt runs even
// when ctx has already ended; attempt reads ctx itself.
func retry(ctx context.Context, interval time.Duration, attempt func() (stop bool, err error)) error {
	for {
		stop, err := attempt()
		if err == nil || stop {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-time.After(interval):
		}
	}
}
