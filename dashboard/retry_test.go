package dashboard

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryEndsOnSuccessStopOrContext(t *testing.T) {
	failed := errors.New("not yet")
	calls := 0
	if err := retry(t.Context(), time.Millisecond, func() (bool, error) {
		calls++
		if calls < 3 {
			return false, failed
		}
		return false, nil
	}); err != nil || calls != 3 {
		t.Fatalf("success after retries: calls=%d err=%v", calls, err)
	}

	terminal := errors.New("terminal")
	calls = 0
	if err := retry(t.Context(), time.Millisecond, func() (bool, error) {
		calls++
		return true, terminal
	}); err != terminal || calls != 1 {
		t.Fatalf("stop: calls=%d err=%v", calls, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := retry(ctx, time.Millisecond, func() (bool, error) { return false, failed }); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, failed) {
		t.Fatalf("context end: %v", err)
	}
}
