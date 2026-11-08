//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
)

// watchWorkload plays the SUT: it writes and reads back objects through
// the rados CLI of the client container until stopped, independently of the
// watch, and records when each completed operation started and ended.
type watchWorkload struct {
	mu     sync.Mutex
	starts []time.Time
	ends   []time.Time
	failed []string
	done   chan struct{}
}

func startWatchWorkload(ctx context.Context, client testcontainers.Container, pool string) *watchWorkload {
	w := &watchWorkload{done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for n := 0; ctx.Err() == nil; n++ {
			started := time.Now()
			name := "watch-" + strconv.Itoa(n)
			err := watchObjectRoundTrip(ctx, client, pool, name)
			w.mu.Lock()
			if err == nil {
				w.starts, w.ends = append(w.starts, started), append(w.ends, time.Now())
			} else if ctx.Err() == nil {
				w.failed = append(w.failed, fmt.Sprintf("%s: %v", name, err))
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func watchObjectRoundTrip(ctx context.Context, client testcontainers.Container, pool, name string) error {
	const script = `printf '%s' "$2" > /tmp/watch-input && rados -p "$1" put "$2" /tmp/watch-input && rados -p "$1" get "$2" -`
	code, r, err := client.Exec(ctx, []string{"timeout", "120", "sh", "-c", script, "sh", pool, name})
	if err != nil {
		return err
	}
	return checkWatchObjectReadback(name, code, r)
}

func checkWatchObjectReadback(name string, code int, r io.Reader) error {
	var out, diagnostics bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &diagnostics, r); err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("rados round trip exited %d: %s", code, diagnostics.Bytes())
	}
	if out.String() != name {
		return fmt.Errorf("rados returned %q, want %q", out.Bytes(), name)
	}
	return nil
}

// completedWithin counts the operations that started at or after from and
// finished before to.
func (w *watchWorkload) completedWithin(from, to time.Time) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for i, s := range w.starts {
		if !s.Before(from) && w.ends[i].Before(to) {
			n++
		}
	}
	return n
}

func (w *watchWorkload) failures() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.failed)
}
