package watch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// script is a scripted Read: each call returns the next step, and the last
// step repeats.
type script struct {
	mu    sync.Mutex
	steps []step
	reads int
}

type step struct {
	value int
	err   error
}

func (s *script) read(context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := min(s.reads, len(s.steps)-1)
	s.reads++
	return s.steps[i].value, s.steps[i].err
}

type event struct {
	meta  Meta
	value int
	err   error
}

func spec(s *script, final func(int) bool) Spec[int, event] {
	return Spec[int, event]{
		Name:     "watch test",
		Settings: Settings{Interval: 2 * time.Millisecond, Timeout: time.Second, Buffer: 16},
		Read:     s.read,
		Equal:    func(a, b int) bool { return a == b },
		Final:    final,
		Event:    func(m Meta, v int, err error) event { return event{m, v, err} },
	}
}

func drain(t *testing.T, s *Stream[event]) []event {
	t.Helper()
	var got []event
	timeout := time.After(5 * time.Second)
	for {
		select {
		case e, ok := <-s.Events():
			if !ok {
				return got
			}
			got = append(got, e)
		case <-timeout:
			t.Fatal("the watch did not end")
		}
	}
}

func TestFinalSampleIsDeliveredAndEndsTheWatch(t *testing.T) {
	s := &script{steps: []step{{value: 1}, {value: 1}, {value: 2}, {value: 3}}}
	stream, err := Start(t.Context(), spec(s, func(v int) bool { return v == 3 }))
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, stream)
	if err := stream.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	want := []Kind{Baseline, Changed, Changed}
	if len(got) != len(want) {
		t.Fatalf("events = %+v", got)
	}
	for i, e := range got {
		if e.meta.Kind != want[i] || e.meta.Sequence != uint64(i+1) {
			t.Fatalf("event %d = %+v", i, e)
		}
	}
	if got[2].value != 3 {
		t.Fatalf("final event = %+v", got[2])
	}
}

func TestFinalBaselineClosesImmediately(t *testing.T) {
	s := &script{steps: []step{{value: 3}}}
	stream, err := Start(t.Context(), spec(s, func(v int) bool { return v == 3 }))
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, stream)
	if len(got) != 1 || got[0].meta.Kind != Baseline || stream.Stop() != nil || s.reads != 1 {
		t.Fatalf("events = %+v, reads = %d", got, s.reads)
	}
}

func TestGapCarriesPartialEvidenceAndFatalEndsTheWatch(t *testing.T) {
	boom, gone := errors.New("boom"), errors.New("identity changed")
	s := &script{steps: []step{{value: 1}, {value: 7, err: boom}, {value: 1}, {err: Fatal(gone)}}}
	stream, err := Start(t.Context(), spec(s, nil))
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, stream)
	if err := stream.Stop(); !errors.Is(err, gone) || errors.As(err, new(fatalError)) {
		t.Fatalf("Stop = %v", err)
	}
	if len(got) != 3 || got[1].meta.Kind != Gap || got[1].value != 7 || !errors.Is(got[1].err, boom) || got[2].meta.Kind != Resumed {
		t.Fatalf("events = %+v", got)
	}
}

func TestFatalBaselineReturnsTheCause(t *testing.T) {
	gone := errors.New("gone")
	s := &script{steps: []step{{err: Fatal(gone)}}}
	if stream, err := Start(t.Context(), spec(s, nil)); stream != nil || err != gone {
		t.Fatalf("Start = %v, %v", stream, err)
	}
}

func TestValidate(t *testing.T) {
	got, err := Validate(0, 0, 0, 5*time.Second)
	if err != nil || got != (Settings{Interval: 5 * time.Second, Timeout: DefaultTimeout, Buffer: DefaultBuffer}) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	for _, bad := range [][3]int64{{-1, 0, 0}, {0, -1, 0}, {0, 0, -1}, {int64(99 * time.Millisecond), 0, 0}, {0, 0, MaxBuffer + 1}} {
		if _, err := Validate(time.Duration(bad[0]), time.Duration(bad[1]), int(bad[2]), time.Second); err == nil {
			t.Errorf("Validate(%v) accepted", bad)
		}
	}
}

func TestNextReadRestsAtLeastAsLongAsTheReadTook(t *testing.T) {
	r := &run[int, event]{spec: Spec[int, event]{Settings: Settings{Interval: 5 * time.Second}}}
	start := time.Unix(1000, 0)
	for _, c := range []struct{ took, want time.Duration }{
		{time.Second, 5 * time.Second},             // short read: one interval
		{2500 * time.Millisecond, 5 * time.Second}, // half the interval: still one interval
		{4 * time.Second, 8 * time.Second},         // long read: rest 4s after it
		{7 * time.Second, 14 * time.Second},        // longer than the interval
	} {
		if got := r.next(start, start.Add(c.took)).Sub(start); got != c.want {
			t.Errorf("read of %v: next read %v after the start, want %v", c.took, got, c.want)
		}
	}
}
