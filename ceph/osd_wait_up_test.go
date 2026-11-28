package ceph

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWaitUpEndsOnTheFirstMalformedDump(t *testing.T) {
	for _, tc := range []struct {
		name, dump string
	}{
		{"missing up", `{"epoch":3,"flags":"","osds":[{"osd":0,"uuid":"u","in":1,"weight":1}]}`},
		{"missing flags", `{"epoch":3,"osds":[{"osd":0,"uuid":"u","up":1,"in":1,"weight":1}]}`},
		{"duplicate ID", `{"epoch":3,"flags":"","osds":[{"osd":0,"uuid":"u","up":0,"in":1,"weight":1},{"osd":0,"uuid":"u","up":0,"in":1,"weight":1}]}`},
		{"up out of range", `{"epoch":3,"flags":"","osds":[{"osd":0,"uuid":"u","up":2,"in":1,"weight":1}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := &poolFixtureContainer{Output: map[string]string{"osd dump --format json": tc.dump}}
			c := &Container{Container: control, settings: options{startupTimeout: time.Second}}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := (osdMap{c.conn()}).waitUp(ctx, []int{0})
			if err == nil || errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "osd.0") {
				t.Fatalf("err = %v, want an immediate decode refusal naming osd.0", err)
			}
			if len(control.Calls) != 1 {
				t.Fatalf("osd dump ran %d times, want 1", len(control.Calls))
			}
		})
	}
}

func TestWaitUpAcceptsUpAndInOSDs(t *testing.T) {
	control := &poolFixtureContainer{Output: map[string]string{"osd dump --format json": `{"epoch":3,"flags":"sortbitwise","osds":[{"osd":0,"uuid":"u","up":1,"in":1,"weight":1},{"osd":1,"uuid":"v","up":1,"in":1,"weight":1}]}`}}
	c := &Container{Container: control, settings: options{startupTimeout: time.Second}}
	if err := (osdMap{c.conn()}).waitUp(t.Context(), []int{0, 1}); err != nil {
		t.Fatal(err)
	}
}

func TestWaitUpNamesTheOSDsStillDown(t *testing.T) {
	control := &poolFixtureContainer{Output: map[string]string{"osd dump --format json": `{"epoch":3,"flags":"","osds":[{"osd":0,"uuid":"u","up":1,"in":1,"weight":1},{"osd":1,"uuid":"v","up":0,"in":1,"weight":1}]}`}}
	c := &Container{Container: control, settings: options{startupTimeout: time.Second}}
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	err := (osdMap{c.conn()}).waitUp(ctx, []int{0, 1, 2})
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for osd.1, osd.2 up/in") {
		t.Fatalf("err = %v", err)
	}
}
