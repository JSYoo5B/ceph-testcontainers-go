//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"errors"
	"fmt"
	"testing"
)

// gateCase stands for one suite case's *testing.T.
type gateCase struct {
	name          string
	failed, fatal bool
	messages      []string
}

func (c *gateCase) Helper()      {}
func (c *gateCase) Name() string { return c.name }
func (c *gateCase) Failed() bool { return c.failed }
func (c *gateCase) Errorf(format string, args ...any) {
	c.failed = true
	c.messages = append(c.messages, fmt.Sprintf(format, args...))
}
func (c *gateCase) Fatalf(format string, args ...any) {
	c.failed, c.fatal = true, true
	c.messages = append(c.messages, fmt.Sprintf(format, args...))
}

func TestSharedClusterGateFailsEveryCaseAfterTheClusterBreaks(t *testing.T) {
	var checks int
	healthy := true
	gate := sharedClusterGate{check: func() error {
		checks++
		if healthy {
			return nil
		}
		return errors.New("PGs not clean")
	}}

	// A passing case is checked before and not after.
	first := &gateCase{name: "Suite/first"}
	gate.before(first)
	gate.after(first)
	if first.failed || checks != 1 || gate.broken != "" {
		t.Fatalf("passing case: %+v checks %d broken %q", first, checks, gate.broken)
	}
	// A failed case on a healthy cluster leaves the gate open.
	second := &gateCase{name: "Suite/second"}
	gate.before(second)
	second.failed = true
	gate.after(second)
	if gate.broken != "" || checks != 3 {
		t.Fatalf("a failed case on a clean cluster broke the gate: %q", gate.broken)
	}
	// A failed case that leaves the cluster unclean breaks it.
	third := &gateCase{name: "Suite/third"}
	gate.before(third)
	healthy = false
	third.failed = true
	gate.after(third)
	if gate.broken != "Suite/third" || len(third.messages) != 1 {
		t.Fatalf("broken %q messages %v", gate.broken, third.messages)
	}
	// Every later case fails before it starts, naming the case that broke it,
	// and the cluster is not checked again.
	healthy = true
	before := checks
	fourth := &gateCase{name: "Suite/fourth"}
	gate.before(fourth)
	if !fourth.fatal || checks != before || len(fourth.messages) != 1 || fourth.messages[0] != "the shared cluster was left unusable by Suite/third; this case did not run" {
		t.Fatalf("a later case: %+v", fourth)
	}
	// An unclean cluster at a setup breaks it too.
	other := sharedClusterGate{check: func() error { return errors.New("no quorum") }}
	setup := &gateCase{name: "Suite/setup"}
	other.before(setup)
	if !setup.fatal || other.broken != "the setup of Suite/setup" {
		t.Fatalf("setup check: %+v broken %q", setup, other.broken)
	}
}
