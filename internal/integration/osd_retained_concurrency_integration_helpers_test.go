//go:build all || integration

package integration_test

import (
	"context"
	"time"
)

// This forwarding context observes phase boundaries without changing deadlines
// or injecting faults into Docker. Done before Deadline proves the caller is
// still waiting for topology; Deadline marks the bounded admitted operation.
// Every instance belongs to one ReopenOSD call.
type retainedReopenContext struct {
	context.Context
	acquired func()
	waiting  func()
	derived  bool
}

func (c *retainedReopenContext) Deadline() (time.Time, bool) {
	c.derived = true
	if c.acquired != nil {
		c.acquired()
	}
	return c.Context.Deadline()
}
func (c *retainedReopenContext) Done() <-chan struct{} {
	if !c.derived && c.waiting != nil {
		c.waiting()
	}
	return c.Context.Done()
}
