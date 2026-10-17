//go:build all || ci

package pkg_test

import "strings"

// runner is called through drive.
type runner interface {
	run() int
}

type fastRunner struct{}

func (fastRunner) run() int { return 1 }

type slowRunner struct{}

func (slowRunner) run() int { return 2 }

func drive(r runner) int {
	return r.run()
}

// Helpers of single batches.

func helperA() string {
	return strings.ToUpper("a")
}

func helperB() int {
	return 2
}

func unused() int {
	return 4
}

var shared = 3
