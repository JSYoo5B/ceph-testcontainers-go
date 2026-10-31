//go:build all || integration

package integration_test

// sharedClusterGate is the whole policy of a suite that serves several cases
// from one cluster: before each case the cluster must pass check, and after a
// failed case it is checked again. A cluster that fails either check marks the
// gate broken, and every later case fails at once naming the case that broke
// it, so no case of a damaged cluster passes as an independent result. It
// repairs, resets and retries nothing.
type sharedClusterGate struct {
	check  func() error
	broken string
}

// gateT is the part of *testing.T the gate uses.
type gateT interface {
	Helper()
	Name() string
	Failed() bool
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func (g *sharedClusterGate) before(t gateT) {
	t.Helper()
	if g.broken != "" {
		t.Fatalf("the shared cluster was left unusable by %s; this case did not run", g.broken)
		return
	}
	if err := g.check(); err != nil {
		g.broken = "the setup of " + t.Name()
		t.Fatalf("the shared cluster is not usable before the case: %v", err)
	}
}

func (g *sharedClusterGate) after(t gateT) {
	t.Helper()
	if t.Failed() && g.broken == "" {
		if err := g.check(); err != nil {
			g.broken = t.Name()
			t.Errorf("the shared cluster is not usable after the failed case: %v", err)
		}
	}
}
