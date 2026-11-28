package rgw

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// budgetRecorder records how much of its deadline the first command of an
// operation still has.
type budgetRecorder struct {
	testcontainers.Container
	first time.Duration
	seen  bool
}

func (r *budgetRecorder) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if deadline, ok := ctx.Deadline(); ok && !r.seen {
		r.first, r.seen = time.Until(deadline), true
	}
	return r.Container.Exec(ctx, args, opts...)
}

// TestTypedOperationsStartTheirAdminBudgetAfterTheLock holds the topology
// mutex for most of the cluster's startup timeout while each typed operation
// waits for it. Every operation must still get the whole admin budget for its
// first command, so the wait cannot have consumed it.
func TestTypedOperationsStartTheirAdminBudgetAfterTheLock(t *testing.T) {
	const budget, held = 3 * time.Second, 2 * time.Second
	// The user operations run on the native user model, the key operations
	// on the key model; each operation gets fresh state.
	userModel := func() (*Gateway, *User, testcontainers.Container) {
		base := gwtest.NewPlacementContainer()
		f := gwtest.NewAccountContainer(base)
		owner := testCluster(base, testhelper.TestConfig{StartupTimeout: budget}, map[string]testcontainers.Container{"rgw": f})
		g := registerTestGateway(newGateway(owner, f, Config{Name: "default"}, 7480, 0, nil, "", ""))
		user, err := g.CreateUser(t.Context(), UserConfig{ID: "budget"})
		if err != nil {
			t.Fatal(err)
		}
		return g, user, f
	}
	keyModel := func() (*Gateway, *User, testcontainers.Container) {
		ctr := &keyTestContainer{faults: map[string]keyFault{}}
		owner := testCluster(nil, testhelper.TestConfig{StartupTimeout: budget}, map[string]testcontainers.Container{rgwServiceName(Config{Name: "default"}): ctr})
		g := registerTestGateway(testGateway(owner, ctr, Config{Name: "default"}))
		user, err := g.CreateUser(t.Context(), UserConfig{ID: "test-user"})
		if err != nil {
			t.Fatal(err)
		}
		return g, user, ctr
	}
	operations := map[string]struct {
		model func() (*Gateway, *User, testcontainers.Container)
		run   func(context.Context, *Gateway, *User) error
	}{
		"CreateUser": {userModel, func(ctx context.Context, g *Gateway, _ *User) error {
			_, err := g.CreateUser(ctx, UserConfig{ID: "other"})
			return err
		}},
		"User.Info": {userModel, func(ctx context.Context, _ *Gateway, u *User) error { _, err := u.Info(ctx); return err }},
		"User.SetQuota": {userModel, func(ctx context.Context, _ *Gateway, u *User) error {
			return u.SetQuota(ctx, Quota{MaxSizeBytes: -1, MaxObjects: -1})
		}},
		"User.Suspend": {userModel, func(ctx context.Context, _ *Gateway, u *User) error { return u.Suspend(ctx, false) }},
		"User.Usage":   {userModel, func(ctx context.Context, _ *Gateway, u *User) error { _, err := u.Usage(ctx); return err }},
		"User.Remove":  {userModel, func(ctx context.Context, _ *Gateway, u *User) error { return u.Remove(ctx) }},
		"CreateS3Key":  {keyModel, func(ctx context.Context, _ *Gateway, u *User) error { _, err := u.CreateS3Key(ctx); return err }},
		"S3Key.Revoke": {keyModel, func(ctx context.Context, _ *Gateway, u *User) error {
			key, err := u.CreateS3Key(context.WithoutCancel(ctx))
			if err != nil {
				return err
			}
			return key.Revoke(ctx)
		}},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g, user, ctr := operation.model()
			recorder := &budgetRecorder{Container: ctr}
			replaceGatewayContainer(g, recorder)
			mutex := testhelper.TestState(g.base().Cluster()).Mutex
			mutex.Lock()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = operation.run(t.Context(), g, user)
			}()
			time.Sleep(held)
			mutex.Unlock()
			<-done
			if !recorder.seen || recorder.first < budget-time.Second/2 {
				t.Fatalf("the first command had %v of its %v admin budget after a %v lock wait (ran %v)", recorder.first, budget, held, recorder.seen)
			}
		})
	}
}
