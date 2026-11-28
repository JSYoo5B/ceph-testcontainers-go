package gwbase_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// countingGateway is a running gateway container that counts the commands
// it is asked to run and runs none.
type countingGateway struct {
	testcontainers.Container
	execs int
}

func (*countingGateway) GetContainerID() string { return "gateway" }

func (*countingGateway) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

func (c *countingGateway) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.execs++
	return 1, nil, errors.New("unexpected command")
}

func TestInactiveIdentitiesAreRefusedBeforeNativeCommands(t *testing.T) {
	ctr := &countingGateway{}
	g := gwtest.NewGateway(ctr, nil)
	removed := &gwbase.User{Origin: g.Core, ID: "user", State: &gwbase.UserState{Created: true, Removed: true}}
	unconfirmed := &gwbase.User{Origin: g.Core, ID: "user", State: &gwbase.UserState{}}
	account := gwtest.ConfirmedAccount(g.Core, gwbase.NativeScope{}, "RGW12345678901234567", "", "tag")
	account.State.Removed = true
	for name, refuse := range map[string]func(context.Context) error{
		"OwnedUser removed": func(ctx context.Context) error { _, err := g.Core.OwnedUser(ctx, removed); return err },
		"OwnedUser unconfirmed": func(ctx context.Context) error {
			_, err := g.Core.OwnedUser(ctx, unconfirmed)
			return err
		},
		"RevokeKey": func(ctx context.Context) error {
			return g.Core.RevokeKey(ctx, removed, &gwbase.Key{AccessKey: "ACCESS", Status: gwbase.KeyActive})
		},
		"OwnedAccount": func(ctx context.Context) error { _, err := g.Core.OwnedAccount(ctx, account); return err },
	} {
		if err := refuse(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || ctr.execs != 0 {
			t.Errorf("%s returned %v after %d commands", name, err, ctr.execs)
		}
	}
}
