package override

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// delayedMutation holds one native write before applying it. Reads still see
// the prior state, independently of the original caller's cancellation.
type delayedMutation struct {
	testcontainers.Container
	match            []string
	started, release chan struct{}
}

func (d *delayedMutation) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if slices.Equal(args[3:3+min(len(d.match), len(args)-3)], d.match) {
		close(d.started)
		select {
		case <-d.release:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	return d.Container.Exec(ctx, args, opts...)
}

type restorable interface{ Restore(context.Context) error }

func TestOverridesRetainDelayedMutations(t *testing.T) {
	for _, name := range []string{"config", "messenger", "blocklist", "flag", "ratios", "module"} {
		for _, restore := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/apply", true: "/restore"}[restore], func(t *testing.T) {
				var c *ceph.Container
				var ctr testcontainers.Container
				var apply func(context.Context) (restorable, error)
				var prefix []string
				switch name {
				case "config":
					f := &configFixture{}
					ctr = f
					c = configCluster(f, ceph.MessengerDefault)
					apply = func(ctx context.Context) (restorable, error) {
						return SetConfig(ctx, c, ceph.ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"})
					}
					prefix = []string{"config", map[bool]string{false: "set", true: "rm"}[restore]}
				case "messenger":
					f := &configFixture{}
					ctr = f
					c = configCluster(f, ceph.MessengerDefault)
					apply = func(ctx context.Context) (restorable, error) {
						return SetMessengerPolicy(ctx, c, ceph.MessengerPolicy{OSDs: []ceph.MessengerConnectionMode{ceph.MessengerCRC}})
					}
					prefix = []string{"config", map[bool]string{false: "set", true: "rm"}[restore]}
				case "blocklist":
					var f *blocklistFixture
					c, f = newBlocklistFixture(t)
					ctr = f
					apply = func(ctx context.Context) (restorable, error) {
						return AddBlocklist(ctx, c, blocklistTestAddress, time.Minute)
					}
					prefix = []string{"osd", "blocklist", map[bool]string{false: "add", true: "rm"}[restore]}
				case "flag":
					var f *osdFlagFixture
					c, f = newOSDFlagFixture(t)
					ctr = f
					apply = func(ctx context.Context) (restorable, error) { return SetOSDFlag(ctx, c, "norecover", true) }
					prefix = []string{"osd", map[bool]string{false: "set", true: "unset"}[restore]}
				case "ratios":
					var f *fullRatiosFixture
					c, f = newFullRatiosFixture(t)
					ctr = f
					apply = func(ctx context.Context) (restorable, error) {
						return SetFullRatios(ctx, c, ceph.FullRatios{NearFull: .1, BackfillFull: .2, Full: .3})
					}
					prefix = []string{"osd", map[bool]string{false: "set-nearfull-ratio", true: "set-full-ratio"}[restore]}
				case "module":
					f := newMGRModuleFixture()
					ctr = f
					c = mgrModuleCluster(f)
					apply = func(ctx context.Context) (restorable, error) { return SetMGRModule(ctx, c, "mirroring", true) }
					prefix = []string{"mgr", "module", map[bool]string{false: "enable", true: "disable"}[restore]}
				}
				var handle restorable
				if restore {
					var err error
					handle, err = apply(t.Context())
					if err != nil {
						t.Fatal(err)
					}
				}
				d := &delayedMutation{Container: ctr, match: prefix, started: make(chan struct{}), release: make(chan struct{})}
				testhelper.TestSetStartupTimeout(c, 20*time.Second)
				testhelper.TestSetControlPlane(c, d)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() {
					if restore {
						result <- handle.Restore(ctx)
					} else {
						var err error
						handle, err = apply(ctx)
						result <- err
					}
				}()
				select {
				case <-d.started:
				case <-time.After(time.Second):
					t.Fatal("write not dispatched")
				}
				cancel()
				if err := <-result; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v", err)
				}
				// A readable prior value cannot release a still-pending write.
				short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
				if err := handle.Restore(short); err == nil {
					t.Fatal("pending write was reported restored")
				}
				stop()
				close(d.release)
				testhelper.TestSetControlPlane(c, ctr)
				// Config and blocklist can have lost their initial readback. Preserve
				// their existing refusal of unconfirmed normalization/expiry ownership.
				err := handle.Restore(t.Context())
				if name == "blocklist" && !restore {
					f := ctr.(*blocklistFixture)
					delete(f.entries, blocklistTestAddress)
					err = handle.Restore(t.Context())
				}
				if err != nil {
					t.Fatal(err)
				}
				successor, err := apply(t.Context())
				if err != nil {
					t.Fatal("settled override blocked successor", err)
				}
				if err := successor.Restore(t.Context()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
