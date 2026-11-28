package placement

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/testcontainers/testcontainers-go"
)

// Apply pins the complete creation documents. Reload accepts other placements
// but also requires this target to be committed locally when the zone has a realm.
type restartPolicy struct {
	exact, published bool
}

func (p *Placement) runningPolicy(ctx context.Context, g *gwbase.Core, policy restartPolicy) (map[string]any, error) {
	if err := p.checkRuntime(ctx, g); err != nil {
		return nil, err
	}
	return p.readPolicy(ctx, g, g.Container(), policy)
}

// readPolicy uses an independent admin target when the gateway is stopped.
// Pool IDs are always checked against the owning cluster.
func (p *Placement) readPolicy(ctx context.Context, g *gwbase.Core, target testcontainers.Container, policy restartPolicy) (map[string]any, error) {
	if err := p.checkPools(ctx, g); err != nil {
		return nil, err
	}
	group, zone, err := nativeScopeIn(ctx, g, target, p.scope)
	if err != nil {
		return nil, err
	}
	if policy.exact {
		if !reflect.DeepEqual(group, p.group) || !reflect.DeepEqual(zone, p.zone) {
			return nil, servicehook.Invalid(errors.New("RGW zone or zonegroup changed since placement creation; refusing activation"))
		}
	} else if _, err := snapshot(p, group, zone); err != nil {
		return nil, err
	}
	if policy.published {
		period, err := currentPeriodIn(ctx, g, target, p.scope)
		if err != nil {
			return nil, err
		}
		if err := committedTarget(period, p); err != nil {
			return nil, err
		}
	}
	return group, nil
}

// placementRestart is the owner of a placement's gateway restart: the
// activation that requested it and the policy it loads.
type placementRestart struct {
	activation *activationState
	policy     restartPolicy
}

// target is the gateway container of g as the restart protocol sees it.
func target(g *gwbase.Core) gwbase.RestartTarget {
	return gwbase.RestartTarget{Cluster: g.Cluster(), Container: g.Container()}
}

// resumeRestart settles the restart that the gateway still owes, whoever
// requested it, and verifies this placement's own restart once its gateway
// runs again. It reports true when that verification completed the
// activation. Another placement's restart, or another requester's, is
// settled by the shared protocol and leaves this placement to dispatch its
// own restart.
func (p *Placement) resumeRestart(ctx context.Context, g *gwbase.Core, wanted restartPolicy) (bool, error) {
	resumed, err := target(g).Resume(ctx)
	if err != nil {
		return false, err
	}
	owner, ok := resumed.Owner.(*placementRestart)
	if !ok || owner.activation != p.activation || !resumed.Running {
		return false, nil
	}
	if resumed.Replaced {
		return false, errors.New("the RGW generation awaiting placement verification was replaced; request a fresh activation for the current gateway")
	}
	wanted.exact = wanted.exact || owner.policy.exact
	wanted.published = wanted.published || owner.policy.published
	if err := p.confirmRestart(ctx, g, owner, wanted); err != nil {
		return false, err
	}
	return true, nil
}

// beforeResume is what a restart of this placement checks before its stopped
// gateway starts again after an interrupted stop: the cluster's pool IDs and,
// through another admin container, the startup scope and the policy.
func (p *Placement) beforeResume(g *gwbase.Core, policy restartPolicy) func(context.Context) error {
	return func(ctx context.Context) error {
		admin := servicehook.CLI(g.Cluster())
		if peer, err := g.ScopeGateway(ctx); err == nil {
			admin = peer.Container()
		}
		if err := restartScope(ctx, g, admin, p.scope); err != nil {
			return fmt.Errorf("verify stopped RGW startup scope before resuming: %w", err)
		}
		_, err := p.readPolicy(ctx, g, admin, policy)
		return err
	}
}

// confirmRestart verifies the policy that the restarted gateway loaded, and
// that its generation did not change meanwhile, before it completes the
// restart record. A failed verification keeps the record for a read-only
// retry.
func (p *Placement) confirmRestart(ctx context.Context, g *gwbase.Core, owner *placementRestart, policy restartPolicy) error {
	if err := restartScope(ctx, g, g.Container(), p.scope); err != nil {
		return err
	}
	if _, err := p.runningPolicy(ctx, g, policy); err != nil {
		return err
	}
	return target(g).Complete(ctx, owner)
}

// restart restarts the gateway once to load the policy, then confirms it.
func (p *Placement) restart(ctx context.Context, g *gwbase.Core, policy restartPolicy) error {
	if err := restartScope(ctx, g, g.Container(), p.scope); err != nil {
		return err
	}
	owner := &placementRestart{activation: p.activation, policy: policy}
	if err := target(g).Restart(ctx, gwbase.Restart{Owner: owner, BeforeStart: p.beforeResume(g, policy), AwaitCompletion: true}); err != nil {
		return fmt.Errorf("restart RGW to load placement: %w", err)
	}
	return p.confirmRestart(ctx, g, owner, policy)
}
