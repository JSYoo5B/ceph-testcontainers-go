package placement

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
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

// Gateway run state and an outstanding Docker request are shared by all
// placements on the cluster. The topology lock serializes access to the map.
type gatewayRestarts map[string]*gatewayRestart

type gatewayRestart struct {
	owner          *activationState
	policy         restartPolicy
	originalStart  string
	startConfirmed bool
	observedStart  *string
}

func restartsFor(cluster *ceph.Container) gatewayRestarts {
	return servicehook.Extension(cluster, "rgw/placement/gateway-restarts", func() any {
		return make(gatewayRestarts)
	}).(gatewayRestarts)
}

// resumeRestart verifies or restores the run state owed by an earlier call.
// Only the initiating placement can resume its stopped generation. An already
// completed restart cannot stand in for another placement's activation.
func (p *Placement) resumeRestart(ctx context.Context, g *gwbase.Core, wanted restartPolicy) (bool, error) {
	ctr := g.Container()
	id := ctr.GetContainerID()
	restarts := restartsFor(g.Cluster())
	record := restarts[id]
	if record == nil {
		return false, nil
	}
	state, err := ctr.State(ctx)
	if err != nil {
		return false, fmt.Errorf("inspect RGW after placement restart: %w", err)
	}
	if state == nil || state.Paused || state.Restarting {
		return false, errors.New("RGW run state cannot confirm the placement restart")
	}
	if record.observedStart != nil && state.StartedAt != *record.observedStart {
		delete(restarts, id)
		if record.owner == p.activation || !state.Running {
			return false, errors.New("the RGW generation awaiting placement verification was replaced; request a fresh activation for the current gateway")
		}
		return false, nil
	}
	sameGeneration := state.StartedAt == record.originalStart
	if !record.startConfirmed && sameGeneration && state.Running {
		return false, errors.New("the earlier RGW placement stop is unconfirmed and its original generation is still running; retry after the container stops or its generation changes")
	}
	if !state.Running && (!sameGeneration || record.startConfirmed) {
		delete(restarts, id)
		return false, errors.New("RGW was stopped outside this placement restart; leaving it stopped")
	}
	if record.owner != p.activation {
		if !state.Running {
			return false, errors.New("retry the placement that stopped this RGW gateway before activating another placement")
		}
		// The old Stop has settled. The new placement must pass fresh admission
		// and dispatch its own restart before it can replace this record.
		return false, nil
	}
	wanted.exact = wanted.exact || record.policy.exact
	wanted.published = wanted.published || record.policy.published
	if !state.Running {
		target := servicehook.CLI(g.Cluster())
		if peer, err := g.ScopeGateway(ctx); err == nil {
			target = peer.Container()
		}
		if err := restartScope(ctx, g, target, p.scope); err != nil {
			return false, fmt.Errorf("verify stopped RGW startup scope before resuming: %w", err)
		}
		if _, err := p.readPolicy(ctx, g, target, wanted); err != nil {
			return false, err
		}
		if err := ctr.Start(ctx); err != nil {
			return false, fmt.Errorf("resume RGW after placement restart: %w", err)
		}
	}
	record.policy = wanted
	record.startConfirmed = true
	if err := p.confirmRestart(ctx, g, record); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Placement) confirmRestart(ctx context.Context, g *gwbase.Core, record *gatewayRestart) error {
	ctr := g.Container()
	state, err := ctr.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect restarted RGW for placement verification: %w", err)
	}
	if state == nil || !state.Running || state.Paused || state.Restarting {
		return errors.New("restarted RGW is unavailable for placement verification")
	}
	if record.observedStart != nil && state.StartedAt != *record.observedStart {
		delete(restartsFor(g.Cluster()), ctr.GetContainerID())
		return errors.New("RGW generation changed during placement verification")
	}
	startedAt := state.StartedAt
	record.observedStart = &startedAt
	if _, err := p.runningPolicy(ctx, g, record.policy); err != nil {
		return err
	}
	// The gateway may also be restarted externally during the policy read.
	state, err = ctr.State(ctx)
	if err != nil {
		return fmt.Errorf("confirm restarted RGW generation: %w", err)
	}
	if state == nil || !state.Running || state.Paused || state.Restarting || state.StartedAt != *record.observedStart {
		return errors.New("RGW generation changed during placement verification")
	}
	delete(restartsFor(g.Cluster()), ctr.GetContainerID())
	return nil
}

// restart dispatches one stop/start pair, then confirms the loaded policy.
// A failed confirmation leaves the record available to a read-only retry.
func (p *Placement) restart(ctx context.Context, g *gwbase.Core, policy restartPolicy) error {
	ctr := g.Container()
	if err := restartScope(ctx, g, ctr, p.scope); err != nil {
		return err
	}
	state, err := ctr.State(ctx)
	if err != nil {
		return fmt.Errorf("inspect RGW before placement restart: %w", err)
	}
	if state == nil || !state.Running || state.Paused || state.Restarting {
		return servicehook.Invalid(errors.New("RGW placement restart requires a running gateway"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	restarts := restartsFor(g.Cluster())
	id := ctr.GetContainerID()
	record := &gatewayRestart{owner: p.activation, policy: policy, originalStart: state.StartedAt}
	restarts[id] = record
	grace := 3 * time.Second
	stopCtx, finishStop := gwbase.SettlementContext(ctx)
	stopErr := ctr.Stop(stopCtx, &grace)
	finishStop()
	startCtx, cancel := gwbase.SettlementContext(ctx)
	defer cancel()
	if stopErr != nil {
		restored, err := p.resumeRestart(startCtx, g, policy)
		if restored {
			return nil
		}
		return errors.Join(fmt.Errorf("stop RGW for placement restart: %w", stopErr), err)
	}
	if err := ctr.Start(startCtx); err != nil {
		return fmt.Errorf("restart RGW to load placement: %w", err)
	}
	record.startConfirmed = true
	return p.confirmRestart(startCtx, g, record)
}
