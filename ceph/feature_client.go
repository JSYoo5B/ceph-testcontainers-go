package ceph

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// featureClient starts and returns the cluster's feature client: the
// container of the client image that runs the commands the essential image
// lacks. It owns the feature-client state (Container.toolkit, see
// featureClientState), bound once by its constructor, and binds it to what
// the startup reads: the native-command state, the run directory, the
// settings, the startup identity, the command transport, the admin client
// customizer and the cluster facts. Its startup lock is taken after
// Container.mu where a caller holds both; see featureClientState.
type featureClient struct {
	toolkit     *featureClientState
	native      *nativeCommands
	runDir      *runDirState
	settings    options
	startup     *startupIdentity
	conn        cephConn
	adminClient func(...ClientContainerOptions) testcontainers.CustomizeRequestOption
	facts       func() ClusterFacts
}

// featureClient returns the feature client of the cluster. A nil cluster
// returns one whose startedContainer reports the cluster unavailable.
func (c *Container) featureClient() featureClient {
	if c == nil {
		return featureClient{}
	}
	return featureClient{
		toolkit: &c.toolkit, native: &c.native, runDir: &c.runDir, settings: c.settings, startup: &c.startup,
		conn: c.conn(), adminClient: c.adminClient, facts: c.staticFacts,
	}
}

// startedContainer returns the feature client, starting it on first use. It
// uses its own mutex: service callers can already hold the topology mutex.
func (f featureClient) startedContainer(ctx context.Context) (testcontainers.Container, error) {
	if f.facts == nil {
		return nil, fmt.Errorf("ceph cluster is unavailable")
	}
	images := f.facts().Images
	if images.Client == images.Essential {
		return f.conn.controlContainer(ctx)
	}
	if images.Client == "" {
		return nil, invalidRequest("client image is required for this operation")
	}
	if err := f.toolkit.lock(ctx); err != nil {
		return nil, err
	}
	defer f.toolkit.unlock()
	if f.native.calls.isClosed() {
		return nil, errClusterTerminated
	}
	if f.toolkit.startupAttempted() {
		if !f.toolkit.startupReady() {
			return nil, fmt.Errorf("client startup failed; terminate the partial cluster")
		}
		return f.toolkit.current(), nil
	}
	ctx, cancel := context.WithTimeout(ctx, f.settings.startupTimeout)
	if !f.native.calls.beginClientStartup(cancel) {
		cancel()
		return nil, errClusterTerminated
	}
	defer func() { f.native.calls.endClientStartup(); cancel() }()
	f.toolkit.markAttempted()
	ctr, err := testcontainers.Run(ctx, images.Client, append([]testcontainers.ContainerCustomizer{
		f.adminClient(), f.startup.ownedLabel(), WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
	}, f.runDir.mounts()...)...)
	f.toolkit.publish(ctr) // A partial start remains owned and is never retried.
	if err == nil && ctr != nil {
		output, versionErr := command(ctx, ctr, "ceph", "--version")
		if versionErr != nil {
			err = versionErr
		} else {
			version, parseErr := parseCephVersion(string(output))
			if parseErr != nil {
				err = parseErr
			} else if version != f.facts().CephVersion { // Unchanged: daemons with DaemonOptions.WithImage do not take part.
				err = fmt.Errorf("client Ceph release %s differs from core %s", version, f.facts().CephVersion)
			}
		}
	}
	if f.native.calls.isClosed() {
		err = errClusterTerminated
	}
	if err == nil && ctr == nil {
		err = fmt.Errorf("client startup returned no container")
	}
	f.toolkit.markReady(err == nil && ctr != nil)
	return ctr, err
}

// radosLookup binds a RADOS query lookup to the feature client. Each call
// selects the client container again.
func (f featureClient) radosLookup() radosClientLookup {
	return func(ctx context.Context) (radosQuery, error) {
		client, err := f.startedContainer(ctx)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, args ...string) ([]byte, error) {
			return command(ctx, client, args...)
		}, nil
	}
}
