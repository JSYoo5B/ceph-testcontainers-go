package federation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RBDMirrorConfig connects two existing clusters through one destination-side
// rbd-mirror daemon. Both clusters must already contain an initialized RBD Pool.
// SourceSite and DestinationSite default to "source" and "destination".
type RBDMirrorConfig struct {
	Source          *ceph.Container
	Destination     *ceph.Container
	Pool            string
	SourceSite      string
	DestinationSite string
}

// RBDMirror owns its daemon and setup CLI containers, not the Ceph clusters.
// Stop and Start control the daemon; Terminate removes all owned containers.
type RBDMirror struct {
	testcontainers.Container
	owned resources
}

// RunRBDMirror enables image-mode mirroring on both pools, imports an rx-only
// peer into the destination, and starts a daemon connected to both clusters.
// image must contain the RBD CLI and rbd-mirror. Enable snapshot mirroring and
// create checkpoints on each intended source image separately with the RBD CLI.
//
// The caller must Terminate a non-nil result even when setup returns an error.
// Terminate removes runtime containers but leaves mirroring mode, peer and auth
// configuration in the disposable clusters; it never destroys pools or data.
// Customizers apply to the daemon only and must preserve its networking, Ceph
// configuration, credentials and entrypoint.
func RunRBDMirror(ctx context.Context, image string, config RBDMirrorConfig, opts ...testcontainers.ContainerCustomizer) (*RBDMirror, error) {
	if err := validatePair(image, config.Source, config.Destination); err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	config, err := normalizeRBDMirrorConfig(config)
	if err != nil {
		return nil, fmt.Errorf("configure RBD mirror: %w", err)
	}
	mirror := &RBDMirror{}
	sourceClient, err := runClient(ctx, image, config.Source, config.Destination.NetworkName(), &mirror.owned)
	if err != nil {
		return mirror, fmt.Errorf("run source RBD setup client: %w", err)
	}
	destinationClient, err := runClient(ctx, image, config.Destination, config.Source.NetworkName(), &mirror.owned)
	if err != nil {
		return mirror, fmt.Errorf("run destination RBD setup client: %w", err)
	}
	for _, site := range []struct {
		client testcontainers.Container
		name   string
	}{
		{sourceClient, config.SourceSite},
		{destinationClient, config.DestinationSite},
	} {
		if _, err := exec(ctx, site.client, "rbd", "mirror", "pool", "enable", "--site-name", site.name, config.Pool, "image"); err != nil {
			return mirror, fmt.Errorf("enable %s RBD pool mirroring: %w", site.name, err)
		}
	}
	token, err := exec(ctx, sourceClient, "rbd", "mirror", "pool", "peer", "bootstrap", "create", "--site-name", config.SourceSite, config.Pool)
	if err != nil {
		return mirror, fmt.Errorf("create RBD mirror peer token: %w", err)
	}
	if len(bytes.TrimSpace(token)) == 0 {
		return mirror, errors.New("source returned an empty RBD mirror peer token")
	}
	const tokenPath = "/tmp/rbd-mirror-peer-token"
	if err := destinationClient.CopyToContainer(ctx, token, tokenPath, 0o600); err != nil {
		return mirror, fmt.Errorf("copy RBD mirror peer token: %w", err)
	}
	if _, err := exec(ctx, destinationClient, "rbd", "mirror", "pool", "peer", "bootstrap", "import",
		"--site-name", config.DestinationSite, "--direction", "rx-only", config.Pool, tokenPath); err != nil {
		return mirror, fmt.Errorf("import rx-only RBD mirror peer: %w", err)
	}
	if _, err := exec(ctx, destinationClient, "rm", "-f", tokenPath); err != nil {
		return mirror, fmt.Errorf("remove RBD mirror peer token file: %w", err)
	}

	clientName := "client.rbd-mirror.tc-" + uuid.NewString()
	keyring, err := config.Destination.Ceph(ctx, "auth", "get-or-create", clientName,
		"mon", "profile rbd-mirror", "osd", "profile rbd")
	if err != nil {
		return mirror, fmt.Errorf("create RBD mirror daemon credentials: %w", err)
	}
	daemonOpts := []testcontainers.ContainerCustomizer{
		config.Destination.WithClient(),
		network.WithNetworkName(nil, config.Source.NetworkName()),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(keyring), ContainerFilePath: "/etc/ceph/rbd-mirror.keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("rbd-mirror"),
		testcontainers.WithCmd("-f", "--name", clientName, "--keyring", "/etc/ceph/rbd-mirror.keyring",
			"--admin-socket", "/tmp/rbd-mirror.asok", "--log-to-stderr=true", "--log-to-file=false"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/tmp/rbd-mirror.asok"}).WithStartupTimeout(time.Minute)),
	}
	daemonOpts = append(daemonOpts, opts...)
	daemon, err := testcontainers.Run(ctx, image, daemonOpts...)
	if daemon != nil {
		mirror.Container = daemon
		mirror.owned.addContainer(daemon)
	}
	if err != nil {
		return mirror, fmt.Errorf("run destination RBD mirror daemon: %w", err)
	}
	return mirror, nil
}

// Terminate removes the daemon and setup clients while preserving both clusters,
// their image data, and the mirroring/auth configuration written during setup.
func (m *RBDMirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	return m.owned.terminate(ctx, opts...)
}

func normalizeRBDMirrorConfig(config RBDMirrorConfig) (RBDMirrorConfig, error) {
	config.Pool = strings.TrimSpace(config.Pool)
	config.SourceSite = strings.TrimSpace(config.SourceSite)
	config.DestinationSite = strings.TrimSpace(config.DestinationSite)
	if config.SourceSite == "" {
		config.SourceSite = "source"
	}
	if config.DestinationSite == "" {
		config.DestinationSite = "destination"
	}
	if config.Pool == "" {
		return config, errors.New("RBD pool is required")
	}
	if strings.HasPrefix(config.Pool, "-") || strings.ContainsAny(config.Pool, "/ \t\r\n") {
		return config, errors.New("RBD pool must be a pool name without namespace or option syntax")
	}
	if strings.HasPrefix(config.SourceSite, "-") || strings.HasPrefix(config.DestinationSite, "-") {
		return config, errors.New("RBD site names must not begin with '-'")
	}
	if config.SourceSite == config.DestinationSite {
		return config, errors.New("source and destination RBD site names must differ")
	}
	return config, nil
}
