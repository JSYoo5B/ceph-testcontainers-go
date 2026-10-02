package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// CephFSMirrorConfig connects existing CephFS filesystems. The caller creates
// both clusters and their MDS/filesystems before starting snapshot mirroring.
type CephFSMirrorConfig struct {
	Source, Destination                     *ceph.Container
	SourceFilesystem, DestinationFilesystem string
	DestinationSite                         string
	Directories                             []string
}

// CephFSMirror owns its daemon and any network attachment added to the source
// MGR. It embeds the daemon for Stop/Start failure injection. It does not own
// either Ceph cluster or the filesystems' data.
type CephFSMirror struct {
	testcontainers.Container
	SourceFilesystem, DestinationFilesystem     string
	SourceClientEntity, DestinationClientEntity string
	Directories                                 []string
	owned                                       resources
}

// RunCephFSMirror configures one-way directory snapshot mirroring between two
// existing filesystems and starts cephfs-mirror in a separate container. image
// must contain a compatible cephfs-mirror binary. MGRs must supply mirroring.
// A non-nil result returned with an error must be terminated for partial cleanup.
// Terminate leaves Ceph auth, peer and directory policies in the caller-owned
// disposable clusters; it does not undo configuration or delete mirrored data.
func RunCephFSMirror(ctx context.Context, image string, config CephFSMirrorConfig, opts ...testcontainers.ContainerCustomizer) (*CephFSMirror, error) {
	if err := validatePair(image, config.Source, config.Destination); err != nil {
		return nil, err
	}
	config, err := normalizeCephFSMirrorConfig(config)
	if err != nil {
		return nil, err
	}
	manager := config.Source.ManagerContainer()
	if manager == nil || manager.GetContainerID() == "" {
		return nil, errors.New("source Ceph manager is unavailable")
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	sourceID, destinationID := "tc-cephfs-mirror-"+suffix, "tc-cephfs-peer-"+suffix
	mirror := &CephFSMirror{
		SourceFilesystem: config.SourceFilesystem, DestinationFilesystem: config.DestinationFilesystem,
		SourceClientEntity: "client." + sourceID, DestinationClientEntity: "client." + destinationID,
		Directories: append([]string(nil), config.Directories...),
	}
	for _, cluster := range []*ceph.Container{config.Source, config.Destination} {
		if _, err := cluster.Ceph(ctx, "mgr", "module", "enable", "mirroring"); err != nil {
			return mirror, fmt.Errorf("enable CephFS mirroring manager module: %w", err)
		}
	}
	// The manager mounts the peer filesystem during bootstrap import to verify
	// its identity and record ceph.mirror.info. The daemon's network attachment
	// alone cannot provide this control-plane connectivity.
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return mirror, fmt.Errorf("create Docker client for CephFS manager networking: %w", err)
	}
	ownedAttachment := false
	mirror.owned.addCleanup("disconnect CephFS manager and close Docker client", func(cleanupCtx context.Context) error {
		if ownedAttachment {
			if _, err := docker.NetworkDisconnect(cleanupCtx, config.Destination.NetworkName(), mobycl.NetworkDisconnectOptions{Container: manager.GetContainerID()}); ignoreMissing(err) != nil {
				// Keep the client available when disconnect must be retried.
				return err
			}
			ownedAttachment = false
		}
		return docker.Close()
	})
	inspection, err := docker.ContainerInspect(ctx, manager.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		return mirror, fmt.Errorf("inspect source CephFS manager: %w", err)
	}
	if inspection.Container.NetworkSettings == nil {
		return mirror, errors.New("source CephFS manager has no Docker network settings")
	}
	if _, attached := inspection.Container.NetworkSettings.Networks[config.Destination.NetworkName()]; !attached {
		if _, err := docker.NetworkConnect(ctx, config.Destination.NetworkName(), mobycl.NetworkConnectOptions{Container: manager.GetContainerID()}); err != nil {
			return mirror, fmt.Errorf("connect source CephFS manager to destination network: %w", err)
		}
		ownedAttachment = true
	}
	keyring, err := config.Source.Ceph(ctx, "auth", "get-or-create", mirror.SourceClientEntity,
		"mon", "profile cephfs-mirror", "mds", "allow r",
		"osd", "allow rw tag cephfs metadata=*, allow r tag cephfs data=*", "mgr", "allow r")
	if err != nil {
		return mirror, fmt.Errorf("create source CephFS mirror credentials: %w", err)
	}
	if _, err := config.Destination.Ceph(ctx, "fs", "authorize", config.DestinationFilesystem, mirror.DestinationClientEntity, "/", "rwps"); err != nil {
		return mirror, fmt.Errorf("authorize destination CephFS mirror peer: %w", err)
	}
	bootstrap, err := config.Destination.Ceph(ctx, "fs", "snapshot", "mirror", "peer_bootstrap", "create",
		config.DestinationFilesystem, mirror.DestinationClientEntity, config.DestinationSite)
	if err != nil {
		return mirror, fmt.Errorf("create destination CephFS peer bootstrap token: %w", err)
	}
	var token struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(bootstrap, &token); err != nil || token.Token == "" {
		return mirror, errors.New("destination did not return a valid CephFS peer bootstrap token")
	}
	if _, err := config.Source.Ceph(ctx, "fs", "snapshot", "mirror", "enable", config.SourceFilesystem); err != nil {
		return mirror, fmt.Errorf("enable CephFS source snapshot mirroring: %w", err)
	}
	if _, err := config.Source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_bootstrap", "import", config.SourceFilesystem, token.Token); err != nil {
		return mirror, fmt.Errorf("import destination CephFS peer: %w", err)
	}
	for _, directory := range config.Directories {
		if _, err := config.Source.Ceph(ctx, "fs", "snapshot", "mirror", "add", config.SourceFilesystem, directory); err != nil {
			return mirror, fmt.Errorf("configure CephFS mirror directory %q: %w", directory, err)
		}
	}
	const socket = "/run/ceph/cephfs-mirror.asok"
	moduleOpts := []testcontainers.ContainerCustomizer{
		config.Source.WithClient(), network.WithNetworkName(nil, config.Destination.NetworkName()),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(keyring), ContainerFilePath: "/etc/ceph/ceph.client." + sourceID + ".keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("cephfs-mirror"),
		testcontainers.WithCmd("--id", sourceID, "-f", "--admin-socket", socket, "--cephfs-mirror-directory-scan-interval", "1"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", socket}).WithStartupTimeout(2 * time.Minute)),
	}
	moduleOpts = append(moduleOpts, opts...)
	daemon, err := testcontainers.Run(ctx, image, moduleOpts...)
	if daemon != nil {
		mirror.Container = daemon
		mirror.owned.addContainer(daemon)
	}
	if err != nil {
		return mirror, fmt.Errorf("run CephFS snapshot mirror daemon: %w", err)
	}
	return mirror, nil
}

// Terminate removes owned containers and the MGR network attachment. It leaves
// caller-owned clusters, auth entities, peer policies and snapshots intact.
// It may be retried after a cleanup error.
func (mirror *CephFSMirror) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	return mirror.owned.terminate(ctx, opts...)
}

func normalizeCephFSMirrorConfig(config CephFSMirrorConfig) (CephFSMirrorConfig, error) {
	for _, name := range []string{config.SourceFilesystem, config.DestinationFilesystem} {
		if name == "" || strings.ContainsAny(name, "/\x00\r\n\t ") {
			return config, errors.New("source and destination CephFS names must be non-empty without slashes or whitespace")
		}
	}
	if config.DestinationSite == "" {
		config.DestinationSite = "destination"
	}
	if strings.ContainsAny(config.DestinationSite, "@/\x00\r\n\t ") {
		return config, errors.New("CephFS destination site cannot contain separators or whitespace")
	}
	if len(config.Directories) == 0 {
		return config, errors.New("at least one CephFS mirror directory is required")
	}
	directories := make([]string, 0, len(config.Directories))
	for _, directory := range config.Directories {
		if !path.IsAbs(directory) || strings.ContainsRune(directory, 0) {
			return config, fmt.Errorf("CephFS mirror directory %q must be an absolute path", directory)
		}
		directory = path.Clean(directory)
		for _, previous := range directories {
			if directory == previous || directory == "/" || previous == "/" || strings.HasPrefix(directory, previous+"/") || strings.HasPrefix(previous, directory+"/") {
				return config, fmt.Errorf("CephFS mirror directories %q and %q overlap", previous, directory)
			}
		}
		directories = append(directories, directory)
	}
	config.Directories = directories
	return config, nil
}
