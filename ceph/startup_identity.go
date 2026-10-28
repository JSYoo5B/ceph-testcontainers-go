package ceph

import (
	"context"
	"errors"
	"fmt"

	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// startupIdentity is what one cluster knows about itself from the moment it is
// created and from its first MON: the owner ID that labels every container the
// cluster creates, whether the first MON image's ceph-authtool accepts
// --key-type, and the ID of the first MON's image. It also holds the one test
// seam that replaces the Docker client of the labeled-container removal, the
// only code besides the label itself that uses the owner ID. It is a component
// of the internal state split described in docs/design/container-decomposition.md,
// the startup-identity group of the membership and control fields.
//
// The type has no mutex and Container.mu was not given a new job. Every field
// is written before the cluster value is shared and is read-only afterwards:
// owner by Run's composite literal in the same statement that creates the
// cluster, keyTypeOption and firstMonitorImageID by Run after the first MON
// started, in the single goroutine that still owns the value, and the seam only
// by tests before they start the cluster. A reader therefore needs no lock and
// takes none; none of the methods calls into Container or takes a lock, so they
// add no edge to the lock order. A cluster that failed to start is returned
// with whatever was recorded, and Terminate still reads the owner ID from it.
//
// The guards in startup_identity_test.go enforce that nothing touches the
// field's members outside this type, that the recorders are called only from
// Run, that the methods call only their pinned collaborators, that the callers
// are the audited set, and that the split adds no mutex.
type startupIdentity struct {
	owner         string
	keyTypeOption bool
	// firstMonitorImageID identifies one container generation, not a role.
	firstMonitorImageID string
	// ownedContainerDocker replaces the Docker client of Terminate's removal
	// of labeled containers in tests.
	ownedContainerDocker func(context.Context) (ownedContainerClient, error)
}

// newStartupIdentity returns the identity of a cluster that is not yet shared.
func newStartupIdentity(owner string) startupIdentity { return startupIdentity{owner: owner} }

// recordKeyTypeOption records whether the first MON image's ceph-authtool has
// --key-type. Run only, before the cluster is shared.
func (s *startupIdentity) recordKeyTypeOption(supported bool) { s.keyTypeOption = supported }

// recordFirstMonitorImage records the ID of the first MON's image. Run only,
// before the cluster is shared.
func (s *startupIdentity) recordFirstMonitorImage(id string) { s.firstMonitorImageID = id }

// ownerID returns the ID that labels the cluster's containers, empty for a
// cluster value that was not created by Run.
func (s *startupIdentity) ownerID() string { return s.owner }

// supportsKeyType reports whether the first MON image's ceph-authtool has
// --key-type; see generateKeyArgs.
func (s *startupIdentity) supportsKeyType() bool { return s.keyTypeOption }

// firstMonitorImage returns the ID of the first MON's image, which errors about
// the essential image's capabilities name.
func (s *startupIdentity) firstMonitorImage() string { return s.firstMonitorImageID }

// ownedLabel labels a container that the cluster creates; see clusterOwnerLabel.
func (s *startupIdentity) ownedLabel() testcontainers.CustomizeRequestOption {
	return testcontainers.WithLabels(map[string]string{clusterOwnerLabel: s.owner})
}

// removeLabeledContainers force-removes every container left with the
// cluster's label, including those whose create reply was lost.
func (s *startupIdentity) removeLabeledContainers(ctx context.Context) error {
	if s.owner == "" {
		return nil
	}
	newClient := s.ownedContainerDocker
	if newClient == nil {
		newClient = func(ctx context.Context) (ownedContainerClient, error) {
			return testcontainers.NewDockerClientWithOpts(ctx)
		}
	}
	docker, err := newClient(ctx)
	if err != nil {
		return fmt.Errorf("list the cluster's containers: %w", err)
	}
	defer func() { _ = docker.Close() }()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", clusterOwnerLabel+"="+s.owner)})
	if err != nil {
		return fmt.Errorf("list the cluster's containers: %w", err)
	}
	var errs []error
	for _, ctr := range list.Items {
		if _, err := docker.ContainerRemove(ctx, ctr.ID, mobycl.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); !onlyMissingHostResource(err) {
			errs = append(errs, fmt.Errorf("remove container %.12s of the cluster: %w", ctr.ID, err))
		}
	}
	return errors.Join(errs...)
}
