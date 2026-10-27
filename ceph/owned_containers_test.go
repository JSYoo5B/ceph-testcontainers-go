package ceph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	dockercontainer "github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// fakeOwnedDocker holds containers by ID with their labels.
type fakeOwnedDocker struct {
	containers map[string]map[string]string
	removeErr  map[string]error
	removed    []string
	// onRemove runs after a container was removed.
	onRemove func(id string)
}

func (d *fakeOwnedDocker) ContainerList(_ context.Context, options mobycl.ContainerListOptions) (mobycl.ContainerListResult, error) {
	var result mobycl.ContainerListResult
	for id, labels := range d.containers {
		match := options.All
		for filter := range options.Filters["label"] {
			key, value, _ := strings.Cut(filter, "=")
			match = match && labels[key] == value
		}
		if match {
			result.Items = append(result.Items, dockercontainer.Summary{ID: id, Labels: labels})
		}
	}
	return result, nil
}

func (d *fakeOwnedDocker) ContainerRemove(_ context.Context, id string, options mobycl.ContainerRemoveOptions) (mobycl.ContainerRemoveResult, error) {
	if err := d.removeErr[id]; err != nil {
		return mobycl.ContainerRemoveResult{}, err
	}
	if !options.Force {
		return mobycl.ContainerRemoveResult{}, errors.New("a running container needs force")
	}
	delete(d.containers, id)
	d.removed = append(d.removed, id)
	if d.onRemove != nil {
		d.onRemove(id)
	}
	return mobycl.ContainerRemoveResult{}, nil
}

func (d *fakeOwnedDocker) Close() error { return nil }

func ownedCluster(owner string, docker *fakeOwnedDocker) *Container {
	return &Container{settings: options{startupTimeout: time.Second}, owner: owner,
		nets:                 networkState{public: &testcontainers.DockerNetwork{Name: "tc-public"}, publicRemoved: true},
		ownedContainerDocker: func(context.Context) (ownedContainerClient, error) { return docker, nil }}
}

// A container that Docker created although the create reply was lost has no
// handle; Terminate removes it by the cluster's label, and nothing else.
func TestTerminateRemovesLabeledContainers(t *testing.T) {
	docker := &fakeOwnedDocker{containers: map[string]map[string]string{
		"lost-osd":      {clusterOwnerLabel: "cluster-a"},
		"other-cluster": {clusterOwnerLabel: "cluster-b"},
		"caller-client": {"purpose": "client"},
	}}
	if err := ownedCluster("cluster-a", docker).Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(docker.removed) != 1 || docker.removed[0] != "lost-osd" || len(docker.containers) != 2 {
		t.Fatalf("removed %v, left %v", docker.removed, docker.containers)
	}

	// A container that is already gone is no error; another failure is,
	// and a later Terminate retries it.
	docker = &fakeOwnedDocker{
		containers: map[string]map[string]string{"gone": {clusterOwnerLabel: "c"}, "stuck": {clusterOwnerLabel: "c"}},
		removeErr:  map[string]error{"gone": fmt.Errorf("no such container: %w", errdefs.ErrNotFound), "stuck": errors.New("device busy")},
	}
	cluster := ownedCluster("c", docker)
	if err := cluster.Terminate(t.Context()); err == nil || !strings.Contains(err.Error(), "stuck") || strings.Contains(err.Error(), "gone") {
		t.Fatalf("Terminate returned %v", err)
	}
	delete(docker.removeErr, "stuck")
	if err := cluster.Terminate(t.Context()); err != nil || docker.containers["stuck"] != nil {
		t.Fatalf("a repeated Terminate returned %v and left %v", err, docker.containers)
	}

	// A cluster without an owner ID, such as a fake, lists nothing.
	unowned := ownedCluster("", nil)
	unowned.ownedContainerDocker = func(context.Context) (ownedContainerClient, error) {
		return nil, errors.New("listed the containers of a cluster without an owner")
	}
	if err := unowned.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// An OSD whose create reply was lost can hold the OSD memory volume, whose
// removal then fails until the labeled containers are removed; Terminate
// releases the volume again after that, in the same call.
func TestTerminateReleasesTheMemoryVolumeAfterTheLabeledContainers(t *testing.T) {
	storage, volumes, _ := newOSDMemoryUnitFixture()
	inUse := errors.New("volume is in use")
	volumes.removeErr = inUse
	docker := &fakeOwnedDocker{containers: map[string]map[string]string{"lost-osd": {clusterOwnerLabel: "c"}}}
	docker.onRemove = func(id string) {
		if id == "lost-osd" {
			volumes.removeErr = nil
		}
	}
	cluster := ownedCluster("c", docker)
	cluster.osdMemory = storage
	if err := cluster.Terminate(t.Context()); err != nil || cluster.osdMemory != nil || !storage.volumeRemoved || volumes.removals != 2 {
		t.Fatalf("Terminate returned %v; memory %+v, removals %d", err, cluster.osdMemory, volumes.removals)
	}
}
