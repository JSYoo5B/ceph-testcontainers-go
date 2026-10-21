package rbd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// ImageWatcher is one client watching an image header. Address is the
// client's IP:port/nonce, the same form BlocklistEntries reports. A client
// that stops answering keeps its watch until the OSD's
// osd_client_watch_timeout expires, 30 seconds by default.
type ImageWatcher struct {
	Address  string
	ClientID uint64
	Cookie   uint64
}

// ImageLock is one lock on an image header. Managed marks the lock librbd
// takes for the exclusive-lock feature; its ID carries the owner's watch
// cookie. Other locks are advisory locks added through rbd lock add.
type ImageLock struct {
	ID      string
	Locker  string
	Address string
	Managed bool
}

// ImageClientStatus reports an image's watchers and locks. The two lists
// come from separate native reads, so a lock handoff between them can show a
// new owner without its watch or the reverse; poll for a stable answer.
type ImageClientStatus struct {
	Watchers []ImageWatcher
	Locks    []ImageLock
}

// ExclusiveOwner returns the managed exclusive lock, or nil if no client
// holds it.
func (status *ImageClientStatus) ExclusiveOwner() *ImageLock {
	if status == nil {
		return nil
	}
	for i := range status.Locks {
		if status.Locks[i].Managed {
			return &status.Locks[i]
		}
	}
	return nil
}

// ImageClients reads which clients watch an image and which hold its
// locks, including clients outside this fixture. Namespace is empty for the
// default namespace. It reads native state only and grants no ownership.
func ImageClients(ctx context.Context, c *ceph.Container, pool, namespace, image string) (*ImageClientStatus, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return imageClients(ctx, c, pool, namespace, image)
}

func imageClients(ctx context.Context, c *ceph.Container, pool, namespace, image string) (*ImageClientStatus, error) {
	if err := validateResourceName("pool", pool); err != nil {
		return nil, err
	}
	if namespace != "" {
		if err := validateResourceName("namespace", namespace); err != nil {
			return nil, err
		}
	}
	if err := validateResourceName("image", image); err != nil {
		return nil, err
	}
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	if err := controlReady(ctx, c); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	spec := []string{"--pool", pool, "--image", image, "--format", "json"}
	if namespace != "" {
		spec = append(spec, "--namespace", namespace)
	}
	name := imageSpec(pool, namespace, image)
	watchers, err := servicehook.ClientCommand(ctx, c, append([]string{"rbd", "status"}, spec...)...)
	if err != nil {
		return nil, fmt.Errorf("read watchers of RBD image %s: %w", name, err)
	}
	locks, err := servicehook.ClientCommand(ctx, c, append([]string{"rbd", "lock", "ls"}, spec...)...)
	if err != nil {
		return nil, fmt.Errorf("read locks of RBD image %s: %w", name, err)
	}
	return parseImageClients(watchers, locks)
}

func imageSpec(pool, namespace, image string) string {
	if namespace == "" {
		return pool + "/" + image
	}
	return pool + "/" + namespace + "/" + image
}

func parseImageClients(watcherData, lockData []byte) (*ImageClientStatus, error) {
	var native struct {
		Watchers []struct {
			Address string `json:"address"`
			Client  uint64 `json:"client"`
			Cookie  uint64 `json:"cookie"`
		} `json:"watchers"`
	}
	if err := json.Unmarshal(watcherData, &native); err != nil || native.Watchers == nil {
		return nil, errors.New("decode RBD image watchers")
	}
	var locks []struct {
		ID      string `json:"id"`
		Locker  string `json:"locker"`
		Address string `json:"address"`
	}
	if err := json.Unmarshal(lockData, &locks); err != nil || locks == nil {
		return nil, errors.New("decode RBD image locks")
	}
	status := &ImageClientStatus{Watchers: []ImageWatcher{}, Locks: []ImageLock{}}
	for _, watcher := range native.Watchers {
		if watcher.Address == "" || watcher.Client == 0 {
			return nil, errors.New("RBD image watcher lacks its client identity")
		}
		status.Watchers = append(status.Watchers, ImageWatcher{Address: watcher.Address, ClientID: watcher.Client, Cookie: watcher.Cookie})
	}
	for _, lock := range locks {
		if lock.ID == "" || lock.Locker == "" || lock.Address == "" {
			return nil, errors.New("RBD image lock lacks its owner identity")
		}
		status.Locks = append(status.Locks, ImageLock{ID: lock.ID, Locker: lock.Locker, Address: lock.Address, Managed: strings.HasPrefix(lock.ID, "auto ")})
	}
	return status, nil
}
