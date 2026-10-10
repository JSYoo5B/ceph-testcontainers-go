package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// RBDImageWatcher is one client watching an image header. Address is the
// client's IP:port/nonce, the same form BlocklistEntries reports. A client
// that stops answering keeps its watch until the OSD's
// osd_client_watch_timeout expires, 30 seconds by default.
type RBDImageWatcher struct {
	Address  string
	ClientID uint64
	Cookie   uint64
}

// RBDImageLock is one lock on an image header. Managed marks the lock librbd
// takes for the exclusive-lock feature; its ID carries the owner's watch
// cookie. Other locks are advisory locks added through rbd lock add.
type RBDImageLock struct {
	ID      string
	Locker  string
	Address string
	Managed bool
}

// RBDImageClientStatus reports an image's watchers and locks. The two lists
// come from separate native reads, so a lock handoff between them can show a
// new owner without its watch or the reverse; poll for a stable answer.
type RBDImageClientStatus struct {
	Watchers []RBDImageWatcher
	Locks    []RBDImageLock
}

// ExclusiveOwner returns the managed exclusive lock, or nil if no client
// holds it.
func (status *RBDImageClientStatus) ExclusiveOwner() *RBDImageLock {
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

// RBDImageClients reads which clients watch an image and which hold its
// locks, including clients outside this fixture. Namespace is empty for the
// default namespace. It reads native state only and grants no ownership.
func RBDImageClients(ctx context.Context, c *Container, pool, namespace, image string) (*RBDImageClientStatus, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return c.rbdImageClients(ctx, pool, namespace, image)
}

func (c *Container) rbdImageClients(ctx context.Context, pool, namespace, image string) (*RBDImageClientStatus, error) {
	if err := validateRBDResourceName("pool", pool); err != nil {
		return nil, err
	}
	if namespace != "" {
		if err := validateRBDResourceName("namespace", namespace); err != nil {
			return nil, err
		}
	}
	if err := validateRBDResourceName("image", image); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.rbdControlReady(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	spec := []string{"--pool", pool, "--image", image, "--format", "json"}
	if namespace != "" {
		spec = append(spec, "--namespace", namespace)
	}
	name := rbdImageSpec(pool, namespace, image)
	watchers, err := command(ctx, c.cliContainer(), append([]string{"rbd", "status"}, spec...)...)
	if err != nil {
		return nil, fmt.Errorf("read watchers of RBD image %s: %w", name, err)
	}
	locks, err := command(ctx, c.cliContainer(), append([]string{"rbd", "lock", "ls"}, spec...)...)
	if err != nil {
		return nil, fmt.Errorf("read locks of RBD image %s: %w", name, err)
	}
	return parseRBDImageClients(watchers, locks)
}

func rbdImageSpec(pool, namespace, image string) string {
	if namespace == "" {
		return pool + "/" + image
	}
	return pool + "/" + namespace + "/" + image
}

func parseRBDImageClients(watcherData, lockData []byte) (*RBDImageClientStatus, error) {
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
	status := &RBDImageClientStatus{Watchers: []RBDImageWatcher{}, Locks: []RBDImageLock{}}
	for _, watcher := range native.Watchers {
		if watcher.Address == "" || watcher.Client == 0 {
			return nil, errors.New("RBD image watcher lacks its client identity")
		}
		status.Watchers = append(status.Watchers, RBDImageWatcher{Address: watcher.Address, ClientID: watcher.Client, Cookie: watcher.Cookie})
	}
	for _, lock := range locks {
		if lock.ID == "" || lock.Locker == "" || lock.Address == "" {
			return nil, errors.New("RBD image lock lacks its owner identity")
		}
		status.Locks = append(status.Locks, RBDImageLock{ID: lock.ID, Locker: lock.Locker, Address: lock.Address, Managed: strings.HasPrefix(lock.ID, "auto ")})
	}
	return status, nil
}
