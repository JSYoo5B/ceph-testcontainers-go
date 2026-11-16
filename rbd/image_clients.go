package rbd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/internal/lockcookie"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd/internal/resourcename"
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
	// ID is the lock ID; for a managed lock it is "auto <watch cookie>".
	ID string
	// Locker is the owner's entity name, such as client.4235.
	Locker string
	// Address is the owner's IP:port/nonce, the form that
	// override.AddBlocklist takes.
	Address string
	// Managed reports an exclusive header lock with the librbd internal tag
	// and an encoded watch cookie. Advisory IDs may use the same spelling.
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
//
//purpose:observation
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
//
//purpose:observation
func ImageClients(ctx context.Context, c *ceph.Container, pool, namespace, image string) (*ImageClientStatus, error) {
	if c == nil {
		return nil, errNilCluster
	}
	return imageClients(ctx, c, pool, namespace, image)
}

func imageClients(ctx context.Context, c *ceph.Container, pool, namespace, image string) (*ImageClientStatus, error) {
	if err := resourcename.Check("pool", pool); err != nil {
		return nil, servicehook.Invalid(err)
	}
	if namespace != "" {
		if err := resourcename.Check("namespace", namespace); err != nil {
			return nil, servicehook.Invalid(err)
		}
	}
	if err := resourcename.Check("image", image); err != nil {
		return nil, servicehook.Invalid(err)
	}
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return nil, err
	}
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
	info, err := servicehook.ClientCommand(ctx, c, append([]string{"rbd", "info"}, spec...)...)
	if err != nil {
		return nil, fmt.Errorf("read header of RBD image %s: %w", name, err)
	}
	header, err := imageLockHeader(info, image)
	if err != nil {
		return nil, fmt.Errorf("identify header of RBD image %s: %w", name, err)
	}
	args := []string{"rados", "--pool", pool, "--format", "json"}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	locks, err := servicehook.ClientCommand(ctx, c, append(args, "lock", "info", header, "rbd_lock")...)
	if err != nil {
		return nil, fmt.Errorf("read locks of RBD image %s: %w", name, err)
	}
	return parseImageClients(watchers, locks)
}

func imageLockHeader(data []byte, image string) (string, error) {
	var info struct {
		Format int    `json:"format"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return "", err
	}
	switch info.Format {
	case 1:
		return image + ".rbd", nil
	case 2:
		if info.ID != "" {
			return "rbd_header." + info.ID, nil
		}
	}
	return "", errors.New("missing or unsupported RBD image header identity")
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
	var locks struct {
		Name    string  `json:"name"`
		Type    string  `json:"type"`
		Tag     *string `json:"tag"`
		Lockers []struct {
			ID      string `json:"cookie"`
			Locker  string `json:"name"`
			Address string `json:"addr"`
		} `json:"lockers"`
	}
	if err := json.Unmarshal(lockData, &locks); err != nil || locks.Name != "rbd_lock" || locks.Tag == nil || locks.Lockers == nil {
		return nil, errors.New("decode RBD header lock")
	}
	switch locks.Type {
	case "exclusive", "exclusive-ephemeral":
		if len(locks.Lockers) > 1 {
			return nil, errors.New("exclusive RBD header lock has multiple owners")
		}
	case "none":
		if len(locks.Lockers) != 0 {
			return nil, errors.New("unlocked RBD header has lockers")
		}
	case "shared":
	default:
		return nil, fmt.Errorf("unknown RBD header lock type %q", locks.Type)
	}
	status := &ImageClientStatus{Watchers: []ImageWatcher{}, Locks: []ImageLock{}}
	for _, watcher := range native.Watchers {
		if watcher.Address == "" || watcher.Client == 0 {
			return nil, errors.New("RBD image watcher lacks its client identity")
		}
		status.Watchers = append(status.Watchers, ImageWatcher{Address: watcher.Address, ClientID: watcher.Client, Cookie: watcher.Cookie})
	}
	for _, lock := range locks.Lockers {
		if lock.ID == "" || lock.Locker == "" || lock.Address == "" {
			return nil, errors.New("RBD image lock lacks its owner identity")
		}
		status.Locks = append(status.Locks, ImageLock{ID: lock.ID, Locker: lock.Locker, Address: lock.Address, Managed: locks.Type == "exclusive" && *locks.Tag == "internal" && lockcookie.Valid(lock.ID)})
	}
	return status, nil
}
