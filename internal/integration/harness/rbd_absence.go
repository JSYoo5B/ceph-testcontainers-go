package harness

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
)

// RBDRequireSnapshotAbsent checks every name in a native snap ls array.
func RBDRequireSnapshotAbsent(data []byte, name string) error {
	var snapshots []struct{ Name string }
	if err := json.Unmarshal(data, &snapshots); err != nil {
		return err
	}
	if snapshots == nil {
		return errors.New("RBD snapshot listing is missing its array")
	}
	for _, snapshot := range snapshots {
		if snapshot.Name == "" {
			return errors.New("RBD snapshot listing contains an unnamed snapshot")
		}
		if snapshot.Name == name {
			return fmt.Errorf("snapshot %q is still present", name)
		}
	}
	return nil
}

// RBDRequireNoPeers checks default-namespace mirror pool info. Native disabled
// pools omit peers; enabled pools report an explicit array, including when empty.
func RBDRequireNoPeers(data []byte) error {
	var info struct {
		Mode  string
		Peers json.RawMessage
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	switch info.Mode {
	case "disabled":
		if info.Peers == nil {
			return nil
		}
	case "image", "pool", "init-only":
	default:
		return fmt.Errorf("RBD pool info has missing or unknown mode %q", info.Mode)
	}
	var peers []json.RawMessage
	if err := json.Unmarshal(info.Peers, &peers); err != nil {
		return err
	}
	if peers == nil {
		return errors.New("RBD pool info is missing its peers array")
	}
	if len(peers) != 0 {
		return fmt.Errorf("RBD peer removal left peers: %s", info.Peers)
	}
	return nil
}

// RBDRequireUnmirrored checks the requested image identity before accepting
// an omitted mirroring field or an explicitly disabled mirroring state.
func RBDRequireUnmirrored(data []byte, image string) error {
	var info struct {
		Name, ID  string
		Mirroring json.RawMessage
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	if info.Name != path.Base(image) || info.ID == "" {
		return fmt.Errorf("RBD image %q lacks its expected name and ID", image)
	}
	if info.Mirroring == nil {
		return nil
	}
	var mirroring struct{ State string }
	if bytes.Equal(bytes.TrimSpace(info.Mirroring), []byte("null")) {
		return errors.New("RBD image mirroring is null")
	}
	if err := json.Unmarshal(info.Mirroring, &mirroring); err != nil {
		return err
	}
	if mirroring.State != "disabled" {
		return fmt.Errorf("RBD image %q mirroring state is %q", image, mirroring.State)
	}
	return nil
}
