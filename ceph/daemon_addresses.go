package ceph

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"slices"
)

// EntityAddress is one entry of a Ceph address vector, as a cluster map
// advertises it.
type EntityAddress struct {
	// Type is the protocol: "v2" for msgr2, "v1" for the legacy protocol, or
	// "any".
	Type string
	// Addr is the IP address and port as Ceph prints it, such as
	// "172.20.0.3:6800" or "[fd00::3]:6800".
	Addr string
	// Nonce tells apart daemon instances that reuse an address. MONs use 0.
	Nonce uint32
}

// DaemonAddress is the name of one MON or MGR and the public address vector
// that its cluster map advertises.
type DaemonAddress struct {
	Name  string
	Addrs []EntityAddress
}

// OSDAddress is one OSD's entry in the OSDMap. The address vectors are those
// of the OSD's last boot; a down OSD keeps them.
type OSDAddress struct {
	ID     int
	Up, In bool
	// Public serves clients; Cluster carries replication and recovery.
	Public, Cluster []EntityAddress
	// HeartbeatFront and HeartbeatBack carry OSD heartbeats on the public and
	// cluster sides.
	HeartbeatFront, HeartbeatBack []EntityAddress
}

// DaemonAddresses lists the daemons of a cluster and the addresses that the
// MON, MGR and OSD maps advertise, with the epoch of each map. The three maps
// are read one after another, not as one snapshot.
type DaemonAddresses struct {
	MonMapEpoch int
	// Monitors are the monmap members in rank order.
	Monitors    []DaemonAddress
	MgrMapEpoch int
	// ActiveManager is nil when no MGR is active. A MGR that has just become
	// active advertises its addresses only when it is available; until then
	// ManagerAvailable is false and its address vector can be empty. Standby
	// MGRs advertise no address in the MGRMap, so StandbyManagers lists only
	// their names.
	ActiveManager    *DaemonAddress
	ManagerAvailable bool
	StandbyManagers  []string
	OSDMapEpoch      int
	// OSDs are the OSDMap entries in ID order.
	OSDs []OSDAddress
}

type nativeAddressVector struct {
	Addrvec *[]struct {
		Type  *string `json:"type"`
		Addr  *string `json:"addr"`
		Nonce *uint32 `json:"nonce"`
	} `json:"addrvec"`
}

func (v nativeAddressVector) entities() ([]EntityAddress, error) {
	if v.Addrvec == nil {
		return nil, errors.New("address vector is missing")
	}
	result := make([]EntityAddress, 0, len(*v.Addrvec))
	for _, entry := range *v.Addrvec {
		if entry.Type == nil || entry.Addr == nil || entry.Nonce == nil {
			return nil, errors.New("address vector entry is incomplete")
		}
		if !slices.Contains([]string{"v1", "v2", "any"}, *entry.Type) {
			return nil, fmt.Errorf("address vector entry has unknown type %q", *entry.Type)
		}
		if _, err := netip.ParseAddrPort(*entry.Addr); err != nil {
			return nil, fmt.Errorf("address vector entry has invalid address %q", *entry.Addr)
		}
		result = append(result, EntityAddress{Type: *entry.Type, Addr: *entry.Addr, Nonce: *entry.Nonce})
	}
	return result, nil
}

func parseMonitorAddresses(data []byte, result *DaemonAddresses) error {
	var dump struct {
		Epoch *int `json:"epoch"`
		Mons  *[]struct {
			Rank        *int                `json:"rank"`
			Name        *string             `json:"name"`
			PublicAddrs nativeAddressVector `json:"public_addrs"`
		} `json:"mons"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Epoch == nil || dump.Mons == nil {
		return errors.New("decode monmap: missing epoch or members")
	}
	result.MonMapEpoch = *dump.Epoch
	for i, mon := range *dump.Mons {
		if mon.Rank == nil || *mon.Rank != i || mon.Name == nil || *mon.Name == "" {
			return errors.New("decode monmap: members are not in rank order or unnamed")
		}
		addrs, err := mon.PublicAddrs.entities()
		if err != nil {
			return fmt.Errorf("decode monmap member %s: %w", *mon.Name, err)
		}
		result.Monitors = append(result.Monitors, DaemonAddress{Name: *mon.Name, Addrs: addrs})
	}
	return nil
}

func parseManagerAddresses(data []byte, result *DaemonAddresses) error {
	var dump struct {
		Epoch       *int                `json:"epoch"`
		ActiveName  *string             `json:"active_name"`
		ActiveAddrs nativeAddressVector `json:"active_addrs"`
		Available   *bool               `json:"available"`
		Standbys    *[]struct {
			Name *string `json:"name"`
		} `json:"standbys"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Epoch == nil || dump.ActiveName == nil || dump.Available == nil || dump.Standbys == nil {
		return errors.New("decode mgrmap: missing epoch, active name, availability or standbys")
	}
	result.MgrMapEpoch, result.ManagerAvailable = *dump.Epoch, *dump.Available
	if *dump.ActiveName != "" {
		addrs, err := dump.ActiveAddrs.entities()
		if err != nil {
			return fmt.Errorf("decode active MGR %s: %w", *dump.ActiveName, err)
		}
		result.ActiveManager = &DaemonAddress{Name: *dump.ActiveName, Addrs: addrs}
	}
	for _, standby := range *dump.Standbys {
		if standby.Name == nil || *standby.Name == "" {
			return errors.New("decode mgrmap: unnamed standby")
		}
		result.StandbyManagers = append(result.StandbyManagers, *standby.Name)
	}
	return nil
}

func parseOSDAddresses(data []byte, result *DaemonAddresses) error {
	var dump struct {
		Epoch *int `json:"epoch"`
		OSDs  *[]struct {
			ID             *int                `json:"osd"`
			Up             *int                `json:"up"`
			In             *int                `json:"in"`
			Public         nativeAddressVector `json:"public_addrs"`
			Cluster        nativeAddressVector `json:"cluster_addrs"`
			HeartbeatFront nativeAddressVector `json:"heartbeat_front_addrs"`
			HeartbeatBack  nativeAddressVector `json:"heartbeat_back_addrs"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Epoch == nil || dump.OSDs == nil {
		return errors.New("decode osdmap: missing epoch or OSDs")
	}
	result.OSDMapEpoch = *dump.Epoch
	for _, osd := range *dump.OSDs {
		if osd.ID == nil || *osd.ID < 0 || osd.Up == nil || osd.In == nil {
			return errors.New("decode osdmap: OSD entry is incomplete")
		}
		entry := OSDAddress{ID: *osd.ID, Up: *osd.Up == 1, In: *osd.In == 1}
		for _, vector := range []struct {
			native nativeAddressVector
			target *[]EntityAddress
		}{{osd.Public, &entry.Public}, {osd.Cluster, &entry.Cluster}, {osd.HeartbeatFront, &entry.HeartbeatFront}, {osd.HeartbeatBack, &entry.HeartbeatBack}} {
			addrs, err := vector.native.entities()
			if err != nil {
				return fmt.Errorf("decode osd.%d: %w", entry.ID, err)
			}
			*vector.target = addrs
		}
		result.OSDs = append(result.OSDs, entry)
	}
	slices.SortFunc(result.OSDs, func(a, b OSDAddress) int { return a.ID - b.ID })
	return nil
}
