package ceph

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
)

// checkDualStackAddresses admits an AddressDualStack cluster only when the
// native maps advertise reachable addresses of both families. Ceph's MGR
// seeds its address vector from one scalar address and can advertise the
// IPv6 wildcard [::] instead of its own address; such a MGR accepts no
// command from another container. Other families are not checked. The check
// waits for an available MGR and then reads the maps once: a wildcard does
// not resolve itself. Without an active MGR, as before the first AddManager,
// only MON and OSD vectors are checked.
func (c *Container) checkDualStackAddresses(ctx context.Context) error {
	if c.settings.addressFamily != AddressDualStack {
		return nil
	}
	var addresses DaemonAddresses
	if err := c.poll(ctx, func() (bool, error) {
		current, err := readDaemonAddresses(ctx, c.cephCLI)
		if err != nil {
			return false, err
		}
		addresses = current
		if current.ActiveManager == nil {
			return true, nil
		}
		return current.ManagerAvailable && len(current.ActiveManager.Addrs) != 0, nil
	}); err != nil {
		return fmt.Errorf("read dual-stack daemon addresses: %w", err)
	}
	if err := dualStackAddressProblem(addresses); err != nil {
		return &OperationError{Kind: ErrUnsupported, Op: "admit dual-stack addresses", Target: "cluster",
			Version: c.staticFacts().CephVersion, Image: c.startup.firstMonitorImage(), Capability: "concrete dual-stack Messenger addresses", Err: err}
	}
	return nil
}

// dualStackAddressProblem reports the first daemon whose advertised vector
// lacks a concrete IPv4 or IPv6 address for one of its protocols or holds an
// unspecified address. It checks every MON, the active MGR when it is
// available, and the four vectors of every up OSD.
func dualStackAddressProblem(addresses DaemonAddresses) error {
	var problems []error
	for _, mon := range addresses.Monitors {
		problems = append(problems, dualStackVectorProblem("mon."+mon.Name, mon.Addrs))
	}
	if mgr := addresses.ActiveManager; mgr != nil && addresses.ManagerAvailable {
		problems = append(problems, dualStackVectorProblem("mgr."+mgr.Name, mgr.Addrs))
	}
	for _, osd := range addresses.OSDs {
		if !osd.Up {
			continue
		}
		for _, plane := range []struct {
			name   string
			vector []EntityAddress
		}{{"public", osd.Public}, {"cluster", osd.Cluster}, {"heartbeat front", osd.HeartbeatFront}, {"heartbeat back", osd.HeartbeatBack}} {
			problems = append(problems, dualStackVectorProblem(fmt.Sprintf("osd.%d %s", osd.ID, plane.name), plane.vector))
		}
	}
	return errors.Join(problems...)
}

// dualStackVectorProblem checks that vector has, for each protocol it
// lists, exactly one concrete IPv4 and one concrete IPv6 address, and no
// unspecified, IPv4-mapped or link-local address.
func dualStackVectorProblem(daemon string, vector []EntityAddress) error {
	if len(vector) == 0 {
		return fmt.Errorf("%s advertises no address", daemon)
	}
	families := make(map[string][2]int)
	var order []string
	for _, entry := range vector {
		port, err := netip.ParseAddrPort(entry.Addr)
		if err != nil {
			return fmt.Errorf("%s advertises the invalid address %s", daemon, entry.Addr)
		}
		ip := port.Addr()
		if ip.IsUnspecified() || ip.Is4In6() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsLoopback() {
			return fmt.Errorf("%s advertises %s:%s, which no other container can reach; vector %s", daemon, entry.Type, entry.Addr, vectorText(vector))
		}
		count, seen := families[entry.Type]
		if !seen {
			order = append(order, entry.Type)
		}
		if ip.Is4() {
			count[0]++
		} else {
			count[1]++
		}
		families[entry.Type] = count
	}
	for _, protocol := range order {
		if count := families[protocol]; count != [2]int{1, 1} {
			return fmt.Errorf("%s advertises %d IPv4 and %d IPv6 %s addresses, want one of each; vector %s", daemon, count[0], count[1], protocol, vectorText(vector))
		}
	}
	return nil
}

func vectorText(vector []EntityAddress) string {
	entries := make([]string, len(vector))
	for i, entry := range vector {
		entries[i] = entry.Type + ":" + entry.Addr
	}
	return "[" + strings.Join(entries, ",") + "]"
}
