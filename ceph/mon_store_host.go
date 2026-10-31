package ceph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// This file is the host-network half of what a MON store generation bound
// (mon_store_address.go). On a bridge Docker reports the container's address;
// on the host network the container has no address of its own, and the ports
// it was created for only say what it was told. What it bound is read from the
// kernel instead: the listening TCP sockets of its ceph-mon process.

// monitorListener is one listening TCP socket of a ceph-mon process.
type monitorListener struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// monitorListenersScript runs in a host-network MON container. It finds the
// ceph-mon process, collects the inodes of its sockets and prints, as one JSON
// list, the local address and port of every listening TCP socket among them.
// The container shares the host's network namespace, so /proc/net lists the
// sockets of the namespace ceph-mon bound in.
const monitorListenersScript = `import json, os, socket
inodes = set()
for pid in os.listdir("/proc"):
    if not pid.isdigit():
        continue
    try:
        with open("/proc/%s/comm" % pid) as f:
            if f.read().strip() != "ceph-mon":
                continue
        for fd in os.listdir("/proc/%s/fd" % pid):
            target = os.readlink("/proc/%s/fd/%s" % (pid, fd))
            if target.startswith("socket:["):
                inodes.add(target[8:-1])
    except OSError:
        pass
if not inodes:
    raise SystemExit("no ceph-mon process with sockets")
out = []
for table, family in (("tcp", socket.AF_INET), ("tcp6", socket.AF_INET6)):
    try:
        with open("/proc/net/" + table) as f:
            lines = f.read().splitlines()[1:]
    except OSError:
        continue
    for line in lines:
        fields = line.split()
        if fields[3] != "0A" or fields[9] not in inodes:
            continue
        address, port = fields[1].split(":")
        raw = bytes.fromhex(address)
        raw = b"".join(raw[i:i + 4][::-1] for i in range(0, len(raw), 4))
        out.append({"ip": socket.inet_ntop(family, raw), "port": int(port, 16)})
print(json.dumps(out))`

// monitorListeners runs monitorListenersScript in ctr.
func monitorListeners(ctx context.Context, ctr testcontainers.Container) ([]monitorListener, error) {
	out, err := command(ctx, ctr, "python3", "-I", "-c", monitorListenersScript)
	if err != nil {
		return nil, err
	}
	var listeners []monitorListener
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out))), &listeners); err != nil {
		return nil, fmt.Errorf("decode the listeners of ceph-mon: %w", err)
	}
	return listeners, nil
}

// observeHost returns the endpoint of a host-network generation: the public
// address and the ports it was created for, in that order, once its ceph-mon
// listens on exactly those ports of that address and on nothing else.
func (g *monitorGenerations) observeHost(ctx context.Context, id string) (monStoreEndpoint, error) {
	ctr, err := g.container(id)
	if err != nil {
		return monStoreEndpoint{}, err
	}
	ports := g.ports[id]
	if len(ports) == 0 {
		return monStoreEndpoint{}, fmt.Errorf("MON %s container %.12s has no recorded host ports", g.name, id)
	}
	listeners, err := g.listening(ctx, ctr)
	if err != nil {
		return monStoreEndpoint{}, fmt.Errorf("read the listeners of MON %s: %w", g.name, err)
	}
	address := g.c.staticFacts().PublicAddress
	if err := matchMonitorListeners(listeners, address, ports); err != nil {
		return monStoreEndpoint{}, fmt.Errorf("MON %s: %w", g.name, err)
	}
	return monStoreEndpoint{ip: address, ports: slices.Clone(ports)}, nil
}

// matchMonitorListeners requires listeners to be exactly one socket on address
// for each of ports.
func matchMonitorListeners(listeners []monitorListener, address string, ports []int) error {
	want, err := netip.ParseAddr(address)
	if err != nil {
		return fmt.Errorf("public address %q: %w", address, err)
	}
	var bound []int
	for _, l := range listeners {
		ip, err := netip.ParseAddr(l.IP)
		if err != nil || ip.Unmap() != want.Unmap() {
			return fmt.Errorf("ceph-mon listens on %s:%d, not on the public address %s", l.IP, l.Port, address)
		}
		bound = append(bound, l.Port)
	}
	slices.Sort(bound)
	expected := slices.Sorted(slices.Values(ports))
	if !slices.Equal(bound, expected) {
		return fmt.Errorf("ceph-mon listens on ports %v of %s, created for %v", bound, address, ports)
	}
	return nil
}
