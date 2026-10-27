package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

// BlocklistEntry reports native OSDMap fencing, including ranges created by
// external tools. Until is the MON's absolute expiration time. A listing grants
// no ownership to remove an existing entry.
type BlocklistEntry struct {
	Address string
	Range   bool
	Until   time.Time
}

// observedBlocklistEntries implements Observer.BlocklistEntries. It takes the topology lock and refuses
// a cluster without pool policy readiness before it reads.
func (c *Container) observedBlocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return c.blocklistEntries(ctx)
}

func (c *Container) blocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	return readBlocklistEntries(ctx, c.cephCLI)
}

func decodeBlocklistEntries(data []byte) ([]BlocklistEntry, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	result := make([]BlocklistEntry, 0)
	seen := make(map[string]bool)
	// Ceph's formatter flushes two consecutive JSON arrays: exact entries,
	// then ranges. Treat neither array nor a decode failure as an empty list.
	for section := 0; ; section++ {
		var entries []struct{ Addr, Range, Until string }
		err := decoder.Decode(&entries)
		if errors.Is(err, io.EOF) && section > 0 {
			break
		}
		if err != nil || entries == nil || section > 1 {
			return nil, errors.New("decode native client blocklist arrays")
		}
		for _, raw := range entries {
			address := raw.Addr
			if (section == 0 && (address == "" || raw.Range != "")) || (section == 1 && (raw.Range == "" || address != "")) {
				return nil, errors.New("invalid native client blocklist entry scope")
			}
			if section == 0 {
				address, err = normalizeBlocklistSession(address, false)
				if err != nil {
					return nil, errors.New("invalid native client blocklist session address")
				}
			} else {
				address = raw.Range
			}
			until, err := time.Parse("2006-01-02T15:04:05.999999-0700", raw.Until)
			if err != nil {
				until, err = time.Parse(time.RFC3339Nano, raw.Until)
			}
			key := strconv.Itoa(section) + " " + address
			if err != nil || until.IsZero() || seen[key] {
				return nil, errors.New("invalid or duplicate native client blocklist expiration")
			}
			seen[key] = true
			result = append(result, BlocklistEntry{Address: address, Range: section == 1, Until: until})
		}
	}
	slices.SortFunc(result, func(a, b BlocklistEntry) int { return strings.Compare(a.Address, b.Address) })
	return result, nil
}

func normalizeBlocklistSession(address string, strict bool) (string, error) {
	if strings.HasPrefix(address, "[v1:") || strings.HasPrefix(address, "[v2:") {
		if !strings.HasSuffix(address, "]") {
			return "", invalid(errors.New("invalid native client address vector"))
		}
		var canonical string
		for _, component := range strings.Split(address[1:len(address)-1], ",") {
			value, err := normalizeBlocklistSession(component, strict)
			if err != nil || (canonical != "" && canonical != value) {
				return "", invalid(errors.New("select one exact IP/port/nonce from the native client address vector"))
			}
			canonical = value
		}
		return canonical, nil
	}
	address = strings.TrimPrefix(strings.TrimPrefix(address, "v1:"), "v2:")
	hostPort, nonceText, ok := strings.Cut(address, "/")
	host, portText, splitErr := net.SplitHostPort(hostPort)
	ip, ipErr := netip.ParseAddr(host)
	port, portErr := strconv.ParseUint(portText, 10, 16)
	nonce, nonceErr := strconv.ParseUint(nonceText, 10, 32)
	if !ok || splitErr != nil || ipErr != nil || ip.Zone() != "" || portErr != nil || nonceErr != nil || (strict && (nonce == 0 || ip.IsUnspecified() || ip.IsMulticast())) {
		return "", invalid(errors.New("client fencing requires an exact IP:port/nonzero-nonce session address"))
	}
	return net.JoinHostPort(ip.String(), strconv.FormatUint(port, 10)) + "/" + strconv.FormatUint(nonce, 10), nil
}
