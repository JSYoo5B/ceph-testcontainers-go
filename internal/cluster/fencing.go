package cluster

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

// BlocklistOverride owns one newly added exact client-session entry. Address
// includes a nonzero nonce, so two clients on one host remain distinct. Copies
// share restoration state. Removing the entry restores OSDMap admission; session
// recovery depends on the client library and callers may need to reconnect.
// External blocklist edits must not race helpers.
type BlocklistOverride struct {
	owner   *Container
	address string
	state   *blocklistOverrideState
}

type blocklistOverrideState struct {
	until           time.Time
	known, restored bool
}

func (change *BlocklistOverride) Address() string { return change.address }

// BlocklistEntries lists exact entries and CIDR ranges without mutating them.
func (c *Container) BlocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return c.blocklistEntries(ctx)
}

// TemporaryBlocklist fences a specific native session for a positive duration
// of at least one second. Use librados GetAddrs (or its equivalent) after connect.
// A v1/v2 address vector is accepted only when every entry denotes the same IP,
// port and nonce; otherwise select one address explicitly. Protocol types become
// native ANY entries. Bare IPs, CIDRs, nonce zero and existing entries are refused.
//
// The handle is retained on uncertain command/readback failure. Restore checks
// its captured expiration before removal, preserving outside renewal. An unknown
// readback cannot authorize removal of a present entry; inspect native state or
// terminate the disposable cluster. Natural expiry is reconciled as absence.
func (c *Container) TemporaryBlocklist(ctx context.Context, address string, duration time.Duration) (*BlocklistOverride, error) {
	address, err := normalizeBlocklistSession(address, true)
	if err != nil {
		return nil, err
	}
	if duration < time.Second {
		return nil, errors.New("client blocklist duration must be at least one second")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	if active := c.blocklistOverrides[address]; active != nil && !active.state.restored {
		return nil, errors.New("restore the existing client blocklist override first")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	entries, err := c.blocklistEntries(ctx)
	if err != nil {
		return nil, err
	}
	if findBlocklistEntry(entries, address) != nil {
		return nil, errors.New("client session already has a native blocklist entry; refusing to renew or adopt it")
	}
	change := &BlocklistOverride{owner: c, address: address, state: &blocklistOverrideState{}}
	if c.blocklistOverrides == nil {
		c.blocklistOverrides = make(map[string]*BlocklistOverride)
	}
	c.blocklistOverrides[address] = change
	_, setErr := c.Ceph(ctx, "osd", "blocklist", "add", address, strconv.FormatFloat(duration.Seconds(), 'f', 6, 64))
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer readCancel()
	entries, readErr := c.blocklistEntries(readCtx)
	if readErr == nil {
		if current := findBlocklistEntry(entries, address); current != nil {
			change.state.until, change.state.known = current.Until, true
		}
	}
	if setErr != nil || readErr != nil {
		return change, errors.Join(setErr, readErr)
	}
	if !change.state.known {
		return change, errors.New("client blocklist add returned success without a stored entry")
	}
	return change, nil
}

// Restore removes only this newly created exact entry. Prior native entries and
// ranges are preserved. A changed expiration is an outside edit and is refused.
// Lost removal replies can be retried; after confirmed absence copies are no-ops.
func (change *BlocklistOverride) Restore(ctx context.Context) error {
	if change == nil || change.owner == nil || change.state == nil {
		return errors.New("client blocklist override is unavailable")
	}
	c := change.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if change.state.restored {
		return nil
	}
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	if tracked := c.blocklistOverrides[change.address]; tracked == nil || tracked.state != change.state {
		return errors.New("client blocklist override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	entries, err := c.blocklistEntries(ctx)
	if err != nil {
		return err
	}
	current := findBlocklistEntry(entries, change.address)
	if current == nil {
		change.finish()
		return nil
	}
	if !change.state.known {
		return errors.New("client blocklist apply readback was unavailable; inspect native state or terminate the cluster")
	}
	if !current.Until.Equal(change.state.until) {
		return errors.New("client blocklist entry changed outside this override; refusing removal")
	}
	if _, err := c.Ceph(ctx, "osd", "blocklist", "rm", change.address); err != nil {
		return err
	}
	entries, err = c.blocklistEntries(ctx)
	if err != nil {
		return err
	}
	if findBlocklistEntry(entries, change.address) != nil {
		return errors.New("client blocklist removal readback still contains the entry")
	}
	change.finish()
	return nil
}

func (change *BlocklistOverride) finish() {
	change.state.restored = true
	delete(change.owner.blocklistOverrides, change.address)
}

func findBlocklistEntry(entries []BlocklistEntry, address string) *BlocklistEntry {
	for _, entry := range entries {
		if !entry.Range && entry.Address == address {
			return &entry
		}
	}
	return nil
}

func (c *Container) blocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	data, err := c.Ceph(ctx, "osd", "blocklist", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	return decodeBlocklistEntries(data)
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
			return "", errors.New("invalid native client address vector")
		}
		var canonical string
		for _, component := range strings.Split(address[1:len(address)-1], ",") {
			value, err := normalizeBlocklistSession(component, strict)
			if err != nil || (canonical != "" && canonical != value) {
				return "", errors.New("select one exact IP/port/nonce from the native client address vector")
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
		return "", errors.New("client fencing requires an exact IP:port/nonzero-nonce session address")
	}
	return net.JoinHostPort(ip.String(), strconv.FormatUint(port, 10)) + "/" + strconv.FormatUint(nonce, 10), nil
}
