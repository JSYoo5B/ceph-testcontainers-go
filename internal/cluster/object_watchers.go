package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ObjectWatcher is one client watching a RADOS object. Address is the
// client's IP:port/nonce, the same form BlocklistEntries reports. A client
// that stops answering keeps its watch until the OSD's
// osd_client_watch_timeout expires, 30 seconds by default; until then a
// notify to the object waits for that client and times out.
type ObjectWatcher struct {
	Address  string
	ClientID uint64
	Cookie   uint64
}

// ObjectWatchers lists the clients watching one object, including clients
// outside this fixture. Namespace is empty for the default namespace. It
// reads native state only and grants no ownership.
func (c *Container) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ObjectWatcher, error) {
	if err := validateExistingPoolName(pool); err != nil {
		return nil, err
	}
	if err := validateRADOSArgument("namespace", namespace, true); err != nil {
		return nil, err
	}
	if err := validateRADOSArgument("object", object, false); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	args := []string{"rados", "--pool", pool}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	// rados ignores --format for listwatchers and always prints text.
	data, err := command(ctx, c.cliContainer(), append(args, "listwatchers", object)...)
	if err != nil {
		return nil, fmt.Errorf("list watchers of object %q in pool %q: %w", object, pool, err)
	}
	return parseObjectWatchers(string(data))
}

// validateRADOSArgument keeps a name from being read as a rados option or
// from splitting the line-oriented output.
func validateRADOSArgument(kind, value string, optional bool) error {
	if value == "" {
		if optional {
			return nil
		}
		return fmt.Errorf("RADOS %s name is empty", kind)
	}
	if strings.HasPrefix(value, "-") || len(value) > 1024 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("RADOS %s name must not start with a dash or contain control characters", kind)
	}
	return nil
}

// parseObjectWatchers reads lines of the form
// "watcher=172.19.0.6:0/3461995546 client.4193 cookie=187650986454688".
func parseObjectWatchers(output string) ([]ObjectWatcher, error) {
	watchers := []ObjectWatcher{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 || !strings.HasPrefix(fields[0], "watcher=") || !strings.HasPrefix(fields[1], "client.") || !strings.HasPrefix(fields[2], "cookie=") {
			return nil, errors.New("decode RADOS object watchers: unexpected line")
		}
		address := strings.TrimPrefix(fields[0], "watcher=")
		id, idErr := strconv.ParseUint(strings.TrimPrefix(fields[1], "client."), 10, 64)
		cookie, cookieErr := strconv.ParseUint(strings.TrimPrefix(fields[2], "cookie="), 10, 64)
		if address == "" || idErr != nil || cookieErr != nil || id == 0 {
			return nil, errors.New("decode RADOS object watchers: invalid watcher identity")
		}
		watchers = append(watchers, ObjectWatcher{Address: address, ClientID: id, Cookie: cookie})
	}
	return watchers, nil
}
