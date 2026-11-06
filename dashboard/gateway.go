package dashboard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// GatewayAdminCaps are the RGW Admin Ops capabilities of the identity that
// AddGateway creates for the Dashboard: metadata=read lets the Dashboard
// open its connection to the gateway, users=read and buckets=read let it
// list and show users and buckets. Dashboard pages that change RGW state, or
// that read other RGW state, are refused by RGW with this identity.
const GatewayAdminCaps = "users=read;buckets=read;metadata=read"

// Errors of GatewayLink.Check that attribute a failed RGW backend. Both come
// from the Dashboard's own answer while the Dashboard itself answered the
// owned account, so they are not a failure of the Dashboard's readiness.
var (
	// ErrGatewayRefused reports that the gateway answered the Dashboard and
	// refused its RGW identity, such as a wrong key, a suspended or removed
	// identity, or missing capabilities.
	ErrGatewayRefused = errors.New("the gateway refused the dashboard's RGW identity")
	// ErrGatewayUnreachable reports that the Dashboard could not reach the
	// gateway: a refused or timed-out connection, or a request that did not
	// finish within the probe's time.
	ErrGatewayUnreachable = errors.New("the dashboard cannot reach the gateway")
)

// GatewayLink is the RGW backend of a Dashboard that AddGateway wired to one
// owned gateway. Copies share its state. Formatting it prints only the RGW
// identity's ID.
type GatewayLink struct {
	*linkState
}

type linkState struct {
	mu        sync.Mutex
	dashboard *state
	gateway   *rgw.Gateway
	user      *rgw.User
	keys      []*rgwKeyOption
	removed   bool
}

// String names the link by its RGW identity.
func (l GatewayLink) String() string {
	if l.linkState == nil || l.user == nil {
		return "Dashboard RGW link"
	}
	return "Dashboard RGW link of " + l.user.ID()
}

// GoString formats the link like String.
func (l GatewayLink) GoString() string { return l.String() }

// AddGateway makes the Dashboard manage gateway, an owned gateway of the
// Dashboard's cluster that is the only RGW daemon in the cluster's service
// map. It creates a dedicated RGW user with GatewayAdminCaps through
// gateway, stores its key as the Dashboard's RGW API access and secret key
// (cluster-wide manager options, so every manager uses them after a
// failover), and returns once Check passes.
//
// The Dashboard selects its gateway from the service map, not from a URL, so
// a cluster with more than one RGW daemon is refused before any change. It
// connects to the first frontend port the gateway advertises there, the plain
// HTTP listener that every fixture gateway serves, also one with TLS; the
// Dashboard's RGW TLS verification option is therefore not involved and not
// changed. A Dashboard has at most one link; the SUT's own RGW credentials
// are never used.
//
// An AddGateway that fails after creating the user returns the link together
// with the error: its Remove undoes what AddGateway changed, and so does
// Dashboard.Restore.
//
//purpose:construction,condition
func (d *Dashboard) AddGateway(ctx context.Context, gateway *rgw.Gateway) (*GatewayLink, error) {
	const op = "dashboard.AddGateway"
	if d == nil || d.state == nil || gateway == nil {
		return nil, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the dashboard and the gateway are required")}
	}
	s := d.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restored {
		return nil, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the dashboard was restored")}
	}
	if s.link != nil {
		return nil, &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: op, Err: errors.New("the dashboard already has a gateway link")}
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(s.cluster))
	defer cancel()
	if err := checkGateway(ctx, s.cluster, gateway); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return nil, err
	}
	user, err := gateway.CreateUser(ctx, rgw.UserConfig{ID: "tc-dashboard-" + hex.EncodeToString(suffix), DisplayName: "Dashboard fixture", AdminCaps: GatewayAdminCaps})
	if user == nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	link := &GatewayLink{&linkState{dashboard: s, gateway: gateway, user: user}}
	s.link = link
	if err != nil {
		return link, fmt.Errorf("%s: %w", op, err)
	}
	access, secret, err := user.CreationKey().Credentials()
	if err != nil {
		return link, fmt.Errorf("%s: %w", op, err)
	}
	for _, key := range []*rgwKeyOption{{option: "RGW_API_ACCESS_KEY", command: "rgw-api-access-key"}, {option: "RGW_API_SECRET_KEY", command: "rgw-api-secret-key"}} {
		value := access
		if key.option == "RGW_API_SECRET_KEY" {
			value = secret
		}
		link.keys = append(link.keys, key)
		if err := key.install(ctx, s.cluster, value); err != nil {
			return link, fmt.Errorf("%s: %w", op, err)
		}
	}
	var last error
	for {
		if last = link.check(ctx); last == nil {
			return link, nil
		}
		select {
		case <-ctx.Done():
			return link, fmt.Errorf("%s: %w", op, errors.Join(ctx.Err(), last))
		case <-time.After(2 * time.Second):
		}
	}
}

// checkGateway requires an owned gateway of c that is the only RGW daemon in
// the service map.
func checkGateway(ctx context.Context, c *ceph.Container, gateway *rgw.Gateway) error {
	gateways, err := rgw.Gateways(ctx, c)
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(gateways, func(owned *rgw.Gateway) bool {
		return owned.GatewayName == gateway.GatewayName && owned.GetContainerID() == gateway.GetContainerID()
	}) {
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: errors.New("the gateway is not an owned gateway of the dashboard's cluster")}
	}
	daemons, err := rgwDaemons(ctx, c)
	if err != nil {
		return err
	}
	if len(daemons) != 1 {
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Err: fmt.Errorf("the service map has %d RGW daemons; the dashboard's gateway selection needs exactly one", len(daemons))}
	}
	return nil
}

// rgwDaemons returns the service map's RGW daemons by their GID.
func rgwDaemons(ctx context.Context, c *ceph.Container) ([]string, error) {
	data, err := c.Ceph(ctx, "service", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	var dump struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.Services == nil {
		return nil, errors.New("decode service map")
	}
	var gids []string
	for name := range dump.Services["rgw"].Daemons {
		if name != "summary" {
			gids = append(gids, name)
		}
	}
	slices.Sort(gids)
	return gids, nil
}

// User returns the dedicated RGW identity that the Dashboard uses. A test
// suspends it to make the gateway refuse the Dashboard; it is removed by
// Remove and must not be removed otherwise.
//
//purpose:observation
func (l *GatewayLink) User() *rgw.User {
	if l == nil || l.linkState == nil {
		return nil
	}
	return l.user
}

// Check asks the Dashboard, with the owned account, for its RGW daemons and
// users, and returns nil only when the Dashboard reports exactly the linked
// gateway, identified by its service map GID and port, and lists the link's
// own RGW user. An empty or partial answer is an error, never a pass.
//
// It distinguishes the backend from the Dashboard: when the Dashboard itself
// does not pass the readiness check of Endpoint, that is the error; when it
// does and the gateway refuses the link's identity the error wraps
// ErrGatewayRefused, and when the Dashboard cannot reach the gateway it wraps
// ErrGatewayUnreachable. It makes one attempt.
//
//purpose:observation
func (l *GatewayLink) Check(ctx context.Context) error {
	if l == nil || l.linkState == nil {
		return errors.New("dashboard gateway link is unavailable")
	}
	l.mu.Lock()
	removed := l.removed
	l.mu.Unlock()
	if removed {
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: "dashboard gateway link", Err: errors.New("the link was removed")}
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(l.dashboard.cluster))
	defer cancel()
	return l.check(ctx)
}

const (
	rgwDaemonPath = "/api/rgw/daemon"
	rgwUserPath   = "/api/rgw/user"
)

func (l *GatewayLink) check(ctx context.Context) error {
	observer, err := ceph.Observe(l.dashboard.cluster)
	if err != nil {
		return err
	}
	_, report, err := l.dashboard.probeActive(ctx, observer, rgwDaemonPath, rgwUserPath)
	if err != nil {
		return err
	}
	// The Dashboard's answers are judged first: with the gateway stopped,
	// the gateway's own address is gone too.
	daemon, _ := report.result(rgwDaemonPath)
	if err := backendError(daemon); err != nil {
		return err
	}
	users, _ := report.result(rgwUserPath)
	if err := backendError(users); err != nil {
		return err
	}
	var listed []struct {
		ServiceMapID string `json:"service_map_id"`
		Port         int    `json:"port"`
	}
	if err := json.Unmarshal([]byte(daemon.Body), &listed); err != nil {
		return fmt.Errorf("decode dashboard %s", rgwDaemonPath)
	}
	gids, err := rgwDaemons(ctx, l.dashboard.cluster)
	if err != nil {
		return err
	}
	endpoint, err := l.gateway.DaemonEndpoint(ctx)
	if err != nil {
		return err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		return fmt.Errorf("gateway endpoint has no port")
	}
	if len(gids) != 1 || len(listed) != 1 || listed[0].ServiceMapID != gids[0] || listed[0].Port != port {
		return fmt.Errorf("dashboard reports RGW daemons %+v, the service map has %v and the gateway listens on port %d", listed, gids, port)
	}
	var ids []string
	if err := json.Unmarshal([]byte(users.Body), &ids); err != nil {
		return fmt.Errorf("decode dashboard %s", rgwUserPath)
	}
	if !slices.Contains(ids, l.user.ID()) {
		return fmt.Errorf("dashboard lists %d RGW users without the link's own user", len(ids))
	}
	return nil
}

// backendError classifies a Dashboard answer about its RGW backend. The
// Dashboard reports a backend failure in the body of a 4xx or 5xx answer,
// quoting the gateway's error code, and a request that does not finish while
// the gateway is unreachable times out.
func backendError(result probeResult) error {
	switch {
	case result.Error == "timeout":
		return fmt.Errorf("%w: the dashboard did not answer %s within %s", ErrGatewayUnreachable, result.Path, probeTimeout)
	case result.Error != "":
		return fmt.Errorf("dashboard %s: %s", result.Path, result.Error)
	case result.Status == 200:
		return nil
	}
	var answer struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal([]byte(result.Body), &answer)
	detail := answer.Detail
	if len(detail) > 300 {
		detail = detail[:300]
	}
	for _, code := range []string{"AccessDenied", "SignatureDoesNotMatch", "InvalidAccessKeyId", "UserSuspended"} {
		if strings.Contains(detail, code) {
			return fmt.Errorf("%w: dashboard %s answered HTTP %d with %s", ErrGatewayRefused, result.Path, result.Status, code)
		}
	}
	for _, marker := range []string{"Connection refused", "Max retries exceeded", "timed out", "No route to host", "Failed to establish"} {
		if strings.Contains(detail, marker) {
			return fmt.Errorf("%w: dashboard %s answered HTTP %d: %s", ErrGatewayUnreachable, result.Path, result.Status, detail)
		}
	}
	return fmt.Errorf("dashboard %s answered HTTP %d: %s", result.Path, result.Status, detail)
}

// Remove restores the Dashboard's RGW API keys to what they were before
// AddGateway and removes the link's RGW user. A key that was changed or
// removed after AddGateway is refused with ceph.ErrInvalidRequest and left as
// it is; the user is still removed. Remove is idempotent and Dashboard.Restore calls it.
//
//purpose:construction,condition,lifetime
func (l *GatewayLink) Remove(ctx context.Context) error {
	if l == nil || l.linkState == nil {
		return errors.New("dashboard gateway link is unavailable")
	}
	s := l.dashboard
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(s.cluster))
	defer cancel()
	if err := l.remove(ctx); err != nil {
		return err
	}
	if s.link == l {
		s.link = nil
	}
	return nil
}

func (l *GatewayLink) remove(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.removed {
		return nil
	}
	var errs []error
	for i := len(l.keys) - 1; i >= 0; i-- {
		if err := l.keys[i].restore(ctx, l.dashboard.cluster); err != nil {
			errs = append(errs, err)
		} else {
			l.keys = slices.Delete(l.keys, i, i+1)
		}
	}
	if l.user != nil {
		if err := l.user.Remove(ctx); err != nil {
			errs = append(errs, err)
		} else {
			l.user = nil
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("remove dashboard gateway link: %w", errors.Join(errs...))
	}
	l.removed = true
	return nil
}

// rgwKeyOption is one of the module's RGW API key options, which AddGateway
// replaces: the stored value before it and the value it installed. The
// options are manager options in the central configuration, written through
// the module's own commands with the value on standard input.
type rgwKeyOption struct {
	option, command string
	existed         bool
	previous        string
	installed       string
}

func (k *rgwKeyOption) name() string { return "mgr/dashboard/" + k.option }

func (k *rgwKeyOption) install(ctx context.Context, c *ceph.Container, value string) error {
	previous, found, err := managerOption(ctx, c, k.name())
	if err != nil {
		return err
	}
	k.existed, k.previous, k.installed = found, previous, value
	return runWithInput(ctx, c, "set dashboard "+k.command, []byte(value), "dashboard", "set-"+k.command, "-i", "-")
}

// restore puts the prior value back, or resets an option that was not set
// before, when the stored value is still the installed one. An install that
// never applied leaves nothing to restore.
func (k *rgwKeyOption) restore(ctx context.Context, c *ceph.Container) error {
	if k.installed == "" {
		return nil
	}
	current, found, err := managerOption(ctx, c, k.name())
	if err != nil {
		return err
	}
	if found == k.existed && current == k.previous {
		// The install never applied, or the prior value is back.
		return nil
	}
	if !found || current != k.installed {
		return &ceph.OperationError{Kind: ceph.ErrInvalidRequest, Op: "restore " + k.name(), Err: errors.New("changed outside the dashboard fixture; refusing restoration")}
	}
	if !k.existed {
		_, err = c.Ceph(ctx, "dashboard", "reset-"+k.command)
	} else {
		err = runWithInput(ctx, c, "restore dashboard "+k.command, []byte(k.previous), "dashboard", "set-"+k.command, "-i", "-")
	}
	if err != nil {
		return fmt.Errorf("restore %s: %w", k.name(), err)
	}
	return nil
}
