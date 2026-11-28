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
	"github.com/moby/moby/api/types/container"
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
	// gateway: a refused or timed-out connection, a request that did not
	// finish within the probe's time, or a failed answer while the linked
	// gateway's container is not running, paused or restarting.
	ErrGatewayUnreachable = errors.New("the dashboard cannot reach the gateway")
)

// GatewayLink is the RGW backend of a Dashboard that AddGateway wired to one
// owned gateway. Copies share its state. Formatting it prints only the RGW
// identity's ID.
type GatewayLink struct {
	*linkState
}

type linkState struct {
	mu        sync.RWMutex
	dashboard *state
	gateway   *rgw.Gateway
	user      *rgw.User
	keys      []*rgwKeyOption
	removed   bool
}

// String names the link by its RGW identity.
func (l GatewayLink) String() string {
	if l.linkState == nil {
		return "Dashboard RGW link"
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.user == nil {
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
// After attempting user creation, AddGateway may return a link with an
// error. Keep it for Remove, or call Dashboard.Restore. Cleanup checks for
// an unconfirmed user: an absent user needs no cleanup, and a present user
// with unconfirmed ownership is preserved and reported.
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
	if err := s.ready(); err != nil {
		return nil, err
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
	for _, k := range []struct{ option, command, value string }{
		{"RGW_API_ACCESS_KEY", "rgw-api-access-key", access},
		{"RGW_API_SECRET_KEY", "rgw-api-secret-key", secret},
	} {
		key := newRGWKeyOption(k.option, k.command)
		link.keys = append(link.keys, key)
		if err := key.install(ctx, s.cluster, k.value); err != nil {
			return link, fmt.Errorf("%s: %w", op, err)
		}
	}
	if err := retry(ctx, 2*time.Second, func() (bool, error) { return s.stopUnlessReady(link.check(ctx)) }); err != nil {
		return link, fmt.Errorf("%s: %w", op, err)
	}
	return link, nil
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
	l.mu.RLock()
	defer l.mu.RUnlock()
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
// ErrGatewayUnreachable. An HTTP failure whose message names neither cause
// wraps ErrGatewayUnreachable when the linked gateway's container is not
// running, paused or restarting, and is left unattributed otherwise. It makes
// one attempt.
//
//purpose:observation
func (l *GatewayLink) Check(ctx context.Context) error {
	if l == nil || l.linkState == nil {
		return errors.New("dashboard gateway link is unavailable")
	}
	l.dashboard.mu.RLock()
	defer l.dashboard.mu.RUnlock()
	if err := l.dashboard.ready(); err != nil {
		return err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.removed {
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
	if err := l.backendFailure(ctx, daemon); err != nil {
		return err
	}
	users, _ := report.result(rgwUserPath)
	if err := l.backendFailure(ctx, users); err != nil {
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

// backendFailure classifies a Dashboard answer about its RGW backend. An
// HTTP failure that the Dashboard's message does not attribute is judged by
// the linked gateway's container: the Dashboard cannot reach a gateway whose
// container is not running, paused or restarting, whatever message it
// answers with.
func (l *GatewayLink) backendFailure(ctx context.Context, result probeResult) error {
	err := backendError(result)
	if !unattributed(result, err) {
		return err
	}
	state, stateErr := l.gateway.State(ctx)
	if stateErr != nil {
		return errors.Join(err, fmt.Errorf("inspect the linked gateway: %w", stateErr))
	}
	return gatewayNotServing(err, state)
}

// unattributed reports whether err is an HTTP failure of the Dashboard's
// answer result that backendError attributed to neither cause.
func unattributed(result probeResult, err error) bool {
	return err != nil && result.Error == "" && result.Status != 0 &&
		!errors.Is(err, ErrGatewayRefused) && !errors.Is(err, ErrGatewayUnreachable)
}

// gatewayNotServing wraps err with ErrGatewayUnreachable when state shows a
// gateway container that cannot answer: absent, not running, paused or
// restarting. Otherwise it returns err unchanged.
func gatewayNotServing(err error, state *container.State) error {
	var condition string
	switch {
	case state == nil:
		condition = "has no state"
	case !state.Running:
		condition = "is not running"
	case state.Paused:
		condition = "is paused"
	case state.Restarting:
		condition = "is restarting"
	default:
		return err
	}
	return fmt.Errorf("%w: %w; the linked gateway's container %s", ErrGatewayUnreachable, err, condition)
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
// it is; the user is still removed. Remove is idempotent, including through a
// copy of the link. Dashboard.Restore also removes the active link.
//
//purpose:condition,lifetime
func (l *GatewayLink) Remove(ctx context.Context) error {
	if l == nil || l.linkState == nil {
		return errors.New("dashboard gateway link is unavailable")
	}
	s := l.dashboard
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.link == nil || s.link.linkState != l.linkState {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(s.cluster))
	defer cancel()
	if err := s.settleProbes(ctx); err != nil {
		return err
	}
	if err := l.remove(ctx); err != nil {
		return err
	}
	s.link = nil
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
	command string
	replacedValue
}

func newRGWKeyOption(option, command string) *rgwKeyOption {
	return &rgwKeyOption{command: command, replacedValue: replacedValue{name: "mgr/dashboard/" + option}}
}

// value reads the stored option and whether it is set.
func (k *rgwKeyOption) value(ctx context.Context, c *ceph.Container) ([]byte, bool, error) {
	value, found, err := managerOption(ctx, c, k.name)
	if !found || err != nil {
		return nil, false, err
	}
	return []byte(value), true, nil
}

func (k *rgwKeyOption) install(ctx context.Context, c *ceph.Container, value string) error {
	previous, found, err := k.value(ctx, c)
	if err != nil {
		return err
	}
	k.existed, k.previous, k.installed = found, previous, []byte(value)
	return k.write.track(runWithInput(ctx, c, "set dashboard "+k.command, k.installed, "dashboard", "set-"+k.command, "-i", "-"))
}

// restore puts the prior value back, or resets an option that was not set
// before, when the stored value is still the installed one.
func (k *rgwKeyOption) restore(ctx context.Context, c *ceph.Container) error {
	return k.replacedValue.restore(ctx,
		func(ctx context.Context) ([]byte, bool, error) { return k.value(ctx, c) },
		func(ctx context.Context) error {
			_, err := c.Ceph(ctx, "dashboard", "reset-"+k.command)
			return err
		},
		func(ctx context.Context, previous []byte) error {
			return runWithInput(ctx, c, "restore dashboard "+k.command, previous, "dashboard", "set-"+k.command, "-i", "-")
		})
}
