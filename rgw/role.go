package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/google/uuid"
)

// RoleConfig describes a new native RGW role that a principal assumes with
// STS AssumeRole. Empty Name generates a unique name; Path defaults to "/".
// The role is created in the empty tenant, outside any account.
//
// TrustPolicy is the role's assume-role policy document: which principals may
// assume it, such as {"Effect":"Allow","Principal":{"AWS":
// ["arn:aws:iam:::user/<user ID>"]},"Action":["sts:AssumeRole"]} in a
// statement list. Policies are the inline permission policies of the role,
// put in order, which bound what temporary credentials of the role may do.
// Documents are single-line JSON strings, stored as given.
type RoleConfig struct {
	Name, Path  string
	TrustPolicy string
	Policies    []RolePolicy
}

// RolePolicy is one inline permission policy of a role: its name, unique in
// the role, and its JSON document.
type RolePolicy struct {
	Name, Document string
}

// Role is a native RGW role that CreateRole created, with the permission
// policies the fixture put on it. It names one native role for good: its
// role ID, read back after creation, so a role deleted and created again
// under the same name outside the fixture is a different role, which the
// handle refuses. Copies of a role handle share its state. Roles remain in
// Ceph when a gateway is removed, until Remove or the disposable cluster's
// termination; Remove runs through the first running registered gateway of
// the scope of the gateway that created the role.
//
// The fixture creates the role and hands off its ARN; the SUT, or a test
// client playing it, calls STS AssumeRole and uses the temporary credentials.
// The gateways need STS enabled before they start (rgw_sts_key and
// rgw_s3_auth_use_sts, applied with override.SetConfig).
type Role struct {
	state *roleState
}

// roleState is the shared state of one role. name, path, trust, origin and
// handoff are set before the handle is returned and never change, so they
// are read without a lock; every other field is read and written with the
// owning cluster's mutex held.
type roleState struct {
	origin            *gwbase.Core
	name, path, trust string
	// handoff is the ARN that ARN returns: the native ARN when CreateRole
	// confirmed the role, empty otherwise.
	handoff string
	// id, arn and document are the native identity read back after the
	// creation: document is the whole native role without its permission
	// policies, so that any outside edit of the role is seen.
	id, arn  string
	document map[string]any
	// confirmed is set once native state showed the created role.
	confirmed, removed bool
	policies           []*rolePolicy
	// unstopped is a command of the role that may still run in the gateway
	// container.
	unstopped *ceph.NativeCommandUnstoppedError
}

// rolePolicy is one permission policy the fixture put, or tried to put, on
// its role. A pending policy may or may not be present; when present, its
// document must be the recorded one.
type rolePolicy struct {
	name, document string
	status         rolePolicyStatus
}

type rolePolicyStatus int

const (
	policyActive rolePolicyStatus = iota
	policyPending
	policyRemoved
)

// roleSettleTimeout bounds the native readback that settles a role command
// whose call failed, also after the call's ctx ended.
const roleSettleTimeout = 10 * time.Second

var (
	roleName   = regexp.MustCompile(`^[A-Za-z0-9_+=,.@][A-Za-z0-9_+=,.@-]{0,63}$`)
	rolePath   = regexp.MustCompile(`^/([\x21-\x7E]*/)?$`)
	policyName = regexp.MustCompile(`^[A-Za-z0-9_+=,.@][A-Za-z0-9_+=,.@-]{0,127}$`)
)

func normalizeRoleConfig(config RoleConfig) (RoleConfig, error) {
	if config.Name == "" {
		config.Name = "tc-" + uuid.NewString()
	}
	if config.Path == "" {
		config.Path = "/"
	}
	if !roleName.MatchString(config.Name) {
		return config, servicehook.Invalid(errors.New("RGW role name must be 1 to 64 letters, digits or _+=,.@- characters, not starting with -"))
	}
	if len(config.Path) > 512 || !rolePath.MatchString(config.Path) {
		return config, servicehook.Invalid(errors.New("RGW role path must start and end with / and use printable ASCII characters"))
	}
	if !validPolicyDocument(config.TrustPolicy) {
		return config, servicehook.Invalid(errors.New("RGW role trust policy must be a single-line JSON object"))
	}
	policies := make([]RolePolicy, 0, len(config.Policies))
	for _, policy := range config.Policies {
		if !policyName.MatchString(policy.Name) || slices.ContainsFunc(policies, func(p RolePolicy) bool { return p.Name == policy.Name }) {
			return config, servicehook.Invalid(errors.New("RGW role policy names must be unique and 1 to 128 letters, digits or _+=,.@- characters"))
		}
		if !validPolicyDocument(policy.Document) {
			return config, servicehook.Invalid(errors.New("RGW role permission policy must be a single-line JSON object"))
		}
		policies = append(policies, policy)
	}
	config.Policies = policies
	return config, nil
}

// validPolicyDocument accepts a JSON object without control characters,
// which the native command line carries as one argument.
func validPolicyDocument(document string) bool {
	var object map[string]any
	return strings.IndexFunc(document, unicode.IsControl) == -1 && json.Unmarshal([]byte(document), &object) == nil && object != nil
}

// CreateRole creates a fresh native role with its trust policy through this
// gateway, with radosgw-admin role create, then puts its permission policies
// in order with role-policy put, so a test can hand the role's ARN to the SUT.
// An existing role of the same name is refused, never adopted or changed,
// and so is a name that another role handle of this cluster still tracks.
// Every outcome is read back from native state with role get, also when a
// call fails or its ctx ends, and no native command is ever repeated:
//
//   - a nil error returns the role with all its policies;
//   - a non-nil role with an error is tracked and its Remove removes what the
//     fixture created: when native state showed the role, ARN returns it and
//     the error says what failed, such as a policy whose outcome is unknown;
//     otherwise whether the role exists is unknown and ARN refuses it;
//   - a nil role with an error means that RGW shows that no role was created,
//     or that nothing was attempted.
//
// Native role edits of the same name from Admin or external clients must not
// race this operation.
//
//purpose:construction,handoff
func (g *Gateway) CreateRole(ctx context.Context, config RoleConfig) (*Role, error) {
	config, err := normalizeRoleConfig(config)
	if err != nil {
		return nil, err
	}
	core := g.base()
	if core.Cluster() == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	if err := servicehook.LockTopology(ctx, core.Cluster()); err != nil {
		return nil, err
	}
	defer servicehook.Unlock(core.Cluster())
	if err := core.ValidateAdmin(); err != nil {
		return nil, err
	}
	registry := gateways(core.Cluster())
	if slices.ContainsFunc(registry.roles, func(r *roleState) bool { return r.name == config.Name && !r.removed }) {
		return nil, servicehook.Invalid(errors.New("an RGW role of this name is still tracked by the fixture; remove it first"))
	}
	ctx, cancel := gwbase.AdminContext(ctx, core.Cluster())
	defer cancel()
	native, err := readRole(ctx, core, config.Name)
	if err != nil {
		return nil, err
	}
	if native != nil {
		return nil, servicehook.Invalid(fmt.Errorf("RGW role %q already exists", config.Name))
	}
	state := &roleState{origin: core, name: config.Name, path: config.Path, trust: config.TrustPolicy}
	// Tracked before the first native change: an unknown outcome still
	// needs Remove.
	registry.roles = append(registry.roles, state)
	if err := state.create(ctx, core); err != nil {
		if state.removed {
			return nil, err
		}
		return state.handle(), err
	}
	for _, policy := range config.Policies {
		if err := state.putPolicy(ctx, core, policy); err != nil {
			return state.handle(), err
		}
	}
	return state.handle(), nil
}

// handle returns the handle of s, fixing the ARN it hands off. The caller
// holds the cluster mutex.
func (s *roleState) handle() *Role {
	if s.confirmed {
		s.handoff = s.arn
	}
	return &Role{state: s}
}

// create runs role create and settles its outcome.
func (s *roleState) create(ctx context.Context, g *gwbase.Core) error {
	_, cmdErr := g.AdminCommand(ctx, "role", "create", "--role-name", s.name, "--path", s.path, "--assume-role-policy-doc", s.trust)
	errors.As(cmdErr, &s.unstopped)
	native, err := settleRole(ctx, g, s.name, cmdErr)
	switch {
	case err != nil:
		return errors.Join(cmdErr, fmt.Errorf("whether RGW created the role is unknown; the role is tracked and its Remove settles and removes it: %w", err))
	case native == nil && s.unstopped == nil:
		s.removed = true
		if cmdErr == nil {
			cmdErr = errors.New("RGW role creation answered")
		}
		return fmt.Errorf("%w; RGW shows that the role was not created, and it no longer can be", cmdErr)
	case native == nil:
		return errors.Join(cmdErr, errors.New("the RGW role creation may still run; the role is tracked and its Remove settles and removes it"))
	}
	s.unstopped = nil
	s.bind(native)
	if !s.created(native) {
		return errors.Join(cmdErr, errors.New("RGW created a role whose name, path, trust policy or policies differ from the request; the role is tracked and its Remove removes it"))
	}
	if cmdErr != nil {
		return errors.Join(cmdErr, errors.New("RGW shows that the role was created; it is tracked and its ARN is usable"))
	}
	return nil
}

// bind records the native identity of the role that the fixture created.
func (s *roleState) bind(native *nativeRole) {
	s.id, s.arn, s.document, s.confirmed = native.id, native.arn, native.document, true
}

// created reports whether native is the role that create requested, before
// any policy was put.
func (s *roleState) created(native *nativeRole) bool {
	return native.name == s.name && native.path == s.path && native.trust == s.trust && len(native.policies) == 0 &&
		strings.HasSuffix(native.arn, ":role"+s.path+s.name)
}

// putPolicy records policy, runs role-policy put and settles its outcome.
func (s *roleState) putPolicy(ctx context.Context, g *gwbase.Core, policy RolePolicy) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("RGW role policies were not all put: %w", err)
	}
	tracked := &rolePolicy{name: policy.Name, document: policy.Document, status: policyPending}
	s.policies = append(s.policies, tracked)
	_, cmdErr := g.AdminCommand(ctx, "role-policy", "put", "--role-name", s.name, "--policy-name", policy.Name, "--policy-doc", policy.Document)
	errors.As(cmdErr, &s.unstopped)
	native, err := settleRole(ctx, g, s.name, cmdErr)
	if err != nil {
		return errors.Join(cmdErr, fmt.Errorf("whether RGW put the role policy is unknown; the role is tracked and its Remove settles it: %w", err))
	}
	if err := s.check(native); err != nil {
		return errors.Join(cmdErr, err)
	}
	document, present := native.policies[policy.Name]
	switch {
	case !present && s.unstopped == nil:
		tracked.status = policyRemoved
		if cmdErr == nil {
			cmdErr = errors.New("RGW role policy put answered")
		}
		return fmt.Errorf("%w; RGW shows that the role policy was not put, and it no longer can be; the role is tracked and its Remove removes it", cmdErr)
	case !present:
		return errors.Join(cmdErr, errors.New("the RGW role policy put may still run; the role is tracked and its Remove settles and removes it"))
	}
	s.unstopped = nil
	tracked.status = policyActive
	if document != policy.Document {
		tracked.document = document
		return errors.Join(cmdErr, errors.New("RGW stored a role policy document that differs from the request; the role is tracked and its Remove removes it"))
	}
	if cmdErr != nil {
		return errors.Join(cmdErr, errors.New("RGW shows that the role policy was put; the role is tracked"))
	}
	return nil
}

// check refuses a present native role that is not the fixture's role as it
// recorded it: another role ID (a replaced role), another identity or
// metadata, or a permission policy the fixture did not put or whose document
// changed. A fixture policy may be absent: it was deleted, or its put did not
// apply.
func (s *roleState) check(native *nativeRole) error {
	if native == nil {
		return errors.New("RGW role is missing")
	}
	if native.id != s.id || native.arn != s.arn || native.name != s.name || native.path != s.path {
		return servicehook.Invalid(errors.New("RGW role was replaced since the fixture created it; refusing to change it"))
	}
	if !reflect.DeepEqual(native.document, s.document) {
		return servicehook.Invalid(errors.New("RGW role trust policy, tags or metadata changed outside the fixture; refusing to change it"))
	}
	for name, document := range native.policies {
		i := slices.IndexFunc(s.policies, func(p *rolePolicy) bool { return p.name == name && p.status != policyRemoved })
		if i < 0 || s.policies[i].document != document {
			return servicehook.Invalid(errors.New("RGW role has a permission policy that the fixture did not put or that changed outside the fixture; refusing to change it"))
		}
	}
	return nil
}

// Name returns the role's name, the one CreateRole generated when RoleConfig
// left it empty.
//
//purpose:observation
func (r *Role) Name() string {
	if r == nil || r.state == nil {
		return ""
	}
	return r.state.name
}

// ARN returns the role's native ARN, read back after its creation, which the
// SUT passes to STS AssumeRole as RoleArn. It stays the same after Remove, so
// a test can hand a removed role to the SUT to check that STS refuses it. A
// role whose creation was not confirmed when CreateRole returned returns an
// error.
//
//purpose:handoff
func (r *Role) ARN() (string, error) {
	if r == nil || r.state == nil || r.state.handoff == "" {
		return "", errors.New("RGW role creation was not confirmed; its ARN is unavailable")
	}
	return r.state.handoff, nil
}

// Remove deletes the fixture's permission policies of this role, with
// role-policy delete, and then the role, with role delete. It removes only
// what the fixture created: a role whose role ID, trust policy, tags or other
// metadata changed, that holds a permission policy the fixture did not put,
// or whose fixture policy changed outside the fixture, is refused with
// ceph.ErrInvalidRequest and left as it is. A role that native state no
// longer holds, and a role already removed, return nil.
//
// A role whose creation outcome was unknown is settled first: a command of
// it that may still run is stopped, and the role is removed when native state
// holds the role the fixture requested. When a removal fails, its outcome is
// read back: what is gone stays removed, what is still present stays tracked,
// and Remove can be called again.
//
// Removing a role does not revoke temporary credentials already issued for
// it in RGW's caches at once; poll with new requests.
//
//purpose:lifetime
func (r *Role) Remove(ctx context.Context) error {
	if r == nil || r.state == nil || r.state.origin.Cluster() == nil {
		return servicehook.Invalid(errors.New("RGW role is not owned by a Ceph cluster"))
	}
	s := r.state
	cluster := s.origin.Cluster()
	if err := servicehook.LockTopology(ctx, cluster); err != nil {
		return err
	}
	defer servicehook.Unlock(cluster)
	if s.removed {
		return nil
	}
	g, err := s.origin.ScopeGateway(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	if s.unstopped != nil {
		if err := s.unstopped.Stop(ctx); err != nil {
			return fmt.Errorf("an earlier command of the RGW role may still run: %w", err)
		}
		s.unstopped = nil
	}
	native, err := readRole(ctx, g, s.name)
	if err != nil {
		return err
	}
	if native == nil {
		s.markRemoved()
		return nil
	}
	if !s.confirmed {
		if !s.created(native) {
			return servicehook.Invalid(errors.New("RGW role of this name is not the role the fixture requested; refusing to remove it"))
		}
		s.bind(native)
	}
	if err := s.check(native); err != nil {
		return err
	}
	for _, policy := range s.policies {
		if _, present := native.policies[policy.name]; !present || policy.status == policyRemoved {
			policy.status = policyRemoved
			continue
		}
		if native, err = s.deletePolicy(ctx, g, policy); err != nil {
			return err
		}
	}
	_, cmdErr := g.AdminCommand(ctx, "role", "delete", "--role-name", s.name)
	errors.As(cmdErr, &s.unstopped)
	native, err = settleRole(ctx, g, s.name, cmdErr)
	switch {
	case err != nil:
		return errors.Join(cmdErr, fmt.Errorf("whether RGW deleted the role is unknown; Remove settles it: %w", err))
	case native == nil:
		s.markRemoved()
		return nil
	case cmdErr == nil:
		cmdErr = errors.New("RGW answered without deleting the role")
	}
	if s.unstopped == nil {
		if err := s.check(native); err != nil {
			return errors.Join(cmdErr, err)
		}
	}
	return cmdErr
}

// deletePolicy runs role-policy delete for policy and settles its outcome;
// it returns the native role read back.
func (s *roleState) deletePolicy(ctx context.Context, g *gwbase.Core, policy *rolePolicy) (*nativeRole, error) {
	_, cmdErr := g.AdminCommand(ctx, "role-policy", "delete", "--role-name", s.name, "--policy-name", policy.name)
	errors.As(cmdErr, &s.unstopped)
	native, err := settleRole(ctx, g, s.name, cmdErr)
	if err != nil {
		return nil, errors.Join(cmdErr, fmt.Errorf("whether RGW deleted the role policy is unknown; Remove settles it: %w", err))
	}
	if native == nil {
		return nil, errors.Join(cmdErr, errors.New("RGW role disappeared while its policy was deleted; Remove settles it"))
	}
	if err := s.check(native); err != nil {
		return nil, errors.Join(cmdErr, err)
	}
	if _, present := native.policies[policy.name]; present {
		if cmdErr == nil {
			cmdErr = errors.New("RGW answered without deleting the role policy")
		}
		return nil, cmdErr
	}
	s.unstopped = nil
	policy.status = policyRemoved
	return native, nil
}

func (s *roleState) markRemoved() {
	s.removed, s.unstopped = true, nil
	for _, policy := range s.policies {
		policy.status = policyRemoved
	}
}

// String names the role.
func (r Role) String() string {
	if r.state == nil {
		return "RGW role"
	}
	return "RGW role " + r.state.name
}

// GoString formats the role like String, so %#v does not walk its state.
func (r Role) GoString() string { return r.String() }

// Format prints String for every verb and flag, so no verb, such as %d or %t,
// walks the fields.
func (r Role) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, r.String()) }

// nativeRole is one decoded native role.
type nativeRole struct {
	id, name, path, arn, trust string
	policies                   map[string]string
	// document is the whole native role without its permission policies.
	document map[string]any
}

// settleRole reads the role name after a command whose call returned err: in
// ctx when the call succeeded, or within roleSettleTimeout of a context that
// ignores the end of ctx when it failed, so that a canceled call still
// learns what it changed.
func settleRole(ctx context.Context, g *gwbase.Core, name string, err error) (*nativeRole, error) {
	if err == nil {
		return readRole(ctx, g, name)
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), roleSettleTimeout)
	defer cancel()
	return readRole(settleCtx, g, name)
}

// readRole reads the native role name with role get; nil when native state
// holds no role of that name. The caller holds the cluster mutex.
func readRole(ctx context.Context, g *gwbase.Core, name string) (*nativeRole, error) {
	data, err := g.AdminCommand(ctx, "role", "get", "--role-name", name)
	if err != nil {
		if roleAbsent(err) {
			return nil, nil
		}
		return nil, err
	}
	return decodeRole(data, name)
}

// roleAbsent reports a role get that failed because no role has the name:
// radosgw-admin exits with ENOENT, which Squid prints and Tentacle does not.
func roleAbsent(err error) bool {
	var failure *ceph.NativeFailure
	return errors.As(err, &failure) && failure.ExitCode == 2 && (failure.Errno == "" || failure.Errno == "ENOENT")
}

// decodeRole decodes the native role of role get and refuses another role's.
// Native output never appears in its error.
func decodeRole(data []byte, name string) (*nativeRole, error) {
	bad := errors.New("decode RGW role")
	document, err := gwbase.DecodeObject(data)
	if err != nil {
		return nil, bad
	}
	field := func(key string) string {
		value, _ := document[key].(string)
		return value
	}
	role := &nativeRole{id: field("RoleId"), name: field("RoleName"), path: field("Path"), arn: field("Arn"), trust: field("AssumeRolePolicyDocument"), policies: map[string]string{}}
	if role.id == "" || role.name == "" || role.path == "" || role.arn == "" || role.trust == "" {
		return nil, bad
	}
	if role.name != name {
		return nil, errors.New("RGW role get named another role")
	}
	if raw, present := document["PermissionPolicies"]; present {
		policies, ok := raw.([]any)
		if !ok {
			return nil, bad
		}
		for _, entry := range policies {
			policy, ok := entry.(map[string]any)
			if !ok {
				return nil, bad
			}
			name, nameOK := policy["PolicyName"].(string)
			value, valueOK := policy["PolicyValue"].(string)
			if _, duplicate := role.policies[name]; !nameOK || !valueOK || name == "" || duplicate {
				return nil, bad
			}
			role.policies[name] = value
		}
		delete(document, "PermissionPolicies")
	}
	role.document = document
	return role, nil
}
