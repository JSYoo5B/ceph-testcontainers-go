package placement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/moby/moby/api/pkg/stdcopy"
)

// StorageClass maps one native S3 storage class to an existing data
// pool. STANDARD is required. Other class names, for example STANDARD_IA, must
// also be accepted by the consumer's S3 SDK. Data pools may be replicated or EC;
// RGW does not require EC overwrites because it writes complete objects.
type StorageClass struct {
	Name, DataPool string
}

// Config adds a fresh, non-default bucket placement target. IndexPool
// and DataExtraPool must be existing replicated pools supporting omap. It never
// changes the zonegroup default or any existing bucket's immutable placement.
// InlineData nil preserves Ceph's true default; false separates object heads
// and data. Native external placement/topology edits must not race these helpers.
//
// DataExtraPool holds incomplete multipart metadata.
type Config struct {
	Name, IndexPool, DataExtraPool string
	StorageClasses                 []StorageClass
	InlineData                     *bool
	// Tags restrict creation of new buckets using this target to users with
	// at least one matching placement tag. An empty list permits every user.
	// These tags do not grant access to existing buckets or their objects.
	Tags []string
}

// Placement identifies a newly created native policy and its exact runtime
// scope. LocationConstraint can be passed to S3 CreateBucket. Creation stores
// configuration; Apply restarts a standalone gateway or publishes a guarded
// realm period. Import that period into other clusters before explicitly
// activating their gateways with Reload. Failed creation may return a partial
// handle for inspection; it is never rolled back by deleting pools, policies,
// buckets or data. Editing the exported fields does not redirect an operation.
type Placement struct {
	origin
	Name, RealmID, ZonegroupID, ZoneID, LocationConstraint string
	scope                                                  gwbase.NativeScope
	config                                                 Config
	poolIDs                                                map[string]int64
	group, zone, previousPeriod                            map[string]any
	confirmed                                              bool
}

// origin binds a Placement to the gateway core that created it (see
// gwbase.Origin). The alias keeps the embedded field unexported.
type origin = gwbase.Origin

// String names the placement target.
func (p Placement) String() string { return "RGW placement " + p.Name }

// GoString formats the placement target like String.
func (p Placement) GoString() string { return p.String() }

// Format prints String for every verb and flag, so no verb, such as %d or %t,
// walks the fields.
func (p Placement) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, p.String()) }

// State is a native policy snapshot. DefaultPlacement is the zonegroup
// default, which this package never modifies. Creation and activation do not
// assign any user's default placement or storage class.
type State struct {
	Config
	RealmID, ZonegroupID, ZoneID, LocationConstraint, DefaultPlacement string
	// Confirmed reports successful creation/readback of this owned handle.
	// It does not assert that a gateway reloaded the policy or a realm published it.
	Confirmed bool
}

var storageClassName = regexp.MustCompile(`^[A-Z][A-Z0-9_-]{0,127}$`)

func normalizeConfig(config Config) (Config, error) {
	if !naming.PoolResource.MatchString(config.Name) || len(config.Name) > 128 {
		return config, servicehook.Invalid(errors.New("invalid RGW placement name"))
	}
	if config.Name == "default-placement" {
		return config, servicehook.Invalid(errors.New("RGW placement creation requires a new non-default name"))
	}
	for _, pool := range []string{config.IndexPool, config.DataExtraPool} {
		if err := validateExistingPool(pool); err != nil {
			return config, err
		}
	}
	config.StorageClasses = slices.Clone(config.StorageClasses)
	var err error
	config.Tags, err = normalizeTags(config.Tags)
	if err != nil {
		return config, err
	}
	seen := make(map[string]bool)
	for _, class := range config.StorageClasses {
		if !storageClassName.MatchString(class.Name) || seen[class.Name] {
			return config, servicehook.Invalid(errors.New("RGW storage classes must have distinct uppercase names"))
		}
		if err := validateExistingPool(class.DataPool); err != nil {
			return config, err
		}
		seen[class.Name] = true
	}
	if !seen["STANDARD"] {
		return config, servicehook.Invalid(errors.New("RGW placement requires a STANDARD data pool"))
	}
	slices.SortFunc(config.StorageClasses, func(a, b StorageClass) int {
		if a.Name == b.Name {
			return 0
		}
		if a.Name == "STANDARD" {
			return -1
		}
		if b.Name == "STANDARD" {
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	if config.InlineData != nil {
		value := *config.InlineData
		config.InlineData = &value
	}
	return config, nil
}

// core returns the gateway core of g; nil for a nil g.
func core(g *rgw.Gateway) *gwbase.Core {
	if g == nil {
		return nil
	}
	return gwbase.Of(g)
}

// Create creates a named target and all its storage classes in the actual
// running zone/zonegroup of gateway g, even if global native defaults changed
// after it started. Existing targets in either scope are rejected. Every pool
// must already exist; native IDs are rechecked before each mutation. Changes are
// sequential and partial failure leaves inspectable configuration. On a realm,
// all participating zones need compatible local mappings before publication.
//
//purpose:construction
func Create(ctx context.Context, g *rgw.Gateway, config Config) (*Placement, error) {
	return create(ctx, core(g), config)
}

func create(ctx context.Context, g *gwbase.Core, config Config) (*Placement, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if g.Cluster() == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.Cluster())
	defer servicehook.Unlock(g.Cluster())
	if err := g.ValidateAdmin(); err != nil {
		return nil, err
	}
	ctx, cancel := gwbase.AdminContext(ctx, g.Cluster())
	defer cancel()
	scope, group, err := admitCreation(ctx, g, config.Name)
	if err != nil {
		return nil, err
	}
	poolIDs, err := placementPoolIDs(ctx, g, config)
	if err != nil {
		return nil, err
	}
	p := &Placement{origin: gwbase.NewOrigin(g), Name: config.Name, RealmID: scope.RealmID, ZonegroupID: scope.GroupID, ZoneID: scope.ZoneID,
		LocationConstraint: fmt.Sprint(group["api_name"]) + ":" + config.Name, scope: scope, config: config, poolIDs: poolIDs}
	if apiName, ok := group["api_name"].(string); !ok || apiName == "" {
		return nil, errors.New("RGW zonegroup has no S3 API name")
	}
	if scope.RealmID != "" {
		if p.previousPeriod, err = currentPeriod(ctx, g, scope); err != nil {
			return nil, err
		}
		if p.previousPeriod["realm_id"] != scope.RealmID {
			return nil, errors.New("RGW current period belongs to a different realm")
		}
	}
	if err := addClasses(ctx, g, p); err != nil {
		return p, err
	}
	return p, confirm(ctx, g, p, group)
}

// admitCreation resolves the gateway's running scope and refuses a name that
// already exists there or a zonegroup without a default placement. It returns
// the scope and the zonegroup document read before any mutation.
func admitCreation(ctx context.Context, g *gwbase.Core, name string) (gwbase.NativeScope, map[string]any, error) {
	scope, err := g.RuntimeScope(ctx)
	if err != nil {
		return gwbase.NativeScope{}, nil, err
	}
	group, zone, err := nativeScope(ctx, g, scope)
	if err != nil {
		return gwbase.NativeScope{}, nil, err
	}
	if groupTarget(group, name) != nil || zonePlacement(zone, name) != nil {
		return gwbase.NativeScope{}, nil, servicehook.Invalid(fmt.Errorf("RGW placement %q already exists in its zonegroup or zone", name))
	}
	if defaultName, _ := group["default_placement"].(string); defaultName == "" {
		return gwbase.NativeScope{}, nil, servicehook.Invalid(errors.New("RGW zonegroup has no default placement; refusing implicit default mutation"))
	}
	return scope, group, nil
}

// placementPoolIDs records the native ID of every pool the policy names, so a
// later mutation can refuse a pool that was replaced. Index and multipart
// metadata pools must be replicated.
func placementPoolIDs(ctx context.Context, g *gwbase.Core, config Config) (map[string]int64, error) {
	states, err := servicehook.PoolStates(ctx, g.Cluster())
	if err != nil {
		return nil, err
	}
	pools := make(map[string]ceph.PoolState)
	for _, state := range states {
		pools[state.Name] = state
	}
	poolIDs := make(map[string]int64)
	for _, name := range []string{config.IndexPool, config.DataExtraPool} {
		state, exists := pools[name]
		if !exists || state.Type != "replicated" {
			return nil, fmt.Errorf("RGW index and multipart metadata pool %q must exist and be replicated", name)
		}
		poolIDs[name] = state.ID
	}
	for _, class := range config.StorageClasses {
		state, exists := pools[class.DataPool]
		if !exists {
			return nil, fmt.Errorf("RGW data pool %q does not exist", class.DataPool)
		}
		poolIDs[class.DataPool] = state.ID
	}
	return poolIDs, nil
}

// addClasses adds each storage class to the zonegroup target and then to the
// zone mapping, rechecking the scope and pools before every mutation. A
// failure leaves the earlier additions in place for inspection.
func addClasses(ctx context.Context, g *gwbase.Core, p *Placement) error {
	config := p.config
	for _, class := range config.StorageClasses {
		if err := checkResources(ctx, g, p); err != nil {
			return err
		}
		groupArgs := []string{"zonegroup", "placement", "add", "--placement-id", config.Name, "--storage-class", class.Name}
		if len(config.Tags) > 0 {
			groupArgs = append(groupArgs, "--tags", strings.Join(config.Tags, ","))
		}
		if _, err := g.Command(ctx, p.scope, groupArgs...); err != nil {
			return err
		}
		if err := checkResources(ctx, g, p); err != nil {
			return err
		}
		args := []string{"zone", "placement", "add", "--placement-id", config.Name, "--storage-class", class.Name, "--data-pool", class.DataPool,
			"--index-pool", config.IndexPool, "--data-extra-pool", config.DataExtraPool}
		if config.InlineData != nil {
			args = append(args, "--placement-inline-data="+strconv.FormatBool(*config.InlineData))
		}
		if _, err := g.Command(ctx, p.scope, args...); err != nil {
			return err
		}
	}
	return nil
}

// confirm reads the zonegroup and zone back, records them as the snapshot
// activation compares against, and confirms the handle only when the default
// placement is unchanged and the native mapping is exactly the policy.
func confirm(ctx context.Context, g *gwbase.Core, p *Placement, before map[string]any) error {
	var err error
	p.group, p.zone, err = nativeScope(ctx, g, p.scope)
	if err != nil {
		return err
	}
	if p.group["default_placement"] != before["default_placement"] {
		return errors.New("RGW default placement changed during policy creation")
	}
	if _, err := snapshot(p, p.group, p.zone); err != nil {
		return err
	}
	p.confirmed = true
	return nil
}

// Status reads the exact native scope of a created or partial policy, through
// the first running registered gateway, by name, in the scope of the gateway that
// created it. Incomplete or changed mappings return an error; use Gateway.Admin
// for detailed partial inspection. It does not claim a running gateway reloaded
// this configuration.
//
//purpose:observation
func (p *Placement) Status(ctx context.Context) (State, error) {
	origin := gwbase.OriginOf(p)
	if origin.Cluster() == nil {
		return State{}, servicehook.Invalid(errors.New("RGW placement is not owned by a Ceph cluster"))
	}
	servicehook.Lock(origin.Cluster())
	defer servicehook.Unlock(origin.Cluster())
	g, err := origin.ScopeGateway(ctx)
	if err != nil {
		return State{}, err
	}
	ctx, cancel := gwbase.AdminContext(ctx, origin.Cluster())
	defer cancel()
	if err := checkResources(ctx, g, p); err != nil {
		return State{}, err
	}
	group, zone, err := nativeScope(ctx, g, p.scope)
	if err != nil {
		return State{}, err
	}
	return snapshot(p, group, zone)
}

// Apply activates a confirmed new policy through gateway g. Standalone mode
// explicitly restarts only g; resolve S3Endpoint again because Docker can
// assign a new mapped host port. Other gateways in the same zone require their
// own Apply. Realm mode requires the current metadata master and commits a new
// period only when its topology differs solely by this new placement.
// Coordinate mappings in other zones before publishing a realm policy, import
// the committed period into those clusters and call Reload there. It never
// changes defaults, deletes resources or silently rolls back a partial failure.
//
//purpose:construction,condition
func (p *Placement) Apply(ctx context.Context, g *rgw.Gateway) error {
	return apply(ctx, core(g), p)
}

func apply(ctx context.Context, g *gwbase.Core, p *Placement) error {
	if g.Cluster() == nil {
		return servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.Cluster())
	defer servicehook.Unlock(g.Cluster())
	if err := validateHandle(g, p); err != nil {
		return err
	}
	if !p.confirmed {
		return errors.New("RGW placement creation has not been confirmed")
	}
	ctx, cancel := gwbase.AdminContext(ctx, g.Cluster())
	defer cancel()
	group, err := admitActivation(ctx, g, p)
	if err != nil {
		return err
	}
	if p.scope.RealmID == "" {
		// An unscoped gateway originally chose native defaults at startup.
		// Restarting it after another actor changes those pointers could serve
		// another zone, despite the policy edits having used correct live IDs.
		return restart(ctx, g, p.scope, "apply")
	}
	return publishPeriod(ctx, g, p, groupTarget(group, p.config.Name))
}

// admitActivation refuses activation unless the scope, the pools and the
// native zonegroup and zone documents are exactly those confirmed at
// creation. It returns the current zonegroup document.
func admitActivation(ctx context.Context, g *gwbase.Core, p *Placement) (map[string]any, error) {
	if err := checkResources(ctx, g, p); err != nil {
		return nil, err
	}
	group, zone, err := nativeScope(ctx, g, p.scope)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(group, p.group) || !reflect.DeepEqual(zone, p.zone) {
		return nil, servicehook.Invalid(errors.New("RGW zone or zonegroup changed since placement creation; refusing activation"))
	}
	return group, nil
}

// restart restarts gateway g so that it loads the policy, after checking that
// its startup defaults still resolve to the policy's scope. verb names the
// operation in its errors.
func restart(ctx context.Context, g *gwbase.Core, scope gwbase.NativeScope, verb string) error {
	if err := restartScope(ctx, g, scope); err != nil {
		return err
	}
	grace := 3 * time.Second
	if err := g.Container().Stop(ctx, &grace); err != nil {
		return fmt.Errorf("stop RGW to %s placement: %w", verb, err)
	}
	if err := g.Container().Start(ctx); err != nil {
		return fmt.Errorf("restart RGW to %s placement: %w", verb, err)
	}
	return nil
}

// publishPeriod commits a realm period whose only change is target. The
// current period must be the one recorded at creation and this zone its
// metadata master; an existing staging period must hold nothing but this
// change. The committed period becomes the handle's new baseline.
func publishPeriod(ctx context.Context, g *gwbase.Core, p *Placement, target map[string]any) error {
	current, err := currentPeriod(ctx, g, p.scope)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, p.previousPeriod) {
		return servicehook.Invalid(errors.New("RGW current period changed since placement creation; refusing publication"))
	}
	if current["master_zone"] != p.scope.ZoneID {
		return errors.New("RGW placement period publication requires its realm metadata master")
	}
	if err := checkStagingPeriod(ctx, g, p.scope, current, target); err != nil {
		return err
	}
	data, err := g.Command(ctx, p.scope, "period", "update")
	if err != nil {
		return err
	}
	staging, err := gwbase.DecodeObject(data)
	if err != nil {
		return err
	}
	if err := periodChange(current, staging, p.scope.GroupID, target); err != nil {
		return err
	}
	if _, err := g.Command(ctx, p.scope, "period", "commit"); err != nil {
		return err
	}
	committed, err := currentPeriod(ctx, g, p.scope)
	if err != nil {
		return err
	}
	if err := periodChange(current, committed, p.scope.GroupID, target); err != nil {
		return err
	}
	p.previousPeriod = committed
	return nil
}

// checkStagingPeriod refuses to overwrite a staging period that holds policy
// other than target. It reads the exact staging object directly: --epoch avoids
// a separate latest-epoch lookup. The native CLI returns errno 2 only for a
// missing object; other failures must never be interpreted as an empty staging
// area or leak native credentials.
func checkStagingPeriod(ctx context.Context, g *gwbase.Core, scope gwbase.NativeScope, current, target map[string]any) error {
	stagingID := scope.RealmID + ":staging"
	code, reader, err := g.Container().Exec(ctx, gwbase.CommandArgs(scope, "period", "get", "--period", stagingID, "--epoch", "1"))
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("read RGW staging period: %w", ctx.Err())
		}
		return errors.New("read RGW staging period failed; native output is redacted")
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return errors.New("decode RGW staging period stream; native output is redacted")
	}
	if code != 0 && (code != 2 || stdout.Len() != 0) {
		return errors.New("read RGW staging period failed; native output is redacted")
	}
	if code != 0 {
		return nil
	}
	pending, err := gwbase.DecodeObject(stdout.Bytes())
	if err != nil {
		return err
	}
	if pending["id"] != stagingID || pending["realm_id"] != scope.RealmID {
		return errors.New("RGW staging period identity differs from the owned realm")
	}
	if !reflect.DeepEqual(periodPolicy(current), periodPolicy(pending)) {
		if err := periodChange(current, pending, scope.GroupID, target); err != nil {
			return servicehook.Invalid(errors.New("RGW existing staging period contains unrelated policy; refusing to overwrite it"))
		}
	}
	return nil
}

// currentPeriod reads the realm's current period in scope.
func currentPeriod(ctx context.Context, g *gwbase.Core, scope gwbase.NativeScope) (map[string]any, error) {
	data, err := g.Command(ctx, scope, "period", "get")
	if err != nil {
		return nil, err
	}
	return gwbase.DecodeObject(data)
}

// Reload activates a confirmed policy in gateway g through restart,
// including a secondary realm zone. In realm mode the latest locally stored
// committed period must contain the exact owned target and zone. Import the
// published period through the multicluster fixture before reloading another
// cluster. This method never publishes, pulls or rewrites federation topology.
// Resolve S3Endpoint again because Docker may assign a new mapped host port.
//
//purpose:construction,condition
func (p *Placement) Reload(ctx context.Context, g *rgw.Gateway) error {
	return reload(ctx, core(g), p)
}

func reload(ctx context.Context, g *gwbase.Core, p *Placement) error {
	if g.Cluster() == nil {
		return servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.Cluster())
	defer servicehook.Unlock(g.Cluster())
	if err := validateHandle(g, p); err != nil {
		return err
	}
	if !p.confirmed {
		return errors.New("RGW placement creation has not been confirmed")
	}
	ctx, cancel := gwbase.AdminContext(ctx, g.Cluster())
	defer cancel()
	if err := checkResources(ctx, g, p); err != nil {
		return err
	}
	group, zone, err := nativeScope(ctx, g, p.scope)
	if err != nil {
		return err
	}
	if _, err := snapshot(p, group, zone); err != nil {
		return err
	}
	if p.scope.RealmID != "" {
		period, err := currentPeriod(ctx, g, p.scope)
		if err != nil {
			return err
		}
		if err := committedTarget(period, p); err != nil {
			return err
		}
	}
	return restart(ctx, g, p.scope, "reload")
}

func committedTarget(period map[string]any, p *Placement) error {
	id, ok := period["id"].(string)
	if !ok || id == "" || strings.HasSuffix(id, ":staging") {
		return errors.New("RGW placement requires a locally committed period")
	}
	if period["realm_id"] != p.scope.RealmID {
		return errors.New("RGW committed placement period belongs to another realm")
	}
	periodMap, _ := period["period_map"].(map[string]any)
	groups, _ := periodMap["zonegroups"].([]any)
	for _, value := range groups {
		group, ok := value.(map[string]any)
		if !ok || group["id"] != p.scope.GroupID || group["realm_id"] != p.scope.RealmID {
			continue
		}
		if !reflect.DeepEqual(groupTarget(group, p.config.Name), groupTarget(p.group, p.config.Name)) {
			return errors.New("RGW placement target is not published in the local committed period")
		}
		zones, _ := group["zones"].([]any)
		for _, value := range zones {
			zone, ok := value.(map[string]any)
			if ok && zone["id"] == p.scope.ZoneID {
				return nil
			}
		}
		return errors.New("RGW owned placement zone is absent from the local committed period")
	}
	return errors.New("RGW owned placement zonegroup is absent from the local committed period")
}

func restartScope(ctx context.Context, g *gwbase.Core, scope gwbase.NativeScope) error {
	for _, resource := range []struct {
		kind, id string
	}{{"zonegroup", scope.GroupID}, {"zone", scope.ZoneID}} {
		data, err := g.AdminCommand(ctx, resource.kind, "get")
		if err != nil {
			return err
		}
		native, err := gwbase.DecodeObject(data)
		if err != nil {
			return err
		}
		if native["id"] != resource.id || native["realm_id"] != scope.RealmID {
			return servicehook.Invalid(errors.New("RGW startup scope resolves to different native defaults; refusing gateway restart"))
		}
	}
	return nil
}

// validateHandle refuses a gateway that no longer admits admin commands or a
// placement of another cluster.
func validateHandle(g *gwbase.Core, p *Placement) error {
	if err := g.ValidateAdmin(); err != nil {
		return err
	}
	if p == nil || !g.SameCluster(gwbase.OriginOf(p)) {
		return servicehook.Invalid(errors.New("RGW placement is not owned by this cluster"))
	}
	return nil
}

func checkResources(ctx context.Context, g *gwbase.Core, p *Placement) error {
	scope, err := g.RuntimeScope(ctx)
	if err != nil {
		return err
	}
	if scope != p.scope {
		return errors.New("RGW gateway's native placement scope changed")
	}
	states, err := servicehook.PoolStates(ctx, g.Cluster())
	if err != nil {
		return err
	}
	for name, id := range p.poolIDs {
		if !slices.ContainsFunc(states, func(state ceph.PoolState) bool { return state.Name == name && state.ID == id }) {
			return fmt.Errorf("RGW placement pool %q disappeared or was replaced", name)
		}
	}
	return nil
}

func nativeScope(ctx context.Context, g *gwbase.Core, scope gwbase.NativeScope) (map[string]any, map[string]any, error) {
	data, err := g.Command(ctx, scope, "zonegroup", "get")
	if err != nil {
		return nil, nil, err
	}
	group, err := gwbase.DecodeObject(data)
	if err != nil {
		return nil, nil, err
	}
	data, err = g.Command(ctx, scope, "zone", "get")
	if err != nil {
		return nil, nil, err
	}
	zone, err := gwbase.DecodeObject(data)
	if err != nil {
		return nil, nil, err
	}
	if group["id"] != scope.GroupID || zone["id"] != scope.ZoneID || group["realm_id"] != scope.RealmID || zone["realm_id"] != scope.RealmID {
		return nil, nil, errors.New("RGW native zone/zonegroup identities changed")
	}
	for _, list := range []struct {
		values any
		key    string
	}{{group["placement_targets"], "name"}, {zone["placement_pools"], "key"}} {
		items, ok := list.values.([]any)
		if !ok {
			return nil, nil, errors.New("RGW native placement listing is missing")
		}
		seen := make(map[string]bool)
		for _, value := range items {
			item, ok := value.(map[string]any)
			if !ok {
				return nil, nil, errors.New("RGW native placement listing is malformed")
			}
			name, ok := item[list.key].(string)
			if !ok || name == "" || seen[name] {
				return nil, nil, errors.New("RGW native placement listing has an invalid or duplicate name")
			}
			seen[name] = true
		}
	}
	zones, ok := group["zones"].([]any)
	if !ok || !slices.ContainsFunc(zones, func(value any) bool { item, ok := value.(map[string]any); return ok && item["id"] == scope.ZoneID }) {
		return nil, nil, errors.New("RGW native zonegroup no longer contains the gateway zone")
	}
	return group, zone, nil
}

func groupTarget(group map[string]any, name string) map[string]any {
	targets, _ := group["placement_targets"].([]any)
	for _, value := range targets {
		target, ok := value.(map[string]any)
		if ok && target["name"] == name {
			return target
		}
	}
	return nil
}

func zonePlacement(zone map[string]any, name string) map[string]any {
	pools, _ := zone["placement_pools"].([]any)
	for _, value := range pools {
		pool, ok := value.(map[string]any)
		if ok && pool["key"] == name {
			info, _ := pool["val"].(map[string]any)
			return info
		}
	}
	return nil
}

func snapshot(p *Placement, group, zone map[string]any) (State, error) {
	target, mapping := groupTarget(group, p.config.Name), zonePlacement(zone, p.config.Name)
	if target == nil || mapping == nil {
		return State{}, errors.New("RGW placement has no complete zonegroup target and zone mapping")
	}
	tags, err := nativeTags(target["tags"])
	if err != nil || !slices.Equal(tags, p.config.Tags) {
		return State{}, errors.New("RGW placement target tags differ from its creation policy")
	}
	classes, ok := mapping["storage_classes"].(map[string]any)
	if !ok {
		return State{}, errors.New("RGW placement storage classes are missing")
	}
	listed, ok := target["storage_classes"].([]any)
	if !ok || len(listed) != len(p.config.StorageClasses) || len(classes) != len(p.config.StorageClasses) {
		return State{}, errors.New("RGW placement storage class set differs from its creation policy")
	}
	if mapping["index_pool"] != p.config.IndexPool || mapping["data_extra_pool"] != p.config.DataExtraPool {
		return State{}, errors.New("RGW placement metadata pools changed")
	}
	inline := true
	if p.config.InlineData != nil {
		inline = *p.config.InlineData
	}
	if mapping["inline_data"] != inline {
		return State{}, errors.New("RGW placement inline data policy changed")
	}
	for _, class := range p.config.StorageClasses {
		info, ok := classes[class.Name].(map[string]any)
		if !ok || info["data_pool"] != class.DataPool || !slices.Contains(listed, any(class.Name)) {
			return State{}, errors.New("RGW placement data pool or storage class changed")
		}
	}
	config, _ := normalizeConfig(p.config)
	apiName, ok := group["api_name"].(string)
	if !ok || apiName == "" {
		return State{}, errors.New("RGW zonegroup S3 API name is missing")
	}
	defaultName, ok := group["default_placement"].(string)
	if !ok || defaultName == "" {
		return State{}, errors.New("RGW zonegroup default placement is missing")
	}
	return State{Config: config, RealmID: p.scope.RealmID, ZonegroupID: p.scope.GroupID, ZoneID: p.scope.ZoneID, LocationConstraint: apiName + ":" + p.config.Name, DefaultPlacement: defaultName, Confirmed: p.confirmed}, nil
}

// Period changes are compared structurally after inserting only the new target
// into the previous map. Staging/committed period identifiers naturally differ.
func periodPolicy(period map[string]any) map[string]any {
	data, _ := json.Marshal(period)
	policy, _ := gwbase.DecodeObject(data)
	// These fields describe a native fork/commit's lineage rather than policy.
	for _, key := range []string{"id", "epoch", "realm_epoch", "predecessor_uuid"} {
		delete(policy, key)
	}
	if periodMap, ok := policy["period_map"].(map[string]any); ok {
		delete(periodMap, "id")
	}
	return policy
}

func periodChange(previous, next map[string]any, groupID string, target map[string]any) error {
	for _, key := range []string{"realm_id", "master_zone", "master_zonegroup", "period_config"} {
		if !reflect.DeepEqual(previous[key], next[key]) {
			return errors.New("RGW staging period changes unrelated realm policy or metadata master")
		}
	}
	expectedPolicy, actualPolicy := periodPolicy(previous), periodPolicy(next)
	expected, expectedOK := expectedPolicy["period_map"].(map[string]any)
	_, actualOK := actualPolicy["period_map"].(map[string]any)
	if !expectedOK || !actualOK || target == nil {
		return errors.New("RGW period map or owned placement target is missing")
	}
	groups, ok := expected["zonegroups"].([]any)
	if !ok {
		return errors.New("RGW previous period has no zonegroups")
	}
	found := false
	for _, value := range groups {
		group, ok := value.(map[string]any)
		if !ok || group["id"] != groupID {
			continue
		}
		found = true
		targets, ok := group["placement_targets"].([]any)
		if !ok {
			return errors.New("RGW previous period zonegroup has no placements")
		}
		// Reapplying a committed policy can retain the identical target.
		var replaced bool
		for i, value := range targets {
			old, ok := value.(map[string]any)
			if ok && old["name"] == target["name"] {
				targets[i] = target
				replaced = true
			}
		}
		if !replaced {
			targets = append(targets, target)
		}
		slices.SortFunc(targets, func(a, b any) int {
			left, _ := a.(map[string]any)
			right, _ := b.(map[string]any)
			return strings.Compare(fmt.Sprint(left["name"]), fmt.Sprint(right["name"]))
		})
		group["placement_targets"] = targets
	}
	if !found || !reflect.DeepEqual(expectedPolicy, actualPolicy) {
		return errors.New("RGW staging period contains unrelated topology or placement changes; refusing publication")
	}
	return nil
}

func validateExistingPool(name string) error {
	if !naming.ExistingPool.MatchString(name) {
		return servicehook.Invalid(errors.New("invalid native pool name"))
	}
	return nil
}
