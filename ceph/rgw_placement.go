package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
)

// RGWStorageClassConfig maps one native S3 storage class to an existing data
// pool. STANDARD is required. Other class names, for example STANDARD_IA, must
// also be accepted by the consumer's S3 SDK. Data pools may be replicated or EC;
// RGW does not require EC overwrites because it writes complete objects.
type RGWStorageClassConfig struct {
	Name, DataPool string
}

// RGWPlacementConfig adds a fresh, non-default bucket placement target. IndexPool
// and DataExtraPool must be existing replicated pools supporting omap. It never
// changes the zonegroup default or any existing bucket's immutable placement.
// InlineData nil preserves Ceph's true default; false separates object heads
// and data. Native external placement/topology edits must not race these helpers.
type RGWPlacementConfig struct {
	Name, IndexPool, DataExtraPool string
	StorageClasses                 []RGWStorageClassConfig
	InlineData                     *bool
	// Tags restrict creation of new buckets using this target to users with
	// at least one matching placement tag. An empty list permits every user.
	// These tags do not grant access to existing buckets or their objects.
	Tags []string
}

// RGWPlacement identifies a newly created native policy and its exact runtime
// scope. LocationConstraint can be passed to S3 CreateBucket. Creation stores
// configuration; ApplyPlacement restarts a standalone gateway or publishes a
// guarded realm period. Import that period into other clusters before explicitly
// activating their gateways with ReloadPlacement. Failed creation may return a
// partial handle for inspection; it is never rolled back by deleting pools,
// policies, buckets or data.
type RGWPlacement struct {
	Name, RealmID, ZonegroupID, ZoneID, LocationConstraint string
	owner                                                  *Container
	scope                                                  rgwPlacementScope
	config                                                 RGWPlacementConfig
	poolIDs                                                map[string]int64
	group, zone, previousPeriod                            map[string]any
	confirmed                                              bool
}

func (p RGWPlacement) String() string   { return "RGW placement " + p.Name }
func (p RGWPlacement) GoString() string { return p.String() }

// RGWPlacementState is a native policy snapshot. DefaultPlacement is the
// zonegroup default, which these helpers never modify. Creation and activation
// do not assign any user's default placement or storage class.
type RGWPlacementState struct {
	RGWPlacementConfig
	RealmID, ZonegroupID, ZoneID, LocationConstraint, DefaultPlacement string
	// Confirmed reports successful creation/readback of this owned handle.
	// It does not assert that a gateway reloaded the policy or a realm published it.
	Confirmed bool
}

type rgwPlacementScope struct {
	realmID, groupID, zoneID, realm, group, zone string
}

var rgwStorageClassName = regexp.MustCompile(`^[A-Z][A-Z0-9_-]{0,127}$`)

var errRGWPlacementScopePending = errors.New("running RGW gateway has not registered its native scope")

func normalizeRGWPlacementConfig(config RGWPlacementConfig) (RGWPlacementConfig, error) {
	if !poolResourceName.MatchString(config.Name) || len(config.Name) > 128 {
		return config, errors.New("invalid RGW placement name")
	}
	if config.Name == "default-placement" {
		return config, errors.New("RGW placement creation requires a new non-default name")
	}
	for _, pool := range []string{config.IndexPool, config.DataExtraPool} {
		if err := validateExistingPoolName(pool); err != nil {
			return config, err
		}
	}
	config.StorageClasses = slices.Clone(config.StorageClasses)
	var err error
	config.Tags, err = normalizeRGWPlacementTags(config.Tags)
	if err != nil {
		return config, err
	}
	seen := make(map[string]bool)
	for _, class := range config.StorageClasses {
		if !rgwStorageClassName.MatchString(class.Name) || seen[class.Name] {
			return config, errors.New("RGW storage classes must have distinct uppercase names")
		}
		if err := validateExistingPoolName(class.DataPool); err != nil {
			return config, err
		}
		seen[class.Name] = true
	}
	if !seen["STANDARD"] {
		return config, errors.New("RGW placement requires a STANDARD data pool")
	}
	slices.SortFunc(config.StorageClasses, func(a, b RGWStorageClassConfig) int {
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

// CreatePlacement creates a named target and all its storage classes in this
// gateway's actual running zone/zonegroup, even if global native defaults changed
// after it started. Existing targets in either scope are rejected. Every pool
// must already exist; native IDs are rechecked before each mutation. Changes are
// sequential and partial failure leaves inspectable configuration. On a realm,
// all participating zones need compatible local mappings before publication.
func (g *RGWContainer) CreatePlacement(ctx context.Context, config RGWPlacementConfig) (*RGWPlacement, error) {
	config, err := normalizeRGWPlacementConfig(config)
	if err != nil {
		return nil, err
	}
	if g == nil || g.owner == nil {
		return nil, errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	scope, err := g.placementRuntimeScope(ctx)
	if err != nil {
		return nil, err
	}
	group, zone, err := g.placementNativeScope(ctx, scope)
	if err != nil {
		return nil, err
	}
	if rgwPlacementTarget(group, config.Name) != nil || rgwZonePlacement(zone, config.Name) != nil {
		return nil, fmt.Errorf("RGW placement %q already exists in its zonegroup or zone", config.Name)
	}
	defaultName, _ := group["default_placement"].(string)
	if defaultName == "" {
		return nil, errors.New("RGW zonegroup has no default placement; refusing implicit default mutation")
	}
	states, err := g.owner.poolStates(ctx)
	if err != nil {
		return nil, err
	}
	pools := make(map[string]PoolState)
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
	p := &RGWPlacement{Name: config.Name, RealmID: scope.realmID, ZonegroupID: scope.groupID, ZoneID: scope.zoneID,
		LocationConstraint: fmt.Sprint(group["api_name"]) + ":" + config.Name, owner: g.owner, scope: scope, config: config, poolIDs: poolIDs}
	if apiName, ok := group["api_name"].(string); !ok || apiName == "" {
		return nil, errors.New("RGW zonegroup has no S3 API name")
	}
	if scope.realmID != "" {
		data, err := g.placementCommand(ctx, scope, "period", "get")
		if err != nil {
			return nil, err
		}
		p.previousPeriod, err = decodeRGWPlacementObject(data)
		if err != nil {
			return nil, err
		}
		if p.previousPeriod["realm_id"] != scope.realmID {
			return nil, errors.New("RGW current period belongs to a different realm")
		}
	}
	for _, class := range config.StorageClasses {
		if err := g.checkPlacementResources(ctx, p); err != nil {
			return p, err
		}
		groupArgs := []string{"zonegroup", "placement", "add", "--placement-id", config.Name, "--storage-class", class.Name}
		if len(config.Tags) > 0 {
			groupArgs = append(groupArgs, "--tags", strings.Join(config.Tags, ","))
		}
		if _, err := g.placementCommand(ctx, scope, groupArgs...); err != nil {
			return p, err
		}
		if err := g.checkPlacementResources(ctx, p); err != nil {
			return p, err
		}
		args := []string{"zone", "placement", "add", "--placement-id", config.Name, "--storage-class", class.Name, "--data-pool", class.DataPool,
			"--index-pool", config.IndexPool, "--data-extra-pool", config.DataExtraPool}
		if config.InlineData != nil {
			args = append(args, "--placement-inline-data="+strconv.FormatBool(*config.InlineData))
		}
		if _, err := g.placementCommand(ctx, scope, args...); err != nil {
			return p, err
		}
	}
	p.group, p.zone, err = g.placementNativeScope(ctx, scope)
	if err != nil {
		return p, err
	}
	if p.group["default_placement"] != group["default_placement"] {
		return p, errors.New("RGW default placement changed during policy creation")
	}
	if _, err := rgwPlacementState(p, p.group, p.zone); err != nil {
		return p, err
	}
	p.confirmed = true
	return p, nil
}

// PlacementStatus reads the exact native scope of a created or partial policy.
// Incomplete or changed mappings return an error; use Admin for detailed partial
// inspection. It does not claim a running gateway reloaded this configuration.
func (g *RGWContainer) PlacementStatus(ctx context.Context, p *RGWPlacement) (RGWPlacementState, error) {
	if g == nil || g.owner == nil {
		return RGWPlacementState{}, errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if err := g.validatePlacementHandle(p); err != nil {
		return RGWPlacementState{}, err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	if err := g.checkPlacementResources(ctx, p); err != nil {
		return RGWPlacementState{}, err
	}
	group, zone, err := g.placementNativeScope(ctx, p.scope)
	if err != nil {
		return RGWPlacementState{}, err
	}
	return rgwPlacementState(p, group, zone)
}

// ApplyPlacement activates a confirmed new policy. Standalone mode explicitly
// restarts only this owned gateway; resolve S3Endpoint again because Docker can
// assign a new mapped host port. Other gateways in the same zone require their
// own ApplyPlacement. Realm mode requires the current metadata master and commits
// a new period only when its topology differs solely by this new placement.
// Coordinate mappings in other zones before publishing a realm policy, import
// the committed period into those clusters and call ReloadPlacement there. It never
// changes defaults, deletes resources or silently rolls back a partial failure.
func (g *RGWContainer) ApplyPlacement(ctx context.Context, p *RGWPlacement) error {
	if g == nil || g.owner == nil {
		return errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if err := g.validatePlacementHandle(p); err != nil {
		return err
	}
	if !p.confirmed {
		return errors.New("RGW placement creation has not been confirmed")
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	if err := g.checkPlacementResources(ctx, p); err != nil {
		return err
	}
	group, zone, err := g.placementNativeScope(ctx, p.scope)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(group, p.group) || !reflect.DeepEqual(zone, p.zone) {
		return errors.New("RGW zone or zonegroup changed since placement creation; refusing activation")
	}
	if p.scope.realmID == "" {
		// An unscoped gateway originally chose native defaults at startup.
		// Restarting it after another actor changes those pointers could serve
		// another zone, despite the policy edits having used correct live IDs.
		if err := g.placementRestartScope(ctx, p.scope); err != nil {
			return err
		}
		grace := 3 * time.Second
		if err := g.Stop(ctx, &grace); err != nil {
			return fmt.Errorf("stop RGW to apply placement: %w", err)
		}
		if err := g.Start(ctx); err != nil {
			return fmt.Errorf("restart RGW to apply placement: %w", err)
		}
		return nil
	}
	data, err := g.placementCommand(ctx, p.scope, "period", "get")
	if err != nil {
		return err
	}
	current, err := decodeRGWPlacementObject(data)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, p.previousPeriod) {
		return errors.New("RGW current period changed since placement creation; refusing publication")
	}
	if current["master_zone"] != p.scope.zoneID {
		return errors.New("RGW placement period publication requires its realm metadata master")
	}
	pending, err := g.placementStagingPeriod(ctx, p.scope)
	if err != nil {
		return err
	}
	if pending != nil && !reflect.DeepEqual(rgwPlacementPeriodPolicy(current), rgwPlacementPeriodPolicy(pending)) {
		if err := rgwPlacementPeriodChange(current, pending, p.scope.groupID, rgwPlacementTarget(group, p.config.Name)); err != nil {
			return errors.New("RGW existing staging period contains unrelated policy; refusing to overwrite it")
		}
	}
	data, err = g.placementCommand(ctx, p.scope, "period", "update")
	if err != nil {
		return err
	}
	staging, err := decodeRGWPlacementObject(data)
	if err != nil {
		return err
	}
	if err := rgwPlacementPeriodChange(current, staging, p.scope.groupID, rgwPlacementTarget(group, p.config.Name)); err != nil {
		return err
	}
	if _, err := g.placementCommand(ctx, p.scope, "period", "commit"); err != nil {
		return err
	}
	data, err = g.placementCommand(ctx, p.scope, "period", "get")
	if err != nil {
		return err
	}
	committed, err := decodeRGWPlacementObject(data)
	if err != nil {
		return err
	}
	if err := rgwPlacementPeriodChange(current, committed, p.scope.groupID, rgwPlacementTarget(group, p.config.Name)); err != nil {
		return err
	}
	p.previousPeriod = committed
	return nil
}

// ReloadPlacement activates a confirmed policy in this owned gateway through
// restart, including a secondary realm zone. In realm mode the latest locally
// stored committed period must contain the exact owned target and zone. Import
// the published period through the multicluster fixture before reloading another
// cluster. This method never publishes, pulls or rewrites federation topology.
// Resolve S3Endpoint again because Docker may assign a new mapped host port.
func (g *RGWContainer) ReloadPlacement(ctx context.Context, p *RGWPlacement) error {
	if g == nil || g.owner == nil {
		return errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if err := g.validatePlacementHandle(p); err != nil {
		return err
	}
	if !p.confirmed {
		return errors.New("RGW placement creation has not been confirmed")
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	if err := g.checkPlacementResources(ctx, p); err != nil {
		return err
	}
	group, zone, err := g.placementNativeScope(ctx, p.scope)
	if err != nil {
		return err
	}
	if _, err := rgwPlacementState(p, group, zone); err != nil {
		return err
	}
	if p.scope.realmID != "" {
		data, err := g.placementCommand(ctx, p.scope, "period", "get")
		if err != nil {
			return err
		}
		period, err := decodeRGWPlacementObject(data)
		if err != nil {
			return err
		}
		if err := rgwPlacementCommittedTarget(period, p); err != nil {
			return err
		}
	}
	if err := g.placementRestartScope(ctx, p.scope); err != nil {
		return err
	}
	grace := 3 * time.Second
	if err := g.Stop(ctx, &grace); err != nil {
		return fmt.Errorf("stop RGW to reload placement: %w", err)
	}
	if err := g.Start(ctx); err != nil {
		return fmt.Errorf("restart RGW to reload placement: %w", err)
	}
	return nil
}

func rgwPlacementCommittedTarget(period map[string]any, p *RGWPlacement) error {
	id, ok := period["id"].(string)
	if !ok || id == "" || strings.HasSuffix(id, ":staging") {
		return errors.New("RGW placement requires a locally committed period")
	}
	if period["realm_id"] != p.scope.realmID {
		return errors.New("RGW committed placement period belongs to another realm")
	}
	periodMap, _ := period["period_map"].(map[string]any)
	groups, _ := periodMap["zonegroups"].([]any)
	for _, value := range groups {
		group, ok := value.(map[string]any)
		if !ok || group["id"] != p.scope.groupID || group["realm_id"] != p.scope.realmID {
			continue
		}
		if !reflect.DeepEqual(rgwPlacementTarget(group, p.config.Name), rgwPlacementTarget(p.group, p.config.Name)) {
			return errors.New("RGW placement target is not published in the local committed period")
		}
		zones, _ := group["zones"].([]any)
		for _, value := range zones {
			zone, ok := value.(map[string]any)
			if ok && zone["id"] == p.scope.zoneID {
				return nil
			}
		}
		return errors.New("RGW owned placement zone is absent from the local committed period")
	}
	return errors.New("RGW owned placement zonegroup is absent from the local committed period")
}

func (g *RGWContainer) placementRestartScope(ctx context.Context, scope rgwPlacementScope) error {
	for _, resource := range []struct {
		kind, id string
	}{{"zonegroup", scope.groupID}, {"zone", scope.zoneID}} {
		data, err := g.adminCommand(ctx, resource.kind, "get")
		if err != nil {
			return err
		}
		native, err := decodeRGWPlacementObject(data)
		if err != nil {
			return err
		}
		if native["id"] != resource.id || native["realm_id"] != scope.realmID {
			return errors.New("RGW startup scope resolves to different native defaults; refusing gateway restart")
		}
	}
	return nil
}

func (g *RGWContainer) validatePlacementHandle(p *RGWPlacement) error {
	if err := g.validateAdminGateway(); err != nil {
		return err
	}
	if p == nil || p.owner != g.owner {
		return errors.New("RGW placement is not owned by this cluster")
	}
	return nil
}

func (g *RGWContainer) checkPlacementResources(ctx context.Context, p *RGWPlacement) error {
	scope, err := g.placementRuntimeScope(ctx)
	if err != nil {
		return err
	}
	if scope != p.scope {
		return errors.New("RGW gateway's native placement scope changed")
	}
	states, err := g.owner.poolStates(ctx)
	if err != nil {
		return err
	}
	for name, id := range p.poolIDs {
		if !slices.ContainsFunc(states, func(state PoolState) bool { return state.Name == name && state.ID == id }) {
			return fmt.Errorf("RGW placement pool %q disappeared or was replaced", name)
		}
	}
	return nil
}

func (g *RGWContainer) placementRuntimeScope(ctx context.Context) (rgwPlacementScope, error) {
	data, err := command(ctx, g.Container, "hostname")
	if err != nil {
		if ctx.Err() != nil {
			return rgwPlacementScope{}, fmt.Errorf("inspect RGW container hostname: %w", ctx.Err())
		}
		return rgwPlacementScope{}, errors.New("inspect RGW container hostname")
	}
	hostname := strings.TrimSpace(string(data))
	if hostname == "" {
		return rgwPlacementScope{}, errors.New("RGW container hostname is empty")
	}
	frontend := "beast port=" + strconv.Itoa(g.port)
	if g.owner.UsesHostNetwork() {
		frontend = "beast endpoint=" + net.JoinHostPort(g.publicAddress, strconv.Itoa(g.port))
	}
	// Registering to the service map follows starting the HTTP frontend.
	// A successful listener probe can therefore precede the first MON report.
	for {
		data, err = g.owner.Ceph(ctx, "service", "dump", "--format", "json")
		if err != nil {
			return rgwPlacementScope{}, err
		}
		scope, err := decodeRGWPlacementScope(data, hostname, frontend)
		if !errors.Is(err, errRGWPlacementScopePending) {
			return scope, err
		}
		select {
		case <-ctx.Done():
			return rgwPlacementScope{}, fmt.Errorf("wait for native RGW service scope: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func decodeRGWPlacementScope(data []byte, hostname, frontend string) (rgwPlacementScope, error) {
	var native struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return rgwPlacementScope{}, errors.New("decode RGW native service map")
	}
	var found *rgwPlacementScope
	for name, raw := range native.Services["rgw"].Daemons {
		// ServiceMap::dump includes this summary string alongside daemon objects.
		if name == "summary" {
			var summary string
			if err := json.Unmarshal(raw, &summary); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return rgwPlacementScope{}, errors.New("decode RGW native service summary")
			}
			continue
		}
		var daemon struct {
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &daemon); err != nil || daemon.Metadata == nil {
			return rgwPlacementScope{}, errors.New("decode RGW native service daemon")
		}
		meta := daemon.Metadata
		// Host-network containers share hostname and PID 1. Their owned listener
		// distinguishes the gateway even when several realms use one Docker host.
		if meta["hostname"] != hostname || meta["pid"] != "1" || meta["frontend_config#0"] != frontend {
			continue
		}
		scope := rgwPlacementScope{realmID: meta["realm_id"], groupID: meta["zonegroup_id"], zoneID: meta["zone_id"], realm: meta["realm_name"], group: meta["zonegroup_name"], zone: meta["zone_name"]}
		if scope.groupID == "" || scope.zoneID == "" || scope.group == "" || scope.zone == "" {
			return rgwPlacementScope{}, errors.New("incomplete RGW native service scope")
		}
		if found != nil && *found != scope {
			return rgwPlacementScope{}, errors.New("ambiguous RGW native service scope for container")
		}
		found = &scope
	}
	if found == nil {
		return rgwPlacementScope{}, errRGWPlacementScopePending
	}
	return *found, nil
}

func rgwPlacementCommandArgs(scope rgwPlacementScope, args ...string) []string {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json", "--zonegroup-id", scope.groupID, "--zone-id", scope.zoneID}
	if scope.realmID != "" {
		argv = append(argv, "--realm-id", scope.realmID, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0")
	}
	return append(argv, args...)
}

func (g *RGWContainer) placementCommand(ctx context.Context, scope rgwPlacementScope, args ...string) ([]byte, error) {
	data, err := command(ctx, g.Container, rgwPlacementCommandArgs(scope, args...)...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("RGW placement command: %w", ctx.Err())
		}
		return nil, errors.New("RGW placement command failed; native output is redacted")
	}
	return data, nil
}

// Read the exact staging object directly: --epoch avoids a separate latest-epoch
// lookup. The native CLI returns errno 2 only for a missing object; other failures
// must never be interpreted as an empty staging area or leak native credentials.
func (g *RGWContainer) placementStagingPeriod(ctx context.Context, scope rgwPlacementScope) (map[string]any, error) {
	id := scope.realmID + ":staging"
	code, reader, err := g.Container.Exec(ctx, rgwPlacementCommandArgs(scope, "period", "get", "--period", id, "--epoch", "1"))
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("read RGW staging period: %w", ctx.Err())
		}
		return nil, errors.New("read RGW staging period failed; native output is redacted")
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, errors.New("decode RGW staging period stream; native output is redacted")
	}
	if code == 2 && stdout.Len() == 0 {
		return nil, nil
	}
	if code != 0 {
		return nil, errors.New("read RGW staging period failed; native output is redacted")
	}
	period, err := decodeRGWPlacementObject(stdout.Bytes())
	if err != nil {
		return nil, err
	}
	if period["id"] != id || period["realm_id"] != scope.realmID {
		return nil, errors.New("RGW staging period identity differs from the owned realm")
	}
	return period, nil
}

func (g *RGWContainer) placementNativeScope(ctx context.Context, scope rgwPlacementScope) (map[string]any, map[string]any, error) {
	data, err := g.placementCommand(ctx, scope, "zonegroup", "get")
	if err != nil {
		return nil, nil, err
	}
	group, err := decodeRGWPlacementObject(data)
	if err != nil {
		return nil, nil, err
	}
	data, err = g.placementCommand(ctx, scope, "zone", "get")
	if err != nil {
		return nil, nil, err
	}
	zone, err := decodeRGWPlacementObject(data)
	if err != nil {
		return nil, nil, err
	}
	if group["id"] != scope.groupID || zone["id"] != scope.zoneID || group["realm_id"] != scope.realmID || zone["realm_id"] != scope.realmID {
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
	if !ok || !slices.ContainsFunc(zones, func(value any) bool { item, ok := value.(map[string]any); return ok && item["id"] == scope.zoneID }) {
		return nil, nil, errors.New("RGW native zonegroup no longer contains the gateway zone")
	}
	return group, zone, nil
}

func decodeRGWPlacementObject(data []byte) (map[string]any, error) {
	var native map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&native); err != nil || native == nil {
		return nil, errors.New("decode RGW native placement configuration")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("RGW native placement configuration contains trailing output")
	}
	return native, nil
}

func rgwPlacementTarget(group map[string]any, name string) map[string]any {
	targets, _ := group["placement_targets"].([]any)
	for _, value := range targets {
		target, ok := value.(map[string]any)
		if ok && target["name"] == name {
			return target
		}
	}
	return nil
}

func rgwZonePlacement(zone map[string]any, name string) map[string]any {
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

func rgwPlacementState(p *RGWPlacement, group, zone map[string]any) (RGWPlacementState, error) {
	target, mapping := rgwPlacementTarget(group, p.config.Name), rgwZonePlacement(zone, p.config.Name)
	if target == nil || mapping == nil {
		return RGWPlacementState{}, errors.New("RGW placement has no complete zonegroup target and zone mapping")
	}
	tags, err := rgwPlacementNativeTags(target["tags"])
	if err != nil || !slices.Equal(tags, p.config.Tags) {
		return RGWPlacementState{}, errors.New("RGW placement target tags differ from its creation policy")
	}
	classes, ok := mapping["storage_classes"].(map[string]any)
	if !ok {
		return RGWPlacementState{}, errors.New("RGW placement storage classes are missing")
	}
	listed, ok := target["storage_classes"].([]any)
	if !ok || len(listed) != len(p.config.StorageClasses) || len(classes) != len(p.config.StorageClasses) {
		return RGWPlacementState{}, errors.New("RGW placement storage class set differs from its creation policy")
	}
	if mapping["index_pool"] != p.config.IndexPool || mapping["data_extra_pool"] != p.config.DataExtraPool {
		return RGWPlacementState{}, errors.New("RGW placement metadata pools changed")
	}
	inline := true
	if p.config.InlineData != nil {
		inline = *p.config.InlineData
	}
	if mapping["inline_data"] != inline {
		return RGWPlacementState{}, errors.New("RGW placement inline data policy changed")
	}
	for _, class := range p.config.StorageClasses {
		info, ok := classes[class.Name].(map[string]any)
		if !ok || info["data_pool"] != class.DataPool || !slices.Contains(listed, any(class.Name)) {
			return RGWPlacementState{}, errors.New("RGW placement data pool or storage class changed")
		}
	}
	config, _ := normalizeRGWPlacementConfig(p.config)
	apiName, ok := group["api_name"].(string)
	if !ok || apiName == "" {
		return RGWPlacementState{}, errors.New("RGW zonegroup S3 API name is missing")
	}
	defaultName, ok := group["default_placement"].(string)
	if !ok || defaultName == "" {
		return RGWPlacementState{}, errors.New("RGW zonegroup default placement is missing")
	}
	return RGWPlacementState{RGWPlacementConfig: config, RealmID: p.scope.realmID, ZonegroupID: p.scope.groupID, ZoneID: p.scope.zoneID, LocationConstraint: apiName + ":" + p.config.Name, DefaultPlacement: defaultName, Confirmed: p.confirmed}, nil
}

// Period changes are compared structurally after inserting only the new target
// into the previous map. Staging/committed period identifiers naturally differ.
func rgwPlacementPeriodPolicy(period map[string]any) map[string]any {
	data, _ := json.Marshal(period)
	policy, _ := decodeRGWPlacementObject(data)
	// These fields describe a native fork/commit's lineage rather than policy.
	for _, key := range []string{"id", "epoch", "realm_epoch", "predecessor_uuid"} {
		delete(policy, key)
	}
	if periodMap, ok := policy["period_map"].(map[string]any); ok {
		delete(periodMap, "id")
	}
	return policy
}

func rgwPlacementPeriodChange(previous, next map[string]any, groupID string, target map[string]any) error {
	for _, key := range []string{"realm_id", "master_zone", "master_zonegroup", "period_config"} {
		if !reflect.DeepEqual(previous[key], next[key]) {
			return errors.New("RGW staging period changes unrelated realm policy or metadata master")
		}
	}
	expectedPolicy, actualPolicy := rgwPlacementPeriodPolicy(previous), rgwPlacementPeriodPolicy(next)
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
