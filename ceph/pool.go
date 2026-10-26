package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

// PoolConfig describes a pool in the disposable, file-backed cluster.
// Zero PGNum selects 8 PGs with autoscaling disabled. A nil ErasureCode creates
// a replicated pool, with the cluster's replica/min-size defaults when omitted
// (normally 2/1, or 1/1 for one initial OSD). Application
// can be rados, rbd, cephfs, rgw or a custom name; empty leaves it unassigned.
type PoolConfig struct {
	Name        string
	PGNum       int
	Replicas    int
	MinSize     int
	Application string
	// FailureDomain selects distinct osd, host or rack buckets. Empty uses osd.
	FailureDomain string
	// CRUSHRoot selects the hierarchy root; empty uses the cluster's default
	// root, which is default unless WithDefaultCRUSHRoot selects another.
	// DeviceClass optionally limits placement to OSDs explicitly configured
	// with that class.
	CRUSHRoot   string
	DeviceClass string
	ErasureCode *ErasureCodeConfig
	// NoHashPSPool clears the pool's hashpspool flag. Ceph then computes each
	// PG's placement seed with the legacy additive formula, the PG number plus
	// the pool ID, instead of hashing the PG number with the pool ID, as for
	// pools that predate the flag. It is set right after the pool is created,
	// before any data is written.
	NoHashPSPool bool
}

// ErasureCodeConfig selects data (K) and parity (M) shards on separate failure
// domains using PoolConfig's placement fields. The fixture uses the portable
// Jerasure reed_sol_van plugin. Replicas must be zero; zero MinSize selects K+1. An explicit
// MinSize of K permits I/O without any spare shard during a failure.
// AllowOverwrites enables partial object writes and is required for RBD and
// CephFS data pools. Their metadata must remain in a replicated pool.
type ErasureCodeConfig struct {
	K               int
	M               int
	AllowOverwrites bool
}

// Pool identifies the pool, CRUSH rule and optional EC profile created for a
// fixture. PoolConfig contains the resolved defaults. Cluster termination
// disposes of its data together with the rest of the cluster.
//
// The operations on a pool are methods of its handle. The handle identifies
// the native pool that it was created for or looked up as, by name and native
// pool ID, so a later pool that reuses the name is not reached through it.
// Copies of a Pool share that identity. A Pool that does not come from
// Container.CreatePool or Container.Pool has no cluster and its operations
// return an error.
type Pool struct {
	PoolConfig
	CRUSHRule          string
	ErasureCodeProfile string
	owner              *Container
	ident              *poolIdentity
}

// poolIdentity is what the copies of one Pool handle share: the native pool
// ID once it is known and whether the handle removed the pool. The owner's
// c.mu guards it, because every operation on a Pool holds that lock.
type poolIdentity struct {
	// bound is false only for a handle that CreatePool returned with an
	// error, before it could read the pool's native ID.
	bound   bool
	id      int64
	removed bool
}

// CreatePool creates a new pool with explicit placement and replication settings.
// It validates the entire config and requires enough eligible owned domains before sending
// any Ceph command. Existing pools, profiles and rules are rejected rather than
// changed. A non-nil Pool returned with an error identifies resources whose
// creation was attempted; inspect them or terminate the disposable cluster.
// That Pool may not know the native ID of its pool yet: it then binds to the
// pool of its name at its first successful operation, so use it only to clean
// up. A Pool returned without an error is always bound to its native pool.
// It never deletes data after a partial command failure. Use WaitForClean before
// application I/O. For RBD, create images in a replicated metadata pool and pass
// the EC pool as data_pool in the native client (or --data-pool to the RBD CLI).
func (c *Container) CreatePool(ctx context.Context, config PoolConfig) (*Pool, error) {
	if gateErr := c.sharedGate("CreatePool"); gateErr != nil {
		return nil, gateErr
	}
	config = resolvePoolDefaults(c.settings, config)
	config, err := normalizePoolConfig(config)
	if err != nil {
		return nil, fmt.Errorf("configure pool: %w", err)
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	if c.Container == nil {
		return nil, errors.New("ceph control container is unavailable")
	}
	domains := c.poolPlacementDomains(config)
	needed := config.Replicas
	if config.ErasureCode != nil {
		needed = config.ErasureCode.K + config.ErasureCode.M
	}
	if domains < needed {
		return nil, fmt.Errorf("pool %q needs %d distinct %s domains in root %q with device class %q; cluster owns %d", config.Name, needed, config.FailureDomain, config.CRUSHRoot, config.DeviceClass, domains)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	pool := &Pool{PoolConfig: config, CRUSHRule: "tc-" + config.Name + "-replicated", owner: c, ident: &poolIdentity{}}
	if config.ErasureCode != nil {
		pool.CRUSHRule = "tc-" + config.Name + "-ec"
		pool.ErasureCodeProfile = pool.CRUSHRule
	}
	checks := []struct {
		name string
		args []string
	}{
		{pool.Name, []string{"osd", "pool", "ls", "--format", "json"}},
		{pool.CRUSHRule, []string{"osd", "crush", "rule", "ls", "--format", "json"}},
	}
	if pool.ErasureCodeProfile != "" {
		checks = append(checks, struct {
			name string
			args []string
		}{pool.ErasureCodeProfile, []string{"osd", "erasure-code-profile", "ls", "--format", "json"}})
	}
	for _, check := range checks {
		data, err := c.cephCLI(ctx, check.args...)
		if err != nil {
			return nil, fmt.Errorf("check existing pool resources: %w", err)
		}
		var names []string
		if err := json.Unmarshal(data, &names); err != nil {
			return nil, fmt.Errorf("decode pool resource names: %w", err)
		}
		if slices.Contains(names, check.name) {
			return nil, invalid(fmt.Errorf("pool resource %q already exists", check.name))
		}
	}
	pg := strconv.Itoa(config.PGNum)
	var commands [][]string
	if ec := config.ErasureCode; ec != nil {
		profile := []string{"osd", "erasure-code-profile", "set", pool.ErasureCodeProfile,
			"plugin=jerasure", "technique=reed_sol_van", "k=" + strconv.Itoa(ec.K), "m=" + strconv.Itoa(ec.M),
			"crush-root=" + config.CRUSHRoot, "crush-failure-domain=" + config.FailureDomain}
		if config.DeviceClass != "" {
			profile = append(profile, "crush-device-class="+config.DeviceClass)
		}
		commands = append(commands,
			profile,
			[]string{"osd", "crush", "rule", "create-erasure", pool.CRUSHRule, pool.ErasureCodeProfile},
			[]string{"osd", "pool", "create", pool.Name, pg, pg, "erasure", pool.ErasureCodeProfile, pool.CRUSHRule, "--autoscale-mode=off"},
		)
		if ec.AllowOverwrites {
			commands = append(commands, []string{"osd", "pool", "set", pool.Name, "allow_ec_overwrites", "true"})
		}
	} else {
		rule := replicatedCRUSHRuleCommand(config, pool.CRUSHRule)
		commands = append(commands,
			rule,
			[]string{"osd", "pool", "create", pool.Name, pg, pg, "replicated", pool.CRUSHRule, "--autoscale-mode=off"},
		)
		size := []string{"osd", "pool", "set", pool.Name, "size", strconv.Itoa(config.Replicas)}
		if config.Replicas == 1 {
			// mon_allow_pool_size_one is enabled only in this disposable fixture.
			size = append(size, "--yes-i-really-mean-it")
		}
		commands = append(commands, size)
	}
	commands = append(commands, []string{"osd", "pool", "set", pool.Name, "min_size", strconv.Itoa(config.MinSize)})
	if config.NoHashPSPool {
		commands = append(commands, []string{"osd", "pool", "set", pool.Name, "hashpspool", "false", "--yes-i-really-mean-it"})
	}
	if config.Application != "" {
		commands = append(commands, []string{"osd", "pool", "application", "enable", pool.Name, config.Application})
	}
	for _, args := range commands {
		if _, err := c.cephCLI(ctx, args...); err != nil {
			return pool, fmt.Errorf("create pool %q: %w", pool.Name, err)
		}
	}
	// A successful result always identifies its native pool. If the ID cannot
	// be read, the pool exists but no handle may claim it by name later, so
	// the result is an error that still carries the handle for cleanup.
	state, err := c.poolState(ctx, pool.Name)
	if err != nil {
		return pool, fmt.Errorf("pool %q was created but its native ID could not be read; the handle is not bound to the pool and binds to the pool of that name at its first successful operation: %w", pool.Name, err)
	}
	pool.ident.bound, pool.ident.id = true, state.ID
	return pool, nil
}

func replicatedCRUSHRuleCommand(config PoolConfig, rule string) []string {
	args := []string{"osd", "crush", "rule", "create-replicated", rule, config.CRUSHRoot, config.FailureDomain}
	if config.DeviceClass != "" {
		args = append(args, config.DeviceClass)
	}
	return args
}

// poolPlacementDomains is called while c.mu serializes topology mutations.
func (c *Container) poolPlacementDomains(config PoolConfig) int {
	domains := make(map[string]struct{})
	for _, osd := range c.osds {
		if osd.purged {
			continue
		}
		placement := osd.Placement()
		root := placement.Root
		if root == "" {
			root = "default"
		}
		if root != config.CRUSHRoot || (config.DeviceClass != "" && placement.DeviceClass != config.DeviceClass) {
			continue
		}
		var domain string
		switch config.FailureDomain {
		case "osd":
			domain = strconv.Itoa(osd.ID)
		case "host":
			domain = placement.Host
		case "rack":
			domain = placement.Rack
		}
		if domain != "" {
			domains[domain] = struct{}{}
		}
	}
	return len(domains)
}

var poolResourceName = naming.PoolResource

func normalizePoolConfig(config PoolConfig) (PoolConfig, error) {
	if len(config.Name) > 128 || !poolResourceName.MatchString(config.Name) {
		return config, invalid(errors.New("pool name must use letters, digits, underscores, dots or dashes and cannot start with a dot or dash"))
	}
	if config.Application != "" && (len(config.Application) > 128 || !poolResourceName.MatchString(config.Application)) {
		return config, invalid(errors.New("pool application must use letters, digits, underscores, dots or dashes"))
	}
	if config.FailureDomain == "" {
		config.FailureDomain = "osd"
	}
	if config.FailureDomain != "osd" && config.FailureDomain != "host" && config.FailureDomain != "rack" {
		return config, invalid(errors.New("pool failure domain must be osd, host or rack"))
	}
	if config.CRUSHRoot == "" {
		config.CRUSHRoot = "default"
	}
	if len(config.CRUSHRoot) > 128 || !poolResourceName.MatchString(config.CRUSHRoot) {
		return config, invalid(errors.New("pool CRUSH root must use letters, digits, underscores, dots or dashes"))
	}
	if config.DeviceClass != "" && (len(config.DeviceClass) > 128 || !poolResourceName.MatchString(config.DeviceClass)) {
		return config, invalid(errors.New("pool device class must use letters, digits, underscores, dots or dashes"))
	}
	if config.PGNum == 0 {
		config.PGNum = 8
	}
	if config.PGNum < 1 || uint64(config.PGNum) > uint64(^uint32(0)) {
		return config, invalid(errors.New("pool PG count must be a positive 32-bit integer"))
	}
	if config.ErasureCode == nil {
		if config.Replicas == 0 {
			config.Replicas = 2
		}
		if config.MinSize == 0 {
			config.MinSize = 1
		}
		if config.Replicas < 1 || config.Replicas > 10 {
			return config, invalid(errors.New("pool replicas must be between 1 and 10"))
		}
		if config.MinSize < 1 || config.MinSize > config.Replicas {
			return config, invalid(errors.New("replicated pool min size must be between 1 and replicas"))
		}
		return config, nil
	}
	ec := *config.ErasureCode
	config.ErasureCode = &ec
	if config.Replicas != 0 {
		return config, invalid(errors.New("erasure-coded pool cannot set replicas; use K and M"))
	}
	// Ceph represents the shard set with 128 entries; bound both terms before
	// adding them so even an oversized caller value cannot overflow an int.
	if ec.K < 2 || ec.M < 1 || ec.K > 127 || ec.M > 126 || ec.K+ec.M > 128 {
		return config, invalid(errors.New("erasure code requires K >= 2, M >= 1 and K+M <= 128"))
	}
	if config.MinSize == 0 {
		config.MinSize = ec.K + 1
	}
	if config.MinSize < ec.K || config.MinSize > ec.K+ec.M {
		return config, invalid(errors.New("erasure-coded pool min size must be between K and K+M"))
	}
	if (config.Application == "rbd" || config.Application == "cephfs") && !ec.AllowOverwrites {
		return config, invalid(errors.New("RBD and CephFS erasure-coded data pools require AllowOverwrites"))
	}
	return config, nil
}
