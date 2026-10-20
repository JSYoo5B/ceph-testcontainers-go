package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"syscall"

	"github.com/testcontainers/testcontainers-go"
)

// sharedBackend is what a SharedCluster needs from one cluster. The
// container backend implements it with the module's unexported paths, which
// the shared gate does not refuse; unit tests replace it with a model.
type sharedBackend interface {
	createPool(ctx context.Context, name string) error
	// prepareServices runs the services' preparation before the baseline.
	prepareServices(ctx context.Context) error
	// ready waits for quorum and active+clean PGs.
	ready(ctx context.Context) error
	// invariants reads the cluster's state except Resources, which
	// resources reads: the services' probes are slower and only the
	// baseline and releases compare them.
	invariants(ctx context.Context, pool string) (sharedInvariants, error)
	resources(ctx context.Context) (map[string]string, error)
	// daemonsRunning fails when a daemon container of the cluster exited.
	daemonsRunning(ctx context.Context) error
	createClient(ctx context.Context, entity string, caps ClientCaps) (*ClientConfig, error)
	deleteClient(ctx context.Context, client *ClientConfig) error
	// purgeNamespace removes every object of one namespace of pool.
	purgeNamespace(ctx context.Context, pool, namespace string) error
	// setSharing sets the gate of the cluster's *Container.
	setSharing(mode sharingMode)
	terminate(ctx context.Context) error
	// cluster returns the container, or nil for a model.
	cluster() *Container
}

type sharedStarter func(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (sharedBackend, error)

// sharedInvariants is the state that a tenant cannot change through its own
// scope. Config values are compared but never reported.
type sharedInvariants struct {
	// Auth maps each entity to its capabilities.
	Auth map[string]string
	// Pools maps each pool name to its policy.
	Pools     map[string]string
	OSDFlags  []string
	Config    map[string]string
	Blocklist []string
	// Health lists the codes of the unmuted health checks.
	Health []string
	// Namespaces counts the objects of each namespace of SharedPool.
	Namespaces map[string]int
	// Resources maps the service resources that the registered probes list
	// to the tenant ID, or other name, that owns them.
	Resources map[string]string
	// Unsettled says why the cluster cannot admit a tenant yet, such as PGs
	// that are not all active+clean; it is empty when it can.
	Unsettled string
}

// tenantOwns reports whether entity is one of the clients that a tenant
// names after its ID.
func tenantOwns(id, entity string) bool {
	return entity == "client."+id || strings.HasPrefix(entity, "client."+id+"-")
}

// admissionDiff lists the conditions that admit a tenant and differ from
// the baseline: health, OSD flags and the blocklist.
func (b sharedInvariants) admissionDiff(current sharedInvariants) []string {
	var problems []string
	if current.Unsettled != "" {
		problems = append(problems, current.Unsettled)
	}
	if !slices.Equal(b.Health, current.Health) {
		problems = append(problems, fmt.Sprintf("health checks are %v, not %v", current.Health, b.Health))
	}
	if !slices.Equal(b.OSDFlags, current.OSDFlags) {
		problems = append(problems, fmt.Sprintf("OSD flags are %v, not %v", current.OSDFlags, b.OSDFlags))
	}
	if !slices.Equal(b.Blocklist, current.Blocklist) {
		problems = append(problems, fmt.Sprintf("the blocklist has %d entries, not %d", len(current.Blocklist), len(b.Blocklist)))
	}
	return problems
}

// releaseDiff lists what differs from the baseline after tenant self was
// cleaned up, ignoring the scopes of the other tenants. lagging reports a
// health difference, which can still settle.
func (b sharedInvariants) releaseDiff(current sharedInvariants, self string, others map[string]bool) (problems []string, lagging bool) {
	ownedByOther := func(entity string) bool {
		for id := range others {
			if tenantOwns(id, entity) {
				return true
			}
		}
		return false
	}
	for _, entity := range sortedKeys(current.Auth) {
		caps, known := b.Auth[entity]
		switch {
		case tenantOwns(self, entity):
			problems = append(problems, "tenant client "+entity+" remains")
		case ownedByOther(entity):
		case !known:
			problems = append(problems, "auth entity "+entity+" was added")
		case caps != current.Auth[entity]:
			problems = append(problems, "capabilities of "+entity+" changed")
		}
	}
	for _, entity := range sortedKeys(b.Auth) {
		if _, ok := current.Auth[entity]; !ok {
			problems = append(problems, "auth entity "+entity+" was removed")
		}
	}
	problems = append(problems, mapDiff("pool", b.Pools, current.Pools)...)
	problems = append(problems, mapDiff("config entry", b.Config, current.Config)...)
	if !slices.Equal(b.OSDFlags, current.OSDFlags) {
		problems = append(problems, fmt.Sprintf("OSD flags are %v, not %v", current.OSDFlags, b.OSDFlags))
	}
	if !slices.Equal(b.Blocklist, current.Blocklist) {
		problems = append(problems, fmt.Sprintf("the blocklist has %d entries, not %d", len(current.Blocklist), len(b.Blocklist)))
	}
	for _, namespace := range sortedKeys(current.Namespaces) {
		count := current.Namespaces[namespace]
		switch {
		case namespace == self:
			problems = append(problems, fmt.Sprintf("namespace %s of pool %s still has %d objects", namespace, SharedPool, count))
		case others[namespace]:
		case count != b.Namespaces[namespace]:
			problems = append(problems, fmt.Sprintf("namespace %q of pool %s has %d objects, not %d", namespace, SharedPool, count, b.Namespaces[namespace]))
		}
	}
	for _, namespace := range sortedKeys(b.Namespaces) {
		if _, ok := current.Namespaces[namespace]; !ok {
			problems = append(problems, fmt.Sprintf("namespace %q of pool %s lost its objects", namespace, SharedPool))
		}
	}
	problems = append(problems, resourceDiff(b.Resources, current.Resources, self, others)...)
	return problems, !slices.Equal(b.Health, current.Health)
}

// mapDiff names the keys added, removed or changed, never the values.
func mapDiff(kind string, baseline, current map[string]string) []string {
	var problems []string
	for _, key := range sortedKeys(current) {
		before, ok := baseline[key]
		switch {
		case !ok:
			problems = append(problems, kind+" "+key+" was added")
		case before != current[key]:
			problems = append(problems, kind+" "+key+" changed")
		}
	}
	for _, key := range sortedKeys(baseline) {
		if _, ok := current[key]; !ok {
			problems = append(problems, kind+" "+key+" was removed")
		}
	}
	return problems
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

// sharedIgnoredHealth lists the health checks that report the Docker host
// rather than the cluster: the free space of the engine's disk flaps around
// the MON's warning threshold whatever the tenants do.
var sharedIgnoredHealth = map[string]bool{"MON_DISK_LOW": true}

// containerBackend shares a *Container.
type containerBackend struct{ c *Container }

func startSharedContainer(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (sharedBackend, error) {
	return sharedRunResult(Run(ctx, image, opts...))
}

// sharedRunResult keeps the partial cluster that a failed Run returns: the
// SharedCluster owns it and terminates it, and when that fails a later Close
// tries again.
func sharedRunResult(c *Container, err error) (sharedBackend, error) {
	if c == nil {
		return nil, err
	}
	return containerBackend{c: c}, err
}

func (b containerBackend) createPool(ctx context.Context, name string) error {
	_, err := b.c.CreatePool(ctx, PoolConfig{Name: name, Application: "rados"})
	return err
}

func (b containerBackend) ready(ctx context.Context) error {
	if err := b.c.WaitForQuorum(ctx); err != nil {
		return err
	}
	return b.c.WaitForPGClean(ctx)
}

func (b containerBackend) invariants(ctx context.Context, pool string) (sharedInvariants, error) {
	var state sharedInvariants
	result, err := b.run(ctx, sharedRequest{Pool: pool, Count: true, Commands: sharedInvariantCommands})
	if err != nil {
		return state, err
	}
	output := func(name string) ([]byte, error) {
		reply, ok := result.Commands[name]
		if !ok || reply.Ret != 0 {
			return nil, fmt.Errorf("read the cluster state: %s failed", name)
		}
		return []byte(reply.Out), nil
	}
	data, err := output("auth")
	if err != nil {
		return state, err
	}
	var listing struct {
		AuthDump []struct {
			Entity string            `json:"entity"`
			Caps   map[string]string `json:"caps"`
		} `json:"auth_dump"`
	}
	if err := json.Unmarshal(data, &listing); err != nil || listing.AuthDump == nil {
		return state, errors.New("decode Cephx identity listing")
	}
	state.Auth = map[string]string{}
	for _, entry := range listing.AuthDump {
		caps := make([]string, 0, len(entry.Caps))
		for _, service := range sortedKeys(entry.Caps) {
			caps = append(caps, service+"="+entry.Caps[service])
		}
		state.Auth[entry.Entity] = strings.Join(caps, ";")
	}
	if data, err = output("pools"); err != nil {
		return state, err
	}
	pools, err := decodePoolStates(data)
	if err != nil {
		return state, err
	}
	state.Pools = map[string]string{}
	for _, p := range pools {
		state.Pools[p.Name] = fmt.Sprintf("id=%d type=%s size=%d/%d pgs=%d rule=%d ec=%s flags=%s quota=%d/%d",
			p.ID, p.Type, p.Size, p.MinSize, p.PGNum, p.CRUSHRule, p.ErasureCodeProfile, p.Flags, p.Quota.MaxObjects, p.Quota.MaxBytes)
	}
	if data, err = output("osdmap"); err != nil {
		return state, err
	}
	dump, err := decodeOSDPolicyDump(data)
	if err != nil {
		return state, err
	}
	state.OSDFlags = slices.Clone(dump.flags)
	sort.Strings(state.OSDFlags)
	if data, err = output("config"); err != nil {
		return state, err
	}
	entries, err := decodeConfiguration(data)
	if err != nil {
		return state, err
	}
	state.Config = map[string]string{}
	for _, entry := range entries {
		state.Config[strings.Trim(entry.Section+"/"+entry.Mask, "/")+"/"+entry.Name] = entry.Value
	}
	if data, err = output("blocklist"); err != nil {
		return state, err
	}
	blocklist, err := decodeBlocklistEntries(data)
	if err != nil {
		return state, err
	}
	for _, entry := range blocklist {
		state.Blocklist = append(state.Blocklist, entry.Address)
	}
	sort.Strings(state.Blocklist)
	if data, err = output("health"); err != nil {
		return state, err
	}
	health, err := decodeHealthDetails(data, "")
	if err != nil {
		return state, err
	}
	for code, check := range health.Checks {
		if !check.Muted && !sharedIgnoredHealth[code] {
			state.Health = append(state.Health, code)
		}
	}
	sort.Strings(state.Health)
	if data, err = output("status"); err != nil {
		return state, err
	}
	var status Status
	if err := json.Unmarshal(data, &status); err != nil {
		return state, errors.New("decode the cluster status")
	}
	if !pgsAreClean(status) {
		state.Unsettled = "PGs are not all active+clean"
	}
	if result.Namespaces == nil {
		return state, fmt.Errorf("decode the namespaces of pool %s", pool)
	}
	state.Namespaces = result.Namespaces
	return state, nil
}

// sharedInvariantCommands are the MON commands whose output a shared
// cluster compares with its baseline, named as sharedRequest.Commands names
// them.
var sharedInvariantCommands = map[string]map[string]string{
	"auth":      {"prefix": "auth ls", "format": "json"},
	"pools":     {"prefix": "osd pool ls", "detail": "detail", "format": "json"},
	"osdmap":    {"prefix": "osd dump", "format": "json"},
	"config":    {"prefix": "config dump", "format": "json"},
	"blocklist": {"prefix": "osd blocklist ls", "format": "json"},
	"health":    {"prefix": "health", "detail": "detail", "format": "json"},
	"status":    {"prefix": "status", "format": "json"},
}

// sharedRequest is what sharedScript does over one connection, in order:
// create an auth entity, remove auth entities, remove the objects of
// namespaces of Pool, count the objects of each namespace of Pool, and run
// MON commands.
type sharedRequest struct {
	CreateAuth *sharedCreateAuth            `json:"create_auth,omitempty"`
	RemoveAuth []string                     `json:"remove_auth,omitempty"`
	Pool       string                       `json:"pool,omitempty"`
	Purge      []string                     `json:"purge,omitempty"`
	Count      bool                         `json:"count,omitempty"`
	Commands   map[string]map[string]string `json:"commands,omitempty"`
}

// sharedCreateAuth adds Entity with a fresh key that the Generate command
// prints and with Caps, as auth add -i does with a keyring: the MONs refuse
// an existing entity with another key. It then reads the entity's keyring.
type sharedCreateAuth struct {
	Entity   string   `json:"entity"`
	Caps     []string `json:"caps"`
	Generate []string `json:"generate"`
}

type sharedResult struct {
	// Created reports the auth add of CreateAuth: the generated key, the
	// MON's return code and the keyring read afterwards.
	Created *struct {
		Key     string `json:"key"`
		Ret     int    `json:"ret"`
		Keyring string `json:"keyring"`
	} `json:"created"`
	// Removed maps each auth entity to the MON's return code.
	Removed    map[string]int `json:"removed"`
	Namespaces map[string]int `json:"namespaces"`
	Commands   map[string]struct {
		Ret int    `json:"ret"`
		Out string `json:"out"`
	} `json:"commands"`
}

// sharedScript runs a sharedRequest with the admin identity. One process
// and one connection replace a Ceph CLI process per command, which costs
// about a quarter of a second each. Keys are removed from auth ls.
const sharedScript = `
import json, rados, subprocess, sys
request = json.loads(sys.argv[1])
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect(timeout=30)
try:
    result = {"removed": {}, "commands": {}}
    create = request.get("create_auth")
    if create:
        key = subprocess.run(create["generate"], check=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout.decode().strip()
        keyring = "[%s]\nkey = %s\n" % (create["entity"], key)
        ret, _, _ = cluster.mon_command(json.dumps({"prefix": "auth add", "entity": create["entity"], "caps": create["caps"]}), keyring.encode(), timeout=30)
        created = {"key": key, "ret": ret, "keyring": ""}
        if ret == 0:
            ret, out, _ = cluster.mon_command(json.dumps({"prefix": "auth get", "entity": create["entity"]}), b"", timeout=30)
            if ret == 0:
                created["keyring"] = out.decode()
        result["created"] = created
    for entity in request.get("remove_auth", []):
        ret, _, _ = cluster.mon_command(json.dumps({"prefix": "auth rm", "entity": entity}), b"", timeout=30)
        result["removed"][entity] = ret
    pool = request.get("pool")
    if pool and (request.get("purge") or request.get("count")):
        ioctx = cluster.open_ioctx(pool)
        try:
            for namespace in request.get("purge", []):
                ioctx.set_namespace(namespace)
                for obj in list(ioctx.list_objects()):
                    ioctx.set_locator_key(obj.locator_key or "")
                    ioctx.remove_object(obj.key)
                ioctx.set_locator_key("")
            if request.get("count"):
                ioctx.set_namespace(rados.LIBRADOS_ALL_NSPACES)
                counts = {}
                for obj in ioctx.list_objects():
                    counts[obj.nspace] = counts.get(obj.nspace, 0) + 1
                result["namespaces"] = counts
        finally:
            ioctx.close()
    for name, command in sorted(request.get("commands", {}).items()):
        ret, out, _ = cluster.mon_command(json.dumps(command), b"", timeout=30)
        text = out.decode("utf-8", "replace")
        if command.get("prefix") == "auth ls" and ret == 0:
            dump = json.loads(text)
            for entry in dump.get("auth_dump", []):
                entry.pop("key", None)
                entry.pop("pending_key", None)
            text = json.dumps(dump)
        result["commands"][name] = {"ret": ret, "out": text}
    print(json.dumps(result))
finally:
    cluster.shutdown()
`

// run runs request in the control container.
func (b containerBackend) run(ctx context.Context, request sharedRequest) (sharedResult, error) {
	var result sharedResult
	control, err := b.c.controlContainerContext(ctx)
	if err != nil {
		return result, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	data, err := command(ctx, control, "python3", "-c", sharedScript, string(encoded))
	if err != nil {
		return result, fmt.Errorf("read the shared cluster's state: %w", err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, errors.New("decode the shared cluster's state")
	}
	return result, nil
}

func (b containerBackend) daemonsRunning(ctx context.Context) error {
	if err := b.c.lockTopology(ctx); err != nil {
		return err
	}
	daemons := map[string]testcontainers.Container{}
	if b.c.Container != nil && !b.c.monitorTerminated {
		daemons["mon"] = b.c.Container
	}
	for name, mon := range b.c.monitors {
		if mon.Container != nil {
			daemons["mon."+name] = mon.Container
		}
	}
	for name, mgr := range b.c.managers {
		if mgr.Container != nil && !mgr.terminated {
			daemons["mgr."+name] = mgr.Container
		}
	}
	for id, osd := range b.c.osds {
		if osd.Container != nil && !osd.purged {
			daemons[fmt.Sprintf("osd.%d", id)] = osd.Container
		}
	}
	for name, ctr := range b.c.services {
		daemons[name] = ctr
	}
	b.c.mu.Unlock()
	b.c.controlMu.RLock()
	if b.c.controlPlane != nil {
		daemons["control"] = b.c.controlPlane
	}
	b.c.controlMu.RUnlock()
	var exited []string
	for _, name := range sortedKeys(daemons) {
		state, err := daemons[name].State(ctx)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", name, classifyDocker(ctx, err))
		}
		if !state.Running {
			exited = append(exited, name)
		}
	}
	if len(exited) > 0 {
		return fmt.Errorf("daemon containers exited: %s", strings.Join(exited, ", "))
	}
	return nil
}

// createClient creates a tenant's client in one script run: a fresh key,
// auth add with it, which the MONs refuse for an existing entity, and auth
// get. CreateClient takes several Ceph CLI processes and a keyring copy,
// about a second under the cluster mutex.
func (b containerBackend) createClient(ctx context.Context, entity string, caps ClientCaps) (*ClientConfig, error) {
	c := b.c
	generate, err := c.generateKeyArgs(c.settings.auth.KeyType)
	if err != nil {
		return nil, err
	}
	capArgs, err := clientCapArgs(caps)
	if err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClusterTerminated
	}
	if len(c.config) == 0 {
		return nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.settleClient(ctx, entity); err != nil {
		return nil, err
	}
	client := &ClientConfig{owner: c, name: entity}
	client.config = clientConfigBytes(c.config, client)
	result, err := b.run(ctx, sharedRequest{CreateAuth: &sharedCreateAuth{Entity: entity, Caps: capArgs, Generate: generate}})
	if err != nil {
		// The script's output can hold the key.
		return client, clientOperationError(ctx, "create "+entity+": command failed", err)
	}
	created := result.Created
	if created == nil || created.Key == "" {
		return client, fmt.Errorf("create %s: no key was generated", entity)
	}
	c.recordClientSecret(created.Key)
	if created.Ret != 0 {
		return client, fmt.Errorf("create %s: the MON answered %d", entity, created.Ret)
	}
	client.created = true
	c.setClientKey(entity, created.Key)
	keyring := []byte(created.Keyring)
	if clientKey(keyring, entity) != created.Key {
		return client, fmt.Errorf("cephx keyring for %s does not contain the created key", entity)
	}
	client.keyring = keyring
	client.ready = true
	return client, nil
}

// deleteClient removes a tenant's client with one auth rm. DeleteClient
// settles the MONs and reads the entry back, which takes seconds and holds
// the cluster mutex; here the release's verification, which must not find
// the client, confirms the removal instead.
func (b containerBackend) deleteClient(ctx context.Context, client *ClientConfig) error {
	c := b.c
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	if client == nil || client.owner != c || !client.created {
		return errors.New("cephx client was not created successfully by this cluster")
	}
	if client.revoked {
		return nil
	}
	if err := c.settleClient(ctx, client.name); err != nil {
		return err
	}
	result, err := b.run(ctx, sharedRequest{RemoveAuth: []string{client.name}})
	if err != nil {
		return err
	}
	// auth rm of an absent entity answers 0 or ENOENT.
	if ret, ok := result.Removed[client.name]; !ok || (ret != 0 && ret != -int(syscall.ENOENT)) {
		return fmt.Errorf("remove %s: the MON answered %d", client.name, ret)
	}
	client.revoked = true
	c.setClientKey(client.name, "")
	return nil
}

func (b containerBackend) purgeNamespace(ctx context.Context, pool, namespace string) error {
	_, err := b.run(ctx, sharedRequest{Pool: pool, Purge: []string{namespace}})
	return err
}

func (b containerBackend) prepareServices(ctx context.Context) error {
	for _, service := range registeredSharedServices() {
		if service.prepare == nil {
			continue
		}
		if err := service.prepare(ctx, b.c); err != nil {
			return fmt.Errorf("prepare %s for a shared cluster: %w", service.kind, err)
		}
	}
	return nil
}

func (b containerBackend) setSharing(mode sharingMode) { b.c.sharing.Store(int32(mode)) }

func (b containerBackend) terminate(ctx context.Context) error { return b.c.terminate(ctx) }

func (b containerBackend) cluster() *Container { return b.c }
