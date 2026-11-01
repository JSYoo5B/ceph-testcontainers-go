package gwtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Scope is the native scope that a running PlacementContainer registers in the
// service map when its zonegroup is in no realm.
//
//testdouble:shared-double
func Scope() gwbase.NativeScope {
	return gwbase.NativeScope{GroupID: "group-id", ZoneID: "zone-id", Group: "active-group", Zone: "active-zone"}
}

// PlacementContainer is a model of one running gateway container and its
// cluster's control commands: the container hostname, the service map entry
// of its daemon, the pool listing, and the native zonegroup, zone and realm
// periods that radosgw-admin reads and changes in the exact scope group-id
// and zone-id. It records every command line in Calls.
//
//testdouble:shared-double
type PlacementContainer struct {
	testcontainers.Container
	Calls                 [][]string
	Group, Zone           map[string]any
	CurrentPeriod         map[string]any
	StagingPeriod         map[string]any
	StagingExit           int
	PoolType, PoolID      float64
	FailClass             string
	ForeignDefault        bool
	PendingRegistrations  int
	StopCount, StartCount int
}

// NewPlacementContainer returns the model of a gateway in the zonegroup
// active-group (API name active-api) and zone active-zone, outside a realm,
// whose zonegroup holds only default-placement. The zone document holds the
// system key PRIVATE-KEY. Pool 1 index is replicated.
//
//testdouble:shared-double
func NewPlacementContainer() *PlacementContainer {
	f := &PlacementContainer{PoolType: 1, PoolID: 1}
	f.Group = map[string]any{"id": "group-id", "name": "active-group", "api_name": "active-api", "realm_id": "", "zones": []any{map[string]any{"id": "zone-id"}}, "default_placement": "default-placement", "placement_targets": []any{map[string]any{"name": "default-placement", "storage_classes": []any{"STANDARD"}, "tags": []any{}}}}
	f.Zone = map[string]any{"id": "zone-id", "name": "active-zone", "realm_id": "", "system_key": map[string]any{"key": "PRIVATE-KEY"}, "placement_pools": []any{}}
	return f
}

// State reports a running gateway, so the placement handle selects it.
//
//testdouble:shared-double
func (f *PlacementContainer) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

// GetContainerID returns the fixed ID gateway.
//
//testdouble:shared-double
func (f *PlacementContainer) GetContainerID() string { return "gateway" }

// Stop counts a gateway stop.
//
//testdouble:shared-double
func (f *PlacementContainer) Stop(context.Context, *time.Duration) error {
	f.StopCount++
	return nil
}

// Start counts a gateway start.
//
//testdouble:shared-double
func (f *PlacementContainer) Start(context.Context) error { f.StartCount++; return nil }

// Exec answers the hostname, service map, pool listing and placement commands.
//
//testdouble:shared-double
func (f *PlacementContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	original := args
	args = Argv(args)
	f.Calls = append(f.Calls, slices.Clone(args))
	var result any
	var raw string
	code := 0
	flag := Flag(args)
	switch {
	case args[0] == "hostname":
		raw = "test-gateway\n"
	case args[0] == "ceph" && slices.Contains(args, "service"):
		if f.PendingRegistrations > 0 {
			f.PendingRegistrations--
			raw = `{"services":{}}`
			break
		}
		result = map[string]any{"services": map[string]any{"rgw": map[string]any{"daemons": map[string]any{"summary": "", "10": map[string]any{"metadata": map[string]any{"hostname": "test-gateway", "pid": "1", "frontend_config#0": "beast port=7480", "zonegroup_id": "group-id", "zonegroup_name": "active-group", "zone_id": "zone-id", "zone_name": "active-zone", "realm_id": f.Group["realm_id"], "realm_name": ""}}}}}}
	case args[0] == "ceph" && slices.Contains(args, "pool"):
		result = []map[string]any{
			{"pool_id": f.PoolID, "pool_name": "index", "type": f.PoolType, "size": 2, "min_size": 1, "pg_num": 1},
			{"pool_id": 2, "pool_name": "extra", "type": 1, "size": 2, "min_size": 1, "pg_num": 1},
			{"pool_id": 3, "pool_name": "data", "type": 1, "size": 2, "min_size": 1, "pg_num": 1},
			{"pool_id": 4, "pool_name": "ec-data", "type": 3, "size": 3, "min_size": 3, "pg_num": 1},
		}
	case args[0] == "radosgw-admin":
		if flag("--zonegroup-id") != "" && (flag("--zonegroup-id") != "group-id" || flag("--zone-id") != "zone-id") {
			return 0, nil, errors.New("wrong native scope")
		}
		if flag("--zonegroup-id") == "" && f.ForeignDefault {
			result = map[string]any{"id": "foreign-default", "realm_id": ""}
			break
		}
		if slices.Contains(args, "period") {
			switch {
			case slices.Contains(args, "get") && flag("--period") != "":
				if flag("--period") != "realm-id:staging" || flag("--epoch") != "1" || flag("--realm-id") != "realm-id" {
					return 0, nil, errors.New("wrong staging scope")
				}
				code = f.StagingExit
				if f.StagingPeriod == nil && code == 0 {
					code = 2
				}
				result = f.StagingPeriod
			case slices.Contains(args, "get"):
				result = f.CurrentPeriod
			case slices.Contains(args, "update"):
				f.StagingPeriod = Clone(f.CurrentPeriod)
				f.StagingPeriod["id"] = "realm-id:staging"
				group := f.StagingPeriod["period_map"].(map[string]any)["zonegroups"].([]any)[0].(map[string]any)
				data, _ := json.Marshal(f.Group["placement_targets"])
				var targets []any
				_ = json.Unmarshal(data, &targets)
				group["placement_targets"] = targets
				result = f.StagingPeriod
			case slices.Contains(args, "commit"):
				f.CurrentPeriod = Clone(f.StagingPeriod)
				f.CurrentPeriod["id"] = "period-id"
				result = f.CurrentPeriod
			default:
				return 0, nil, errors.New("unexpected period command")
			}
			break
		}
		class := flag("--storage-class")
		if class != "" && class == f.FailClass {
			raw = "PRIVATE-KEY native failure"
			code = 1
			break
		}
		if slices.Contains(args, "zonegroup") {
			if slices.Contains(args, "add") {
				target := GroupTarget(f.Group, flag("--placement-id"))
				if target == nil {
					target = map[string]any{"name": flag("--placement-id"), "tags": []any{}, "storage_classes": []any{}}
					f.Group["placement_targets"] = append(f.Group["placement_targets"].([]any), target)
				}
				target["storage_classes"] = append(target["storage_classes"].([]any), class)
			}
			result = f.Group
		} else {
			if slices.Contains(args, "add") {
				mapping := ZonePlacement(f.Zone, flag("--placement-id"))
				if mapping == nil {
					mapping = map[string]any{"index_pool": flag("--index-pool"), "data_extra_pool": flag("--data-extra-pool"), "inline_data": true, "storage_classes": map[string]any{}}
					f.Zone["placement_pools"] = append(f.Zone["placement_pools"].([]any), map[string]any{"key": flag("--placement-id"), "val": mapping})
				}
				mapping["storage_classes"].(map[string]any)[class] = map[string]any{"data_pool": flag("--data-pool")}
				if slices.Contains(args, "--placement-inline-data=false") {
					mapping["inline_data"] = false
				}
			}
			result = f.Zone
		}
	default:
		return 0, nil, fmt.Errorf("unexpected command %v", args)
	}
	if raw == "" && code == 0 {
		data, _ := json.Marshal(result)
		raw = string(data)
	}
	return Reply(original, code, Stream(false, []byte(raw)))
}

// Flag returns the reader of the value that follows a flag in args.
//
//testdouble:shared-double
func Flag(args []string) func(string) string {
	return func(name string) string {
		for i, arg := range args {
			if arg == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
}

// Clone returns a deep copy of a native document, with numbers preserved.
//
//testdouble:shared-double
func Clone(document map[string]any) map[string]any {
	data, _ := json.Marshal(document)
	clone, _ := gwbase.DecodeObject(data)
	return clone
}

// GroupTarget returns the placement target name of a zonegroup document.
//
//testdouble:shared-double
func GroupTarget(group map[string]any, name string) map[string]any {
	targets, _ := group["placement_targets"].([]any)
	for _, value := range targets {
		target, ok := value.(map[string]any)
		if ok && target["name"] == name {
			return target
		}
	}
	return nil
}

// ZonePlacement returns the placement mapping name of a zone document.
//
//testdouble:shared-double
func ZonePlacement(zone map[string]any, name string) map[string]any {
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

// AccountContainer extends a PlacementContainer with a model of the native
// users and the one account that radosgw-admin reads and changes: user and
// account listing, creation, removal, suspension, capabilities, quotas and
// the account's metadata with its version tag. Failed commands answer with
// output that contains PRIVATE-SECRET.
//
//testdouble:shared-double
type AccountContainer struct {
	*PlacementContainer
	Users          map[string]map[string]any
	Account        map[string]any
	VersionTag     string
	MissingTag     bool
	FailRemove     bool
	UnrelatedQuota bool
	OnUserCreate   func(map[string]any)
	LostUserCreate bool
}

// NewAccountContainer returns the model over base without users or account,
// whose account metadata version tag is native-account-lifetime.
//
//testdouble:shared-double
func NewAccountContainer(base *PlacementContainer) *AccountContainer {
	return &AccountContainer{PlacementContainer: base, Users: map[string]map[string]any{}, VersionTag: "native-account-lifetime"}
}

// SeedAccount records the native account id with name and tenant, as account
// create leaves it.
//
//testdouble:shared-double
func (f *AccountContainer) SeedAccount(id, name, tenant string) {
	f.Account = map[string]any{"id": id, "name": name, "tenant": tenant, "email": "", "max_users": 1000, "max_roles": 1000, "max_groups": 1000, "max_buckets": 1000, "max_access_keys": 4, "quota": map[string]any{"enabled": false, "max_size": -1, "max_objects": -1}, "bucket_quota": map[string]any{"enabled": false, "max_size": -1, "max_objects": -1}}
}

// Exec answers radosgw-admin user and account commands and passes the others
// to the PlacementContainer.
//
//testdouble:shared-double
func (f *AccountContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	original := args
	args = Argv(args)
	if args[0] != "radosgw-admin" {
		return f.PlacementContainer.Exec(ctx, original, opts...)
	}
	f.Calls = append(f.Calls, slices.Clone(args))
	flag := Flag(args)
	var result any
	code := 0
	switch {
	case slices.Contains(args, "metadata"):
		if f.Account == nil {
			code, result = 2, "PRIVATE-SECRET missing account"
			break
		}
		tag := f.VersionTag
		if f.MissingTag {
			tag = ""
		}
		result = map[string]any{"key": "account:" + f.Account["id"].(string), "ver": map[string]any{"tag": tag, "ver": 2}, "data": f.Account}
	case slices.Contains(args, "account") && !slices.Contains(args, "quota"):
		switch {
		case slices.Contains(args, "list"):
			ids := []string{}
			if f.Account != nil {
				ids = append(ids, f.Account["id"].(string))
			}
			result = ids
		case slices.Contains(args, "create"):
			f.SeedAccount(flag("--account-id"), flag("--account-name"), flag("--tenant"))
			f.Account["email"] = flag("--email")
			if flag("--max-buckets") != "" {
				value, _ := strconv.Atoi(flag("--max-buckets"))
				f.Account["max_buckets"] = value
			}
			result = f.Account
		case slices.Contains(args, "rm"):
			if f.FailRemove {
				code = 13
				result = "PRIVATE-SECRET native nonempty account"
			} else {
				f.Account = nil
			}
		}
	case slices.Contains(args, "quota") && flag("--account-id") != "":
		field := "quota"
		if flag("--quota-scope") == "bucket" {
			field = "bucket_quota"
		}
		quota := f.Account[field].(map[string]any)
		if slices.Contains(args, "set") {
			rawSize := flag("--max-size")
			size, _ := strconv.ParseInt(strings.TrimRight(rawSize, "BK"), 10, 64)
			if strings.HasSuffix(rawSize, "K") {
				size *= 1024
			}
			// Native account quota chooses the int64_t overload. Both -1 byte
			// and -1 KiB truncate to zero, so the disabled size needs -2 KiB.
			size = ((size + 1023) / 1024) * 1024
			if size < -1 {
				size = -1
			}
			objects, _ := strconv.ParseInt(flag("--max-objects"), 10, 64)
			quota["max_size"], quota["max_objects"] = size, objects
		} else {
			quota["enabled"] = slices.Contains(args, "enable")
		}
		if f.UnrelatedQuota {
			f.Account["max_roles"] = 2
		}
		result = f.Account
	case slices.Contains(args, "user") && slices.Contains(args, "list"):
		ids := []string{}
		for id := range f.Users {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		result = ids
	case slices.Contains(args, "user") && slices.Contains(args, "create"):
		id := flag("--uid")
		local := id
		if _, right, found := strings.Cut(id, "$"); found {
			local = right
		}
		var user map[string]any
		_ = json.Unmarshal([]byte(UserDocument(local, "SECRET-"+id, false, gwbase.Quota{MaxSizeBytes: -1, MaxObjects: -1})), &user)
		user["keys"] = []gwbase.NativeKey{{AccessKey: "ACCESS-" + id, SecretKey: "SECRET-" + id}}
		// Native CLI UserInfo::dump emits canonical user_id without separate
		// tenant/full_user_id fields; Admin Ops uses a different serializer.
		user["user_id"], user["type"], user["account_id"] = id, "rgw", ""
		if slices.Contains(args, "--account-root") {
			user["type"], user["account_id"] = "root", flag("--account-id")
		}
		user["display_name"] = flag("--display-name")
		f.Users[id] = user
		if f.OnUserCreate != nil {
			f.OnUserCreate(user)
		}
		result = user
		if f.LostUserCreate {
			code, result = 13, "PRIVATE-SECRET lost user creation response"
		}
	default:
		id := flag("--uid")
		user := f.Users[id]
		if user == nil {
			code = 2
			result = "PRIVATE-SECRET missing user"
			break
		}
		switch {
		case slices.Contains(args, "rm"):
			delete(f.Users, id)
		case slices.Contains(args, "suspend"):
			user["suspended"] = 1
		case slices.Contains(args, "user") && slices.Contains(args, "enable") && !slices.Contains(args, "quota"):
			user["suspended"] = 0
		case slices.Contains(args, "caps"):
			user["caps"] = []gwbase.AdminCapability{{Type: "users", Permission: "read"}}
		case slices.Contains(args, "quota"):
			field := "user_quota"
			if flag("--quota-scope") == "bucket" {
				field = "bucket_quota"
			}
			quota := user[field].(map[string]any)
			if slices.Contains(args, "set") {
				rawSize := flag("--max-size")
				size, _ := strconv.ParseInt(strings.TrimRight(rawSize, "BK"), 10, 64)
				if strings.HasSuffix(rawSize, "K") {
					size *= 1024
				}
				// Native set_quota_info explicitly normalizes any negative value,
				// unlike the account modify path's pre-clamp signed rounding.
				if size < 0 {
					size = -1
				} else {
					size = ((size + 1023) / 1024) * 1024
				}
				objects, _ := strconv.ParseInt(flag("--max-objects"), 10, 64)
				quota["max_size"], quota["max_objects"] = size, objects
			} else {
				quota["enabled"] = slices.Contains(args, "enable")
			}
		}
		result = user
	}
	data, _ := json.Marshal(result)
	return Reply(original, code, Stream(code != 0, data))
}
