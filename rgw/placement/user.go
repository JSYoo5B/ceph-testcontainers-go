package placement

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
)

// UserDefault selects an owned placement as a user's default for future
// buckets. StorageClass defaults to STANDARD. Tags nil preserves the user's
// current placement tags; a nonempty list replaces them exactly. An explicitly
// empty list is unsupported by radosgw-admin and is rejected. Replace matching
// tags with nonmatching tags to revoke access to a restricted target.
// Placement tags authorize new bucket placement, not object access.
type UserDefault struct {
	StorageClass string
	Tags         []string
}

func normalizeTags(tags []string) ([]string, error) {
	tags = slices.Clone(tags)
	for _, tag := range tags {
		if tag == "" || len(tag) > 128 || strings.Contains(tag, ",") || strings.IndexFunc(tag, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) != -1 {
			return nil, servicehook.Invalid(errors.New("RGW placement tags must be nonempty, at most 128 bytes, and contain no whitespace, commas or control characters"))
		}
	}
	slices.Sort(tags)
	for i := 1; i < len(tags); i++ {
		if tags[i] == tags[i-1] {
			return nil, servicehook.Invalid(errors.New("RGW placement tags must be distinct"))
		}
	}
	return tags, nil
}

func nativeTags(value any) ([]string, error) {
	values, ok := value.([]any)
	if !ok {
		return nil, errors.New("RGW native placement tags are missing or malformed")
	}
	tags := make([]string, len(values))
	for i, value := range values {
		var ok bool
		tags[i], ok = value.(string)
		if !ok {
			return nil, errors.New("RGW native placement tag is malformed")
		}
	}
	return normalizeTags(tags)
}

// SetUserDefault sets the default placement/storage class of owned user u
// to this placement and, optionally, replaces its placement tags. It uses the
// actual native zone IDs of the gateway that serves u, validates the confirmed
// placement and its unchanged pool IDs and class mappings, and verifies that
// unrelated native user policy and keys did not change. Mutable public
// placement fields never select the native target.
//
// Existing buckets keep their immutable placement. This changes user metadata;
// it does not activate pending zone configuration or publish a realm period.
// Apply or Reload must activate the target before S3 uses it. Native metadata
// caches may delay new S3 requests. External user/placement edits must not
// race this operation. A failed readback may follow a persisted change;
// rgw.User.Info reads native state, and the same request can be retried.
//
//purpose:construction,condition
func (p *Placement) SetUserDefault(ctx context.Context, u *rgw.User, config UserDefault) error {
	var user *gwbase.User
	if u != nil {
		user = gwbase.UserOf(u)
	}
	return setUserDefault(ctx, p, user, config)
}

func setUserDefault(ctx context.Context, p *Placement, user *gwbase.User, config UserDefault) error {
	if config.StorageClass == "" {
		config.StorageClass = "STANDARD"
	}
	if !storageClassName.MatchString(config.StorageClass) {
		return errors.New("RGW user storage class must use an uppercase class name")
	}
	if config.Tags != nil && len(config.Tags) == 0 {
		return errors.New("radosgw-admin cannot clear placement tags with an empty list; use nil to preserve tags or nonmatching tags to revoke target access")
	}
	var err error
	config.Tags, err = normalizeTags(config.Tags)
	if err != nil {
		return err
	}
	if !user.Owned() {
		return servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	cluster := user.Origin.Cluster()
	servicehook.Lock(cluster)
	defer servicehook.Unlock(cluster)
	g, err := user.Origin.ScopeGateway(ctx)
	if err != nil {
		return err
	}
	if err := validateHandle(g, p); err != nil {
		return err
	}
	if !p.confirmed {
		return errors.New("RGW placement creation has not been confirmed")
	}
	if !user.Active() {
		return errors.New("RGW user must be an active identity created in this cluster")
	}
	if user.State.NativeScope != nil && *user.State.NativeScope != p.scope {
		return errors.New("RGW user and placement have different native runtime scopes")
	}
	ctx, cancel := gwbase.AdminContext(ctx, g.Cluster())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if !slices.ContainsFunc(p.config.StorageClasses, func(class StorageClass) bool { return class.Name == config.StorageClass }) {
		return errors.New("RGW user storage class is absent from the owned placement")
	}
	before, err := ownedUser(ctx, g, user, p.scope)
	if err != nil {
		return err
	}
	args := []string{"user", "modify", "--uid", user.ID, "--placement-id", p.config.Name, "--storage-class", config.StorageClass}
	wantTags := before.Info.PlacementTags
	if config.Tags != nil {
		wantTags = config.Tags
		args = append(args, "--tags", strings.Join(config.Tags, ","))
	}
	if _, err := g.Command(ctx, p.scope, args...); err != nil {
		return err
	}
	after, err := ownedUser(ctx, g, user, p.scope)
	if err != nil {
		return err
	}
	// rgw_placement_rule encodes STANDARD as the placement name alone.
	// Its later decode clears storage_class, which u info dumps verbatim.
	// Compare the canonical class while retaining native UserInfo readback.
	actualClass := after.Info.DefaultStorageClass
	if actualClass == "" {
		actualClass = "STANDARD"
	}
	if after.Info.DefaultPlacement != p.config.Name || actualClass != config.StorageClass || !slices.Equal(after.Info.PlacementTags, wantTags) {
		return errors.New("RGW user placement readback differs from requested policy")
	}
	if !reflect.DeepEqual(unrelatedUserPolicy(before.Document), unrelatedUserPolicy(after.Document)) {
		return errors.New("RGW unrelated user policy changed during placement update")
	}
	return nil
}

// ownedUser reads user in the placement's exact native scope and confirms its
// created key and creation identity.
func ownedUser(ctx context.Context, g *gwbase.Core, user *gwbase.User, scope gwbase.NativeScope) (*gwbase.NativeUser, error) {
	data, err := g.Command(ctx, scope, "user", "info", "--uid", user.ID)
	if err != nil {
		return nil, err
	}
	native, err := gwbase.DecodeUser(data)
	if err != nil {
		return nil, err
	}
	if native.Info.ID != user.ID || !slices.ContainsFunc(native.Keys, func(key gwbase.NativeKey) bool {
		return key.AccessKey == user.AccessKey && key.SecretKey == user.SecretKey
	}) {
		return nil, servicehook.Invalid(errors.New("RGW user has different credentials in the actual native scope; refusing placement update"))
	}
	if err := g.ValidateCreationIdentity(ctx, user, native); err != nil {
		return nil, err
	}
	return native, nil
}

func unrelatedUserPolicy(document map[string]any) map[string]any {
	policy := make(map[string]any, len(document))
	for key, value := range document {
		switch key {
		case "default_placement", "default_storage_class", "placement_tags":
			continue
		}
		policy[key] = value
	}
	return policy
}
