package rgw

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// UserPlacementConfig selects an owned placement as a user's default for
// future buckets. StorageClass defaults to STANDARD. Tags nil preserves the
// user's current placement tags; a nonempty list replaces them exactly. An
// explicitly empty list is unsupported by radosgw-admin and is rejected.
// Replace matching tags with nonmatching tags to revoke access to a restricted
// target. Placement tags authorize new bucket placement, not object access.
type UserPlacementConfig struct {
	StorageClass string
	Tags         []string
}

func normalizeRGWPlacementTags(tags []string) ([]string, error) {
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

func rgwPlacementNativeTags(value any) ([]string, error) {
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
	return normalizeRGWPlacementTags(tags)
}

// SetPlacement sets an owned user's default placement/storage class and,
// optionally, replaces its placement tags. It uses the gateway's actual native
// zone IDs, validates the confirmed placement and its unchanged pool IDs and
// class mappings, and verifies that unrelated native user policy and keys did
// not change. Mutable public placement fields never select the native target.
//
// Existing buckets keep their immutable placement. This changes user metadata;
// it does not activate pending zone configuration or publish a realm period.
// ApplyPlacement/ReloadPlacement must activate the target before S3 uses it.
// Native metadata caches may delay new S3 requests. External user/placement
// edits must not race this operation. A failed readback may follow a persisted
// change; Info reads native state, and the same request can be retried.
func (u *User) SetPlacement(ctx context.Context, p *Placement, config UserPlacementConfig) error {
	if config.StorageClass == "" {
		config.StorageClass = "STANDARD"
	}
	if !rgwStorageClassName.MatchString(config.StorageClass) {
		return errors.New("RGW user storage class must use an uppercase class name")
	}
	if config.Tags != nil && len(config.Tags) == 0 {
		return errors.New("radosgw-admin cannot clear placement tags with an empty list; use nil to preserve tags or nonmatching tags to revoke target access")
	}
	var err error
	config.Tags, err = normalizeRGWPlacementTags(config.Tags)
	if err != nil {
		return err
	}
	if u == nil || u.owner == nil {
		return servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	servicehook.Lock(u.owner)
	defer servicehook.Unlock(u.owner)
	g, err := scopeGateway(ctx, u.owner, u.scope)
	if err != nil {
		return err
	}
	if err := g.validatePlacementHandle(p); err != nil {
		return err
	}
	if !p.confirmed {
		return errors.New("RGW placement creation has not been confirmed")
	}
	if u.state == nil || !u.state.created || u.state.removed || u.state.identity == nil || u.accessKey == "" || u.secretKey == "" {
		return errors.New("RGW user must be an active identity created in this cluster")
	}
	if u.state.nativeScope != nil && *u.state.nativeScope != p.scope {
		return errors.New("RGW user and placement have different native runtime scopes")
	}
	ctx, cancel := adminContext(ctx, g.owner)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if !slices.ContainsFunc(p.config.StorageClasses, func(class StorageClassConfig) bool { return class.Name == config.StorageClass }) {
		return errors.New("RGW user storage class is absent from the owned placement")
	}
	before, err := g.placementOwnedUser(ctx, u, p.scope)
	if err != nil {
		return err
	}
	args := []string{"user", "modify", "--uid", u.id, "--placement-id", p.config.Name, "--storage-class", config.StorageClass}
	wantTags := before.info.PlacementTags
	if config.Tags != nil {
		wantTags = config.Tags
		args = append(args, "--tags", strings.Join(config.Tags, ","))
	}
	if _, err := g.placementCommand(ctx, p.scope, args...); err != nil {
		return err
	}
	after, err := g.placementOwnedUser(ctx, u, p.scope)
	if err != nil {
		return err
	}
	// rgw_placement_rule encodes STANDARD as the placement name alone.
	// Its later decode clears storage_class, which u info dumps verbatim.
	// Compare the canonical class while retaining native UserInfo readback.
	actualClass := after.info.DefaultStorageClass
	if actualClass == "" {
		actualClass = "STANDARD"
	}
	if after.info.DefaultPlacement != p.config.Name || actualClass != config.StorageClass || !slices.Equal(after.info.PlacementTags, wantTags) {
		return errors.New("RGW user placement readback differs from requested policy")
	}
	if !reflect.DeepEqual(rgwUserUnrelatedPlacementPolicy(before.document), rgwUserUnrelatedPlacementPolicy(after.document)) {
		return errors.New("RGW unrelated user policy changed during placement update")
	}
	return nil
}

func (g *Gateway) placementOwnedUser(ctx context.Context, user *User, scope rgwPlacementScope) (*rgwNativeUser, error) {
	data, err := g.placementCommand(ctx, scope, "user", "info", "--uid", user.id)
	if err != nil {
		return nil, err
	}
	native, err := decodeRGWUser(data)
	if err != nil {
		return nil, err
	}
	if native.info.ID != user.id || !slices.ContainsFunc(native.keys, func(key rgwNativeKey) bool { return key.AccessKey == user.accessKey && key.SecretKey == user.secretKey }) {
		return nil, servicehook.Invalid(errors.New("RGW user has different credentials in the actual native scope; refusing placement update"))
	}
	if err := g.validateUserCreationIdentity(ctx, user, native); err != nil {
		return nil, err
	}
	return native, nil
}

func rgwUserUnrelatedPlacementPolicy(document map[string]any) map[string]any {
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
