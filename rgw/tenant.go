package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/harnesshook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/namedregistry"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicekind"
)

// tenantUserID is the local ID of a tenant's S3 user.
const tenantUserID = "user"

// TenantUser is the RGW tenant of a shared-cluster tenant: the RGW tenant
// named after the tenant's ID and its S3 user ID$user. Bucket names are
// scoped by the RGW tenant, so tenants can use the same bucket names, list
// only their own buckets, and get AccessDenied on another tenant's buckets.
// The tenant's release removes every user of the RGW tenant with their
// buckets and objects.
type TenantUser struct {
	tenant    *ceph.Tenant
	gateway   *Gateway
	accessKey string
	secretKey string
}

// Lease creates the RGW tenant of tenant t and its S3 user ID$user on the
// shared cluster's gateway, the one named default or else the first by
// name. Start gateways with WithGateways in ceph.WithSharedRunOptions. A
// tenant leases RGW once.
func Lease(ctx context.Context, t *ceph.Tenant) (*TenantUser, error) {
	if t == nil {
		return nil, harnesshook.Invalid(errors.New("tenant is required"))
	}
	port, err := servicehook.Tenant(t)
	if err != nil {
		return nil, harnesshook.Invalid(fmt.Errorf("tenant %s is released", t.ID()))
	}
	if err := port.RequireService(servicekind.RGW); err != nil {
		return nil, err
	}
	if err := port.Live(); err != nil {
		return nil, harnesshook.Invalid(fmt.Errorf("tenant %s is released", t.ID()))
	}
	// The gateway state is keyed by the cluster, which the port reaches
	// without handing it out.
	var g *Gateway
	_ = port.WithTopology(func() error {
		g = pickSharedGateway(namedregistry.From[*Gateway](port.Extension, servicekind.RGW).Sorted())
		return nil
	})
	if g == nil {
		return nil, harnesshook.Invalid(errors.New("the shared cluster runs no RGW gateway; start one with rgw.WithGateways in ceph.WithSharedRunOptions"))
	}
	u := &TenantUser{tenant: t, gateway: g}
	// Attached first, so that the release also removes a user whose
	// creation failed after it applied.
	if err := port.AttachScope("rgw tenant "+t.ID(), u.release); err != nil {
		return nil, err
	}
	if err := port.WithTopology(func() error { return u.create(ctx) }); err != nil {
		return nil, err
	}
	return u, nil
}

// create makes the S3 user of u's RGW tenant. The caller holds the cluster
// mutex.
func (u *TenantUser) create(ctx context.Context) error {
	g, t := u.gateway, u.tenant
	if err := g.validateAdminGateway(); err != nil {
		return err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	data, err := g.adminCommand(ctx, "user", "create", "--tenant", t.ID(), "--uid", tenantUserID, "--display-name", "tenant "+t.ID())
	if err != nil {
		return fmt.Errorf("create the RGW user of tenant %s: %w", t.ID(), err)
	}
	var created struct {
		Keys []struct {
			User      string `json:"user"`
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &created); err != nil || len(created.Keys) != 1 || created.Keys[0].AccessKey == "" || created.Keys[0].SecretKey == "" {
		return errors.New("decode the S3 keys of a tenant's RGW user")
	}
	u.accessKey, u.secretKey = created.Keys[0].AccessKey, created.Keys[0].SecretKey
	return nil
}

// Tenant returns the RGW tenant, the tenant's ID. S3 paths of another RGW
// tenant's bucket take the form /TENANT:BUCKET.
func (u *TenantUser) Tenant() string { return u.tenant.ID() }

// UserID returns the S3 user's full ID, ID$user.
func (u *TenantUser) UserID() string { return u.tenant.ID() + "$" + tenantUserID }

// Credentials returns the S3 user's access and secret key.
func (u *TenantUser) Credentials() (accessKey, secretKey string) { return u.accessKey, u.secretKey }

// Region returns the gateway's S3 region.
func (u *TenantUser) Region() string { return u.gateway.Region }

// S3Endpoint returns the gateway's endpoint for S3 clients on the host; see
// Gateway.S3Endpoint.
func (u *TenantUser) S3Endpoint(ctx context.Context) (string, error) {
	return u.gateway.S3Endpoint(ctx)
}

// DaemonEndpoint returns the gateway's endpoint for clients in containers on
// the cluster's network; see Gateway.DaemonEndpoint.
func (u *TenantUser) DaemonEndpoint(ctx context.Context) (string, error) {
	return u.gateway.DaemonEndpoint(ctx)
}

func (u *TenantUser) release(ctx context.Context) ([]string, error) {
	g, id := u.gateway, u.tenant.ID()
	servicehook.Lock(g.owner)
	defer servicehook.Unlock(g.owner)
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	users, buckets, err := tenantMetadata(ctx, g, id)
	if err != nil {
		return nil, err
	}
	// --purge-data removes the user's buckets with their objects.
	for _, user := range users {
		if _, err := g.adminCommand(ctx, "user", "rm", "--uid", user, "--purge-data"); err != nil {
			return nil, fmt.Errorf("remove RGW user %s: %w", user, err)
		}
	}
	if len(users) > 0 {
		if _, buckets, err = tenantMetadata(ctx, g, id); err != nil {
			return nil, err
		}
	}
	for _, bucket := range buckets {
		if _, err := g.adminCommand(ctx, "bucket", "rm", "--bucket", bucket, "--purge-objects"); err != nil {
			return nil, fmt.Errorf("remove RGW bucket %s: %w", bucket, err)
		}
	}
	users, buckets, err = tenantMetadata(ctx, g, id)
	if err != nil {
		return nil, err
	}
	var remains []string
	for _, user := range users {
		remains = append(remains, "user "+user)
	}
	for _, bucket := range buckets {
		remains = append(remains, "bucket "+bucket)
	}
	return remains, nil
}

// tenantMetadata lists the users and buckets of RGW tenant id. The caller
// holds the cluster mutex.
func tenantMetadata(ctx context.Context, g *Gateway, id string) (users, buckets []string, err error) {
	allUsers, allBuckets, err := rgwMetadata(ctx, g)
	if err != nil {
		return nil, nil, err
	}
	for _, user := range allUsers {
		if owner, _, ok := strings.Cut(user, "$"); ok && owner == id {
			users = append(users, user)
		}
	}
	for _, bucket := range allBuckets {
		if owner, _, ok := strings.Cut(bucket, "/"); ok && owner == id {
			buckets = append(buckets, bucket)
		}
	}
	return users, buckets, nil
}

// rgwMetadata lists every user and bucket of the gateway's zone: users as
// tenant$id and buckets as tenant/name, without a tenant for the legacy
// namespace. The caller holds the cluster mutex.
func rgwMetadata(ctx context.Context, g *Gateway) (users, buckets []string, err error) {
	for _, list := range []struct {
		section string
		into    *[]string
	}{{"user", &users}, {"bucket", &buckets}} {
		data, err := g.adminCommand(ctx, "metadata", "list", list.section)
		if err != nil {
			return nil, nil, fmt.Errorf("list RGW %ss: %w", list.section, err)
		}
		if err := json.Unmarshal(data, list.into); err != nil || *list.into == nil {
			return nil, nil, fmt.Errorf("decode the RGW %s listing", list.section)
		}
		slices.Sort(*list.into)
	}
	return users, buckets, nil
}

// sharedGateway returns the gateway that tenants use, the one named
// default or else the first by name, or nil.
func sharedGateway(c *ceph.Container) *Gateway {
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	return pickSharedGateway(namedregistry.From[*Gateway](func(kind string, create func() any) any { return servicehook.Extension(c, kind, create) }, servicekind.RGW).Sorted())
}

// pickSharedGateway returns the gateway named default or else the first of
// the name-sorted gateways, or nil.
func pickSharedGateway(sorted []*Gateway) *Gateway {
	for _, g := range sorted {
		if g.GatewayName == "default" {
			return g
		}
	}
	if len(sorted) == 0 {
		return nil
	}
	return sorted[0]
}

// prepareShared creates the pools of the shared gateway's zone that RGW
// would otherwise create on first use, such as the bucket index and data
// pools on the first bucket, so that the baseline already has them.
func prepareShared(ctx context.Context, c *ceph.Container) error {
	g := sharedGateway(c)
	if g == nil {
		return nil
	}
	servicehook.Lock(c)
	data, err := func() ([]byte, error) {
		if err := g.validateAdminGateway(); err != nil {
			return nil, err
		}
		ctx, cancel := g.adminContext(ctx)
		defer cancel()
		return g.adminCommand(ctx, "zone", "get")
	}()
	servicehook.Unlock(c)
	if err != nil {
		return fmt.Errorf("read the RGW zone: %w", err)
	}
	pools, err := zonePools(data)
	if err != nil {
		return err
	}
	observer, err := ceph.Observe(c)
	if err != nil {
		return err
	}
	existing, err := observer.Pools(ctx)
	if err != nil {
		return err
	}
	for _, pool := range existing {
		pools = slices.DeleteFunc(pools, func(name string) bool { return name == pool.Name })
	}
	for _, pool := range pools {
		if _, err := c.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rgw"}); err != nil {
			return fmt.Errorf("create RGW pool %s: %w", pool, err)
		}
	}
	return nil
}

// zonePools lists the pools that a zone names, without their namespaces:
// the top-level *_pool fields and domain_root, and each placement target's
// index, data and extra pools.
func zonePools(data []byte) ([]string, error) {
	var zone map[string]json.RawMessage
	if err := json.Unmarshal(data, &zone); err != nil {
		return nil, errors.New("decode the RGW zone")
	}
	var pools []string
	add := func(value string) {
		name, _, _ := strings.Cut(value, ":")
		if name != "" && !slices.Contains(pools, name) {
			pools = append(pools, name)
		}
	}
	for key, raw := range zone {
		var value string
		if (strings.HasSuffix(key, "_pool") || key == "domain_root") && json.Unmarshal(raw, &value) == nil {
			add(value)
		}
	}
	var placement struct {
		Pools []struct {
			Val struct {
				IndexPool      string `json:"index_pool"`
				DataExtraPool  string `json:"data_extra_pool"`
				StorageClasses map[string]struct {
					DataPool string `json:"data_pool"`
				} `json:"storage_classes"`
			} `json:"val"`
		} `json:"placement_pools"`
	}
	if err := json.Unmarshal(data, &placement); err != nil {
		return nil, errors.New("decode the RGW zone's placement pools")
	}
	for _, target := range placement.Pools {
		add(target.Val.IndexPool)
		add(target.Val.DataExtraPool)
		for _, class := range target.Val.StorageClasses {
			add(class.DataPool)
		}
	}
	slices.Sort(pools)
	return pools, nil
}

// probeShared lists the users and buckets of the shared gateway's zone,
// each owned by its RGW tenant.
func probeShared(ctx context.Context, c *ceph.Container) (map[string]string, error) {
	g := sharedGateway(c)
	if g == nil {
		return nil, nil
	}
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	users, buckets, err := rgwMetadata(ctx, g)
	if err != nil {
		return nil, err
	}
	resources := map[string]string{}
	for _, user := range users {
		owner, _, _ := strings.Cut(user, "$")
		resources["rgw user "+user] = owner
	}
	for _, bucket := range buckets {
		owner, _, _ := strings.Cut(bucket, "/")
		resources["rgw bucket "+bucket] = owner
	}
	return resources, nil
}

func init() {
	servicehook.RegisterSharedService(servicekind.RGW, prepareShared, probeShared)
}
