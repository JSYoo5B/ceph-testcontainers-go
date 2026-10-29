# RGW harness

## Introduction

The `rgw/harness` package leases an RGW tenant of a [shared cluster](ceph-harness.md#shared-cluster)
to a tenant. It composes the RGW service of the [rgw](rgw.md)
package with the common [ceph/harness](ceph-harness.md) package, and the
rgw package does not import it.

All the harness packages are named `harness`; import it with an alias:

```golang
import rgwharness "github.com/JSYoo5B/ceph-testcontainers-go/rgw/harness"
```

A shared cluster prepares and probes a service only when the program gives it
the service's support, so list it when you construct the cluster:

```golang
var shared = cephharness.NewShared(ceph.DefaultImage,
	cephharness.WithServices(rgwharness.Support()),
	cephharness.WithSharedRunOptions( /* the RGW options of ceph.Run */ ))
```

`Support` registers nothing and holds no resource. Without it the `Lease`
below fails with `ceph.ErrInvalidRequest` before it creates anything.

## Lease

A tenant of a [shared cluster](ceph-harness.md#shared-cluster) leases an RGW tenant of
its own. Start the gateway with `WithGateways` in `cephharness.WithSharedRunOptions`.

```golang
func Support() cephharness.ServiceSupport
func Lease(ctx context.Context, t *cephharness.Tenant) (*TenantUser, error)

func (u *TenantUser) Tenant() string
func (u *TenantUser) UserID() string
func (u *TenantUser) Credentials() (accessKey, secretKey string)
func (u *TenantUser) Region() string
func (u *TenantUser) S3Endpoint(ctx context.Context) (string, error)
func (u *TenantUser) DaemonEndpoint(ctx context.Context) (string, error)
```

`Lease` creates the S3 user `ID$user` in the RGW tenant `ID` (the tenant's ID)
on the gateway named `default`, or else the first gateway by name. Use
`S3Endpoint` from the host and `DaemonEndpoint` from a container on the
cluster's network, with path-style addressing and `Credentials`.

Bucket names are scoped by the RGW tenant. Checked natively, two tenants can
both create a bucket of the same name, `ListBuckets` lists only the tenant's
own buckets, and a request for another tenant's bucket (`/OTHER:bucket`)
returns 403 AccessDenied.

The tenant never gets the `*rgw.Gateway`, whose container holds the admin
keyring. The shared cluster creates the pools of the gateway's zone before
its baseline, because RGW would otherwise create the bucket pools on the
first bucket. The tenant's release removes every user of the RGW tenant with
their buckets and objects, then any bucket of the tenant that remains, and
fails the test and poisons the cluster if a user or bucket remains.
