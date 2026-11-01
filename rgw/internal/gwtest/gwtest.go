// Package gwtest holds the unit-test fixtures that the tests of package rgw
// and its feature packages share: the supervisor framing of radosgw-admin
// calls, canned native user documents, a synthetic cluster, a foundation core
// over a gateway container for the feature packages, which cannot construct
// an rgw.Gateway, and small models of the native zone, placement, period,
// user and account state that the typed operations read and change. Only
// _test.go files import it.
package gwtest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
)

// Argv strips the shell supervisor of a radosgw-admin call, so a fake sees
// the radosgw-admin command line.
//
//testdouble:shared-double
func Argv(args []string) []string {
	if len(args) > 7 && args[0] == "sh" && args[2] == supervised.ShellScript {
		return args[7:]
	}
	return args
}

// IsCancel reports the canceler of a supervised call, which a fake confirms:
// its supervisor never ran.
//
//testdouble:shared-double
func IsCancel(args []string) bool {
	return len(args) > 2 && args[0] == "sh" && args[2] == supervised.ShellCancelScript
}

// Reply answers a call as the gateway's supervisor does: it ends the standard
// error of a call that original wrapped with the supervisor's end line.
//
//testdouble:shared-double
func Reply(original []string, code int, stream *bytes.Buffer) (int, io.Reader, error) {
	if len(original) > 2 && original[2] == supervised.ShellScript {
		end := []byte(supervised.ShellEndPrefix + strconv.Itoa(code) + "\n")
		header := [8]byte{byte(stdcopy.Stderr)}
		binary.BigEndian.PutUint32(header[4:], uint32(len(end)))
		stream.Write(header[:])
		stream.Write(end)
	}
	return code, stream, nil
}

// Stream frames data as one Docker exec stream record, on standard error
// when stderr is set and on standard output otherwise.
//
//testdouble:shared-double
func Stream(stderr bool, data []byte) *bytes.Buffer {
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if stderr {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.Write(data)
	return &stream
}

// UserDocument is the native user information of an ordinary user id with
// the key pair PRIVATE-ACCESS and secret, the suspension and the user quota.
//
//testdouble:shared-double
func UserDocument(id, secret string, suspended bool, quota gwbase.Quota) string {
	suspendedInt := 0
	if suspended {
		suspendedInt = 1
	}
	return fmt.Sprintf(`{"user_id":%q,"type":"rgw","display_name":"Testcontainers","email":"test@example.org","suspended":%d,"admin":false,"system":false,"max_buckets":1000,"caps":[],"keys":[{"access_key":"PRIVATE-ACCESS","secret_key":%q}],"user_quota":{"enabled":%t,"max_size":%d,"max_objects":%d},"bucket_quota":{"enabled":false,"max_size":-1,"max_objects":-1}}`, id, suspendedInt, secret, quota.Enabled, quota.MaxSizeBytes, quota.MaxObjects)
}

// Cluster builds a cluster value whose control container is control and whose
// owned service containers are services.
//
//testdouble:shared-double
func Cluster(control testcontainers.Container, config testhelper.TestConfig, services map[string]testcontainers.Container) *ceph.Container {
	c := testhelper.TestCluster(control, config)
	for name, ctr := range services {
		servicehook.SetService(c, name, ctr)
	}
	return c
}

// Gateway is the foundation core of the synthetic gateway "default" of a
// synthetic cluster, listening on port 7480, with the registry state that
// package rgw would inject held in its fields: the gateway container the core
// reads and the registered gateway cores.
//
//testdouble:shared-double
type Gateway struct {
	Core    *gwbase.Core
	Cluster *ceph.Container
	// Container is the gateway container that Core reads; Replace changes it
	// together with the cluster's service.
	Container testcontainers.Container
	// Registered is the registry that Core reads, in name order.
	Registered []*gwbase.Core
}

// NewGateway returns the registered gateway "default" whose container is ctr,
// of a synthetic cluster whose control container is control and whose
// startup timeout is one minute.
//
//testdouble:shared-double
func NewGateway(ctr, control testcontainers.Container) *Gateway {
	g := &Gateway{Cluster: Cluster(control, testhelper.TestConfig{StartupTimeout: time.Minute}, map[string]testcontainers.Container{"rgw": ctr}), Container: ctr}
	g.Core = gwbase.NewCore(gwbase.Params{Cluster: g.Cluster, Name: "default", Port: 7480, Container: &g.Container,
		Gateways: func() []*gwbase.Core { return g.Registered }})
	g.Registered = []*gwbase.Core{g.Core}
	return g
}

// Replace makes ctr the gateway container and the cluster's service.
//
//testdouble:shared-double
func (g *Gateway) Replace(ctr testcontainers.Container) {
	g.Container = ctr
	servicehook.SetService(g.Cluster, "rgw", ctr)
}

// Foreign returns the core of a gateway "default" of another synthetic cluster
// that registers no gateway, so no gateway serves a handle it created.
//
//testdouble:shared-double
func Foreign() *gwbase.Core {
	return gwbase.NewCore(gwbase.Params{Cluster: Cluster(nil, testhelper.TestConfig{}, nil), Name: "default"})
}

// ConfirmedAccount returns the confirmed identity of an account id with
// tenant, created through origin in scope, with the native metadata version
// tag tag, as package rgw/account records it.
//
//testdouble:shared-double
func ConfirmedAccount(origin *gwbase.Core, scope gwbase.NativeScope, id, tenant, tag string) *gwbase.Account {
	return &gwbase.Account{Origin: origin, Scope: scope, ID: id, Tenant: tenant, State: &gwbase.AccountState{Confirmed: true, VersionTag: tag}}
}

// RootAccount is an account handle over a fabricated identity, which
// satisfies rgw.RootAccount like an account of package rgw/account.
//
//testdouble:shared-double
type RootAccount struct{ gwbase.AccountHandle }

// NewRootAccount returns the handle of a.
//
//testdouble:shared-double
func NewRootAccount(a *gwbase.Account) *RootAccount {
	return &RootAccount{AccountHandle: gwbase.NewAccountHandle(a)}
}
