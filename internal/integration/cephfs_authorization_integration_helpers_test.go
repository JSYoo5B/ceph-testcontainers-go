//go:build all || (integration && features)

package integration_test

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func testCephFSSubvolumeClientAuthorization(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 9*time.Minute)
	defer cancel()
	const filesystem = "client-authorization"
	image, _ := integrationImages(t)
	options := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithCephFS(ceph.CephFSConfig{Name: filesystem})}
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	cluster, admin := newServiceCluster(t, options...)
	fs := cluster.Filesystems()[0]
	group, err := fs.CreateSubvolumeGroup(ctx, ceph.CephFSSubvolumeGroupConfig{Name: "tenants"})
	if err != nil {
		t.Fatal(err)
	}
	volumes := make([]*ceph.CephFSSubvolume, 0, 2)
	infos := make([]*ceph.CephFSSubvolumeInfo, 0, 2)
	for _, name := range []string{"source", "neighbor"} {
		volume, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: name, GroupName: group.Name, SizeBytes: 8 << 20, NamespaceIsolated: true})
		if err != nil {
			t.Fatal(err)
		}
		info, err := fs.SubvolumeInfo(ctx, volume.Name, volume.GroupName)
		if err != nil {
			t.Fatal(err)
		}
		volumes, infos = append(volumes, volume), append(infos, info)
		cephFSSnapshotIO(t, ctx, admin, filesystem, volume.Path, "", "seed", info.DataPool, info.PoolNamespace, 8<<20)
		cephFSSubvolumeExec(t, ctx, admin, "python3", "-c", `import rados, sys
pool, namespace = sys.argv[1:]
c = rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin')
c.connect(timeout=10)
try:
    with c.open_ioctx(pool) as io:
        io.set_namespace(namespace)
        io.write_full('authorization-probe', b'native namespace data')
finally:
    c.shutdown()`, info.DataPool, info.PoolNamespace)
	}
	source, neighbor := volumes[0], volumes[1]
	sourceInfo, neighborInfo := infos[0], infos[1]
	if sourceInfo.PoolNamespace == "" || sourceInfo.PoolNamespace == neighborInfo.PoolNamespace {
		t.Fatal("test did not create independent native namespaces")
	}
	writer, err := fs.AuthorizeSubvolume(ctx, source, ceph.CephFSSubvolumeAuthorizationConfig{ClientID: "subvolume-writer", Access: "rw"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := fs.AuthorizeSubvolume(ctx, source, ceph.CephFSSubvolumeAuthorizationConfig{ClientID: "subvolume-reader", Access: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := fs.AuthorizeSubvolume(ctx, source, ceph.CephFSSubvolumeAuthorizationConfig{ClientID: writer.AuthID, Access: "r"}); err == nil || duplicate != nil {
		t.Fatal("existing principal was adopted or modified")
	}
	listing, err := fs.SubvolumeAuthorizedClients(ctx, source.Name, source.GroupName)
	if err != nil || !slices.Equal(listing, []ceph.CephFSSubvolumeAuthorizedClient{
		{AuthID: reader.AuthID, Access: "r"}, {AuthID: writer.AuthID, Access: "rw"},
	}) {
		t.Fatalf("native authorization list=%+v error=%v", listing, err)
	}
	if err := fs.RemoveSubvolume(ctx, source); err == nil {
		t.Fatal("authorized subvolume was removed before revocation")
	}
	newClient := func(identity *ceph.ClientConfig) testcontainers.Container {
		t.Helper()
		client, err := testcontainers.Run(ctx, image, cluster.WithClientIdentity(identity),
			ceph.WithIdleEntrypoint(),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import cephfs, rados"})))
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	writerClient, readerClient := newClient(writer.Client), newClient(reader.Client)
	type sessionResult struct {
		code   int
		output []byte
		err    error
	}
	probe := func(client testcontainers.Container, identity *ceph.ClientConfig, own, other *ceph.CephFSSubvolume, ownInfo, otherInfo *ceph.CephFSSubvolumeInfo, phase string) {
		t.Helper()
		var adminResult chan sessionResult
		if phase == "revoked" {
			// Both principals retain valid MDS/OSD access to the neighbor.
			// Require strict source denial, authoritative absent grants and
			// concurrently healthy fresh admin mounts/namespace reads.
			caps, err := cluster.ClientCapabilities(ctx, identity)
			if err != nil || strings.Contains(caps.MDS, "path="+own.Path) || strings.Contains(caps.OSD, "namespace="+ownInfo.PoolNamespace) {
				t.Fatalf("revoked principal still has original native clauses: %+v error=%v", caps, err)
			}
			listing, err := fs.SubvolumeAuthorizedClients(ctx, own.Name, own.GroupName)
			if err != nil || slices.ContainsFunc(listing, func(entry ceph.CephFSSubvolumeAuthorizedClient) bool { return entry.AuthID == identity.User() }) {
				t.Fatalf("revoked principal still has native volumes authorization: %+v error=%v", listing, err)
			}
			adminResult = make(chan sessionResult, 1)
			go func() {
				code, output, err := admin.Exec(ctx, []string{"python3", "-c", cephFSAuthorizationAdminConnectivity,
					filesystem, own.Path, ownInfo.DataPool, ownInfo.PoolNamespace})
				var data []byte
				if output != nil {
					data, _ = io.ReadAll(output)
				}
				adminResult <- sessionResult{code: code, output: data, err: err}
			}()
		}
		output := cephFSSubvolumeExec(t, ctx, client, "python3", "-c", cephFSAuthorizationProbe,
			filesystem, identity.User(), identity.Name(), identity.KeyringPath(), own.Path, other.Path,
			ownInfo.DataPool, ownInfo.PoolNamespace, otherInfo.PoolNamespace, phase)
		t.Logf("fresh native authorization probe: %s", bytes.TrimSpace(output))
		if adminResult != nil {
			select {
			case result := <-adminResult:
				if result.err != nil || result.code != 0 || !bytes.Contains(result.output, []byte("fresh admin connectivity confirmed")) {
					t.Fatalf("mount refusal lacked simultaneous healthy admin proof: exit=%d error=%v output=%s", result.code, result.err, result.output)
				}
			case <-ctx.Done():
				t.Fatal("fresh admin connectivity proof did not finish", ctx.Err())
			}
		}
	}
	probe(writerClient, writer.Client, source, neighbor, sourceInfo, neighborInfo, "rw")
	probe(readerClient, reader.Client, source, neighbor, sourceInfo, neighborInfo, "r")

	// Add independently managed neighbor RO and MGR rights only after each
	// principal's source-only isolation proof. Keeping a usable OSD cap avoids
	// testing an absent-service-ticket timeout after the last grant is removed;
	// the source namespace must instead return native PermissionError while the
	// same principal/key still reads the neighbor. Deauthorize must preserve
	// these unrelated rights exactly for both the original RO and RW grants.
	originalKeyrings := make(map[*ceph.CephFSSubvolumeAuthorization][]byte)
	for _, grant := range []*ceph.CephFSSubvolumeAuthorization{writer, reader} {
		caps, err := cluster.ClientCapabilities(ctx, grant.Client)
		if err != nil {
			t.Fatal(err)
		}
		caps.MGR = "allow r"
		caps.MDS += ", allow r path=" + neighbor.Path
		caps.OSD += ", allow r pool=" + neighborInfo.DataPool + " namespace=" + neighborInfo.PoolNamespace
		if err := cluster.UpdateClientCaps(ctx, grant.Client, caps); err != nil {
			t.Fatal(err)
		}
		_, keyring, err := grant.Client.ConnectionConfig()
		if err != nil {
			t.Fatal(err)
		}
		originalKeyrings[grant] = keyring
	}
	probe(writerClient, writer.Client, neighbor, source, neighborInfo, sourceInfo, "r-both")
	probe(readerClient, reader.Client, neighbor, source, neighborInfo, sourceInfo, "r-both")
	assertPreserved := func(grant *ceph.CephFSSubvolumeAuthorization) {
		t.Helper()
		currentCaps, err := cluster.ClientCapabilities(ctx, grant.Client)
		if err != nil || currentCaps.MDS != "allow r path="+neighbor.Path || currentCaps.OSD != "allow r pool="+neighborInfo.DataPool+" namespace="+neighborInfo.PoolNamespace || currentCaps.MGR != "allow r" {
			t.Fatalf("unrelated rights were not preserved: %+v error=%v", currentCaps, err)
		}
		_, currentKeyring, err := grant.Client.ConnectionConfig()
		if err != nil || !bytes.Equal(originalKeyrings[grant], currentKeyring) {
			t.Fatal("deauthorization changed client credentials", err)
		}
	}
	const ready, trigger = "/tmp/owned-cephfs-session.ready", "/tmp/owned-cephfs-session.evict"
	result := make(chan sessionResult, 1)
	go func() {
		code, output, err := writerClient.Exec(ctx, []string{"python3", "-c", cephFSAuthorizationHeldSession,
			filesystem, writer.Client.User(), writer.Client.KeyringPath(), source.Path, ready, trigger})
		var data []byte
		if output != nil {
			data, _ = io.ReadAll(output)
		}
		result <- sessionResult{code: code, output: data, err: err}
	}()
	if err := wait.ForExec([]string{"test", "-f", ready}).WithStartupTimeout(30*time.Second).WaitUntilReady(ctx, writerClient); err != nil {
		t.Fatal("held native session was not ready", err)
	}
	copy := *writer
	copy.AuthID, copy.SubvolumeName, copy.GroupName, copy.Path, copy.Client = "foreign", "foreign", "foreign", "/foreign", nil
	if err := fs.DeauthorizeSubvolume(ctx, &copy); err != nil {
		t.Fatal(err)
	}
	assertPreserved(writer)
	probe(writerClient, writer.Client, source, neighbor, sourceInfo, neighborInfo, "revoked")
	probe(writerClient, writer.Client, neighbor, source, neighborInfo, sourceInfo, "r")
	if err := fs.EvictSubvolumeClients(ctx, writer); err != nil {
		t.Fatal(err)
	}
	cephFSSubvolumeExec(t, ctx, writerClient, "python3", "-c", "open('"+trigger+"', 'w').close()")
	select {
	case session := <-result:
		if session.err != nil || session.code != 0 || !bytes.Contains(session.output, []byte("held native session rejected")) {
			t.Fatalf("eviction did not affect the held subvolume mount: exit=%d error=%v output=%s", session.code, session.err, session.output)
		}
	case <-ctx.Done():
		t.Fatal("evicted native session did not finish", ctx.Err())
	}
	if err := fs.DeauthorizeSubvolume(ctx, writer); err != nil {
		t.Fatal("copied deauthorization state was not shared", err)
	}
	if err := fs.DeauthorizeSubvolume(ctx, reader); err != nil {
		t.Fatal(err)
	}
	assertPreserved(reader)
	probe(readerClient, reader.Client, source, neighbor, sourceInfo, neighborInfo, "revoked")
	probe(readerClient, reader.Client, neighbor, source, neighborInfo, sourceInfo, "r")
	listing, err = fs.SubvolumeAuthorizedClients(ctx, source.Name, source.GroupName)
	if err != nil || len(listing) != 0 {
		t.Fatalf("native deauthorization metadata remains: %+v error=%v", listing, err)
	}
	for _, grant := range []*ceph.CephFSSubvolumeAuthorization{writer, reader} {
		if err := cluster.DeleteClient(ctx, grant.Client); err != nil {
			t.Fatal(err)
		}
	}
	for _, volume := range volumes {
		if err := fs.RemoveSubvolume(ctx, volume); err != nil {
			t.Fatal(err)
		}
	}
	if err := fs.RemoveSubvolumeGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	t.Log("native subvolume RO/RW path and RADOS namespace constraints, list, fresh revocation, preserved unrelated rights/key and scoped established-session eviction passed")
}

const cephFSAuthorizationProbe = `import cephfs, errno, json, os, rados, sys, threading
filesystem, user, entity, keyring, root, other, pool, namespace, other_namespace, phase = sys.argv[1:]
deadline = threading.Timer(60, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
mount_refusal_errno = None
def make_fs():
    fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id=user)
    fs.conf_set('keyring', keyring)
    fs.conf_set('client_mount_timeout', '10')
    fs.conf_set('rados_osd_op_timeout', '5')
    return fs
def denied(operation, record_mount=False):
    global mount_refusal_errno
    try:
        operation()
    except cephfs.Error as error:
        actual = abs(error.args[0])
        allowed = (errno.EPERM, errno.EACCES, errno.EROFS)
        if record_mount:
            allowed = (errno.EPERM, errno.EACCES)
            mount_refusal_errno = actual
        assert actual in allowed, str(error)
        return
    raise AssertionError('unauthorized filesystem operation succeeded')
fs = make_fs()
try:
    if phase == 'revoked':
        denied(lambda: fs.mount(mount_root=root.encode(), filesystem_name=filesystem.encode()), record_mount=True)
    else:
        fs.mount(mount_root=root.encode(), filesystem_name=filesystem.encode())
        fd = fs.open('/data', os.O_RDONLY)
        try:
            assert fs.read(fd, 0, 524289) == bytes(range(256)) * 2048
        finally:
            fs.close(fd)
        if phase == 'rw':
            fd = fs.open('/own-write', os.O_CREAT | os.O_WRONLY, 0o600)
            try:
                assert fs.write(fd, b'owned write', 0) == 11
                fs.fsync(fd, 0)
            finally:
                fs.close(fd)
            denied(lambda: fs.setxattr('/', 'ceph.dir.layout.pool_namespace', other_namespace.encode(), 0))
        else:
            denied(lambda: fs.open('/denied-write', os.O_CREAT | os.O_WRONLY, 0o600))
        fs.unmount()
finally:
    fs.shutdown()
if phase != 'revoked':
    other_fs = make_fs()
    try:
        if phase == 'r-both':
            other_fs.mount(mount_root=other.encode(), filesystem_name=filesystem.encode())
            fd = other_fs.open('/data', os.O_RDONLY)
            try:
                assert other_fs.read(fd, 0, 524289) == bytes(range(256)) * 2048
            finally:
                other_fs.close(fd)
        else:
            denied(lambda: other_fs.mount(mount_root=other.encode(), filesystem_name=filesystem.encode()))
    finally:
        other_fs.shutdown()
c = rados.Rados(conffile='/etc/ceph/ceph.conf', name=entity)
c.conf_set('keyring', keyring)
c.conf_set('rados_mon_op_timeout', '5')
c.conf_set('rados_osd_op_timeout', '5')
c.connect(timeout=10)
def rados_denied(operation):
    try:
        operation()
    except rados.PermissionError:
        return
    raise AssertionError('unauthorized direct RADOS operation succeeded')
try:
    if phase == 'revoked':
        def revoked_read():
            with c.open_ioctx(pool) as io:
                io.set_namespace(namespace)
                io.read('authorization-probe', 64)
        rados_denied(revoked_read)
    else:
        with c.open_ioctx(pool) as io:
            io.set_namespace(namespace)
            assert io.read('authorization-probe', 64) == b'native namespace data'
            if phase == 'rw':
                io.write_full('direct-own', b'owned direct write')
                assert io.read('direct-own', 64) == b'owned direct write'
            else:
                rados_denied(lambda: io.write_full('direct-denied', b'forbidden'))
            io.set_namespace(other_namespace)
            if phase == 'r-both':
                assert io.read('authorization-probe', 64) == b'native namespace data'
            else:
                rados_denied(lambda: io.read('authorization-probe', 64))
            io.set_namespace('')
            rados_denied(lambda: io.write_full('unscoped-denied', b'forbidden'))
finally:
    c.shutdown()
deadline.cancel()
print(json.dumps({'phase': phase, 'principal': entity, 'path': root, 'pool_namespace': namespace, 'mount_refusal_errno': mount_refusal_errno}))
`

const cephFSAuthorizationAdminConnectivity = `import cephfs, os, rados, sys, threading, time
filesystem, root, pool, namespace = sys.argv[1:]
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
started, probes = time.monotonic(), 0
while True:
    fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
    fs.conf_set('client_mount_timeout', '5')
    fs.conf_set('rados_osd_op_timeout', '5')
    try:
        fs.mount(mount_root=root.encode(), filesystem_name=filesystem.encode())
        fd = fs.open('/data', os.O_RDONLY)
        try:
            assert fs.read(fd, 0, 524289) == bytes(range(256)) * 2048
        finally:
            fs.close(fd)
    finally:
        fs.shutdown()
    c = rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin')
    c.conf_set('rados_mon_op_timeout', '5')
    c.conf_set('rados_osd_op_timeout', '5')
    c.connect(timeout=5)
    try:
        with c.open_ioctx(pool) as io:
            io.set_namespace(namespace)
            assert io.read('authorization-probe', 64) == b'native namespace data'
    finally:
        c.shutdown()
    probes += 1
    if time.monotonic() - started >= 12:
        break
    time.sleep(1)
assert probes >= 2
deadline.cancel()
print('fresh admin connectivity confirmed: ' + str(probes), flush=True)
`

const cephFSAuthorizationHeldSession = `import cephfs, os, sys, threading, time
filesystem, user, keyring, root, ready, trigger = sys.argv[1:]
deadline = threading.Timer(150, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id=user)
fs.conf_set('keyring', keyring)
fs.conf_set('client_mount_timeout', '10')
fs.conf_set('rados_osd_op_timeout', '5')
fs.mount(mount_root=root.encode(), filesystem_name=filesystem.encode())
fd = fs.open('/held-before-revoke', os.O_CREAT | os.O_WRONLY, 0o600)
fs.write(fd, b'live established session', 0)
fs.fsync(fd, 0)
fs.close(fd)
open(ready, 'w').close()
while not os.path.exists(trigger):
    time.sleep(0.1)
try:
    fd = fs.open('/after-eviction', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    fs.write(fd, b'must be rejected', 0)
    fs.fsync(fd, 0)
    fs.close(fd)
except cephfs.Error as error:
    print('held native session rejected: ' + str(error), flush=True)
else:
    raise AssertionError('evicted native session continued writing')
finally:
    fs.shutdown()
    deadline.cancel()
`
