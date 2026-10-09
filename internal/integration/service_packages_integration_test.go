//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_service_packages))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

// Each service package's Run fills in that service's defaults, and options
// from all three packages start one cluster that serves RBD, CephFS and S3
// clients at the same time.
func TestServicePackages(t *testing.T) {
	parallelWhenEnabled(t)
	image, roles := integrationImages(t)
	t.Run("rbd_run_default_pool", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
		defer cancel()
		cluster, client := newServiceClusterRun(t, rbd.Run, image, append(roles, ceph.WithOSDCount(2))...)
		servicePackageApplication(t, ctx, cluster, "rbd", "rbd")
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		servicePackageProbe(t, ctx, client, "rbd", "rbd")
	})
	t.Run("cephfs_run_default_filesystem", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
		defer cancel()
		cluster, client := newServiceClusterRun(t, cephfs.Run, image, append(roles, ceph.WithOSDCount(2))...)
		filesystems := cephfs.Filesystems(cluster)
		if len(filesystems) != 1 || filesystems[0].FilesystemName != "tc-cephfs" {
			t.Fatalf("default filesystem was not created: %+v", filesystems)
		}
		if err := filesystems[0].WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		servicePackageProbe(t, ctx, client, "cephfs", "tc-cephfs")
	})
	t.Run("rgw_run_default_gateway", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
		defer cancel()
		cluster, _ := newServiceClusterRun(t, rgw.Run, image, append(roles, ceph.WithOSDCount(1))...)
		gateways := rgw.Gateways(cluster)
		if len(gateways) != 1 || gateways[0].AccessKey == "" {
			t.Fatalf("default gateway or its user was not created: %+v", gateways)
		}
		servicePackageS3(t, ctx, gateways[0], gateways[0].AccessKey, gateways[0].SecretKey, "default")
	})
	for _, host := range []bool{false, true} {
		name := "combined_bridge"
		if host {
			name = "combined_host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			opts := append(roles, ceph.WithOSDCount(2),
				rbd.WithPools(ceph.PoolConfig{Name: "volumes"}),
				cephfs.WithFilesystems(cephfs.Config{Name: "shared"}),
				rgw.WithGateways(rgw.Config{Name: "s3"}),
			)
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceClusterRun(t, ceph.Run, image, opts...)
			servicePackageApplication(t, ctx, cluster, "volumes", "rbd")
			filesystems, gateways := cephfs.Filesystems(cluster), rgw.Gateways(cluster)
			if len(filesystems) != 1 || filesystems[0].FilesystemName != "shared" || len(gateways) != 1 || gateways[0].GatewayName != "s3" {
				t.Fatalf("combined services differ: filesystems=%+v gateways=%+v", filesystems, gateways)
			}
			if err := filesystems[0].WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			// All three protocols use the same OSDs in one session of work.
			servicePackageProbe(t, ctx, client, "rbd", "volumes")
			servicePackageProbe(t, ctx, client, "cephfs", "shared")
			servicePackageS3(t, ctx, gateways[0], gateways[0].AccessKey, gateways[0].SecretKey, name)
			servicePackageProbe(t, ctx, client, "rbd", "volumes")
			servicePackageProbe(t, ctx, client, "cephfs", "shared")
			pools, err := cluster.Pools(ctx)
			if err != nil {
				t.Fatal(err)
			}
			names := map[string]bool{}
			for _, pool := range pools {
				names[pool.Name] = true
			}
			for _, want := range []string{"volumes", "shared-metadata", "shared-data", "default.rgw.buckets.index"} {
				if !names[want] {
					t.Logf("SERVICE_PACKAGES pools=%v", names)
					t.Fatalf("combined cluster lacks pool %s", want)
				}
			}
			t.Logf("SERVICE_PACKAGES combined host=%v pools=%d rbd=volumes cephfs=shared rgw=s3", host, len(pools))
		})
	}
}

func servicePackageApplication(t *testing.T, ctx context.Context, cluster *ceph.Container, pool, application string) {
	t.Helper()
	var applications map[string]any
	if err := json.Unmarshal(mustCeph(t, ctx, cluster, "osd", "pool", "application", "get", pool, "--format", "json"), &applications); err != nil {
		t.Fatal(err)
	}
	if _, ok := applications[application]; !ok || len(applications) != 1 {
		t.Fatalf("pool %s applications %v, want only %s", pool, applications, application)
	}
}

// The probe keeps earlier phases' data: each call writes a fresh object and
// verifies every object that an earlier call wrote.
func servicePackageProbe(t *testing.T, ctx context.Context, client testcontainers.Container, kind, target string) {
	t.Helper()
	t.Logf("SERVICE_PACKAGES %s", execOutput(t, ctx, client, "python3", "-c", servicePackageScript, kind, target))
}

func servicePackageS3(t *testing.T, ctx context.Context, gateway *rgw.Gateway, access, secret, phase string) {
	t.Helper()
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := s3HTTPClient{endpoint: endpoint, accessKey: access, secretKey: secret, region: gateway.Region, http: &http.Client{Timeout: 30 * time.Second}}
	bucket := "/tc-service-" + strings.ReplaceAll(phase, "_", "-")
	payload := []byte("service packages share one cluster: " + phase)
	s3FeatureRequest(t, ctx, client, http.MethodPut, bucket, nil, nil, http.StatusOK)
	s3FeatureRequest(t, ctx, client, http.MethodPut, bucket+"/object", payload, nil, http.StatusOK)
	body, _ := s3FeatureRequest(t, ctx, client, http.MethodGet, bucket+"/object", nil, nil, http.StatusOK)
	if string(body) != string(payload) {
		t.Fatalf("S3 object differs: %q", body)
	}
	t.Logf("SERVICE_PACKAGES s3 bucket=%s bytes=%d", bucket, len(body))
}

const servicePackageScript = `import sys,json,hashlib,uuid
kind,target=sys.argv[1:]
def payload(name): return hashlib.sha256(name.encode()).digest()*1024
if kind=='rbd':
    import rados,rbd
    cluster=rados.Rados(conffile='/etc/ceph/ceph.conf'); cluster.connect()
    try:
        with cluster.open_ioctx(target) as io:
            name='image-'+uuid.uuid4().hex[:8]
            rbd.RBD().create(io,name,4<<20)
            with rbd.Image(io,name) as image: image.write(payload(name),0)
            names=sorted(n for n in rbd.RBD().list(io) if n.startswith('image-'))
            for existing in names:
                with rbd.Image(io,existing,read_only=True) as image:
                    data=payload(existing); assert image.read(0,len(data))==data,existing
            print(json.dumps({'rbd':target,'images':len(names)}))
    finally: cluster.shutdown()
else:
    import cephfs
    fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
    fs.conf_set('client_mount_timeout','30'); fs.mount(filesystem_name=target.encode())
    try:
        try: fs.mkdir(b'/service',0o755)
        except cephfs.ObjectExists: pass
        name='file-'+uuid.uuid4().hex[:8]
        fd=fs.open(('/service/'+name).encode(),'w',0o644); fs.write(fd,payload(name),0); fs.fsync(fd,0); fs.close(fd)
        names=[]
        d=fs.opendir(b'/service')
        entry=fs.readdir(d)
        while entry:
            if entry.d_name.startswith(b'file-'): names.append(entry.d_name.decode())
            entry=fs.readdir(d)
        fs.closedir(d)
        for existing in names:
            data=payload(existing); fd=fs.open(('/service/'+existing).encode(),'r',0)
            assert fs.read(fd,0,len(data)+1)==data,existing; fs.close(fd)
        print(json.dumps({'cephfs':target,'files':len(names)}))
    finally: fs.unmount(); fs.shutdown()
`
