//go:build all || (integration && diagnostics)

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func testClusterDiagnostics(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{
		ceph.WithPools(ceph.PoolConfig{Name: "tc-diagnostics", Application: "rados"}),
		cephfs.WithFilesystems(cephfs.Config{Name: "tc-diagnostics-fs"}),
		rgw.WithGateways(rgw.Config{Name: "diagnostics"}),
	}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	} else {
		opts = append(opts, ceph.WithSeparateClusterNetwork())
	}
	cluster, client := newServiceCluster(t, opts...)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	diagnosticsDataProbe(t, ctx, client, "seed")
	owned := diagnosticsOwnedContainers(cluster)
	observed := append(slices.Clone(owned), client)
	config := ceph.DiagnosticsConfig{
		Timeout: time.Minute, OperationTimeout: 10 * time.Second,
		MaxOutputBytes: 256 << 10, LogTail: 50, Concurrency: 4,
		AdditionalContainers: []ceph.DiagnosticsContainer{{Role: "client", Name: "probe", Container: client}},
	}
	collect := func(t *testing.T, phase string, expectError bool) *ceph.DiagnosticsReport {
		t.Helper()
		before := diagnosticsCaptureState(t, ctx, cluster, observed, true)
		report, err := cluster.CollectDiagnostics(ctx, config)
		if report == nil || (err != nil) != expectError {
			t.Fatalf("%s collection: report present=%t error present=%t; expected error=%t; error=%v; failed artifacts=%s", phase, report != nil, err != nil, expectError, err, diagnosticsArtifactErrors(report))
		}
		diagnosticsAssertReport(t, report, host, expectError)
		after := diagnosticsCaptureState(t, ctx, cluster, observed, true)
		diagnosticsAssertUnchanged(t, before, after, phase)
		diagnosticsDataProbe(t, ctx, client, "verify")
		return report
	}

	t.Run("healthy", func(t *testing.T) {
		report := collect(t, "healthy", false)
		diagnosticsAssertCoverage(t, report, observed, host, true)
		var status ceph.Status
		artifact := diagnosticsFindArtifact(t, report, "ceph", "status", "")
		if err := json.Unmarshal([]byte(artifact.Data), &status); err != nil || status.FSID != diagnosticsFSID(t, ctx, cluster) {
			t.Fatal("diagnostic status lacks the independently observed native FSID")
		}
		t.Logf("complete diagnostics: %d daemon/client containers, 9 native Ceph snapshots, native network metadata; RADOS/CephFS bytes and Docker/native identities unchanged", len(observed))
	})

	t.Run("stopped-daemon", func(t *testing.T) {
		gateway := rgw.Gateways(cluster)[0]
		stopTimeout := 5 * time.Second
		if err := gateway.Stop(ctx, &stopTimeout); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), time.Minute)
			defer restoreCancel()
			if err := gateway.Start(restoreCtx); err != nil {
				t.Errorf("restore stopped gateway: %v", err)
			}
		})
		report := collect(t, "stopped RGW", false)
		diagnosticsAssertCoverage(t, report, observed, host, true)
		artifact := diagnosticsFindArtifact(t, report, "container-inspect", "", gateway.GetContainerID())
		running, found := diagnosticsJSONBool(artifact.Data, "running")
		if !found || running {
			t.Fatal("diagnostics did not preserve the observed stopped RGW state")
		}
		t.Log("stopped RGW inspect and retained logs collected without restarting it; native Ceph snapshots and retained RADOS/CephFS bytes survived")
	})

	t.Run("partial-container-error", func(t *testing.T) {
		image, _ := integrationImages(t)
		removed, err := testcontainers.Run(ctx, image, cluster.WithClient(),
			ceph.WithIdleEntrypoint())
		if removed != nil {
			testcontainers.CleanupContainer(t, removed)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := removed.Terminate(ctx); err != nil {
			t.Fatal(err)
		}
		config.AdditionalContainers = append(config.AdditionalContainers,
			ceph.DiagnosticsContainer{Role: "client", Name: "removed-probe", Container: removed})
		report := collect(t, "removed caller-owned client", true)
		for _, kind := range []string{"container-inspect", "container-logs"} {
			artifact := diagnosticsFindArtifact(t, report, kind, "", removed.GetContainerID())
			if artifact.Error == "" {
				t.Fatalf("removed caller-owned client has no %s artifact error", kind)
			}
		}
		diagnosticsAssertCoverage(t, report, observed, host, true)
		config.AdditionalContainers = config.AdditionalContainers[:1]
		t.Log("actual removed caller-owned container produced inspect/log errors while every owned container and native Ceph snapshot remained available; no replacement or native/data mutation")
	})

	t.Run("deadline", func(t *testing.T) {
		before := diagnosticsCaptureState(t, ctx, cluster, observed, true)
		deadlineCtx, deadlineCancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer deadlineCancel()
		started := time.Now()
		report, err := cluster.CollectDiagnostics(deadlineCtx, config)
		if report == nil || !errors.Is(err, context.DeadlineExceeded) || report.Complete {
			t.Fatal("expired deadline did not return an inspectable incomplete report and deadline error")
		}
		if time.Since(started) > 5*time.Second {
			t.Fatal("collection did not honor the expired overall deadline")
		}
		diagnosticsAssertUnchanged(t, before, diagnosticsCaptureState(t, ctx, cluster, observed, true), "deadline")
		diagnosticsDataProbe(t, ctx, client, "verify")
		t.Log("deadline returns incomplete artifacts and wraps context.DeadlineExceeded without changing running daemons, network/native identities or retained bytes")
	})
}

type diagnosticsStableState struct {
	DockerContainers []string
	DockerNetworks   []string
	Containers       map[string]string
	Native           map[string]string
}

func diagnosticsOwnedContainers(cluster *ceph.Container) []testcontainers.Container {
	var result []testcontainers.Container
	seen := make(map[string]bool)
	appendContainer := func(container testcontainers.Container) {
		if container != nil && !seen[container.GetContainerID()] {
			seen[container.GetContainerID()] = true
			result = append(result, container)
		}
	}
	for _, mon := range cluster.Monitors() {
		appendContainer(mon.Container)
	}
	for _, mgr := range cluster.Managers() {
		appendContainer(mgr.Container)
	}
	for _, osd := range cluster.OSDs() {
		appendContainer(osd.Container)
	}
	for _, service := range cluster.ServiceContainers() {
		appendContainer(service)
	}
	appendContainer(cluster.ControlContainer())
	return result
}

func diagnosticsCaptureState(t *testing.T, ctx context.Context, cluster *ceph.Container, containers []testcontainers.Container, native bool) diagnosticsStableState {
	t.Helper()
	state := diagnosticsStableState{Containers: make(map[string]string), Native: make(map[string]string)}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer docker.Close()
	all, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, container := range all.Items {
		state.DockerContainers = append(state.DockerContainers, container.ID)
	}
	slices.Sort(state.DockerContainers)
	networks, err := docker.NetworkList(ctx, mobycl.NetworkListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, network := range networks.Items {
		state.DockerNetworks = append(state.DockerNetworks, network.ID)
	}
	slices.Sort(state.DockerNetworks)
	for _, container := range containers {
		inspect, err := container.Inspect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		networks := make(map[string]string)
		for name, endpoint := range inspect.NetworkSettings.Networks {
			aliases := slices.Clone(endpoint.Aliases)
			slices.Sort(aliases)
			networks[name] = fmt.Sprintf("%v/%v/%s/%s/%v", endpoint.IPAddress, endpoint.Gateway, endpoint.NetworkID, endpoint.EndpointID, aliases)
		}
		value := struct {
			State       any
			Restart     int
			NetworkMode string
			Networks    map[string]string
			Ports       any
		}{inspect.State, inspect.RestartCount, string(inspect.HostConfig.NetworkMode), networks, inspect.NetworkSettings.Ports}
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		state.Containers[container.GetContainerID()] = string(data)
	}
	if native {
		state.Native["fsid"] = diagnosticsFSID(t, ctx, cluster)
		for name, args := range map[string][]string{
			"pools":  {"osd", "pool", "ls", "detail", "--format", "json"},
			"monmap": {"mon", "dump", "--format", "json"},
			"config": {"config", "dump", "--format", "json"},
		} {
			data, err := cluster.Ceph(ctx, args...)
			if err != nil {
				t.Fatal(err)
			}
			var value any
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatalf("decode independent native %s", name)
			}
			canonical, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			state.Native[name] = string(canonical)
		}
		identities, err := cluster.OSDStates(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, identity := range identities {
			state.Native["osd."+strconv.Itoa(identity.ID)] = fmt.Sprintf("%v", identity)
		}
		fs := cephfs.Filesystems(cluster)[0]
		mds, err := fs.MDSStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		state.Native["filesystem"] = fmt.Sprintf("%s/%d/%d", fs.FilesystemName, mds.FilesystemID, mds.MaxMDS)
		auth, err := cluster.Ceph(ctx, "auth", "ls", "--format", "json")
		if err != nil {
			t.Fatal("independent native auth membership unavailable")
		}
		type authIdentity struct {
			Entity string            `json:"entity"`
			Caps   map[string]string `json:"caps"`
		}
		var membership struct {
			Entries []authIdentity `json:"auth_dump"`
		}
		if json.Unmarshal(auth, &membership) != nil || len(membership.Entries) == 0 {
			t.Fatal("native auth membership is not a nonempty structured snapshot")
		}
		slices.SortFunc(membership.Entries, func(a, b authIdentity) int { return strings.Compare(a.Entity, b.Entity) })
		authIdentities, err := json.Marshal(membership)
		if err != nil {
			t.Fatal("encode native auth identities and capabilities")
		}
		state.Native["auth-identities-and-caps"] = string(authIdentities)
	}
	return state
}

func diagnosticsAssertUnchanged(t *testing.T, before, after diagnosticsStableState, phase string) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("%s collection changed Docker container membership/state/network or native FSID/topology/pool/config identity", phase)
	}
}

func diagnosticsFSID(t *testing.T, ctx context.Context, cluster *ceph.Container) string {
	t.Helper()
	status, err := cluster.Status(ctx)
	if err != nil || status.FSID == "" {
		t.Fatal("independent native FSID unavailable")
	}
	return status.FSID
}

func diagnosticsAssertReport(t *testing.T, report *ceph.DiagnosticsReport, host, incomplete bool) {
	t.Helper()
	if report.SchemaVersion != 1 || report.HostNetwork != host || report.Closed || report.Complete == incomplete || report.StartedAt.IsZero() || report.FinishedAt.Before(report.StartedAt) {
		t.Fatal("diagnostics report lost schema/network/lifecycle/completeness/timing metadata")
	}
	if len(report.Artifacts) == 0 {
		t.Fatal("diagnostics report contains no artifacts")
	}
}

func diagnosticsAssertCoverage(t *testing.T, report *ceph.DiagnosticsReport, containers []testcontainers.Container, host, native bool) {
	t.Helper()
	for _, container := range containers {
		for _, kind := range []string{"container-inspect", "container-logs"} {
			artifact := diagnosticsFindArtifact(t, report, kind, "", container.GetContainerID())
			if artifact.Error != "" || artifact.Role == "" || artifact.Name == "" || artifact.Truncated {
				t.Fatalf("%s/%s of required container is missing, failed or truncated: error=%s; failed artifacts=%s", kind, artifact.Name, artifact.Error, diagnosticsArtifactErrors(report))
			}
			if kind == "container-inspect" && !json.Valid([]byte(artifact.Data)) {
				t.Fatal("container inspect artifact is not valid allowlisted JSON")
			}
		}
	}
	if native {
		for _, name := range []string{"version", "status", "health", "pgs", "quorum", "monmap", "mgrmap", "osdmap", "fsmap"} {
			artifact := diagnosticsFindArtifact(t, report, "ceph", name, "")
			if artifact.Error != "" || artifact.Data == "" || artifact.Truncated {
				t.Fatalf("native %s artifact missing, failed or truncated: error=%s; failed artifacts=%s", name, artifact.Error, diagnosticsArtifactErrors(report))
			}
		}
	}
	expectedNetworks := 2
	if host {
		expectedNetworks = 1 // Shared host metadata; no owned Docker network.
	}
	if diagnosticsCountKind(report, "network-inspect") != expectedNetworks {
		t.Fatalf("diagnostics did not capture %d owned networks", expectedNetworks)
	}
	for _, artifact := range report.Artifacts {
		if artifact.Kind == "network-inspect" && (artifact.Error != "" || !json.Valid([]byte(artifact.Data))) {
			t.Fatalf("network inspect %s artifact is not an available native Docker snapshot: error=%s; failed artifacts=%s", artifact.Name, artifact.Error, diagnosticsArtifactErrors(report))
		}
	}
}

// Collection errors have already passed through the API redactor. Preserve only
// failure identities and those safe explanations; never dump artifact Data.
func diagnosticsArtifactErrors(report *ceph.DiagnosticsReport) string {
	if report == nil {
		return "report unavailable"
	}
	var failures []string
	for _, artifact := range report.Artifacts {
		if artifact.Error != "" {
			failures = append(failures, fmt.Sprintf("%s/%s: %s", artifact.Kind, artifact.Name, artifact.Error))
		}
	}
	return strings.Join(failures, "; ")
}

func diagnosticsFindArtifact(t *testing.T, report *ceph.DiagnosticsReport, kind, name, containerID string) ceph.DiagnosticArtifact {
	t.Helper()
	var matches []ceph.DiagnosticArtifact
	for _, artifact := range report.Artifacts {
		if artifact.Kind == kind && (name == "" || artifact.Name == name) && (containerID == "" || artifact.ContainerID == containerID) {
			matches = append(matches, artifact)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one artifact kind=%s name=%s, got %d", kind, name, len(matches))
	}
	return matches[0]
}

func diagnosticsCountKind(report *ceph.DiagnosticsReport, kind string) int {
	count := 0
	for _, artifact := range report.Artifacts {
		if artifact.Kind == kind {
			count++
		}
	}
	return count
}

func diagnosticsJSONBool(data, key string) (bool, bool) {
	var value any
	if json.Unmarshal([]byte(data), &value) != nil {
		return false, false
	}
	var search func(any) (bool, bool)
	search = func(value any) (bool, bool) {
		switch value := value.(type) {
		case map[string]any:
			for name, child := range value {
				if strings.EqualFold(name, key) {
					if found, ok := child.(bool); ok {
						return found, true
					}
				}
				if found, ok := search(child); ok {
					return found, true
				}
			}
		case []any:
			for _, child := range value {
				if found, ok := search(child); ok {
					return found, true
				}
			}
		}
		return false, false
	}
	return search(value)
}

func diagnosticsDataProbe(t *testing.T, ctx context.Context, client testcontainers.Container, phase string) {
	t.Helper()
	execCommand(t, ctx, client, "python3", "-c", `import cephfs,rados,sys
payload=bytes(range(256))*128
with rados.Rados(conffile="/etc/ceph/ceph.conf",conf={"rados_mon_op_timeout":"10","rados_osd_op_timeout":"10"}) as cluster:
    with cluster.open_ioctx("tc-diagnostics") as io:
        if sys.argv[1]=="seed": io.write_full("retained",payload)
        assert io.read("retained",len(payload))==payload
fs=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout","20")
fs.conf_set("client_fs","tc-diagnostics-fs")
fs.mount()
try:
    if sys.argv[1]=="seed":
        fd=fs.open("/retained","w",0o600)
        assert fs.write(fd,payload,0)==len(payload)
        fs.fsync(fd,False)
        fs.close(fd)
    fd=fs.open("/retained","r")
    assert fs.read(fd,0,len(payload))==payload
    fs.close(fd)
finally: fs.shutdown()
print("diagnostics native RADOS/CephFS retained bytes verified")`, phase)
}
