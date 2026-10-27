//go:build (all || integration) && (((debug_overhead || debug_ms_window) && !ci) || (ci_optional && (!ci_batch || ci_batch_debug_overhead || ci_batch_debug_ms_window)))

package debugoverhead_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type debugOverheadArm struct {
	Trial             int                         `json:"trial"`
	Position          int                         `json:"position"`
	Level             string                      `json:"level"`
	Applied           ceph.DebugLevelReceipt      `json:"applied"`
	Restored          ceph.DebugLevelReceipt      `json:"restored"`
	Before            debugOverheadSample         `json:"before"`
	After             debugOverheadSample         `json:"after"`
	CPUNanos          uint64                      `json:"cpu_delta_ns"`
	MemoryDelta       int64                       `json:"memory_usage_delta_bytes"`
	LogStart          time.Time                   `json:"log_start"`
	LogEnd            time.Time                   `json:"log_end"`
	NormalOutputBytes int64                       `json:"docker_log_payload_bytes"`
	Workload          debugOverheadWorkloadReport `json:"workload"`
	Operation         debugOperationInterval      `json:"operation_interval"`
}

// runDebugOverhead keeps the workload and accounting identical across explicit options.
func runDebugOverhead(t *testing.T, option string) {
	t.Helper()
	if option != "debug_osd" && option != "debug_ms" {
		t.Fatal("measurement option must be selected explicitly")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image := os.Getenv("CEPH_TEST_IMAGE")
	if image == "" {
		image = ceph.DefaultImage
	}
	images := ceph.ImageSet{All: image}
	if os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE") != "" || os.Getenv("SCENARIO_IMAGE_LAYOUT") == "roles" {
		images = ceph.ImageSet{Essential: os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), Client: os.Getenv("CEPH_TEST_CLIENT_IMAGE")}
		if images.Essential == "" || images.Client == "" {
			t.Fatal("debug measurement requires explicit essential and client role images")
		}
		image = images.Essential
	}
	opts := []testcontainers.ContainerCustomizer{ceph.WithImages(images)}
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1))...)
	var finalization *ceph.Finalization
	if cluster != nil {
		var registerErr error
		finalization, registerErr = cluster.TerminateOnCleanup(t, ceph.FinalizationConfig{Archive: true, Scenario: "osd-" + option + "-overhead"})
		if registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	osd := clusteraccess.OSDs(t, cluster)[0]
	inspection, err := docker.Client.ContainerInspect(ctx, osd.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil || inspection.Container.State == nil || inspection.Container.Config == nil || inspection.Container.HostConfig == nil {
		t.Fatal("OSD inspection unavailable", err)
	}
	ctr := inspection.Container
	gen := containerGeneration{ID: ctr.ID, StartedAt: ctr.State.StartedAt}
	logConfig := ctr.HostConfig.LogConfig
	if logConfig.Type != "json-file" || logConfig.Config["max-size"] != "" && logConfig.Config["max-size"] != "-1" {
		t.Fatalf("measurement needs unrotated json-file Docker logs, got driver %q, max-size %q", logConfig.Type, logConfig.Config["max-size"])
	}
	client, err := cluster.ClientContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clientInspect, err := client.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	quorum, err := observeCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const pool, operations, objectBytes = "tc-debug-overhead", 128, 32768
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Replicas: 1, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// Create and exercise the object before any measured arm. All measured
	// writes then replace an existing object. This fixed warm-up is
	// unmeasured and does not claim that all cache/ring history was reset.
	warmupText := execOutput(t, ctx, client, "python3", "-c", debugOverheadWorkload, pool, fmt.Sprint(operations), fmt.Sprint(objectBytes))
	var warmup debugOverheadWorkloadReport
	if err := json.Unmarshal([]byte(warmupText), &warmup); err != nil || warmup.Writes != operations || warmup.Reads != operations || warmup.ObjectBytes != objectBytes {
		t.Fatalf("fixed warm-up did not complete: %s %v", warmupText, err)
	}
	time.Sleep(time.Second)
	osds, err := cluster.OSDs(ctx)
	if err != nil {
		t.Fatalf("list OSDs: %v", err)
	}
	var osd0 *ceph.OSDContainer
	for _, osd := range osds {
		if osd.ID == 0 {
			osd0 = osd
		}
	}
	if osd0 == nil {
		t.Fatal("osd.0 is not owned by the cluster")
	}
	get := func(name string) string {
		t.Helper()
		out, err := osd0.AdminSocket(ctx, "config", "get", name)
		var fields map[string]json.RawMessage
		if err != nil || json.Unmarshal(out, &fields) != nil || len(fields[name]) == 0 {
			t.Fatalf("effective option %s unavailable: %s %v", name, out, err)
		}
		var text string
		if json.Unmarshal(fields[name], &text) == nil {
			return text
		}
		return string(fields[name])
	}
	options := map[string]string{}
	for _, name := range []string{"log_to_file", "log_to_stderr", "err_to_stderr", "log_to_syslog", "log_to_journald", "log_max_new", "log_max_recent"} {
		options[name] = get(name)
	}
	for _, name := range []string{"log_max_new", "log_max_recent"} {
		value, err := strconv.Atoi(options[name])
		if err != nil || value < 1 || value > 1000000 {
			t.Fatalf("measurement needs finite logging queues: %s=%q", name, options[name])
		}
	}
	prior := get(option)
	var arms []debugOverheadArm
	levels := []string{"0/0", "0/20", "20/20"}
	for trial := range 3 {
		for position := range 3 {
			level := levels[(trial+position)%len(levels)]
			arm := debugOverheadArm{Trial: trial + 1, Position: position + 1, Level: level}
			window, applyErr := cluster.OverrideDebugLevel(ctx, ceph.DebugLevelSetting{Daemon: "osd.0", Name: option, Level: level})
			if window != nil {
				finalization.TrackDebugLevel(window)
				finalization.Cleanup(fmt.Sprintf("restore trial %d position %d", trial+1, position+1), window.Restore)
			}
			if applyErr != nil {
				t.Fatal(applyErr)
			}
			arm.Applied = window.Receipt()
			if arm.Applied.State != ceph.DebugLevelApplied || arm.Applied.Applied != level || arm.Applied.Prior != prior ||
				arm.Applied.Generation != gen.ID+"@"+gen.StartedAt || get(option) != level {
				t.Fatalf("window was not admitted on the selected generation: %+v", arm.Applied)
			}
			arm.Before, err = sampleDebugOverhead(ctx, docker.Client, gen)
			if err != nil {
				t.Fatal(err)
			}
			arm.LogStart, err = debugOverheadClock(ctx, docker.Client)
			if err != nil {
				t.Fatal(err)
			}
			arm.Operation = debugOperationInterval{StartedAt: time.Now().UTC(), BudgetNanos: time.Minute.Nanoseconds()}
			out := execOutput(t, ctx, client, "python3", "-c", debugOverheadWorkload, pool, fmt.Sprint(operations), fmt.Sprint(objectBytes))
			arm.Operation.FinishedAt = time.Now().UTC()
			if err := arm.Operation.validate(); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(out), &arm.Workload); err != nil || arm.Workload.Writes != operations || arm.Workload.Reads != operations ||
				arm.Workload.ObjectBytes != objectBytes || arm.Workload.ElapsedNanos == 0 || len(arm.Workload.PairNanos) != operations {
				t.Fatalf("workload did not complete the fixed operations: %s %v", out, err)
			}
			// A fixed drain observation is part of every accounting window. It
			// is not proof that Ceph's asynchronous log pipeline became empty.
			time.Sleep(time.Second)
			arm.LogEnd, err = debugOverheadClock(ctx, docker.Client)
			if err != nil {
				t.Fatal(err)
			}
			arm.After, err = sampleDebugOverhead(ctx, docker.Client, gen)
			if err != nil {
				t.Fatal(err)
			}
			arm.CPUNanos, arm.MemoryDelta, err = debugOverheadDelta(arm.Before, arm.After)
			if err != nil {
				t.Fatal(err)
			}
			arm.NormalOutputBytes, err = collectDebugOverheadLogs(ctx, docker.Client, gen, ctr.Config.Tty, arm.LogStart, arm.LogEnd)
			if err != nil {
				t.Fatal(err)
			}
			restore, done := context.WithTimeout(context.Background(), time.Minute)
			err = window.Restore(restore)
			done()
			arm.Restored = window.Receipt()
			if err != nil || arm.Restored.State != ceph.DebugLevelRestored || arm.Restored.Restored != prior || get(option) != prior {
				t.Fatalf("prior %s was not restored: %+v %v", option, arm.Restored, err)
			}
			if warmup.SHA256 != arm.Workload.SHA256 {
				t.Fatal("the arms did not use the same known bytes")
			}
			arms = append(arms, arm)
		}
	}
	imageIDs := []string{ctr.Image, clientInspect.Image}
	slices.Sort(imageIDs)
	imageIDs = slices.Compact(imageIDs)
	// A fresh process performs the final read after every measured window
	// and every restoration; it is not part of an arm's latency or counters.
	readback := execOutput(t, ctx, client, "python3", "-c", `
import hashlib,json,rados,sys
cluster=rados.Rados(conffile='/etc/ceph/ceph.conf'); cluster.connect()
io=cluster.open_ioctx(sys.argv[1]); size=int(sys.argv[2])
expected=bytes((i*37+11)%256 for i in range(size))
actual=io.read('same-object',size+1,0)
assert actual==expected
io.close(); cluster.shutdown()
print(json.dumps(dict(bytes=len(actual),sha256=hashlib.sha256(actual).hexdigest())))
`, pool, fmt.Sprint(objectBytes))
	var freshReader map[string]any
	if err := json.Unmarshal([]byte(readback), &freshReader); err != nil || freshReader["sha256"] != arms[0].Workload.SHA256 {
		t.Fatalf("independent final bytes unavailable: %s %v", readback, err)
	}
	report := map[string]any{"schema": "ceph-debug-overhead/v1", "fsid": quorum.MonMap.FSID, "daemon": "osd.0", "generation": gen,
		"option": option, "trials_per_arm": 3, "images": imageIDs, "docker_log_driver": logConfig.Type,
		"ceph_version":   strings.TrimSpace(execOutput(t, ctx, clusteraccess.ControlContainer(t, cluster), "ceph", "--version")),
		"client_version": strings.TrimSpace(execOutput(t, ctx, client, "rados", "--version")),
		"platform":       runtime.GOOS + "/" + runtime.GOARCH, "go_version": runtime.Version(),
		"effective_log_options": options, "fixed_post_workload_wait_ns": time.Second.Nanoseconds(), "log_limit_bytes_per_arm": debugOverheadLogLimit,
		"recent_ring_reset": false, "dump_recent_executed": false, "warmup": warmup, "arms": arms, "fresh_reader": freshReader,
		"notes": []string{"measurement controls, not recommended levels", "same generation and recent ring across arms; restored thresholds do not clear the ring",
			"CPU is Docker cumulative container CPU time; memory is two total-usage endpoints including caches, not peak or process RSS",
			"normal output counts bounded Docker stdout/stderr payloads within engine-clock windows; file/syslog/journal output bytes are not counted",
			"accounting intervals include connection, instrumentation, one-second wait and background work; workload pair latency excludes connection setup",
			"three cyclic orders are paired observations, not randomized independent samples or additive causal overhead estimates"}}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("DEBUG_OVERHEAD_RECEIPT %s", encoded)
}

func execOutput(t *testing.T, parent context.Context, ctr testcontainers.Container, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	code, reader, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
	if err != nil || len(data) > 1<<20 || code != 0 {
		t.Fatalf("measurement command %s exited %d: %s (%v)", args[0], code, data, err)
	}
	return strings.TrimSpace(string(data))
}

// observeCluster returns the Observer of cluster; a nil cluster yields a nil
// Observer whose queries return an error.
func observeCluster(cluster *ceph.Container) *ceph.Observer {
	observer, _ := ceph.Observe(cluster)
	return observer
}
