// Command roleoverhead measures a warm, native Linux core and its lazy toolkit.
// It is an opt-in experiment, not a performance assertion or a CI prerequisite.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	container "github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

const ownerLabel = "org.testcontainers.ceph.cluster"

type config struct {
	All, Essential, Client, Output, ImageSource string
	Rounds, Samples                             int
	Interval, Settle                            time.Duration
}

type observation struct {
	ID, ImageID string
	Read        time.Time
	CPU         uint64 // cumulative Linux cgroup CPU nanoseconds
	Memory      uint64 // raw cgroup memory usage
	WorkingSet  uint64 // memory minus inactive_file (v2) / total_inactive_file (v1)
}

type phase struct {
	Snapshots       [][]observation
	CPUPercent      float64 // 100% is one fully occupied CPU, summed across containers
	MeanMemoryBytes float64
	MeanWorkingSet  float64
}

type trial struct {
	Round                            int
	Mode                             string
	Warmup                           bool
	StartSeconds, FirstClientSeconds float64
	RBDProofSeconds                  float64
	Core, WithClient                 phase
	CleanupPassed                    bool
}

type report struct {
	Schema                   int
	Config                   config
	Revision                 string
	Dirty                    bool
	Platform, Docker, Kernel string
	Cgroup, StorageDriver    string
	CPUs                     int
	MemoryBytes              int64
	ImageIDs                 map[string]string
	Trials                   []trial
	Complete                 bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.All, "all", "", "locally available aggregate image")
	flag.StringVar(&cfg.Essential, "essential", "", "locally available essential image")
	flag.StringVar(&cfg.Client, "client", "", "locally available client image")
	flag.StringVar(&cfg.ImageSource, "image-source", "", "image assembly source revision (provenance supplied by operator)")
	flag.StringVar(&cfg.Output, "output", "role-overhead.json", "JSON output; saved after every trial, including failures")
	flag.IntVar(&cfg.Rounds, "rounds", 5, "measured paired rounds, after one warmup pair")
	flag.IntVar(&cfg.Samples, "samples", 11, "statistics samples per phase")
	flag.DurationVar(&cfg.Interval, "interval", time.Second, "minimum gap between statistics snapshots")
	flag.DurationVar(&cfg.Settle, "settle", 5*time.Second, "idle settling time before each measured phase")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	if runtime.GOOS != "linux" || cfg.All == "" || cfg.Essential == "" || cfg.Client == "" || cfg.ImageSource == "" ||
		cfg.Rounds < 1 || cfg.Samples < 2 || cfg.Interval <= 0 || cfg.Settle < 0 {
		return errors.New("require native Linux, all three images, image-source, positive rounds/interval, at least two samples and nonnegative settle")
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	if !strings.HasPrefix(docker.DaemonHost(), "unix://") {
		return errors.New("run on the Docker host with a local Unix socket")
	}
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		return err
	}
	if info.Info.OSType != "linux" {
		return errors.New("require a Linux Docker engine")
	}
	if info.Info.CgroupVersion != "1" && info.Info.CgroupVersion != "2" {
		return fmt.Errorf("unsupported cgroup version %q", info.Info.CgroupVersion)
	}
	r := report{Schema: 1, Config: cfg, Platform: "linux/" + runtime.GOARCH, Docker: info.Info.ServerVersion,
		Kernel: info.Info.KernelVersion, Cgroup: info.Info.CgroupVersion, StorageDriver: info.Info.Driver,
		CPUs: info.Info.NCPU, MemoryBytes: info.Info.MemTotal, ImageIDs: map[string]string{}}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				r.Revision = setting.Value
			case "vcs.modified":
				r.Dirty = setting.Value == "true"
			}
		}
	}
	// Resolve before any side effects; the experiment never pulls or retags.
	for name, ref := range map[string]string{"all": cfg.All, "essential": cfg.Essential, "client": cfg.Client} {
		image, err := docker.ImageInspect(ctx, ref)
		if err != nil {
			return err
		}
		if image.Os != "linux" || image.Architecture != runtime.GOARCH {
			return fmt.Errorf("%s is not a native Linux image", name)
		}
		r.ImageIDs[name] = image.ID
	}
	for round := 0; round <= cfg.Rounds; round++ {
		modes := []string{"all", "split"}
		if round%2 == 1 {
			slices.Reverse(modes)
		}
		for _, mode := range modes {
			result, trialErr := measureTrial(ctx, docker, cfg, r.ImageIDs, round, mode, r.Cgroup)
			r.Trials = append(r.Trials, result)
			if err := save(cfg.Output, r); err != nil {
				return errors.Join(trialErr, err)
			}
			if trialErr != nil {
				return trialErr
			}
			fmt.Fprintf(os.Stderr, "round=%d mode=%s start=%.3fs client=%.3fs containers=%d->%d cleanup=%t\n",
				round, mode, result.StartSeconds, result.FirstClientSeconds, len(result.Core.Snapshots[0]), len(result.WithClient.Snapshots[0]), result.CleanupPassed)
		}
	}
	r.Complete = true
	return save(cfg.Output, r)
}

func save(path string, r report) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func measureTrial(ctx context.Context, docker *testcontainers.DockerClient, cfg config, images map[string]string, round int, mode, cgroupVersion string) (result trial, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result = trial{Round: round, Mode: mode, Warmup: round == 0}
	selection := ceph.ImageSet{All: images["all"]}
	if mode == "split" {
		selection = ceph.ImageSet{Essential: images["essential"], Client: images["client"]}
	}
	started := time.Now()
	c, err := ceph.RunWithImages(ctx, selection, ceph.WithOSDCount(1))
	result.StartSeconds = time.Since(started).Seconds()
	var owner string
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		if c == nil {
			resultErr = errors.Join(resultErr, errors.New("no cluster handle: cleanup not confirmed"))
			return
		}
		terminateErr := c.Terminate(cleanup)
		if owner == "" {
			resultErr = errors.Join(resultErr, terminateErr, errors.New("no owner label: cleanup not confirmed"))
			return
		}
		remaining, listErr := docker.ContainerList(cleanup, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", ownerLabel+"="+owner)})
		result.CleanupPassed = terminateErr == nil && listErr == nil && len(remaining.Items) == 0
		resultErr = errors.Join(resultErr, terminateErr, listErr)
		if !result.CleanupPassed {
			resultErr = errors.Join(resultErr, errors.New("owned container cleanup not confirmed"))
		}
	}()
	if err != nil {
		return result, err
	}
	inspect, err := docker.ContainerInspect(ctx, c.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		return result, err
	}
	owner = inspect.Container.Config.Labels[ownerLabel]
	if owner == "" {
		return result, errors.New("cluster has no ownership label")
	}
	result.Core, err = measurePhase(ctx, docker, owner, cfg, cgroupVersion)
	if err != nil {
		return result, err
	}
	coreID := images["all"]
	if mode == "split" {
		coreID = images["essential"]
	}
	if err := composition(result.Core, coreID, ""); err != nil {
		return result, err
	}
	started = time.Now()
	toolkit, err := c.ClientContainer(ctx)
	result.FirstClientSeconds = time.Since(started).Seconds()
	if err != nil {
		return result, err
	}
	started = time.Now()
	if _, err := c.CreatePool(ctx, ceph.PoolConfig{Name: "overhead", PGNum: 1, Application: "rbd"}); err != nil {
		return result, err
	}
	if err := rbd.InitPool(ctx, c, "overhead"); err != nil {
		return result, err
	}
	if err := c.WaitForClean(ctx); err != nil {
		return result, err
	}
	// Exercise the selected native toolkit, retaining an actual RBD data proof.
	code, output, err := toolkit.Exec(ctx, []string{"python3", "-c", `import rados, rbd
with rados.Rados(conffile='/etc/ceph/ceph.conf') as cluster:
    with cluster.open_ioctx('overhead') as pool:
        rbd.RBD().create(pool, 'probe', 1048576)
        with rbd.Image(pool, 'probe') as image:
            data = b'role-overhead-proof' * 256
            image.write(data, 0)
            assert image.read(0, len(data)) == data
        rbd.RBD().remove(pool, 'probe')
`})
	if err != nil || code != 0 {
		var detail []byte
		if output != nil {
			detail, _ = io.ReadAll(io.LimitReader(output, 4096))
		}
		return result, fmt.Errorf("RBD proof exit %d: %s: %w", code, detail, err)
	}
	result.RBDProofSeconds = time.Since(started).Seconds()
	result.WithClient, err = measurePhase(ctx, docker, owner, cfg, cgroupVersion)
	if err == nil {
		clientID := ""
		if mode == "split" {
			clientID = images["client"]
		}
		err = composition(result.WithClient, coreID, clientID)
	}
	return result, err
}

func composition(p phase, coreID, clientID string) error {
	for _, snapshot := range p.Snapshots {
		core, client := 0, 0
		for _, value := range snapshot {
			switch value.ImageID {
			case coreID:
				core++
			case clientID:
				client++
			default:
				return errors.New("unexpected runtime image identity")
			}
		}
		wantClient := 0
		if clientID != "" {
			wantClient = 1
		}
		if core != 3 || client != wantClient {
			return fmt.Errorf("expected three core containers and %d toolkit, got %d and %d", wantClient, core, client)
		}
	}
	return nil
}

func pause(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

func measurePhase(ctx context.Context, docker *testcontainers.DockerClient, owner string, cfg config, cgroupVersion string) (p phase, err error) {
	if err := pause(ctx, cfg.Settle); err != nil {
		return p, err
	}
	for i := 0; i < cfg.Samples; i++ {
		if i > 0 {
			if err := pause(ctx, cfg.Interval); err != nil {
				return p, err
			}
		}
		list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", ownerLabel+"="+owner)})
		if err != nil {
			return p, err
		}
		var snapshot []observation
		for _, ctr := range list.Items {
			if ctr.State != container.StateRunning {
				return p, errors.New("owned container is not running")
			}
			stats, err := docker.ContainerStats(ctx, ctr.ID, mobycl.ContainerStatsOptions{})
			if err != nil {
				return p, err
			}
			var value container.StatsResponse
			err = json.NewDecoder(stats.Body).Decode(&value)
			closeErr := stats.Body.Close()
			if err != nil || closeErr != nil {
				return p, errors.Join(err, closeErr)
			}
			memory, err := workingSet(value.MemoryStats, cgroupVersion)
			if err != nil {
				return p, err
			}
			snapshot = append(snapshot, observation{ID: ctr.ID, ImageID: ctr.ImageID, Read: value.Read,
				CPU: value.CPUStats.CPUUsage.TotalUsage, Memory: value.MemoryStats.Usage, WorkingSet: memory})
		}
		slices.SortFunc(snapshot, func(a, b observation) int { return strings.Compare(a.ID, b.ID) })
		p.Snapshots = append(p.Snapshots, snapshot)
	}
	return summarize(p)
}

func workingSet(m container.MemoryStats, cgroupVersion string) (uint64, error) {
	var key string
	switch cgroupVersion {
	case "1":
		key = "total_inactive_file"
	case "2":
		key = "inactive_file"
	default:
		return 0, fmt.Errorf("unsupported cgroup version %q", cgroupVersion)
	}
	inactive := m.Stats[key]
	if inactive < m.Usage {
		return m.Usage - inactive, nil
	}
	return 0, nil
}

func summarize(p phase) (phase, error) {
	if len(p.Snapshots) < 2 || len(p.Snapshots[0]) == 0 {
		return p, errors.New("insufficient statistics")
	}
	first := p.Snapshots[0]
	for sample, snapshot := range p.Snapshots {
		if len(snapshot) != len(first) {
			return p, errors.New("container inventory changed during phase")
		}
		for i, value := range snapshot {
			if value.ID != first[i].ID || value.ImageID != first[i].ImageID {
				return p, errors.New("container identity changed during phase")
			}
			if sample > 0 {
				previous := p.Snapshots[sample-1][i]
				if !value.Read.After(previous.Read) || value.CPU < previous.CPU {
					return p, errors.New("CPU clock/counter did not advance monotonically")
				}
			}
			p.MeanMemoryBytes += float64(value.Memory) / float64(len(p.Snapshots))
			p.MeanWorkingSet += float64(value.WorkingSet) / float64(len(p.Snapshots))
		}
	}
	for i, last := range p.Snapshots[len(p.Snapshots)-1] {
		elapsed := last.Read.Sub(first[i].Read)
		if elapsed <= 0 || last.CPU < first[i].CPU {
			return p, errors.New("CPU clock/counter did not advance monotonically")
		}
		p.CPUPercent += 100 * float64(last.CPU-first[i].CPU) / float64(elapsed)
	}
	return p, nil
}
