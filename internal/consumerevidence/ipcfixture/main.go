// Command ipcfixture is a fixture process that a test drives over standard
// input and output, as a consumer whose fixture is a separate process without
// a testing.TB runs one. It starts a cluster of three MONs, two MGRs and no
// OSD, answers each command line with one JSON line, and on "close", on the
// end of its input or on an interrupt ends the cluster with
// consumerevidence.Finish. The protocol and its records are this example's,
// not the module's.
//
// Commands:
//
//	gate LABEL       the native MGR keyring gate
//	transition       restart the active MGR and record the transition receipt
//	reject           a stand-in SUT authentication: one native client with a
//	                 wrong key addresses the active MGR, once
//	bundle REQUEST   the first-failure bundle of a JSON AuthRequest
//	fail REASON      the driver's failure signal, kept until the end
//	close            end the fixture
//
// With a failure signal, Finish writes the diagnostics report into the -out
// directory before Terminate, and the process exits with status 1 even when
// the collection and Terminate succeeded. The input ending without "close" is
// a failure signal of its own: the driver went away.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/consumerevidence"
)

// reply is one line of the protocol.
type reply struct {
	Reply  string `json:"reply"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Result any    `json:"result,omitempty"`
}

// ready describes the started fixture to the driver, which checks its
// cleanup independently.
type ready struct {
	FSID       string   `json:"fsid"`
	Monitors   []string `json:"monitors"`
	Managers   []string `json:"managers"`
	Containers []string `json:"containers"`
	Network    string   `json:"network"`
}

const (
	startBound   = 10 * time.Minute
	commandBound = 10 * time.Minute
)

func main() {
	image := flag.String("image", "", "the essential image, or the image of every role without -client-image")
	clientImage := flag.String("client-image", "", "the client role image")
	out := flag.String("out", "", "the directory of the diagnostics report")
	flag.Parse()
	if *image == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "ipcfixture: -image and -out are required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	encoder := json.NewEncoder(os.Stdout)
	send := func(r reply) {
		if err := encoder.Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, "ipcfixture: write reply:", err)
		}
	}

	images := ceph.ImageSet{All: *image}
	if *clientImage != "" {
		images = ceph.ImageSet{Essential: *image, Client: *clientImage}
	}
	startCtx, cancel := context.WithTimeout(ctx, startBound)
	cluster, err := ceph.Run(startCtx, *image, ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithManagerCount(2).WithNoInitialOSDs()))
	var started ready
	if err == nil {
		started, err = waitReady(startCtx, cluster)
	}
	cancel()
	var signalErr error
	if err != nil {
		send(reply{Reply: "ready", Error: err.Error()})
		signalErr = fmt.Errorf("start: %w", err)
	} else {
		send(reply{Reply: "ready", OK: true, Result: started})
		signalErr = serve(ctx, cluster, send)
	}
	if cluster == nil {
		send(reply{Reply: "finished", Error: signalErr.Error()})
		os.Exit(1)
	}
	err = consumerevidence.Finish(cluster, signalErr, func(_ context.Context, report *diagnostics.Report) error {
		data, err := json.Marshal(report)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(*out, "diagnostics.json"), data, 0o600)
	}, consumerevidence.FinishBounds{Collect: 2 * time.Minute, Save: 30 * time.Second, Terminate: 3 * time.Minute,
		Diagnostics: diagnostics.Config{Timeout: 90 * time.Second}})
	if err != nil {
		send(reply{Reply: "finished", Error: err.Error()})
		os.Exit(1)
	}
	send(reply{Reply: "finished", OK: true})
}

// waitReady waits until all three MONs are in the quorum and one MGR is
// active with the other standing by, and describes the fixture.
func waitReady(ctx context.Context, cluster *ceph.Container) (ready, error) {
	observer, err := ceph.Observe(cluster)
	if err != nil {
		return ready{}, err
	}
	var started ready
	for {
		quorum, qerr := observer.QuorumStatus(ctx)
		status, serr := observer.ManagerStatus(ctx)
		if qerr == nil && serr == nil && len(quorum.QuorumNames) == 3 && status.Available && len(status.Standbys) == 1 {
			started.FSID = quorum.MonMap.FSID
			break
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ready{}, fmt.Errorf("MONs and MGRs not ready: quorum %v %v, MGRs %+v %v: %w", quorum.QuorumNames, qerr, status, serr, ctx.Err())
		case <-timer.C:
		}
	}
	monitors, err := cluster.Monitors(ctx)
	if err != nil {
		return ready{}, err
	}
	managers, err := cluster.Managers(ctx)
	if err != nil {
		return ready{}, err
	}
	control, err := cluster.ControlContainer(ctx)
	if err != nil {
		return ready{}, err
	}
	started.Containers = []string{cluster.GetContainerID(), control.GetContainerID()}
	for _, monitor := range monitors {
		started.Monitors = append(started.Monitors, monitor.DaemonName)
		started.Containers = append(started.Containers, monitor.GetContainerID())
	}
	for _, manager := range managers {
		started.Managers = append(started.Managers, manager.DaemonName)
		started.Containers = append(started.Containers, manager.GetContainerID())
	}
	slices.Sort(started.Containers)
	started.Containers = slices.Compact(started.Containers)
	facts, err := ceph.FactsOf(cluster)
	started.Network = facts.NetworkName
	return started, err
}

// serve answers commands until "close", the end of the input or ctx, and
// returns the failure signal for Finish.
func serve(ctx context.Context, cluster *ceph.Container, send func(reply)) error {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 64<<10), 1<<20)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	var failures []error
	for {
		var line string
		var open bool
		select {
		case <-ctx.Done():
			return errors.Join(append(failures, fmt.Errorf("interrupted: %w", ctx.Err()))...)
		case line, open = <-lines:
		}
		if !open {
			return errors.Join(append(failures, errors.New("the driver closed the input without close"))...)
		}
		command, argument, _ := strings.Cut(strings.TrimSpace(line), " ")
		switch command {
		case "close":
			return errors.Join(failures...)
		case "fail":
			failures = append(failures, errors.New("driver failure signal: "+argument))
			send(reply{Reply: command, OK: true})
		default:
			call, cancel := context.WithTimeout(ctx, commandBound)
			result, err := run(call, cluster, command, argument)
			cancel()
			r := reply{Reply: command, OK: err == nil, Result: result}
			if err != nil {
				r.Error = err.Error()
			}
			send(r)
		}
	}
}

// run runs one recipe command.
func run(ctx context.Context, cluster *ceph.Container, command, argument string) (any, error) {
	switch command {
	case "gate":
		return consumerevidence.CheckGate(ctx, cluster, argument, consumerevidence.GateBounds{
			Query: 20 * time.Second, LogScan: 30 * time.Second, Control: 30 * time.Second, LogBytes: 64 << 20})
	case "transition":
		return consumerevidence.RecordTransition(ctx, cluster, consumerevidence.TransitionBounds{
			Deadline: 5 * time.Minute, Query: 20 * time.Second, Pause: 2 * time.Second, StopGrace: 10 * time.Second})
	case "reject":
		return reject(ctx, cluster)
	case "bundle":
		var request consumerevidence.AuthRequest
		decoder := json.NewDecoder(strings.NewReader(argument))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			return nil, fmt.Errorf("bundle request: %w", err)
		}
		bundle := consumerevidence.CollectAuthBundle(ctx, cluster, request, consumerevidence.AuthBounds{
			Budget: 2 * time.Minute, Query: 15 * time.Second, ControlOutput: 4 << 10})
		data, err := json.Marshal(bundle)
		if err != nil {
			return nil, err
		}
		if err := consumerevidence.CheckAuthBundleSecrecy(ctx, cluster, data); err != nil {
			return nil, err
		}
		return struct {
			Bundle  consumerevidence.AuthBundle  `json:"bundle"`
			Verdict consumerevidence.AuthVerdict `json:"verdict"`
		}{bundle, consumerevidence.ClassifyAuthBundle(bundle)}, nil
	}
	return nil, errors.New("unknown command " + command)
}

// reject is the stand-in SUT: one native client authenticates as
// client.admin with a key of its own making and addresses the active MGR.
// The MONs refuse it. It runs once and is never retried; its record is the
// first rejection that a bundle keeps.
func reject(ctx context.Context, cluster *ceph.Container) (consumerevidence.Rejection, error) {
	observer, err := ceph.Observe(cluster)
	if err != nil {
		return consumerevidence.Rejection{}, err
	}
	status, err := observer.ManagerStatus(ctx)
	if err != nil {
		return consumerevidence.Rejection{}, err
	}
	if !status.Available {
		return consumerevidence.Rejection{}, errors.New("no MGR is available")
	}
	runner, err := cluster.ControlContainer(ctx)
	if err != nil {
		return consumerevidence.Rejection{}, err
	}
	target := "mgr." + status.ActiveName
	rejection := consumerevidence.Rejection{Run: "ipcfixture", Attempt: 1, At: time.Now().UTC(), Target: target, TargetGID: status.ActiveGID}
	result, err := cluster.RunNativeCommand(ctx, runner, ceph.NativeCommand{
		Argv: []string{"sh", "-c", `k=$(mktemp) || exit 99
ceph-authtool -C -g -n client.admin "$k" >/dev/null || { rm -f "$k"; exit 98; }
ceph -k "$k" -n client.admin --connect-timeout 10 tell "$1" version
rc=$?
rm -f "$k"
exit $rc`, "sh", target},
		Timeout: time.Minute, MaxStdout: 4 << 10, MaxStderr: 16 << 10, Label: "stand-in SUT authentication",
	})
	if err != nil {
		return rejection, err
	}
	stderr := strings.Split(strings.TrimSpace(string(result.Stderr)), "\n")
	rejection.Result, rejection.Error = -result.ExitCode, stderr[len(stderr)-1]
	if result.ExitCode != 13 {
		return rejection, fmt.Errorf("the stand-in SUT was not refused with EACCES: exit %d", result.ExitCode)
	}
	return rejection, nil
}
