package ceph

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// ManagerMetrics is the observation that runs a supervised command in the
// control container. Its implementation is here against two narrow contracts.
// The exported method keeps its place on Container and passes
// Container.ManagerServices and a control lookup bound to this container. Each
// call advertises and selects the control container again, in the order the
// inline code did: services first, then the endpoint check, then the lookup.

// managerServicesReader returns the HTTP endpoints that the active manager
// advertises, keyed by module name.
type managerServicesReader func(ctx context.Context) (map[string]string, error)

// supervisedRun runs one bounded command in an already selected container.
type supervisedRun func(ctx context.Context, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (supervised.Report, error)

// supervisedLookup selects the container for one supervised command. A lookup
// failure is returned before any command is attempted.
type supervisedLookup func(ctx context.Context) (supervisedRun, error)

func readManagerMetrics(ctx context.Context, services managerServicesReader, lookup supervisedLookup) (ManagerMetrics, error) {
	advertised, err := services(ctx)
	if err != nil {
		return ManagerMetrics{}, err
	}
	endpoint := advertised["prometheus"]
	if endpoint == "" {
		return ManagerMetrics{}, invalidRequest("the MGR prometheus module serves no metrics: enable it with OverrideMGRModule and wait for ManagerServices")
	}
	if !strings.HasSuffix(endpoint, "/") {
		endpoint += "/"
	}
	endpoint += "metrics"
	run, err := lookup(ctx)
	if err != nil {
		return ManagerMetrics{}, fmt.Errorf("select the control container: %w", err)
	}
	report, err := run(ctx, "scrape manager metrics", managerMetricsTimeout, managerMetricsLimit, 64<<10,
		"python3", "-c", managerMetricsScript, endpoint)
	if err != nil {
		return ManagerMetrics{}, fmt.Errorf("scrape %s: %w", endpoint, err)
	}
	switch {
	case report.TimedOut:
		return ManagerMetrics{}, fmt.Errorf("scrape %s timed out after %v", endpoint, managerMetricsTimeout)
	case report.ExitCode != 0:
		return ManagerMetrics{}, fmt.Errorf("scrape %s: %w", endpoint, nativeFailure("python3", report.ExitCode, nil, report.Stderr))
	case report.StdoutTruncated:
		return ManagerMetrics{}, fmt.Errorf("scrape %s: the metrics exceed %d bytes", endpoint, managerMetricsLimit)
	}
	metrics, err := parseMetrics(string(report.Stdout))
	if err != nil {
		return ManagerMetrics{}, fmt.Errorf("parse the metrics of %s: %w", endpoint, err)
	}
	if len(metrics.Samples) == 0 {
		// A standby manager answers with an empty page.
		return ManagerMetrics{}, fmt.Errorf("%s served no samples", endpoint)
	}
	metrics.URL = endpoint
	return metrics, nil
}

// controlSupervisedLookup binds the lookup to this container. Each call
// selects the control container again, as the inline code did.
func (c *Container) controlSupervisedLookup() supervisedLookup {
	return func(ctx context.Context) (supervisedRun, error) {
		control, err := c.controlContainerContext(ctx)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, argv ...string) (supervised.Report, error) {
			return supervised.Run(ctx, control, label, processLimit, stdoutLimit, stderrLimit, argv...)
		}, nil
	}
}
