package ceph

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// managerMetricsLimit bounds one scrape's output, and managerMetricsTimeout
// its time.
const (
	managerMetricsLimit   = 32 << 20
	managerMetricsTimeout = 30 * time.Second
)

// managerMetricsScript fetches a URL and writes its body to stdout. The
// control image's python3 is a baseline requirement; curl is not.
const managerMetricsScript = `import sys,urllib.request
with urllib.request.urlopen(sys.argv[1],timeout=20) as r:
 sys.stdout.buffer.write(r.read())
`

// MetricSample is one sample of the Prometheus text exposition format.
type MetricSample struct {
	// Name is the sample's metric name, such as ceph_osd_op_w.
	Name string
	// Labels are the sample's labels, such as ceph_daemon="osd.0".
	Labels map[string]string
	// Value is the sample's value, which may be NaN or ±Inf.
	Value float64
	// Timestamp is the sample's timestamp in milliseconds, or 0 without one.
	Timestamp int64
}

// ManagerMetrics is one scrape of the active manager's prometheus module.
type ManagerMetrics struct {
	// URL is the scraped endpoint.
	URL string
	// Types maps a metric family to the type in its # TYPE line.
	Types map[string]string
	// Help maps a metric family to the text of its # HELP line.
	Help map[string]string
	// Samples are the samples in the order the module printed them.
	Samples []MetricSample
}

// Select returns the samples named name whose labels include every label in
// labels; nil labels select every sample of that name.
func (m ManagerMetrics) Select(name string, labels map[string]string) []MetricSample {
	var selected []MetricSample
	for _, sample := range m.Samples {
		if sample.Name != name {
			continue
		}
		match := true
		for key, value := range labels {
			if got, ok := sample.Labels[key]; !ok || got != value {
				match = false
				break
			}
		}
		if match {
			selected = append(selected, sample)
		}
	}
	return selected
}

// Value returns the value of the one sample that Select finds; none, or more
// than one, is an error.
func (m ManagerMetrics) Value(name string, labels map[string]string) (float64, error) {
	selected := m.Select(name, labels)
	if len(selected) != 1 {
		return 0, fmt.Errorf("%d samples of %s match %v, want one", len(selected), name, labels)
	}
	return selected[0].Value, nil
}

// ManagerMetrics scrapes the active manager's prometheus module once and
// parses the Prometheus text exposition format. It never enables the module:
// enable it with OverrideMGRModule first, and wait until ManagerServices
// lists it. A module that serves no endpoint is ErrInvalidRequest. The
// scrape runs in the control container, on the cluster's network, at the
// address that ManagerServices advertises; it is bounded to 30 seconds and
// 32 MiB, and a failed fetch or unparsable output is a *NativeFailure or an
// error.
func (c *Container) ManagerMetrics(ctx context.Context) (ManagerMetrics, error) {
	services, err := c.ManagerServices(ctx)
	if err != nil {
		return ManagerMetrics{}, err
	}
	endpoint := services["prometheus"]
	if endpoint == "" {
		return ManagerMetrics{}, invalidRequest("the MGR prometheus module serves no metrics: enable it with OverrideMGRModule and wait for ManagerServices")
	}
	if !strings.HasSuffix(endpoint, "/") {
		endpoint += "/"
	}
	endpoint += "metrics"
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return ManagerMetrics{}, fmt.Errorf("select the control container: %w", err)
	}
	report, err := supervised.Run(ctx, control, "scrape manager metrics", managerMetricsTimeout, managerMetricsLimit, 64<<10,
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

// parseMetrics reads the Prometheus text exposition format.
func parseMetrics(text string) (ManagerMetrics, error) {
	metrics := ManagerMetrics{Types: map[string]string{}, Help: map[string]string{}}
	seen := map[string]bool{}
	for number, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			fields := strings.Fields(line)
			if len(fields) >= 3 && fields[1] == "TYPE" {
				metrics.Types[fields[2]] = strings.Join(fields[3:], " ")
			} else if len(fields) >= 3 && fields[1] == "HELP" {
				metrics.Help[fields[2]] = strings.TrimSpace(strings.SplitN(line, fields[2], 2)[1])
			}
			continue
		}
		sample, err := parseSample(line)
		if err != nil {
			return ManagerMetrics{}, fmt.Errorf("line %d: %w", number+1, err)
		}
		key := sample.Name + labelKey(sample.Labels)
		if seen[key] {
			return ManagerMetrics{}, fmt.Errorf("line %d: a second sample of %s", number+1, key)
		}
		seen[key] = true
		metrics.Samples = append(metrics.Samples, sample)
	}
	return metrics, nil
}

// labelKey is a sample's label set in a canonical order.
func labelKey(labels map[string]string) string {
	keys := slices.Sorted(maps.Keys(labels))
	var b strings.Builder
	b.WriteByte('{')
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(key + "=" + strconv.Quote(labels[key]))
	}
	b.WriteByte('}')
	return b.String()
}

// parseSample reads one sample line: name, optional {labels}, value and
// optional timestamp.
func parseSample(line string) (MetricSample, error) {
	sample := MetricSample{Labels: map[string]string{}}
	end := strings.IndexAny(line, "{ \t")
	if end <= 0 {
		return MetricSample{}, fmt.Errorf("no metric name in %q", line)
	}
	sample.Name = line[:end]
	rest := line[end:]
	if strings.HasPrefix(rest, "{") {
		var err error
		if rest, err = parseLabels(rest[1:], sample.Labels); err != nil {
			return MetricSample{}, fmt.Errorf("%s: %w", sample.Name, err)
		}
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 || len(fields) > 2 {
		return MetricSample{}, fmt.Errorf("%s: want a value and an optional timestamp, have %q", sample.Name, rest)
	}
	value, err := parseMetricValue(fields[0])
	if err != nil {
		return MetricSample{}, fmt.Errorf("%s: %w", sample.Name, err)
	}
	sample.Value = value
	if len(fields) == 2 {
		if sample.Timestamp, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
			return MetricSample{}, fmt.Errorf("%s: timestamp %q", sample.Name, fields[1])
		}
	}
	return sample, nil
}

// parseLabels reads name="value" pairs up to the closing brace, with the
// format's escapes \\, \" and \n, and returns what follows the brace.
func parseLabels(text string, labels map[string]string) (string, error) {
	for {
		text = strings.TrimLeft(text, " ")
		if strings.HasPrefix(text, "}") {
			return text[1:], nil
		}
		equal := strings.IndexByte(text, '=')
		if equal <= 0 || len(text) <= equal+1 || text[equal+1] != '"' {
			return "", fmt.Errorf("malformed label in %q", text)
		}
		name := strings.TrimSpace(text[:equal])
		var value strings.Builder
		i := equal + 2
		closed := false
		for ; i < len(text); i++ {
			switch text[i] {
			case '\\':
				if i+1 >= len(text) {
					return "", fmt.Errorf("label %s ends in an escape", name)
				}
				i++
				switch text[i] {
				case 'n':
					value.WriteByte('\n')
				case '\\', '"':
					value.WriteByte(text[i])
				default:
					return "", fmt.Errorf("label %s has the escape \\%c", name, text[i])
				}
				continue
			case '"':
				closed = true
			default:
				value.WriteByte(text[i])
				continue
			}
			break
		}
		if !closed {
			return "", fmt.Errorf("label %s is not closed", name)
		}
		if _, duplicate := labels[name]; duplicate {
			return "", fmt.Errorf("label %s appears twice", name)
		}
		labels[name] = value.String()
		text = strings.TrimLeft(text[i+1:], " ")
		if strings.HasPrefix(text, ",") {
			text = text[1:]
		} else if !strings.HasPrefix(text, "}") {
			return "", fmt.Errorf("expected , or } after label %s", name)
		}
	}
}

// parseMetricValue reads a sample value, including NaN and ±Inf.
func parseMetricValue(text string) (float64, error) {
	switch text {
	case "NaN":
		return math.NaN(), nil
	case "+Inf", "Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, fmt.Errorf("value %q", text)
	}
	return value, nil
}
