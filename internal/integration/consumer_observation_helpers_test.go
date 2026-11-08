//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/consumerevidence"
)

func consumerActiveControl(r consumerevidence.GateReceipt) bool {
	return r.Map.Available && r.Map.Active.Name != "" && r.Map.Active.GID != 0 &&
		r.Control != nil && r.Control.Command == "tell version" && r.Control.Target == "mgr."+r.Map.Active.Name && r.Control.Complete && r.Control.ExitCode == 0
}

func consumerRunningInspection(report diagnostics.Report, role, name, id string) error {
	for _, a := range report.Artifacts {
		if a.Kind != "container-inspect" || a.Role != role || a.Name != name {
			continue
		}
		var inspect struct {
			ID    string
			State struct{ Running bool }
		}
		if report.Closed || a.Truncated || a.Error != "" {
			return fmt.Errorf("incomplete inspection of %s.%s: closed=%t truncated=%t error=%q", role, name, report.Closed, a.Truncated, a.Error)
		}
		if err := json.Unmarshal([]byte(a.Data), &inspect); err != nil {
			return fmt.Errorf("decode inspection of %s.%s: %w", role, name, err)
		}
		if id == "" || a.ContainerID != id || inspect.ID != id || !inspect.State.Running {
			return fmt.Errorf("expected running %s.%s (%s), got artifact %s for %s, running=%t", role, name, id, a.ContainerID, inspect.ID, inspect.State.Running)
		}
		return nil
	}
	return fmt.Errorf("missing inspection for %s.%s (%s)", role, name, id)
}

func archivedIntervalOrdered(down, boot, marked, wrong time.Time) bool {
	return !down.IsZero() && !marked.IsZero() && down.Before(boot) && !wrong.Before(marked)
}

func osdTailPastInterval(tail diagnostics.Artifact, pattern *regexp.Regexp) bool {
	return tail.Error == "" && strings.TrimSpace(tail.Data) != "" && !pattern.MatchString(tail.Data)
}
