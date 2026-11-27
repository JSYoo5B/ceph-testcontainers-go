package preflight

import (
	"fmt"
	"strings"
)

// String renders the report as concise text with the same conclusions as its
// JSON: the verdict of each capability, the engine facts and each check that
// was not observed, with its reason and guidance.
func (r *Report) String() string {
	if r == nil {
		return "preflight: no report"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "preflight (schema %d, level %s)\n", r.SchemaVersion, r.Level)
	fmt.Fprintf(&b, "process: %s/%s\n", r.Process.OS, r.Process.Architecture)
	e := r.Engine
	fmt.Fprintf(&b, "engine facts: %s/%s, server %s, kernel %s, os %s, transport %s",
		known(e.OSType), known(e.Architecture), known(e.ServerVersion), known(e.KernelVersion), known(e.OperatingSystem), known(e.Transport))
	fmt.Fprintf(&b, ", desktop %s, rootless %s, userns %s", fact(e.Desktop), fact(e.Rootless), fact(e.UserNamespaces))
	if e.Backend != "" {
		fmt.Fprintf(&b, ", backend %s", e.Backend)
	}
	b.WriteString("\n")
	for _, capability := range r.Capabilities {
		fmt.Fprintf(&b, "%s: %s\n", capability, r.Verdict(capability))
	}
	for _, check := range r.Checks {
		if check.State == Observed {
			continue
		}
		fmt.Fprintf(&b, "  %s: %s", check.Key, check.State)
		if check.Kind != "" {
			fmt.Fprintf(&b, " (%s)", check.Kind)
		}
		if check.Reason != "" {
			fmt.Fprintf(&b, ": %s", check.Reason)
		}
		if check.Guidance != "" {
			fmt.Fprintf(&b, "; %s", check.Guidance)
		}
		b.WriteString("\n")
	}
	if h := r.Helper; h != nil {
		fmt.Fprintf(&b, "helper: started %t, released %t", h.Started, h.Released)
		if h.Cleanup != "" {
			fmt.Fprintf(&b, ", container %s: %s", known(h.ContainerID), h.Cleanup)
		}
		b.WriteString("\n")
	}
	b.WriteString("observed prerequisites do not prove that an operation, its effect or a recovery succeeds\n")
	return b.String()
}

func known(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func fact(value *bool) string {
	switch {
	case value == nil:
		return "unknown"
	case *value:
		return "yes"
	}
	return "no"
}
