//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_group_image_namespace)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package rbd_group_image_namespace_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.0-all"

func TestRBDGroupImageNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log(harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	out := harness.ExecOutput(t, ctx, client, "python3", "-c", probe)
	t.Log(out)
	outcomes := make(map[string]bool)
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Split(line, "|")
		if len(fields) != 3 {
			continue
		}
		operation, verdict, symptoms := fields[0], fields[1], fields[2]
		if (operation != "add" && operation != "remove") || (verdict != "REPRODUCED" && verdict != "NOT_REPRODUCED") || outcomes[operation] {
			t.Fatalf("invalid membership result %q", line)
		}
		outcomes[operation] = true
		t.Logf("EXPERIMENT_RESULT case=rbd_group_image_namespace/%s result=%s image=%s symptoms=%s", operation, verdict, image, symptoms)
	}
	if len(outcomes) != 2 {
		t.Fatalf("incomplete membership outcomes: %v", outcomes)
	}
}

const probe = `import json
import subprocess

pool = "group-ns"
namespace = "selected"
selected = pool + "/" + namespace


def run(*args):
    return subprocess.check_output(args, text=True, timeout=30).strip()


def members(prefix, group):
    rows = json.loads(
        run("rbd", "group", "image", "list", prefix + "/" + group, "--format", "json")
    )
    return sorted((row["pool"], row["namespace"], row["image"]) for row in rows)


def both(group):
    return members(selected, group), members(pool, group)


def require_members(group, expected):
    observed = both(group)
    if observed != expected:
        raise ValueError(("unexpected group membership", group, expected, observed))


def spec(prefix, group, operation):
    run("rbd", "group", "image", operation, prefix + "/" + group, prefix + "/" + group + "-img")


def options(operation):
    run(
        "rbd",
        "group",
        "image",
        operation,
        "--group-pool",
        pool,
        "--group-namespace",
        namespace,
        "--group",
        "flags",
        "--image-pool",
        pool,
        "--image-namespace",
        namespace,
        "--image",
        "flags-img",
    )


run("ceph", "osd", "pool", "create", pool, "1")
run("rbd", "pool", "init", pool)
run("rbd", "namespace", "create", selected)
for group in ("spec", "flags"):
    for prefix in (pool, selected):
        run("rbd", "group", "create", prefix + "/" + group)
        run("rbd", "create", prefix + "/" + group + "-img", "--size", "1M")
    require_members(group, ([], []))

spec(selected, "spec", "add")
require_members("spec", ([(pool, namespace, "spec-img")], []))
spec(selected, "spec", "remove")
require_members("spec", ([], []))

named_member = [(pool, namespace, "flags-img")]
default_member = [(pool, "", "flags-img")]
options("add")
after_add = both("flags")
print("after add:", after_add)
if after_add == (named_member, []):
    print("add|NOT_REPRODUCED|[]")
elif after_add == ([], default_member):
    print("add|REPRODUCED|[added to the default group]")
else:
    raise ValueError(("unexpected add membership", after_add))

# Seed both memberships, independently of the add outcome.
for prefix, existing in zip((selected, pool), after_add):
    if not existing:
        spec(prefix, "flags", "add")
require_members("flags", (named_member, default_member))
options("remove")
after_remove = both("flags")
print("after remove:", after_remove)
if after_remove == ([], default_member):
    print("remove|NOT_REPRODUCED|[]")
elif after_remove == (named_member, []):
    print("remove|REPRODUCED|[removed from the default group]")
else:
    raise ValueError(("unexpected remove membership", after_remove))
`
