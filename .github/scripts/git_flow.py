#!/usr/bin/env python3
"""Plan, apply and verify repository rules with the authenticated GitHub CLI."""

import argparse
import base64
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import quote

import promotion_reuse

REPO = "JSYoo5B/ceph-testcontainers-go"
ACTIONS = 15368
BASE_TAG = "v1.0.0"
BASE_SHA = "43a17659bbd60776f987b773d68fe24011cd7220"
CODE = ("Unit tests", "Race detector", "Vet, compile and CI tooling",
        "Compiled tag coverage and helpers", "Vulnerability check (Go 1.26.x)",
        "Vulnerability check (Go 1.27.x)")
LINE = r"(release|support)/(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
VERSION = r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
PREFIX = "Git Flow / "
GUARD = PREFIX + "reserve line names"
# The commit status of .github/scripts/required_ci.py. Its rule is optional:
# it exists only after "apply --requested-ci", and verify checks it when it
# is installed.
REQUESTED = "Required requested CI"
REQUESTED_RULE = PREFIX + "requested CI"
REQUIRED_SHORT = "Required Short checks"
REQUIRED_SHORT_RULE = PREFIX + "required Short"
MAINTAINED = ["refs/heads/main", "refs/heads/develop", "refs/heads/release/**/*", "refs/heads/support/**/*"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def api(path, *, method="GET", body=None, pages=False):
    args = ["gh", "api", "--method", method, "-H", "X-GitHub-Api-Version: 2022-11-28",
            f"repos/{REPO}/{path}".rstrip("/")]
    if pages:
        args.extend(["--paginate", "--slurp"])
    if body is not None:
        args.extend(["--input", "-"])
    result = subprocess.run(args, input=json.dumps(body) if body is not None else None,
                            check=True, capture_output=True, text=True)
    data = json.loads(result.stdout)
    if pages:
        return [item for page in data for item in (page if pages is True else page[pages])]
    return data


def rule(kind, **params):
    return {"type": kind, **({"parameters": params} if params else {})}


def ruleset(name, target, refs, rules, excludes=()):
    return {"name": name, "target": target, "enforcement": "active", "bypass_actors": [],
            "conditions": {"ref_name": {"include": list(refs), "exclude": sorted(excludes)}},
            "rules": rules}


def gate(branch):
    return f"Release candidate validated ({branch})"


def checks(names, *, strict=False):
    return rule("required_status_checks", required_status_checks=[
        {"context": name, "integration_id": ACTIONS} for name in names],
        strict_required_status_checks_policy=strict, do_not_enforce_on_create=True)


def candidate(branch):
    return ruleset(PREFIX + branch, "branch", ["refs/heads/" + branch], [checks([gate(branch)], strict=True)])


def requested():
    """Require the requested CI status on every maintained branch.

    The branches are those whose pull requests start Code checks, because
    that request is what makes the gate publish the status on a new head. A
    narrower rule would leave the status advisory where reviewers can still
    require coverage; a wider one would wait on pull requests that nothing
    evaluates."""
    return ruleset(REQUESTED_RULE, "branch", MAINTAINED, [checks([REQUESTED])])


def required_short():
    return ruleset(REQUIRED_SHORT_RULE, "branch", ["refs/heads/develop"], [checks([REQUIRED_SHORT])])


def optional(managed, branch=None):
    """The optional rulesets that are installed and apply to this branch."""
    policies = [requested()] if REQUESTED_RULE in managed else []
    if REQUIRED_SHORT_RULE in managed and branch in (None, "develop"):
        policies.append(required_short())
    return policies

def reserved(lines):
    return ruleset(GUARD, "branch", ["refs/heads/release/**/*", "refs/heads/support/**/*"],
                   [rule("creation")], ["refs/heads/" + name for name in lines])


def baseline():
    pr = rule("pull_request", required_approving_review_count=0, dismiss_stale_reviews_on_push=True,
              require_last_push_approval=False, require_code_owner_review=False,
              required_review_thread_resolution=True, allowed_merge_methods=["merge"])
    return [
        ruleset(PREFIX + "reviewed changes", "branch", MAINTAINED,
                [rule("deletion"), rule("non_fast_forward"), pr, checks(CODE)]),
        candidate("main"),
        ruleset(PREFIX + "immutable version tags", "tag", ["refs/tags/v*"],
                [rule("update"), rule("deletion")]),
    ]


def covers(actual, desired):
    """Ignore service-supplied parameter defaults and unordered rule/check arrays."""
    if isinstance(desired, dict):
        return isinstance(actual, dict) and all(k in actual and covers(actual[k], v) for k, v in desired.items())
    if isinstance(desired, list):
        return isinstance(actual, list) and len(actual) == len(desired) and all(
            any(covers(a, d) for a in actual) for d in desired)
    return actual == desired


def managed_rules():
    listed = api("rulesets?includes_parents=true&per_page=100", pages=True)
    result = {}
    for row in listed:
        if row["name"].startswith(PREFIX):
            require(row["source"] == REPO and row["source_type"] == "Repository",
                    f"managed name collides with an inherited rule: {row['name']}")
            require(row["name"] not in result, f"duplicate ruleset {row['name']}")
            result[row["name"]] = api(f"rulesets/{row['id']}")
    return result


def installed_lines(managed):
    if GUARD not in managed:
        return set()
    refs = managed[GUARD]["conditions"]["ref_name"]["exclude"]
    lines = {ref.removeprefix("refs/heads/") for ref in refs}
    require(all(re.fullmatch(LINE, name) for name in lines), "unexpected reserved-line exclusion")
    require(covers(managed[GUARD], reserved(lines)), "reserved-line rule drift")
    return lines


def ensure(managed, desired, mode):
    name = desired["name"]
    current = managed.get(name)
    if current:
        require(covers(current, desired), f"ruleset drift: {name}; inspect before proceeding")
        return
    require(mode != "verify", f"missing ruleset: {name}")
    print("Create ruleset", json.dumps(desired, sort_keys=True))
    if mode == "apply":
        managed[name] = api("rulesets", method="POST", body=desired)
        require(covers(managed[name], desired), f"unexpected installed ruleset: {name}")


def effective(branch):
    return api("rules/branches/" + quote(branch, safe="") + "?per_page=100", pages=True)


def verify_branch(managed, branch, expected):
    active = effective(branch)
    expected = [*expected, *optional(managed, branch)]
    contexts = set(CODE)
    if any(policy["name"] == PREFIX + branch for policy in expected):
        contexts.add(gate(branch))
    if any(policy["name"] == REQUESTED_RULE for policy in expected):
        contexts.add(REQUESTED)
    if any(policy["name"] == REQUIRED_SHORT_RULE for policy in expected):
        contexts.add(REQUIRED_SHORT)
    for item in active:
        params = item.get("parameters", {})
        if item["type"] == "pull_request":
            require(params.get("required_approving_review_count") == 0
                    and not params.get("require_last_push_approval") and not params.get("require_code_owner_review"),
                    f"{branch}: an effective rule requires a separate approver")
        if item["type"] == "required_status_checks":
            require(all(c["context"] in contexts and c.get("integration_id") == ACTIONS
                        for c in params["required_status_checks"]), f"{branch}: unexpected effective status check")
    for policy in expected:
        rules = [r for r in active if r.get("ruleset_id") == managed[policy["name"]]["id"]]
        for required in policy["rules"]:
            require(any(covers(r, required) for r in rules),
                    f"{branch}: missing effective {required['type']} from {policy['name']}")


def verify(managed, lines):
    core, main, tags = baseline()
    desired = [core, main, tags, reserved(lines)] + [candidate(line) for line in sorted(lines)] + optional(managed)
    require(set(managed) == {p["name"] for p in desired}, "unexpected managed ruleset or incomplete line setup")
    for policy in desired:
        ensure(managed, policy, "verify")
    verify_branch(managed, "main", [core, main])
    verify_branch(managed, "develop", [core])
    for line in lines:
        verify_branch(managed, line, [core, candidate(line)])
        require(not any(r.get("ruleset_id") == managed[GUARD]["id"] for r in effective(line)),
                f"creation rule still applies to registered line {line}")
    for branch in ["release/unregistered", "support/unregistered"]:
        verify_branch(managed, branch, [core, reserved(lines)])


def without_requested_status():
    """The open pull requests to a maintained branch whose head commit has no
    requested CI status yet. Once the rule exists each of them waits for one,
    and only a new pull request event or a dispatch of the gate brings it."""
    waiting = []
    for pull in api("pulls?state=open&per_page=100", pages=True):
        base, sha = pull["base"]["ref"], pull["head"]["sha"]
        if base not in {"main", "develop"} and not re.fullmatch(LINE, base):
            continue
        statuses = api(f"commits/{sha}/status?per_page=100", pages="statuses")
        if not any(status["context"] == REQUESTED for status in statuses):
            waiting.append(f"#{pull['number']} ({base}, {sha[:12]})")
    return waiting


def require_requested(managed, lines, mode):
    """Install the requested CI rule beside the existing rulesets, which are
    verified first and never changed."""
    verify(managed, lines)
    if REQUESTED_RULE in managed:
        print("Already installed:", REQUESTED_RULE)
        return
    waiting = without_requested_status()
    for pull in waiting:
        print(f"No {REQUESTED} status: {pull}")
    require(mode != "apply" or not waiting,
            f"{len(waiting)} open pull request(s) have no {REQUESTED} status; "
            'run `gh workflow run "Requested CI gate"` and plan again')
    ensure(managed, requested(), mode)


def refs():
    rows = api("branches?per_page=100", pages=True)
    return {row["name"]: row["commit"]["sha"] for row in rows}


def published_commit(tag):
    release = api("releases/tags/" + tag)
    require(not release["draft"] and not release["prerelease"], f"{tag} is not a published stable release")
    obj = api("git/ref/tags/" + tag)["object"]
    while obj["type"] == "tag":
        obj = api("git/tags/" + obj["sha"])["object"]
    require(obj["type"] == "commit", f"{tag} has no release commit")
    return obj["sha"]


def latest_stable_patch(line):
    prefix = "v" + line + "."
    releases = api("releases?per_page=100", pages=True)
    tags = [r["tag_name"] for r in releases if not r["draft"] and not r["prerelease"]
            and r["tag_name"].startswith(prefix) and re.fullmatch(VERSION, r["tag_name"])]
    require(tags, f"no published stable release on {line}")
    return max(tags, key=lambda tag: int(tag.rsplit(".", 1)[1]))


def seed(name, sha, branches, apply):
    if name in branches:
        require(branches[name] == sha, f"{name} already exists at another SHA; its ref was preserved")
        return
    print(f"Create {name} at {sha}")
    if apply:
        created = api("git/refs", method="POST", body={"ref": "refs/heads/" + name, "sha": sha})
        require(created["object"]["sha"] == sha, f"{name}: unexpected seed commit")


def candidate_evidence(branch, sha):
    workflows = {"code.yml": CODE, "release-matrix.yml": [gate(branch)]}
    for filename, names in workflows.items():
        query = f"event=push&branch={quote(branch, safe='')}&head_sha={sha}&per_page=100"
        runs = api(f"actions/workflows/{filename}/runs?{query}", pages="workflow_runs")
        runs = [run for run in runs if run["event"] == "push" and run["head_branch"] == branch
                and run["head_sha"] == sha and run["path"] == f".github/workflows/{filename}"]
        require(runs, f"no push run of {filename} at {branch}@{sha}")
        run = max(runs, key=lambda r: r["run_number"])
        require(run["status"] == "completed" and run["conclusion"] == "success",
                f"{filename} run {run['id']} attempt {run['run_attempt']} has not passed")
        attempt = f"actions/runs/{run['id']}/attempts/{run['run_attempt']}"
        jobs = api(f"{attempt}/jobs?per_page=100", pages="jobs")
        for name in names:
            observed = [job for job in jobs if job["name"] == name]
            require(len(observed) == 1 and observed[0]["head_sha"] == sha
                    and observed[0]["run_id"] == run["id"] and observed[0]["run_attempt"] == run["run_attempt"]
                    and observed[0]["status"] == "completed" and observed[0]["conclusion"] == "success",
                    f"{name}: missing or unsuccessful in run {run['id']} attempt {run['run_attempt']}")
        print(f"{branch}@{sha}: {run['html_url']}/attempts/{run['run_attempt']} ({filename})")
        if filename == "release-matrix.yml" and branch in promotion_reuse.BRANCHES:
            promotion_evidence(branch, sha, run, jobs)


def promotion_evidence(branch, sha, run, jobs):
    """Audit how the push run of the Release matrix qualified sha: by its own
    full matrix, or by reusing the promotion PR's run. A reuse is verified
    again from the repository, not taken from the push run's word."""
    executed = [job for job in jobs if " / " in job["name"] and job["conclusion"] == "success"]
    reuse = [job for job in jobs if job["name"] == promotion_reuse.REUSE_JOB and job["conclusion"] == "success"]
    name = promotion_reuse.artifact_name(promotion_reuse.REUSE, run["run_attempt"])
    artifacts = [a for a in api(f"actions/runs/{run['id']}/artifacts?per_page=100", pages="artifacts")
                 if a["name"] == name and not a.get("expired")]
    files = promotion_reuse.download_files(artifacts[0]) if len(artifacts) == 1 else {}
    receipt = files.get(promotion_reuse.RECEIPT_FILE[promotion_reuse.REUSE])
    recorded = json.loads(receipt) if receipt else None
    if not (reuse and recorded and recorded.get("disposition") == "reused"):
        require(executed, f"run {run['id']} neither executed the matrix nor recorded a promotion reuse")
        print(f"{branch}@{sha}: validated by its own full matrix"
              + (": " + "; ".join(recorded["reasons"]) if recorded and recorded.get("reasons") else ""))
        return
    require(not executed, f"run {run['id']} recorded a promotion reuse and executed matrix jobs")
    live, reasons, evidence = promotion_reuse.decide(api, promotion_reuse.download_files, REPO, branch, sha,
                                                     recorded["coverage"]["digest"])
    require(not reasons, "the recorded promotion reuse does not verify: " + "; ".join(reasons))
    for field in ("publication", "pull_request", "tested", "source", "coverage", "images", "retried", "evidence",
                  "observed"):
        require(live[field] == recorded.get(field), f"the reuse receipt's {field} differs from the repository")
    # The publication's own copy of the evidence must be the file that
    # qualified it, so that the audit does not depend on the promotion run.
    require(files.get(promotion_reuse.EVIDENCE_FILE) == evidence,
            "the publication run did not keep the evidence that qualified it")
    source = recorded["source"]
    print(f"{branch}@{sha}: reuses PR #{recorded['pull_request']} run {source['url']}/attempts/{source['run_attempt']}, "
          f"tested {recorded['tested']['sha']} with tree {recorded['tested']['tree']}; "
          f"{recorded['evidence']['entries']} preserved job records, "
          f"{len(recorded['retried'])} first failures passed their retry")


def cutover_ready(pr_number, branches):
    require(pr_number is not None, "apply requires --policy-pr NUMBER after merge")
    pr = api(f"pulls/{pr_number}")
    sha = pr["merge_commit_sha"]
    require(pr["merged"] and pr["base"]["ref"] == "main" and sha == branches["main"],
            "cutover requires the merged policy PR at the current main tip")
    script = ".github/scripts/git_flow.py"
    files = api(f"pulls/{pr_number}/files?per_page=100", pages=True)
    require(any(f["filename"] == script for f in files), "the PR did not deliver the cutover tool")
    merged = api(f"contents/{script}?ref={sha}")
    require(base64.b64decode(merged["content"]) == Path(__file__).read_bytes(),
            "run the tool delivered at the merged policy SHA")
    require(api(f"compare/{BASE_SHA}...{sha}")["status"] == "ahead", "policy merge must descend from v1.0.0")
    require(api("releases/latest")["tag_name"] == BASE_TAG, "latest stable release changed before cutover")
    candidate_evidence("main", sha)
    return sha


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["plan", "apply", "verify", "evidence"])
    parser.add_argument("--policy-pr", type=int, help="merged cutover PR, for the initial apply")
    parser.add_argument("--line", help="prepare and seed release/X.Y or support/X.Y")
    parser.add_argument("--from", dest="source", help="develop or a published stable tag, for --line")
    parser.add_argument("--at", help="reviewed seed SHA from the line registration plan")
    parser.add_argument("--requested-ci", action="store_true",
                        help="plan or apply the rule that requires the requested CI status on maintained branches")
    parser.add_argument("--branch", help="target branch for evidence")
    parser.add_argument("--sha", help="full candidate commit for evidence")
    args = parser.parse_args(argv)
    require(not args.at or (args.mode == "apply" and args.line), "--at is for applying a line registration")
    if args.mode == "evidence":
        require(not (args.line or args.source or args.policy_pr or args.requested_ci),
                "evidence takes --branch and --sha")
        require(args.branch == "main" or re.fullmatch(LINE, args.branch or ""), "expected a maintained candidate branch")
        require(re.fullmatch("[a-f0-9]{40}", args.sha or ""), "expected a full commit SHA")
        require(refs().get(args.branch) == args.sha, "candidate is not the current branch tip")
        candidate_evidence(args.branch, args.sha)
        return
    require(not (args.branch or args.sha), "--branch and --sha apply only to evidence")
    require(bool(args.line) == bool(args.source), "--line and --from must be supplied together")
    require(not args.line or re.fullmatch(LINE, args.line), "invalid release/support line")
    require(not (args.line and args.policy_pr), "separate cutover from opening a line")
    require(args.mode != "verify" or not (args.line or args.policy_pr), "verify checks all managed lines")
    require(not args.requested_ci or (args.mode in {"plan", "apply"} and not (args.line or args.policy_pr)),
            "--requested-ci is a plan or apply of its own")
    repo = api("")
    default = repo["default_branch"]
    require(repo["allow_merge_commit"], "enable merge commits")
    require(default in {"main", "develop"}, "expected main or develop as the default branch")
    if args.mode in {"plan", "apply"} and not (args.line or args.requested_ci):
        require(default == "main", "initial cutover requires main as the default branch")
    managed, branches = managed_rules(), refs()
    lines = installed_lines(managed)
    print(f"Default {default}, main at {branches['main']}, admin {repo['permissions']['admin']}")
    require(published_commit(BASE_TAG) == BASE_SHA, "published v1.0.0 no longer points at its original commit")
    if args.mode == "apply":
        require(repo["permissions"]["admin"], "the actor needs repository administration permission")
    unknown = {b for b in branches if b.startswith(("release/", "support/"))} - lines
    require(not unknown, f"unregistered existing lines: {sorted(unknown)}")
    if args.mode == "verify":
        require("develop" in branches, "missing develop")
        require(lines <= branches.keys(), "a registered line has not been seeded; resume its apply")
        verify(managed, lines)
        return
    if args.requested_ci:
        require("develop" in branches, "apply the cutover first")
        require(lines <= branches.keys(), "a registered line has not been seeded; resume its apply")
        require_requested(managed, lines, args.mode)
    elif args.line:
        require("develop" in branches, "apply the cutover first")
        require(args.line.split("/")[0] == "release" or args.source != "develop",
                "support lines start from their published stable tag")
        if args.source == "develop":
            sha = branches["develop"]
        else:
            require(re.fullmatch(VERSION, args.source) and args.source[1:].rsplit(".", 1)[0] == args.line.split("/")[1],
                    "a patch or support branch must start from a stable tag on that line")
            if args.line.startswith("support/"):
                latest = latest_stable_patch(args.line.split("/")[1])
                require(args.source == latest, f"{args.line} must start from the latest stable patch {latest}")
            sha = published_commit(args.source)
        if args.mode == "apply":
            require(args.at == sha, "supply the current reviewed seed SHA from the plan with --at")
        # A previous interrupted attempt may already have installed the exact
        # gate. Keep the wildcard creation guard until it has been verified.
        own = candidate(args.line)
        old = {k: v for k, v in managed.items() if k != own["name"] or args.line in lines}
        verify(old, lines)
        ensure(managed, own, args.mode)
        if args.line not in lines:
            lines.add(args.line)
            guard = reserved(lines)
            print("Allow creation after installing exact target gate", args.line)
            if args.mode == "apply":
                verify_branch(managed, args.line, [baseline()[0], own])
                previous = api(f"rulesets/{managed[GUARD]['id']}")
                require(covers(previous, reserved(lines - {args.line})), "creation guard changed; retry from a fresh plan")
                managed[GUARD] = api(f"rulesets/{previous['id']}", method="PUT", body=guard)
        seed(args.line, sha, branches, args.mode == "apply")
    else:
        if args.mode == "apply":
            sha = cutover_ready(args.policy_pr, branches)
            require(branches.get("develop", sha) == sha, "develop has diverged; use verify after adoption")
        else:
            sha = branches["main"]
            print("Preview only. The merged policy SHA and its checks are required by apply.")
        policies = [reserved(lines), *baseline()]
        require(set(managed) <= {p["name"] for p in policies} | {candidate(line)["name"] for line in lines}
                | {REQUESTED_RULE, REQUIRED_SHORT_RULE}, "unexpected managed ruleset")
        for policy in policies:
            if policy["name"] in managed:
                ensure(managed, policy, "verify")
        for policy in policies:
            ensure(managed, policy, args.mode)
        if args.mode == "apply":
            require(refs()["main"] == sha, "main advanced during cutover")
        seed("develop", sha, branches, args.mode == "apply")
    if args.mode == "apply":
        verify(managed_rules(), lines)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as exc:
        sys.exit(getattr(exc, "stderr", None) or str(exc))
