// Package blockio contains the device-mapper ownership and state primitives
// shared by block I/O fixtures and their integration checks.
package blockio

// Script runs the owned keeper control protocol. A dispatched kernel step
// outlives the invocation until its recorded status says that it finished.
// Cancellation is not rollback; callers retain its step identity and poll it.
const Script = scriptPrelude + scriptActions + scriptEnd

const scriptPrelude = `import json, os, subprocess, sys, time
action, name = sys.argv[1], sys.argv[2]
work = "/tc-blk"
out = {"action": action, "name": name}
def run(*argv):
    r = subprocess.run(argv, capture_output=True, text=True, timeout=20)
    return r.returncode, r.stdout.strip(), r.stderr.strip()[-400:]
def rcfile(tag):
    return "%s/ctl-%s-%s.rc" % (work, name, tag)
def detached(tag, argv):
    for suffix in ("", ".err"):
        try:
            os.unlink(rcfile(tag) + suffix)
        except FileNotFoundError:
            pass
    script = '"$@" 2>"%s.err"; echo $? > "%s.tmp" && mv "%s.tmp" "%s"' % ((rcfile(tag),) * 4)
    subprocess.Popen(["sh", "-c", script, "sh"] + argv, start_new_session=True,
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
def settled(tag, wait):
    deadline = time.monotonic() + wait
    while True:
        try:
            with open(rcfile(tag)) as f:
                rc = int(f.read().strip())
            with open(rcfile(tag) + ".err") as f:
                err = f.read().strip()[-400:]
            return {"done": True, "rc": rc, "error": err}
        except FileNotFoundError:
            pass
        if time.monotonic() >= deadline:
            return {"done": False}
        time.sleep(0.1)
`

const scriptActions = `if action == "observe":
    rc, info, err = run("dmsetup", "info", name)
    out["exists"] = rc == 0
    if rc != 0:
        listed, names, listing_error = run("dmsetup", "info", "-c", "--noheadings", "-o", "name")
        if listed != 0 or name in [line.strip() for line in names.splitlines()]:
            out["error"] = err or listing_error or "dmsetup info failed without an absence receipt"
    if rc == 0:
        fields = {}
        for line in info.splitlines():
            key, _, value = line.partition(":")
            fields[key.strip()] = value.strip()
        out["state"] = fields.get("State", "")
        out["uuid"] = fields.get("UUID", "")
        out["open"] = int(fields.get("Open count", "-1"))
        out["event"] = int(fields.get("Event number", "-1"))
        major, _, minor = fields.get("Major, minor", "").partition(",")
        out["major"], out["minor"] = int(major), int(minor)
        tables = fields.get("Tables present", "")
        def table(inactive):
            argv = ["dmsetup", "table"] + (["--inactive"] if inactive else []) + [name]
            rc, value, error = run(*argv)
            if rc != 0:
                raise SystemExit("cannot observe device table: " + error)
            return value
        out["live"] = table(False) if "LIVE" in tables else None
        out["inactive"] = table(True) if "INACTIVE" in tables else None
        with open("/sys/dev/block/%d:%d/inflight" % (out["major"], out["minor"])) as f:
            out["in_flight"] = [int(x) for x in f.read().split()]
elif action == "step":
    op, wait, sid = sys.argv[3], float(sys.argv[4]), sys.argv[5]
    selector = ["--uuid", os.environ["TC_DM_UUID"]] if os.environ.get("TC_DM_UUID") else [name]
    argv = ["dmsetup", op] + selector
    if op == "load":
        argv += ["--table", sys.argv[6]]
    elif op == "remove":
        if len(sys.argv) > 6:
            selector = ["--uuid", sys.argv[6]]
        argv = ["dmsetup", "remove", "--verifyudev"] + selector
    out["op"], out["id"] = op, sid
    # Claimed before launch: a stopped dispatch without this marker never ran.
    tag = "step-" + sid
    status = rcfile(tag)
    os.close(os.open(status + ".started", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
    try:
        detached(tag, argv)
    except OSError as exc:
        # Launch failed before a child could publish its exit status.
        with open(status + ".err", "w") as f:
            f.write("cannot launch step: " + str(exc))
        with open(status + ".tmp", "w") as f:
            f.write("1\n")
        os.replace(status + ".tmp", status)
    out.update(settled(tag, wait))
elif action == "settled":
    out["id"] = sys.argv[3]
    out["started"] = os.path.exists(rcfile("step-" + sys.argv[3]) + ".started")
    out.update(settled("step-" + sys.argv[3], 0))
elif action == "loopid":
    loop = sys.argv[3]
    try:
        with open("/sys/block/%s/loop/backing_file" % os.path.basename(loop)) as f:
            out["file"] = f.read().strip()
        out["inode"] = os.stat(out["file"]).st_ino
        out["holders"] = sorted(os.listdir("/sys/block/%s/holders" % os.path.basename(loop)))
    except OSError as e:
        out["error"] = str(e)
`

const scriptEnd = `else:
    raise SystemExit("unknown action " + action)
print(json.dumps(out))`
