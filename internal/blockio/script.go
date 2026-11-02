// Package blockio contains the device-mapper ownership and state primitives
// shared by block I/O fixtures and their integration checks.
package blockio

// Script runs the owned keeper control protocol. A dispatched kernel step
// outlives the invocation until its recorded status says that it finished.
// Cancellation is not rollback; callers retain its step identity and poll it.
const Script = scriptPrelude + scriptActions + scriptEnd

// TestScript adds direct-I/O probes and a temporary loop opener to Script's
// shared control implementation. Production control does not dispatch these
// test workload actions.
const TestScript = scriptPrelude + scriptTestPrelude + scriptActions + scriptTestActions + scriptEnd

const scriptPrelude = `import hashlib, json, os, subprocess, sys, time
action, name = sys.argv[1], sys.argv[2]
work = "/tc-blk"
out = {"action": action, "name": name}
node = "/dev/mapper/" + name
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

const scriptActions = `if action == "create":
    uuid, lower, sectors = sys.argv[3], sys.argv[4], int(sys.argv[5])
    rc, _, err = run("dmsetup", "create", "--verifyudev", name, "--uuid", uuid, "--table", "0 %d linear %s 0" % (sectors, lower))
    out["created"], out["error"] = rc == 0, err
elif action == "observe":
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
        argv = ["dmsetup", "remove", "--verifyudev"] + selector
    out["op"], out["id"] = op, sid
    detached("step-" + sid, argv)
    out.update(settled("step-" + sid, wait))
elif action == "settled":
    out["id"] = sys.argv[3]
    out.update(settled("step-" + sys.argv[3], 0))
elif action == "loop":
    path, size = sys.argv[3], int(sys.argv[4])
    with open(path, "xb") as f:
        f.truncate(size)
    rc, loop, err = run("losetup", "--find", "--show", path)
    out["loop"], out["error"], out["file"] = loop if rc == 0 else "", err, path
    out["inode"] = os.stat(path).st_ino
    if rc == 0:
        st = os.stat(loop)
        out["major"], out["minor"] = os.major(st.st_rdev), os.minor(st.st_rdev)
elif action == "loopid":
    loop = sys.argv[3]
    try:
        with open("/sys/block/%s/loop/backing_file" % os.path.basename(loop)) as f:
            out["file"] = f.read().strip()
        out["inode"] = os.stat(out["file"]).st_ino
        out["holders"] = sorted(os.listdir("/sys/block/%s/holders" % os.path.basename(loop)))
    except OSError as e:
        out["error"] = str(e)
elif action == "rmfile":
    path, inode = sys.argv[3], int(sys.argv[4])
    if os.stat(path).st_ino != inode:
        raise SystemExit("%s is not inode %d" % (path, inode))
    os.unlink(path)
    out["file_removed"] = True
elif action == "unloop":
    loop, path = sys.argv[3], sys.argv[4]
    base = os.path.basename(loop)
    holders = sorted(os.listdir("/sys/block/%s/holders" % base))
    out["holders"] = holders
    if holders:
        out["detached"], out["error"] = False, "held by " + " ".join(holders)
    else:
        rc, _, err = run("losetup", "-d", loop)
        out["error"] = err
        # losetup -d on a busy loop only sets autoclear and succeeds; the
        # loop is released once its backing file is gone from sysfs.
        released = rc == 0 and not os.path.exists("/sys/block/%s/loop/backing_file" % base)
        out["detached"], out["lazy"] = released, rc == 0 and not released
        if released:
            os.unlink(path)
            out["file_removed"] = True
`

const scriptTestPrelude = `def pattern(seed, blocks):
    block = hashlib.sha256(seed.encode()).digest() * (65536 // 32)
    return block * blocks
`

const scriptTestActions = `elif action == "write":
    offset, blocks, seed = int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
    data = pattern(seed, blocks)
    src = "%s/ctl-%s-pattern" % (work, name)
    with open(src, "wb") as f:
        f.write(data)
    out["sha256"] = hashlib.sha256(data).hexdigest()
    detached("write", ["dd", "if=" + src, "of=" + node, "bs=65536", "seek=%d" % offset, "count=%d" % blocks, "oflag=direct", "status=none"])
    out["started"] = True
elif action == "written":
    out.update(settled("write", 0))
elif action == "read":
    offset, blocks = int(sys.argv[3]), int(sys.argv[4])
    r = subprocess.run(["dd", "if=" + node, "bs=65536", "skip=%d" % offset, "count=%d" % blocks, "iflag=direct", "status=none"], capture_output=True, timeout=20)
    out["rc"] = r.returncode
    out["sha256"] = hashlib.sha256(r.stdout).hexdigest() if r.returncode == 0 else ""
elif action == "hold":
    mark = "%s/ctl-hold-%s" % (work, os.path.basename(sys.argv[3]))
    try:
        os.unlink(mark)
    except FileNotFoundError:
        pass
    detached("hold", ["sh", "-c", 'exec 3<"$1"; : >"$3"; sleep "$2"', "sh", sys.argv[3], sys.argv[4], mark])
    deadline = time.monotonic() + 10
    while not os.path.exists(mark) and time.monotonic() < deadline:
        time.sleep(0.05)
    out["started"] = os.path.exists(mark)
`

const scriptEnd = `else:
    raise SystemExit("unknown action " + action)
print(json.dumps(out))`
