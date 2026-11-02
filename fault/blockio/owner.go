package blockio

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	control "github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/google/uuid"
	mobycl "github.com/moby/moby/client"
)

const ownerProgram = "/tc-blockio-owner.py"

func randomID() string { return uuid.NewString() }

type ownerReply struct {
	Lease                 string `json:"lease"`
	Instance              string `json:"instance"`
	Attempt               string `json:"attempt"`
	Done                  *bool  `json:"done"`
	Error, ErrorKind      string
	Targets               []string `json:"targets"`
	File                  string   `json:"file"`
	Inode, FileDev        uint64
	Loop                  string `json:"loop"`
	LoopMajor, LoopMinor  uint32
	Bytes                 int64 `json:"bytes"`
	Detached, FileRemoved bool
}

func (b *Container) owner(ctx context.Context, action string) (ownerReply, error) {
	argv := []string{"python3", "-I", ownerProgram, action, b.name, strconv.FormatInt(b.size, 10), b.instance}
	detachAction := action == "start-detach" || action == "detached"
	if detachAction {
		if b.detachID == "" {
			return ownerReply{}, failure(ceph.ErrNativeFailure, "detach invocation identity is missing")
		}
		argv = append(argv, b.detachID)
	}
	raw, err := b.command(ctx, argv...)
	if err != nil {
		return ownerReply{}, err
	}
	var reply ownerReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		return reply, failure(ceph.ErrNativeFailure, "invalid owner receipt: %v", err)
	}
	if reply.Lease != b.name || reply.Instance == "" || b.instance != "" && reply.Instance != b.instance {
		return reply, failure(ceph.ErrNativeFailure, "keeper generation or lease identity changed; resources retained")
	}
	if detachAction && reply.Attempt != b.detachID {
		return reply, failure(ceph.ErrNativeFailure, "detach receipt belongs to another invocation; resources retained")
	}
	if reply.ErrorKind != "" {
		kind := ceph.ErrNativeFailure
		if reply.ErrorKind == "image" {
			kind = ceph.ErrImageCapability
		}
		if reply.ErrorKind == "environment" {
			kind = ceph.ErrEnvironmentUnavailable
		}
		return reply, failure(kind, "%s", reply.Error)
	}
	return reply, nil
}

func (b *Container) acceptOwner(r ownerReply) error {
	if r.File != "/tc-blk/data-"+b.name || r.Inode == 0 || r.FileDev == 0 || r.Loop == "" || r.LoopMajor == 0 || r.Bytes != b.size {
		return failure(ceph.ErrNativeFailure, "owned file/loop identity or device size is incomplete")
	}
	b.loop = control.LoopIdentity{Loop: r.Loop, File: r.File, Inode: r.Inode, Dev: fmt.Sprintf("%d:%d", r.LoopMajor, r.LoopMinor)}
	b.fileDev = r.FileDev
	return nil
}

// ownerScript keeps creation and detachment receipts inside the keeper.
// Dispatch acknowledgements can be lost; their detached process and manifest
// remain discoverable without a second mutation. The private tmpfs and helper
// must stay alive until these operations and their consumers are excluded.
const ownerScript = `import json, os, re, shutil, stat, subprocess, sys
action, name, size, expected = sys.argv[1:5]
size = int(size)
attempt=sys.argv[5] if action in ("start-detach","detach","detached") and len(sys.argv)>5 else ""
if action in ("start-detach","detach","detached") and not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}",attempt):
    raise SystemExit("detach invocation identity is missing or invalid")
work = "/tc-blk"
detach_path=work+"/detach-"+attempt+".json"
def instance():
    with open("/proc/sys/kernel/random/boot_id") as f: boot=f.read().strip()
    with open("/proc/1/stat") as f: ticks=f.read().rsplit(") ",1)[1].split()[19]
    return boot+":"+ticks+":"+str(os.stat("/proc/self/ns/pid").st_ino)
generation=instance()
def emit(value):
    print(json.dumps(dict(value,lease=name,instance=generation,attempt=attempt)),flush=True)
def command(*args):
    p=subprocess.run(args,capture_output=True,text=True)
    if p.returncode: raise RuntimeError("%s exited %d: %s"%(args[0],p.returncode,p.stderr[-400:]))
    return p.stdout.strip()
def save(path,value):
    tmp=path+".tmp"
    with open(tmp,"w") as f: json.dump(value,f);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)
def read(path):
    try:
        with open(path) as f: return json.load(f)
    except FileNotFoundError: return {"done":False}
def load():
    value=read(work+"/create.json")
    if value.get("lease",name)!=name or value.get("instance",generation)!=generation:
        raise RuntimeError("owner manifest identity changed")
    return value
def check_file(value):
    s=os.stat(value["file"],follow_symlinks=False)
    if not stat.S_ISREG(s.st_mode) or s.st_dev!=value["FileDev"] or s.st_ino!=value["Inode"]:
        raise RuntimeError("backing file identity changed")
def associated(path):
    return [line.strip() for line in command("losetup","--list","--associated",path,"--noheadings","--output","NAME").splitlines() if line.strip()]
def spawn(which, attempt=""):
    # Exclusive claim before launching: duplicate calls cannot replay creation
    # or detachment, including a delayed arrival after a cancelled caller.
    mark=work+"/"+which+("-"+attempt if attempt else "")+".started"
    fd=os.open(mark,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
    argv=[sys.executable,"-I",__file__,which,name,str(size),generation]
    if attempt: argv.append(attempt)
    try:
        subprocess.Popen(argv,start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    except Exception as error:
        if which!="detach": raise
        save(detach_path,{"done":True,"Error":str(error),"attempt":attempt})
if expected and expected!=generation:
    raise SystemExit("keeper generation changed; refusing action")
if action=="exec":
    os.environ["TC_DM_UUID"]="TC-BLK-"+name
    os.execvp(sys.argv[5],sys.argv[5:])
elif action=="probe":
    missing=[p for p in ("sh","sleep","dmsetup","losetup","blockdev") if shutil.which(p) is None]
    if missing: emit({"ErrorKind":"image","Error":"missing executables: "+", ".join(missing)})
    else:
        try:
            command("losetup","--version");command("blockdev","--version");command("dmsetup","help")
        except Exception as e: emit({"ErrorKind":"image","Error":str(e)});sys.exit(0)
        roots=[]
        with open("/proc/self/mountinfo") as f:
            for line in f:
                left,_,right=line.partition(" - ")
                if left.split()[4]==work: roots.append(right.split()[0])
        if roots!=["tmpfs"]: emit({"ErrorKind":"environment","Error":"private tmpfs mount was not retained"});sys.exit(0)
        cap=os.statvfs(work);total=cap.f_blocks*cap.f_frsize
        if not size+(64<<20)<=total<size+(64<<20)+4096:
            emit({"ErrorKind":"environment","Error":"private tmpfs size differs from the requested bound"});sys.exit(0)
        # Check driver registration before dmsetup can try to load dm_mod.
        if not os.path.exists("/sys/class/misc/device-mapper") or not os.path.exists("/sys/class/misc/loop-control"):
            emit({"ErrorKind":"environment","Error":"device-mapper or loop driver is not registered"});sys.exit(0)
        if not all(os.path.exists(p) and stat.S_ISCHR(os.stat(p).st_mode) and os.access(p,os.R_OK|os.W_OK) for p in ("/dev/mapper/control","/dev/loop-control")):
            emit({"ErrorKind":"environment","Error":"host control devices are not accessible"});sys.exit(0)
        try: targets=[line.split()[0] for line in command("dmsetup","targets").splitlines() if line.strip()]
        except Exception as e: emit({"ErrorKind":"environment","Error":str(e)});sys.exit(0)
        emit({"targets":targets})
elif action=="start":
    spawn("build");emit({"done":False})
elif action=="build":
    m={"lease":name,"instance":generation,"done":False,"file":work+"/data-"+name,"bytes":size}
    save(work+"/create.json",m)
    try:
        with open(m["file"],"xb") as f:
            s=os.fstat(f.fileno());m.update(Inode=s.st_ino,FileDev=s.st_dev);save(work+"/create.json",m)
            f.truncate(size)
        m["loop"]=command("losetup","--find","--show",m["file"]);save(work+"/create.json",m)
        s=os.stat(m["loop"]);m.update(LoopMajor=os.major(s.st_rdev),LoopMinor=os.minor(s.st_rdev));save(work+"/create.json",m)
        command("dmsetup","create","--verifyudev",name,"--uuid","TC-BLK-"+name,"--table","0 %d linear %s 0"%(size//512,m["loop"]))
        actual=int(command("blockdev","--getsize64","/dev/mapper/"+name))
        if actual!=size: raise RuntimeError("created mapping has another size")
    except Exception as e: m["Error"]=str(e)
    m["done"]=True;save(work+"/create.json",m)
elif action=="inspect": emit(load())
elif action=="file":
    m=load()
    if not m.get("done"): emit(m);sys.exit(0)
    if os.path.exists(m["file"]):
        if "Inode" not in m: raise RuntimeError("backing file ownership was not recorded")
        check_file(m)
    loops=associated(m["file"]) if os.path.exists(m["file"]) else []
    if len(loops)>1: raise RuntimeError("owned file has multiple loop associations")
    if loops:
        loop=loops[0].strip()
        if m.get("loop") and m["loop"]!=loop: raise RuntimeError("loop association identity changed")
        m["loop"]=loop;s=os.stat(loop);m.update(LoopMajor=os.major(s.st_rdev),LoopMinor=os.minor(s.st_rdev))
    m["Detached"]=not loops
    if os.path.exists("/dev/mapper/"+name):m["bytes"]=int(command("blockdev","--getsize64","/dev/mapper/"+name))
    emit(m)
elif action=="start-detach": spawn("detach",attempt);emit({"done":False})
elif action=="detach":
    r={"done":False,"attempt":attempt};save(detach_path,r)
    try:
        m=load();check_file(m);loops=associated(m["file"])
        if loops!=[m["loop"]]: raise RuntimeError("loop association changed before detach")
        holders=os.listdir("/sys/block/%s/holders"%os.path.basename(m["loop"]))
        if holders: raise RuntimeError("loop still has mapping holders")
        command("losetup","--detach",m["loop"])
        r["Detached"]=not associated(m["file"])
    except Exception as e:r["Error"]=str(e)
    r["done"]=True;save(detach_path,r)
elif action=="detached":
    r=read(detach_path)
    if r.get("attempt",attempt)!=attempt: raise RuntimeError("detach manifest identity changed")
    if r.get("done") and not r.get("Error"):
        m=load();check_file(m);r["Detached"]=not associated(m["file"])
    emit(r)
elif action=="unlink":
    m=load()
    if not os.path.lexists(m["file"]):emit({"done":True,"FileRemoved":True});sys.exit(0)
    if associated(m["file"]): raise RuntimeError("backing file still has a loop association")
    if os.path.lexists(m["file"]):check_file(m);os.unlink(m["file"])
    emit({"done":True,"FileRemoved":True})
else: raise SystemExit("unknown owner action")
`

func (b *Container) chainEvidence(ctx context.Context) (faulthook.ChainEvidence, error) {
	ev := faulthook.ChainEvidence{Lease: b.name, BorrowsClosed: faulthook.FactYes}
	view, err := b.observe(ctx)
	if err != nil {
		return ev, err
	}
	if !view.Exists || view.UUID != b.record.UUID || view.Major != b.record.Major || view.Minor != b.record.Minor || control.Classify(b.record, b.linear, view) != control.Applied {
		return ev, failure(ceph.ErrNativeFailure, "owned mapping identity/table is not established")
	}
	file, err := b.owner(ctx, "file")
	if err != nil {
		return ev, err
	}
	if file.Loop != b.loop.Loop || file.Inode != b.loop.Inode || file.FileDev != b.fileDev || file.Detached {
		return ev, failure(ceph.ErrNativeFailure, "owned loop or backing identity changed")
	}
	ev.Device = faulthook.BlockDevice{HostPath: "/dev/mapper/" + b.name, Major: view.Major, Minor: view.Minor, SizeBytes: file.Bytes}
	ev.ScanComplete, ev.Openers = faulthook.FactYes, view.Open
	if b.osd == nil {
		ev.DaemonStopped = faulthook.FactYes
	} else {
		result, err := b.docker.ContainerInspect(ctx, b.osd.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil && !faulthook.OnlyMissingHostResource(err) {
			return ev, err
		}
		if err != nil || result.Container.State != nil && !result.Container.State.Running {
			ev.DaemonStopped = faulthook.FactYes
		} else {
			ev.DaemonStopped = faulthook.FactNo
		}
	}
	return ev, nil
}

func (b *Container) waitOwner(ctx context.Context, action string) (ownerReply, error) {
	for {
		r, err := b.owner(ctx, action)
		if err != nil {
			return r, err
		}
		if r.Done != nil && *r.Done {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
