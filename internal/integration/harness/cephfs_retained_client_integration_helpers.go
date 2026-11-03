//go:build all || integration

package harness

// CephFSActorScript is the Python source of an actor. Its own type keeps a
// filesystem name or path from being passed as the script by mistake.
type CephFSActorScript string

// CephFSActorHelpers starts every actor script: it reads the filesystem, the
// work directory and the run's token from argv and defines publish, which
// renames a receipt for one phase into place, and gate, which waits until the
// test writes the token into a named gate file.
const CephFSActorHelpers = `
import cephfs, json, os, sys, time
name, workdir, token = sys.argv[1], sys.argv[2], sys.argv[3]
def publish(phase, **fields):
    fields.update(token=token, phase=phase, pid=os.getpid())
    target = "%s/receipt-%d" % (workdir, phase)
    with open(target + ".tmp", "w") as out:
        json.dump(fields, out)
    os.rename(target + ".tmp", target)
def gate(name):
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        try:
            with open(workdir + "/" + name) as opened:
                if opened.read() == token:
                    return
        except FileNotFoundError:
            pass
        time.sleep(0.2)
    sys.exit("gate %s did not open" % name)
`
