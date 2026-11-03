package fsbase

import "strings"

// libCephFSPrologue mounts the filesystem named by the first argument as
// client.admin and leaves the remaining arguments in args. The mount and
// OSD timeouts end well inside the watchdog, so a slow mount fails with a
// mount error instead of being killed with exit status 124, and the mount
// runs inside try, so a failed mount still shuts the client down.
const libCephFSPrologue = `import cephfs, os, sys, threading
filesystem, args = sys.argv[1], sys.argv[2:]
deadline = threading.Timer(30, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '15')
fs.conf_set('rados_osd_op_timeout', '10')
try:
    fs.mount(filesystem_name=filesystem.encode())
`

// libCephFSEpilogue shuts the client down, which unmounts it, and stops the
// watchdog.
const libCephFSEpilogue = `finally:
    fs.shutdown()
    deadline.cancel()
`

// LibCephFSScript returns a program for "python3 -c" that runs body with the
// filesystem named by its first argument mounted through libcephfs as fs and
// its other arguments in args, under a 30 second watchdog that exits with
// status 124. body is unindented Python; it imports what it needs beyond
// cephfs, os, sys and threading.
func LibCephFSScript(body string) string {
	var script strings.Builder
	script.WriteString(libCephFSPrologue)
	for line := range strings.Lines(body) {
		if strings.TrimSpace(line) != "" {
			script.WriteString("    ")
		}
		script.WriteString(line)
	}
	if !strings.HasSuffix(body, "\n") {
		script.WriteString("\n")
	}
	script.WriteString(libCephFSEpilogue)
	return script.String()
}
