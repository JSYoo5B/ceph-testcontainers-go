package capture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// probeScript runs in a short-lived helper in the target's namespace, as the
// image's user with NET_RAW only. It reports the contract's executables, the
// user, tcpdump's version, the filter compiled without a device, the
// namespace and the interface's MAC address, read with SIOCGIFHWADDR, which
// needs no capability, as one JSON object. It changes nothing.
const probeScript = `import fcntl,json,os,shutil,socket,struct,subprocess,sys
iface,filt=sys.argv[1],sys.argv[2]
out={"uid":os.getuid(),"euid":os.geteuid(),"missing":[n for n in ("sh","sleep","tcpdump","python3") if shutil.which(n) is None]}
def run(*argv):
 try: p=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20)
 except PermissionError as e: return [-1,"",str(e)]
 except (OSError,subprocess.SubprocessError) as e: return [-2,"",str(e)]
 return [p.returncode,p.stdout.decode(errors="replace")[-2000:],p.stderr.decode(errors="replace")[-2000:]]
if not out["missing"]:
 out["version"]=run("tcpdump","--version")
 out["filter"]=run("tcpdump","-y","EN10MB","-d","--",filt)
try: out["namespace"]=os.readlink("/proc/self/ns/net")
except OSError: pass
try:
 s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
 info=fcntl.ioctl(s.fileno(),0x8927,struct.pack("256s",iface.encode()[:15]))
 out["mac"]=":".join("%02x"%b for b in info[18:24])
except OSError as e: out["mac_error"]=str(e)
print(json.dumps(out))
`

func probeArgv(iface, filter string) []string {
	return []string{"python3", "-c", probeScript, iface, filter}
}

// probeResult is a probe container's end.
type probeResult struct {
	imageID        string
	exitCode       int
	stdout, stderr []byte
}

// commandResult is the [code, stdout, stderr] of one probe command; code -1
// is an exec refused with EPERM, -2 another launch failure.
type commandResult []any

func (c commandResult) parts() (int, string, bool) {
	if len(c) != 3 {
		return 0, "", false
	}
	code, ok := c[0].(float64)
	stdout, ok1 := c[1].(string)
	stderr, ok2 := c[2].(string)
	return int(code), strings.TrimSpace(stdout + "\n" + stderr), ok && ok1 && ok2
}

type probeReport struct {
	UID       *int          `json:"uid"`
	EUID      *int          `json:"euid"`
	Missing   []string      `json:"missing"`
	Version   commandResult `json:"version"`
	Filter    commandResult `json:"filter"`
	Namespace string        `json:"namespace"`
	MAC       string        `json:"mac"`
	MACError  string        `json:"mac_error"`
}

// parseProbe reads the probe's report from its last line of output. A probe
// that printed none could not run python3 or the script.
func parseProbe(result probeResult) (probeReport, error) {
	lines := bytes.Split(bytes.TrimSpace(result.stdout), []byte("\n"))
	var report probeReport
	if err := json.Unmarshal(lines[len(lines)-1], &report); err != nil {
		text := strings.TrimSpace(string(result.stderr))
		if permissionDenied(text) {
			return report, fmt.Errorf("%w: the capture probe exited with status %d: %s", ErrPermissionDenied, result.exitCode, text)
		}
		return report, fmt.Errorf("%w: the capture probe exited with status %d without a report: %s", ErrImageUnsupported, result.exitCode, text)
	}
	return report, nil
}

// check applies the contract and the request to the probe's report.
func (r probeReport) check(req Request, endpointMAC string) error {
	switch {
	case r.Missing == nil || r.UID == nil || r.EUID == nil:
		return fmt.Errorf("%w: the capture probe report is incomplete", ErrImageUnsupported)
	case len(r.Missing) > 0:
		return fmt.Errorf("%w: missing %s", ErrImageUnsupported, strings.Join(r.Missing, ", "))
	case *r.UID == 0 || *r.EUID == 0:
		return fmt.Errorf("%w: the helper runs as root; the contract needs the image's own non-root user", ErrImageUnsupported)
	}
	code, text, ok := r.Version.parts()
	switch {
	case !ok:
		return fmt.Errorf("%w: the capture probe did not run tcpdump --version", ErrImageUnsupported)
	case code == -1 || (code != 0 && permissionDenied(text)):
		return fmt.Errorf("%w: tcpdump --version: %s", ErrPermissionDenied, text)
	case code != 0 || !strings.Contains(text, "tcpdump version"):
		return fmt.Errorf("%w: tcpdump --version exited with status %d: %s", ErrImageUnsupported, code, text)
	}
	code, text, ok = r.Filter.parts()
	switch {
	case !ok:
		return fmt.Errorf("%w: the capture probe did not compile the filter", ErrImageUnsupported)
	case code == 0:
	case strings.Contains(text, "can't parse filter expression") || strings.Contains(text, "syntax error"):
		return invalidRequest("the capture filter does not compile: %s", text)
	case code == -1 || permissionDenied(text):
		return fmt.Errorf("%w: compile the filter: %s", ErrPermissionDenied, text)
	default:
		return fmt.Errorf("%w: compile the filter: exit status %d: %s", ErrImageUnsupported, code, text)
	}
	switch {
	case r.MACError != "" || r.MAC == "":
		return invalidRequest("interface %s is not in the target's network namespace: %s", req.Interface, r.MACError)
	case !strings.EqualFold(r.MAC, endpointMAC):
		return invalidRequest("interface %s has MAC address %s, not that of the target's endpoint on %s (%s)", req.Interface, r.MAC, req.Network, endpointMAC)
	}
	return nil
}

// toolVersion is tcpdump --version's output on one line.
func (r probeReport) toolVersion() string {
	_, text, _ := r.Version.parts()
	return strings.Join(strings.Fields(text), " ")
}
