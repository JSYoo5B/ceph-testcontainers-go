package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// probeScript makes the readiness requests of one probe inside the control
// container, which reaches the managers on the cluster network (the host
// network for a host-network cluster). It reads its request as JSON from
// standard input, so the password never appears in arguments, and prints one
// JSON report. It never follows a redirect: a standby answers 303 with the
// active manager's URL. The presented certificate is compared by its SHA-256
// fingerprint, not verified against a CA, because the trust anchor is the
// caller's and the fixture only checks that the listener serves the
// certificate it installed.
const probeScript = `import hashlib, http.client, json, socket, ssl, sys, urllib.parse
req = json.load(sys.stdin)
url = urllib.parse.urlsplit(req["url"])
report = {"results": []}
def conn():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return http.client.HTTPSConnection(url.hostname, url.port or 443, timeout=req["timeout"], context=ctx)
def call(method, path, body=None, token=None):
    c = conn()
    try:
        headers = {"Accept": "application/vnd.ceph.api.v1.0+json", "Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        c.request(method, path, body=None if body is None else json.dumps(body), headers=headers)
        if "fingerprint" not in report:
            report["fingerprint"] = hashlib.sha256(c.sock.getpeercert(True)).hexdigest()
        r = c.getresponse()
        return {"status": r.status, "location": r.getheader("Location") or "", "body": r.read(65536).decode("utf-8", "replace")}
    except socket.timeout:
        return {"error": "timeout"}
    except (OSError, http.client.HTTPException) as e:
        return {"error": type(e).__name__ + ": " + str(e)[:200]}
    finally:
        c.close()
report["rejected"] = call("POST", "/api/auth", {"username": req["username"], "password": req["wrong"]})
login = call("POST", "/api/auth", {"username": req["username"], "password": req["password"]})
token = ""
if login.get("status") == 201:
    try:
        token = json.loads(login["body"]).get("token") or ""
    except ValueError:
        pass
report["login"] = {k: v for k, v in login.items() if k != "body"}
report["token"] = token
if token:
    for path in req["paths"]:
        result = call("GET", path, token=token)
        result["path"] = path
        report["results"].append(result)
json.dump(report, sys.stdout)
`

// probeRequest is the standard input of probeScript.
type probeRequest struct {
	URL      string   `json:"url"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	Wrong    string   `json:"wrong"`
	Paths    []string `json:"paths"`
	Timeout  float64  `json:"timeout"`
}

// probeResult is one HTTP exchange of probeScript.
type probeResult struct {
	Path     string `json:"path"`
	Status   int    `json:"status"`
	Location string `json:"location"`
	Body     string `json:"body"`
	Error    string `json:"error"`
}

// probeReport is the output of probeScript. Token is the session token of the
// owned account, which the fixture registers as a secret and never returns.
type probeReport struct {
	Fingerprint string        `json:"fingerprint"`
	Rejected    probeResult   `json:"rejected"`
	Login       probeResult   `json:"login"`
	Token       string        `json:"token"`
	Results     []probeResult `json:"results"`
}

// result returns the exchange of path.
func (r probeReport) result(path string) (probeResult, bool) {
	for _, result := range r.Results {
		if result.Path == path {
			return result, true
		}
	}
	return probeResult{}, false
}

// probeTimeout bounds each HTTP exchange of a probe.
const probeTimeout = 20 * time.Second

// runProbe runs probeScript against url with the account's credentials and
// GETs paths with the session token.
func runProbe(ctx context.Context, c *ceph.Container, url, username, password string, paths ...string) (probeReport, error) {
	request, err := json.Marshal(probeRequest{URL: url, Username: username, Password: password, Wrong: password + "-wrong", Paths: paths, Timeout: probeTimeout.Seconds()})
	if err != nil {
		return probeReport{}, err
	}
	input := ceph.NewNativeInput()
	command := ceph.NativeCommand{
		Argv:    []string{"python3", "-I", "-c", probeScript},
		Timeout: time.Duration(len(paths)+3)*probeTimeout + 10*time.Second, MaxStdout: 1 << 20, MaxStderr: 64 << 10,
		Label: "dashboard readiness probe", Input: input, MaxInput: len(request),
	}
	go func() {
		_ = input.Write(ctx, request)
		_ = input.Close()
	}()
	result, err := c.RunNativeCommand(ctx, servicehook.CLI(c), command)
	if err != nil {
		return probeReport{}, err
	}
	if result.ExitCode != 0 {
		return probeReport{}, fmt.Errorf("dashboard readiness probe exited %d", result.ExitCode)
	}
	return decodeProbeReport(result.Stdout)
}

func decodeProbeReport(data []byte) (probeReport, error) {
	var report probeReport
	if err := json.Unmarshal(data, &report); err != nil {
		return probeReport{}, errors.New("decode dashboard readiness probe report")
	}
	return report, nil
}

// fingerprint is the SHA-256 of a DER certificate, as probeScript reports it.
func fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}

// summary is the part of GET /api/summary that identifies the manager that
// answered.
type summary struct {
	ManagerID string `json:"mgr_id"`
}

// checkReadiness judges one probe of the account against the active manager:
// the listener presents the installed certificate, a wrong password is
// refused, the right one logs in, and /api/summary is answered by active.
func checkReadiness(report probeReport, want, active string) error {
	switch {
	case report.Rejected.Error != "":
		return fmt.Errorf("dashboard listener unreachable: %s", report.Rejected.Error)
	case report.Fingerprint != want:
		return errors.New("dashboard listener presents a certificate other than the installed one")
	case report.Rejected.Status == 303:
		return errors.New("dashboard listener is a standby that redirects to the active manager")
	case report.Rejected.Status != 400 && report.Rejected.Status != 401:
		return fmt.Errorf("dashboard accepted or failed a wrong password with HTTP %d", report.Rejected.Status)
	case report.Login.Error != "":
		return fmt.Errorf("dashboard login: %s", report.Login.Error)
	case report.Login.Status != 201 || report.Token == "":
		return fmt.Errorf("dashboard refused the owned account with HTTP %d", report.Login.Status)
	}
	result, ok := report.result(summaryPath)
	if !ok || result.Error != "" || result.Status != 200 {
		return fmt.Errorf("dashboard %s answered HTTP %d %s", summaryPath, result.Status, result.Error)
	}
	var answered summary
	if err := json.Unmarshal([]byte(result.Body), &answered); err != nil || answered.ManagerID == "" {
		return fmt.Errorf("decode dashboard %s", summaryPath)
	}
	if answered.ManagerID != active {
		return fmt.Errorf("dashboard %s was answered by mgr.%s, the active manager is mgr.%s", summaryPath, answered.ManagerID, active)
	}
	return nil
}

const summaryPath = "/api/summary"
