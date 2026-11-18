//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestJournalFreshFilesProducer(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	const substitute = `import sys, types
fault = sys.argv.pop(1)
class NativeError(Exception): pass
class FS:
    def __init__(self, **kwargs): self.files = {}
    def conf_set(self, *args): pass
    def mount(self): pass
    def unmount(self): pass
    def shutdown(self): pass
    def open(self, path, mode, *args):
        if mode == "r" and fault == "missing-file": raise NativeError("missing file")
        return path
    def close(self, fd): pass
    def fsync(self, *args): pass
    def write(self, fd, data, offset):
        self.files[fd] = data
        return len(data) - 1 if fault == "short-write" else len(data)
    def read(self, fd, offset, size):
        if fault == "read-error": raise NativeError("read failed")
        data = self.files[fd]
        if fault == "wrong-content": data = b"x" * len(data)
        if fault == "short-read": data = data[:-1]
        return data[:size]
sys.modules["cephfs"] = types.SimpleNamespace(LibCephFS=FS, Error=NativeError)
`
	for _, fault := range []string{"valid", "short-write", "read-error", "missing-file", "wrong-content", "short-read"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			output, runErr := exec.CommandContext(ctx, python, "-c", substitute+journalFiles, fault, "verify-seed", "test", "after", "2").CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("producer exceeded bound: %v", ctx.Err())
			}
			if fault == "short-write" || fault == "read-error" || fault == "missing-file" {
				if runErr == nil {
					t.Fatalf("producer accepted %s: %s", fault, output)
				}
				return
			}
			if runErr != nil {
				t.Fatalf("producer: %v: %s", runErr, output)
			}
			err := validateJournalFreshFiles(output, "after", 2)
			if fault != "valid" {
				if err == nil {
					t.Fatalf("consumer accepted %s: %s", fault, output)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, mutation := range []string{"missing-path", "foreign-path", "missing-write", "short-write", "missing-size", "wrong-digest"} {
				t.Run(mutation, func(t *testing.T) {
					var changed map[string]map[string]any
					if err := json.Unmarshal(output, &changed); err != nil {
						t.Fatal(err)
					}
					switch mutation {
					case "missing-path":
						delete(changed, "/after-1")
					case "foreign-path":
						changed["/foreign-1"] = changed["/after-1"]
						delete(changed, "/after-1")
					case "missing-write":
						delete(changed["/after-0"], "written")
					case "short-write":
						changed["/after-0"]["written"] = 1
					case "missing-size":
						delete(changed["/after-0"], "size")
					case "wrong-digest":
						changed["/after-0"]["sha256"] = strings.Repeat("0", 64)
					}
					raw, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					if err := validateJournalFreshFiles(raw, "after", 2); err == nil {
						t.Fatalf("accepted %s", mutation)
					}
				})
			}
		})
	}
}
