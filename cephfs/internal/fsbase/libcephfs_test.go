package fsbase

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The shared program mounts inside try, ends the mount and OSD timeouts
// before the watchdog and indents the body into the try block.
func TestLibCephFSScriptMountsInsideTheWatchdog(t *testing.T) {
	script := LibCephFSScript("import json\nfor x in args:\n    print(json.dumps(x))")
	seconds := func(pattern string) int {
		t.Helper()
		match := regexp.MustCompile(pattern).FindStringSubmatch(script)
		if match == nil {
			t.Fatalf("%s is missing from\n%s", pattern, script)
		}
		value, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	watchdog := seconds(`threading\.Timer\((\d+), lambda: os\._exit\(124\)\)`)
	mount := seconds(`conf_set\('client_mount_timeout', '(\d+)'\)`)
	osd := seconds(`conf_set\('rados_osd_op_timeout', '(\d+)'\)`)
	if mount >= watchdog || osd >= watchdog {
		t.Fatalf("timeouts %ds and %ds do not end before the %ds watchdog", mount, osd, watchdog)
	}
	want := "try:\n    fs.mount(filesystem_name=filesystem.encode())\n    import json\n    for x in args:\n        print(json.dumps(x))\nfinally:\n    fs.shutdown()\n    deadline.cancel()\n"
	if !strings.HasSuffix(script, want) {
		t.Fatalf("body is not inside the try block:\n%s", script)
	}
}
