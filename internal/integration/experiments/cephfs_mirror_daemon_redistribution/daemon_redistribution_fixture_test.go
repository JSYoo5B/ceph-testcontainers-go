//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package cephfs_mirror_daemon_redistribution_test

import (
	"strings"
	"testing"
)

// These cases feed synthetic readings to judgeRedistribution. They are not
// native evidence of what the MGR does.
func TestJudgeRedistribution(t *testing.T) {
	directories := []string{"/daemon-a", "/daemon-b", "/daemon-c", "/daemon-d"}
	all := func(owner string) map[string]string {
		owners := map[string]string{}
		for _, d := range directories {
			owners[d] = owner
		}
		return owners
	}
	spread := map[string]string{"/daemon-a": "4100", "/daemon-b": "4200", "/daemon-c": "4100", "/daemon-d": "4200"}
	for name, tc := range map[string]struct {
		distribution, owners map[string]string
		registered           int
		want                 string
	}{
		"spread over both": {map[string]string{"4100": "2 directories", "4200": "2 directories"}, spread, 2,
			"absent: the 4 directories are spread over instances 4100 (2) and 4200 (2)"},
		"all stay with the survivor": {map[string]string{"4100": "4 directories"}, all("4100"), 2,
			"symptom: all 4 directories stay with the surviving instance 4100 while 2 services are registered"},
		"the replacement listed without directories": {map[string]string{"4100": "4 directories", "4200": "0 directories"}, all("4100"), 3, "symptom: "},
		"the replacement not registered":             {map[string]string{"4100": "4 directories"}, all("4100"), 1, "condition not established: 1 services"},
		"all moved to the replacement":               {map[string]string{"4200": "4 directories"}, all("4200"), 2, "another fault: the directories are held by"},
		"a directory unmapped": {map[string]string{"4100": "3 directories"}, map[string]string{"/daemon-a": "4100", "/daemon-b": "4100", "/daemon-c": "4100"}, 2,
			"another fault: directory /daemon-d is not mapped"},
		"three instances":       {map[string]string{"4100": "2 directories", "4200": "1 directories", "4300": "1 directories"}, spread, 3, "another fault: "},
		"a malformed count":     {map[string]string{"4100": "four"}, all("4100"), 2, "another fault: the distribution has instance"},
		"two listed, one holds": {map[string]string{"4100": "4 directories", "4200": "1 directories"}, all("4100"), 2, "another fault: "},
	} {
		symptom, detail, err := judgeRedistribution(directories, "4100", tc.distribution, tc.owners, tc.registered)
		got := "absent: " + detail
		switch {
		case err != nil:
			got = err.Error()
		case symptom:
			got = "symptom: " + detail
		}
		if !strings.HasPrefix(got, tc.want) {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}
