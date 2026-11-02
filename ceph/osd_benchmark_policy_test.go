package ceph

import (
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestOSDBenchmarkPolicyCompositionAndCopies(t *testing.T) {
	o, err := storageOptions(t, OSDStorage{})
	if err != nil || o.osdBenchmark != "" {
		t.Fatal("zero storage changed benchmark configuration", o, err)
	}
	base := OSDStorage{}.WithBlockSize(128 << 20)
	selected := base.WithStartupBenchmark(OSDBenchmarkSkip)
	other := selected.WithStartupBenchmark(OSDBenchmarkNative)
	for _, test := range []struct {
		value OSDStorage
		want  OSDBenchmarkPolicy
	}{{base, ""}, {selected, OSDBenchmarkSkip}, {other, OSDBenchmarkNative}} {
		o, err := storageOptions(t, test.value)
		if err != nil || o.osdBenchmark != test.want || o.blockSize != 128<<20 {
			t.Fatalf("derived value changed its source or another field: %+v %v", o, err)
		}
	}
	for _, values := range [][]OSDStorage{
		{OSDStorage{}.WithStartupBenchmark(OSDBenchmarkSkip), OSDStorage{}.WithStartupBenchmark(OSDBenchmarkSkip)},
		{OSDStorage{}.WithStartupBenchmark(OSDBenchmarkSkip), OSDStorage{}, OSDStorage{}.WithStartupBenchmark(OSDBenchmarkNative)},
	} {
		if _, err := storageOptions(t, values...); err == nil || !strings.Contains(err.Error(), "startup benchmark") {
			t.Fatal("repeated benchmark carriers were accepted", err)
		}
	}
	for _, order := range [][]OSDStorage{
		{OSDStorage{}.WithObjectStore(OSDMemStore), selected},
		{selected, OSDStorage{}.WithObjectStore(OSDMemStore)},
	} {
		o, err := storageOptions(t, order...)
		if err != nil || o.osdBenchmark != OSDBenchmarkSkip || o.osdObjectStore != OSDMemStore {
			t.Fatal("benchmark policy changed object-store composition", o, err)
		}
	}
}

func TestOSDBenchmarkPolicyInvalidValuesRefusedBeforeEffects(t *testing.T) {
	for _, policy := range []OSDBenchmarkPolicy{"", "force", "false", "skip; command"} {
		_, err := admit(t, "image", WithOSDStorage(OSDStorage{}.WithStartupBenchmark(policy)))
		wantRefused(t, err)
	}
}

func TestOSDBenchmarkPolicyIsCapturedByEveryLaunchRecipe(t *testing.T) {
	for _, policy := range []OSDBenchmarkPolicy{"", OSDBenchmarkNative, OSDBenchmarkSkip} {
		t.Run(string(policy), func(t *testing.T) {
			config := "[global]\nfsid = synthetic-cluster\n[osd]\nosd_mclock_skip_benchmark = false\nosd_mclock_force_run_benchmark_on_init = true\n"
			c := &Container{settings: options{osdBenchmark: policy}, bootstrap: bootstrapRecord{config: []byte(config)}}
			r := c.newOSDLaunchRecipe(&OSDContainer{ID: 7}, nil)
			c.settings.osdBenchmark = OSDBenchmarkSkip
			for range 2 {
				request := osdBenchmarkPolicyRequest(t, r)
				if request != string(policy) {
					t.Fatalf("recipe inherited changed owner settings: %q, want %q", request, policy)
				}
			}
			if string(c.bootstrap.rawConfig()) != config {
				t.Fatal("policy rewrote bootstrap configuration")
			}
		})
	}
}

func osdBenchmarkPolicyRequest(t *testing.T, r *osdLaunchRecipe) string {
	t.Helper()
	req := testcontainers.GenericContainerRequest{}
	if err := r.Customize(&req); err != nil {
		t.Fatal(err)
	}
	return req.Env["CEPH_OSD_STARTUP_BENCHMARK"]
}

func TestOSDBenchmarkPolicyScriptUsesLocalAdmissionAndLiveDaemon(t *testing.T) {
	for _, test := range []struct{ policy, want string }{
		{"", ""}, {"native", "--osd-mclock-skip-benchmark=false"}, {"skip", "--osd-mclock-skip-benchmark=true"},
	} {
		for _, ready := range []bool{false, true} {
			t.Run(test.policy+map[bool]string{false: "/fresh", true: "/restart"}[ready], func(t *testing.T) {
				f := newOSDBootstrapFixture(t)
				f.env["CEPH_OSD_STARTUP_BENCHMARK"] = test.policy
				if ready {
					f.ready(t)
				}
				calls, out, err := f.run(t)
				if err != nil || len(calls) == 0 {
					t.Fatalf("bootstrap: %v %s %v", calls, out, err)
				}
				for i, call := range calls {
					has := strings.Contains(call, "--osd-mclock-skip-benchmark=")
					if i == len(calls)-1 {
						if !strings.HasPrefix(call, "-f ") || has != (test.want != "") || (test.want != "" && strings.Count(call, test.want) != 1) {
							t.Fatalf("unexpected daemon policy arguments: %s", call)
						}
					} else if has && !strings.Contains(call, "--show-config-value osd_mclock_skip_benchmark") {
						t.Fatalf("policy leaked into mkfs or unrelated config lookup: %s", call)
					}
				}
			})
		}
	}
}

func TestOSDBenchmarkPolicyScriptRefusesUnknownMarkerBeforeNativeCalls(t *testing.T) {
	f := newOSDBootstrapFixture(t)
	f.env["CEPH_OSD_STARTUP_BENCHMARK"] = "unknown"
	calls, out, err := f.run(t)
	if err == nil || len(calls) != 0 || !strings.Contains(out, "invalid startup benchmark policy") {
		t.Fatalf("bad marker touched native process: calls=%v out=%s error=%v", calls, out, err)
	}
}

func TestOSDBenchmarkPolicyScriptRefusesUnavailableOrIgnoredOptionBeforeStoreWrites(t *testing.T) {
	for _, control := range []struct{ key, value string }{
		{"TC_FAIL_BENCHMARK", "1"}, {"TC_BENCHMARK_VALUE", "false"},
	} {
		t.Run(control.key, func(t *testing.T) {
			f := newOSDBootstrapFixture(t)
			f.env["CEPH_OSD_STARTUP_BENCHMARK"] = "skip"
			f.env[control.key] = control.value
			calls, out, err := f.run(t)
			if err == nil || len(calls) != 1 || !strings.Contains(calls[0], "--show-config-value osd_mclock_skip_benchmark") || !strings.Contains(out, "OSD bootstrap refused") {
				t.Fatalf("unsupported/ignored option proceeded: calls=%v out=%s error=%v", calls, out, err)
			}
		})
	}
}
