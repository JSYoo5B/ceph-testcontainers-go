package sharedinvariants

import (
	"slices"
	"testing"
)

func baseline() Invariants {
	return Invariants{
		Auth:       map[string]string{"client.admin": "allow *"},
		Pools:      map[string]string{"tenants": "size 1"},
		OSDFlags:   []string{"noout"},
		Config:     map[string]string{"k": "v"},
		Blocklist:  []string{"a"},
		Health:     []string{"X"},
		Namespaces: map[string]int{},
		Resources:  map[string]string{},
	}
}

func clone(in Invariants) Invariants {
	out := in
	out.Auth = cloneMap(in.Auth)
	out.Pools = cloneMap(in.Pools)
	out.Config = cloneMap(in.Config)
	out.Resources = cloneMap(in.Resources)
	out.Namespaces = map[string]int{}
	for k, v := range in.Namespaces {
		out.Namespaces[k] = v
	}
	return out
}

func cloneMap(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func TestTenantOwns(t *testing.T) {
	for _, c := range []struct {
		id, entity string
		want       bool
	}{
		{"t1", "client.t1", true},
		{"t1", "client.t1-read", true},
		{"t1", "client.t10", false},
		{"t1", "client.t1x-read", false},
		{"t1", "osd.t1", false},
	} {
		if got := TenantOwns(c.id, c.entity); got != c.want {
			t.Errorf("TenantOwns(%q, %q) = %v, want %v", c.id, c.entity, got, c.want)
		}
	}
}

func TestAdmissionDiff(t *testing.T) {
	b := baseline()
	if got := b.AdmissionDiff(clone(b)); len(got) != 0 {
		t.Fatalf("identical state differs: %v", got)
	}
	cur := clone(b)
	cur.Unsettled = "PGs are not active+clean"
	cur.Health = []string{"X", "Y"}
	cur.OSDFlags = nil
	cur.Blocklist = append(cur.Blocklist, "b")
	want := []string{
		"PGs are not active+clean",
		"health checks are [X Y], not [X]",
		"OSD flags are [], not [noout]",
		"the blocklist has 2 entries, not 1",
	}
	if got := b.AdmissionDiff(cur); !slices.Equal(got, want) {
		t.Fatalf("AdmissionDiff = %q, want %q", got, want)
	}
	// Auth, pools and config are not admission conditions.
	cur = clone(b)
	cur.Auth["client.new"] = "x"
	cur.Pools["p"] = "x"
	cur.Config["k"] = "other"
	if got := b.AdmissionDiff(cur); len(got) != 0 {
		t.Fatalf("auth, pool and config changes are admission problems: %v", got)
	}
}

// afterRelease is the state after tenant t1 released while t2 is live: the
// baseline plus t2's scope.
func afterRelease(b Invariants) Invariants {
	cur := clone(b)
	cur.Auth["client.t2"] = "rw"
	cur.Namespaces["t2"] = 2
	cur.Resources["bucket/b1"] = "t2"
	return cur
}

func TestReleaseDiffClean(t *testing.T) {
	b := baseline()
	problems, lagging := b.ReleaseDiff(afterRelease(b), "tenants", "t1", map[string]bool{"t2": true})
	if len(problems) != 0 || lagging {
		t.Fatalf("clean release reports %q lagging = %v", problems, lagging)
	}
}

func TestReleaseDiffProblems(t *testing.T) {
	b := baseline()
	others := map[string]bool{"t2": true}
	cases := []struct {
		name   string
		mutate func(*Invariants)
		want   []string
	}{
		{"own client remains", func(c *Invariants) { c.Auth["client.t1"] = "rw"; c.Auth["client.t1-read"] = "r" }, []string{"tenant client client.t1 remains", "tenant client client.t1-read remains"}},
		{"own namespace remains", func(c *Invariants) { c.Namespaces["t1"] = 3 }, []string{"namespace t1 of pool tenants still has 3 objects"}},
		{"auth added", func(c *Invariants) { c.Auth["client.rogue"] = "x" }, []string{"auth entity client.rogue was added"}},
		{"auth caps changed", func(c *Invariants) { c.Auth["client.admin"] = "allow r" }, []string{"capabilities of client.admin changed"}},
		{"auth removed", func(c *Invariants) { delete(c.Auth, "client.admin") }, []string{"auth entity client.admin was removed"}},
		{"other tenant client ignored", func(c *Invariants) { c.Auth["client.t2-extra"] = "x" }, nil},
		{"pool changed", func(c *Invariants) { c.Pools["tenants"] = "size 3" }, []string{"pool tenants changed"}},
		{"config removed", func(c *Invariants) { delete(c.Config, "k") }, []string{"config entry k was removed"}},
		{"flags", func(c *Invariants) { c.OSDFlags = nil }, []string{"OSD flags are [], not [noout]"}},
		{"blocklist", func(c *Invariants) { c.Blocklist = nil }, []string{"the blocklist has 0 entries, not 1"}},
		{"other namespace changed", func(c *Invariants) { c.Namespaces["t2"] = 9 }, nil},
		{"unknown namespace", func(c *Invariants) { c.Namespaces["t3"] = 4 }, []string{`namespace "t3" of pool tenants has 4 objects, not 0`}},
		{"baseline namespace lost", func(c *Invariants) { delete(c.Namespaces, "base") }, []string{`namespace "base" of pool tenants lost its objects`}},
		{"resource remains", func(c *Invariants) { c.Resources["bucket/b2"] = "t1" }, []string{"bucket/b2 remains"}},
		{"resource added", func(c *Invariants) { c.Resources["bucket/b3"] = "nobody" }, []string{"bucket/b3 was added"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base := b
			base.Namespaces = map[string]int{"base": 1}
			cur := afterRelease(base)
			cur.Namespaces["base"] = 1
			c.mutate(&cur)
			problems, lagging := base.ReleaseDiff(cur, "tenants", "t1", others)
			if !slices.Equal(problems, c.want) {
				t.Fatalf("problems = %q, want %q", problems, c.want)
			}
			if lagging {
				t.Fatal("unchanged health reported lagging")
			}
		})
	}
}

func TestReleaseDiffLagging(t *testing.T) {
	b := baseline()
	cur := afterRelease(b)
	cur.Health = []string{"X", "Z"}
	problems, lagging := b.ReleaseDiff(cur, "tenants", "t1", map[string]bool{"t2": true})
	if len(problems) != 0 || !lagging {
		t.Fatalf("health difference: problems = %q lagging = %v", problems, lagging)
	}
}

func TestReleaseDiffUsesPoolName(t *testing.T) {
	b := baseline()
	cur := afterRelease(b)
	cur.Namespaces["t1"] = 1
	problems, _ := b.ReleaseDiff(cur, "other", "t1", map[string]bool{"t2": true})
	if want := []string{"namespace t1 of pool other still has 1 objects"}; !slices.Equal(problems, want) {
		t.Fatalf("problems = %q, want %q", problems, want)
	}
}

func TestMapDiffNamesKeysNotValues(t *testing.T) {
	got := mapDiff("config entry", map[string]string{"a": "secret1", "b": "x"}, map[string]string{"a": "secret2", "c": "y"})
	want := []string{"config entry a changed", "config entry c was added", "config entry b was removed"}
	if !slices.Equal(got, want) {
		t.Fatalf("mapDiff = %q, want %q", got, want)
	}
}

func TestResourceDiff(t *testing.T) {
	base := map[string]string{"r1": "t2", "r2": ""}
	cur := map[string]string{"r1": "t2", "r3": "t1", "r4": "t2", "r5": "nobody"}
	got := ResourceDiff(base, cur, "t1", map[string]bool{"t2": true})
	want := []string{"r3 remains", "r5 was added", "r2 was removed"}
	if !slices.Equal(got, want) {
		t.Fatalf("ResourceDiff = %q, want %q", got, want)
	}
}

func TestSortedKeys(t *testing.T) {
	if got := SortedKeys(map[string]int{"b": 1, "a": 2, "c": 3}); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("SortedKeys = %q", got)
	}
	if got := SortedKeys[int](nil); len(got) != 0 {
		t.Fatalf("SortedKeys(nil) = %q", got)
	}
}
