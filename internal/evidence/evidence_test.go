package evidence

import (
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// The redactor replaces values in one pass, so that a value that occurs in
// the replacement text, given many times, does not rewrite its own output.
func TestRedactorDoesNotRewriteItsOutput(t *testing.T) {
	redact := Redactor(slices.Repeat([]string{"E"}, 32))
	if got := redact("E and E"); got != "[REDACTED] and [REDACTED]" {
		t.Fatalf("redacted %q", got)
	}
	redact = Redactor([]string{"secret", "secret-longer", ""})
	if got := redact("secret-longer secret"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redacted %q", got)
	}
}

type selectTestContainer struct {
	testcontainers.Container
	id string
}

func (c *selectTestContainer) GetContainerID() string { return c.id }

// Select keeps one member per container ID, so an additional container that
// the cluster tracks keeps the cluster's description; it names unnamed
// additions, drops typed nil handles and cuts the sorted list at the limit.
func TestSelectDeduplicatesNamesAndLimits(t *testing.T) {
	var missing *selectTestContainer
	mon := &selectTestContainer{id: "mon"}
	inv := Inventory{Members: []Member{{Role: "osd", Name: "0", Container: &selectTestContainer{id: "osd"}}, {Role: "mon", Name: "a", Container: mon}}}
	members, omitted := inv.Select([]Member{{Container: mon}, {Role: "mirror", Container: missing}, {}}, 8)
	if omitted != 0 || len(members) != 4 {
		t.Fatalf("members %+v, omitted %d", members, omitted)
	}
	want := []string{"extra/extra-2", "mirror/extra-1", "mon/a", "osd/0"}
	for i, member := range members {
		if got := member.Role + "/" + member.Name; got != want[i] {
			t.Fatalf("member %d is %s, want %s", i, got, want[i])
		}
	}
	if members[1].Container != nil {
		t.Fatal("a typed nil handle was kept")
	}
	members, omitted = inv.Select(nil, 1)
	if omitted != 1 || len(members) != 1 || members[0].Name != "a" {
		t.Fatalf("limited members %+v, omitted %d", members, omitted)
	}
}
