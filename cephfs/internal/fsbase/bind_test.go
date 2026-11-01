package fsbase

import "testing"

type bindOwner struct{ Handle }

// TestOfDetachesAValueCopyOfTheBoundOwner pins that only the bound descriptor
// resolves to its Core: a value copy, which shares the Handle but not the
// descriptor-local state, gets the detached Core that is never registered.
func TestOfDetachesAValueCopyOfTheBoundOwner(t *testing.T) {
	owner := &bindOwner{Handle: NewHandle(NewCore(Params{Name: "tc", Live: func(*Core) bool { return true }}))}
	Bind(owner)
	copied := *owner

	if Of(owner) != owner.c {
		t.Fatal("the bound owner did not resolve to its core")
	}
	detached := Of(&copied)
	if detached == owner.c || detached == nil {
		t.Fatalf("copy resolved to %p, want a detached core", detached)
	}
	if detached.live(detached) {
		t.Fatal("the detached core reports itself live")
	}
	if detached.Name() != "tc" || Of(&copied) != detached {
		t.Fatal("the detached core lost the identity or is not stable")
	}
}
