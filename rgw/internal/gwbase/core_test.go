package gwbase

import "testing"

type gatewayOwner struct{ Handle }

type userOwner struct{ UserHandle }

type accountOwner struct{ AccountHandle }

type originOwner struct{ Origin }

// TestHandleCopiesShareTheirCore pins the documented semantics of the rgw
// handles: unlike the cephfs foundation, a value copy is not detached, so a
// copied gateway, user, account or placement handle resolves to the same
// state as the original.
func TestHandleCopiesShareTheirCore(t *testing.T) {
	core := NewCore(Params{Name: "default"})
	gateway := &gatewayOwner{Handle: NewHandle(core)}
	copiedGateway := *gateway
	if Of(gateway) != core || Of(&copiedGateway) != core {
		t.Fatal("a gateway copy resolved to another core")
	}
	user := &userOwner{UserHandle: NewUserHandle(&User{Origin: core, State: &UserState{}})}
	copiedUser := *user
	UserOf(&copiedUser).State.Removed = true
	if UserOf(user) != UserOf(&copiedUser) || !UserOf(user).State.Removed {
		t.Fatal("a user copy did not share its lifecycle state")
	}
	account := &accountOwner{AccountHandle: NewAccountHandle(&Account{Origin: core, State: &AccountState{}})}
	copiedAccount := *account
	if AccountOf(account) != AccountOf(&copiedAccount) {
		t.Fatal("an account copy resolved to another account")
	}
	placement := &originOwner{Origin: NewOrigin(core)}
	copiedPlacement := *placement
	if OriginOf(placement) != core || OriginOf(&copiedPlacement) != core {
		t.Fatal("a placement copy resolved to another origin")
	}
}

// TestNilHandlesHaveNoState pins that a nil handle pointer, which a typed nil
// interface value can carry, resolves to no state instead of panicking, and
// that no state is owned.
func TestNilHandlesHaveNoState(t *testing.T) {
	var gateway *gatewayOwner
	var user *userOwner
	var account *accountOwner
	var placement *originOwner
	if Of(gateway) != nil || UserOf(user) != nil || AccountOf(account) != nil || OriginOf(placement) != nil || Of(nil) != nil {
		t.Fatal("a nil handle resolved to state")
	}
	if UserOf(&userOwner{}).Owned() || AccountOf(&accountOwner{}).Owned() || (&User{}).Owned() || (&Account{}).Owned() {
		t.Fatal("a handle without a cluster is owned")
	}
	var missing *Core
	if missing.Cluster() != nil || missing.Container() != nil || missing.SameCluster(NewCore(Params{})) || (&User{}).ServedBy(missing) {
		t.Fatal("a nil core has an identity")
	}
}
