package ceph

import (
	"maps"
	"slices"
)

// authState holds the records that the calls changing an auth entry keep
// between calls, for clients and for daemons.
//
// The records have no lock of their own: Container.mu guards every part. The
// methods therefore say "mu held", and the caller either takes
// Container.mu itself or is a helper whose doc comment names that contract.
// The set of those helpers is pinned by the guards in auth_state_test.go, which
// also check that no field is used outside this type, that the methods call
// nothing that locks, and that the split adds no mutex.
//
// authFence values and clientRevocationState values are handed out as
// pointers, and the caller changes them in place under the same lock.
type authState struct {
	// fences records, per auth entity (a client or a daemon), what an
	// earlier RotateKey or client key change left uncertain.
	fences map[string]*authFence
	// secrets holds every client key that the module generated or confirmed,
	// old and attempted ones included, for diagnostics to redact.
	secrets map[string]bool
	// keys holds, per client identity that this cluster created, the key that
	// the module last confirmed for it. A descriptor with another key, or of a
	// deleted identity, is stale, also when it is a copy.
	keys map[string]string
	// strays holds files with keys in the control container that earlier auth
	// calls could not remove for sure; later ones remove them. A file whose
	// copy failed stays listed, as true, because Docker may still write it.
	strays map[string]bool
	// revocations holds, per client entity, the state of the temporary
	// revocation that has removed it until its Restore.
	revocations map[string]*clientRevocationState
}

// fence returns the fence of entity, or nil. mu held.
func (a *authState) fence(entity string) *authFence { return a.fences[entity] }

// fenceFor returns the fence of entity and creates it when it is missing.
// mu held.
func (a *authState) fenceFor(entity string) *authFence {
	if a.fences == nil {
		a.fences = make(map[string]*authFence)
	}
	fence := a.fences[entity]
	if fence == nil {
		fence = &authFence{}
		a.fences[entity] = fence
	}
	return fence
}

// dropFence forgets the fence of entity. mu held.
func (a *authState) dropFence(entity string) { delete(a.fences, entity) }

// confirmedKey returns the key that the module last confirmed for the client
// identity, and whether the identity is known. mu held.
func (a *authState) confirmedKey(entity string) (string, bool) {
	key, ok := a.keys[entity]
	return key, ok
}

// confirmKey records the key that the module confirmed for a client identity,
// or "" for a deleted one. A key is also kept for redaction. mu held.
func (a *authState) confirmKey(entity, key string) {
	if key == "" {
		delete(a.keys, entity)
		return
	}
	if a.keys == nil {
		a.keys = make(map[string]string)
	}
	a.keys[entity] = key
	a.addSecret(key)
}

// addSecret keeps a client key for diagnostics to redact, also after the key
// was replaced or deleted, since copies of it may remain. mu held.
func (a *authState) addSecret(key string) {
	if key == "" {
		return
	}
	if a.secrets == nil {
		a.secrets = make(map[string]bool)
	}
	a.secrets[key] = true
}

// secretValues returns every kept client key, sorted. mu held.
func (a *authState) secretValues() []string { return slices.Sorted(maps.Keys(a.secrets)) }

// addStray records a file with a key for later removal; copying means that
// Docker may still write it. mu held.
func (a *authState) addStray(path string, copying bool) {
	if a.strays == nil {
		a.strays = make(map[string]bool)
	}
	a.strays[path] = a.strays[path] || copying
}

// strayPaths returns the files that earlier calls could not remove, sorted.
// mu held.
func (a *authState) strayPaths() []string { return slices.Sorted(maps.Keys(a.strays)) }

// forgetRemovedStrays forgets the files that a removal confirmed, which are
// all but those whose copy may still complete. mu held.
func (a *authState) forgetRemovedStrays() {
	maps.DeleteFunc(a.strays, func(_ string, copying bool) bool { return !copying })
}

// revocation returns the temporary revocation of entity, or nil. mu held.
func (a *authState) revocation(entity string) *clientRevocationState {
	return a.revocations[entity]
}

// setRevocation records the temporary revocation of entity. mu held.
func (a *authState) setRevocation(entity string, state *clientRevocationState) {
	if a.revocations == nil {
		a.revocations = make(map[string]*clientRevocationState)
	}
	a.revocations[entity] = state
}

// dropRevocation forgets the temporary revocation of entity. mu held.
func (a *authState) dropRevocation(entity string) { delete(a.revocations, entity) }

// dropRevocationIf forgets the revocation of entity when it is state. mu held.
func (a *authState) dropRevocationIf(entity string, state *clientRevocationState) {
	if a.revocations[entity] == state {
		delete(a.revocations, entity)
	}
}
