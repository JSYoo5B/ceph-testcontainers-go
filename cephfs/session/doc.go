// Package session observes the client sessions of an existing CephFS
// filesystem and overrides its MDSMap session policy, so that a test can see
// a silent or frozen client evicted within seconds, a new client refused
// while open sessions keep working, or only clients that lack a required
// client feature refused and evicted.
//
// List, Timeouts and RequiredFeatures read native state; Watch polls the
// sessions and reports admission, state changes and removal from samples
// that two reads of the active-rank holders bracket. OverrideTimeouts
// changes both limits, OverrideRefusal makes the MDS reject new sessions and
// OverrideRequiredFeatures makes it reject and evict only clients that lack
// the named features; each returns a handle whose Restore puts back what it
// changed. These functions act on a filesystem that cephfs.Run, cephfs.Start
// or cephfs.WithFilesystems created; they start no container. Evicting the
// sessions of one subvolume principal is
// cephfs/subvolume.Authorization.EvictClients.
package session
