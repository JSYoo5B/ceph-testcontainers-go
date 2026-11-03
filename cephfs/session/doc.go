// Package session observes the client sessions of an existing CephFS
// filesystem and overrides its MDSMap session policy, so that a test can see
// a silent or frozen client evicted within seconds, or a new client refused
// while open sessions keep working.
//
// List and Timeouts read native state. OverrideTimeouts changes both limits
// and OverrideRefusal makes the MDS reject new sessions; each returns a
// handle whose Restore puts the captured state back. These
// functions act on a filesystem that cephfs.Run, cephfs.Start or
// cephfs.WithFilesystems created; they start no container. Evicting the
// sessions of one subvolume principal is
// cephfs/subvolume.Authorization.EvictClients.
package session
