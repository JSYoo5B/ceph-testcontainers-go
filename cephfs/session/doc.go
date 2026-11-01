// Package session observes the client sessions of an existing CephFS
// filesystem and overrides its MDSMap session limits, so that a test can see
// a silent or frozen client evicted within seconds.
//
// List and Timeouts read native state. OverrideTimeouts changes both limits
// and returns a handle whose Restore puts the captured values back. These
// functions act on a filesystem that cephfs.Run, cephfs.Start or
// cephfs.WithFilesystems created; they start no container. Evicting the
// sessions of one subvolume principal is
// cephfs/subvolume.Authorization.EvictClients.
package session
