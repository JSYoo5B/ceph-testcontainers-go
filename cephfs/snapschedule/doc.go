// Package snapschedule adds snapshot schedules, with their retention, to
// directories of an existing CephFS filesystem through the MGR snap_schedule
// module, so that a test can check how its application handles scheduled
// snapshots and their pruning.
//
// Add returns a Lease that observes the schedule and its snapshots and whose
// Restore removes what the lease added; the snapshots stay. Add acts on a
// filesystem that cephfs.Run, cephfs.Start or cephfs.WithFilesystems created;
// it starts no container and never enables the module.
package snapschedule
