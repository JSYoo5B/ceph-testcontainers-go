// Package multicluster holds helpers shared by the cross-cluster fixtures of
// the rbd, cephfs and rgw packages: ownership of the extra containers and
// network attachments they create, cluster pair validation, CLI execution and
// bounded locking. It owns no clusters.
package multicluster
