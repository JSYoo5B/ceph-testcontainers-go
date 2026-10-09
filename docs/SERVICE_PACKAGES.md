# 서비스별 공개 패키지

공개 API는 base 클러스터를 다루는 `ceph`와 RADOS client 서비스별 `cephfs`, `rgw`, `rbd` 패키지로 나뉜다. 예전에는 단일 클러스터 API(`ceph`)와 클러스터 사이 연결 API(`multicluster`)로 나눴다. 그 결과 `ceph.Run`의 option이 서비스마다 늘어났고, 같은 서비스의 기능이 두 패키지에 흩어졌다. 이제 각 서비스 패키지가 클러스터 안의 fixture와 클러스터 사이의 mirror·multisite 구성을 함께 담는다.

## 패키지와 Run

| 패키지 | 담당 | `Run`의 기본 구성 |
| --- | --- | --- |
| `ceph` | MON·MGR·OSD 클러스터, pool·CRUSH, Cephx, 설정, 장애 주입, health·PG 관측 | 서비스 없이 클러스터만 시작 |
| `cephfs` | filesystem·MDS, subvolume·snapshot·clone, pin, quiesce, 권한, CephFS mirror | filesystem `tc-cephfs`와 active MDS 1개 |
| `rgw` | gateway, S3 user·tenant·account, quota, placement·storage class, TLS, multisite·sync policy | 기본 gateway와 test user |
| `rbd` | 초기화된 RBD pool, namespace, RBD mirror, backup·restore | 초기화된 replicated pool `rbd` |

서비스 패키지의 `Run`은 `ceph.Run`과 같은 option을 받는다. 해당 서비스의 option(`cephfs.WithFilesystems`, `rgw.WithGateways`, `rbd.WithPools`)을 주면 기본값 대신 그 구성을 쓴다. 서비스 option은 모두 `ceph.Option`이므로 `ceph.Run`에 여러 서비스 option을 함께 넘겨 한 클러스터에서 RBD, CephFS, S3 client를 함께 테스트할 수 있다. 실행 중인 클러스터에는 `cephfs.Start`, `rgw.Start`, `rbd.InitPool`로 서비스를 추가한다.

서비스 `Run`은 그 서비스에 storage나 manager가 필요하므로 `ceph.WithNoInitialOSDs`처럼 이를 없애는 bootstrap option과 함께 쓰면 컨테이너를 만들기 전에 거부한다. 단계별 bootstrap은 `ceph.Run`으로 시작한 뒤 서비스를 추가한다.

`rbd.WithPools`는 RBD image용 replicated pool을 만들고 `rbd pool init`까지 수행한다. Application은 비워 두거나 `rbd`여야 하며, EC data pool은 `ceph.WithPools`로 만든 뒤 image의 data pool로 지정한다. 이 pool은 일반 초기 pool과 같은 검증(이름 중복, placement domain, bootstrap 조건)을 거친다.

## 구현 구조

구현은 `internal/cluster`(클러스터와 클러스터 안 서비스)와 `internal/multicluster`(클러스터 사이 연결)에 있다. 공개 패키지의 `api.go`는 [internal/facadegen](../internal/facadegen/main.go)이 생성한다. 타입은 구현 타입의 alias, 함수는 문서 주석을 옮긴 wrapper다. 구현의 공개 선언을 바꾼 뒤 `go run ./internal/facadegen`으로 다시 생성하지 않으면 `internal/facadegen`의 단위 테스트가 실패한다. 서비스별 `Run`과 패키지 문서는 직접 작성한 `run.go`, `doc.go`에 있다.

Type alias이므로 godoc에서 서비스 타입의 method는 구현 타입 쪽에 표시된다. [API 목록](API_CAPABILITIES.md)은 공개 이름마다 구현 선언 위치를 연결한다.

## 이전 이름 대응

`Container`의 서비스 method는 서비스 패키지 함수가 됐다.

| 이전 | 현재 |
| --- | --- |
| `cluster.StartCephFSWithConfig(ctx, config, opts...)` | `cephfs.Start(ctx, cluster, config, opts...)` |
| `cluster.StartCephFS(ctx, opts...)` | `cephfs.Start(ctx, cluster, cephfs.Config{}, opts...)` |
| `cluster.StartRGWWithConfig(ctx, config, opts...)` | `rgw.Start(ctx, cluster, config, opts...)` |
| `cluster.StartRGW(ctx)` | `rgw.Start(ctx, cluster, rgw.Config{})` |
| `cluster.RemoveRGW(ctx, name)` | `rgw.Remove(ctx, cluster, name)` |
| `cluster.Gateways()` | `rgw.Gateways(cluster)` |
| `cluster.GatewaysContext(ctx)` | `rgw.GatewaysContext(ctx, cluster)` |
| `cluster.Filesystems()` | `cephfs.Filesystems(cluster)` |
| `cluster.InitRBDPool(ctx, pool)` | `rbd.InitPool(ctx, cluster, pool)` |
| `cluster.CreateRBDNamespace(ctx, pool, name)` | `rbd.CreateNamespace(ctx, cluster, pool, name)` |
| `cluster.ListRBDNamespaces(ctx, pool)` | `rbd.ListNamespaces(ctx, cluster, pool)` |
| `cluster.RemoveRBDNamespace(ctx, namespace)` | `rbd.RemoveNamespace(ctx, cluster, namespace)` |

타입, option과 함수의 이름은 다음과 같이 바뀌었다. 여기 없는 `ceph`의 이름은 그대로다.

### cephfs

| 이전 | 현재 |
| --- | --- |
| `ceph.CephFSCloneConfig` | `cephfs.CloneConfig` |
| `ceph.CephFSConfig` | `cephfs.Config` |
| `ceph.CephFSContainer` | `cephfs.Filesystem` |
| `ceph.CephFSDataPool` | `cephfs.DataPool` |
| `ceph.CephFSDataPoolState` | `cephfs.DataPoolState` |
| `ceph.CephFSMDSStatus` | `cephfs.FilesystemStatus` |
| `ceph.CephFSPinDistributed` | `cephfs.PinDistributed` |
| `ceph.CephFSPinExport` | `cephfs.PinExport` |
| `ceph.CephFSPinOverride` | `cephfs.PinOverride` |
| `ceph.CephFSPinPolicy` | `cephfs.PinPolicy` |
| `ceph.CephFSPinRandom` | `cephfs.PinRandom` |
| `ceph.CephFSPinSetting` | `cephfs.PinSetting` |
| `ceph.CephFSPinType` | `cephfs.PinType` |
| `ceph.CephFSQuiesce` | `cephfs.Quiesce` |
| `ceph.CephFSQuiesceConfig` | `cephfs.QuiesceConfig` |
| `ceph.CephFSQuiesceState` | `cephfs.QuiesceState` |
| `ceph.CephFSSnapshotPendingClone` | `cephfs.SnapshotPendingClone` |
| `ceph.CephFSSubvolume` | `cephfs.Subvolume` |
| `ceph.CephFSSubvolumeAuthorization` | `cephfs.SubvolumeAuthorization` |
| `ceph.CephFSSubvolumeAuthorizationConfig` | `cephfs.SubvolumeAuthorizationConfig` |
| `ceph.CephFSSubvolumeAuthorizedClient` | `cephfs.SubvolumeAuthorizedClient` |
| `ceph.CephFSSubvolumeClone` | `cephfs.SubvolumeClone` |
| `ceph.CephFSSubvolumeCloneStatus` | `cephfs.SubvolumeCloneStatus` |
| `ceph.CephFSSubvolumeConfig` | `cephfs.SubvolumeConfig` |
| `ceph.CephFSSubvolumeGroup` | `cephfs.SubvolumeGroup` |
| `ceph.CephFSSubvolumeGroupConfig` | `cephfs.SubvolumeGroupConfig` |
| `ceph.CephFSSubvolumeGroupInfo` | `cephfs.SubvolumeGroupInfo` |
| `ceph.CephFSSubvolumeInfo` | `cephfs.SubvolumeInfo` |
| `ceph.CephFSSubvolumeSnapshot` | `cephfs.SubvolumeSnapshot` |
| `ceph.CephFSSubvolumeSnapshotInfo` | `cephfs.SubvolumeSnapshotInfo` |
| `ceph.Filesystems` | `cephfs.Filesystems` |
| `ceph.MDSContainer` | `cephfs.MDS` |
| `ceph.MDSStatus` | `cephfs.MDSStatus` |
| `ceph.StartCephFS` | `cephfs.Start` |
| `ceph.WithCephFS` | `cephfs.WithFilesystems` |
| `ceph.WithMDSImage` | `cephfs.WithMDSImage` |
| `multicluster.CephFSMirror` | `cephfs.Mirror` |
| `multicluster.CephFSMirrorConfig` | `cephfs.MirrorConfig` |
| `multicluster.CephFSMirrorDaemon` | `cephfs.MirrorDaemon` |
| `multicluster.CephFSMirrorDirectoryAddition` | `cephfs.MirrorDirectoryAddition` |
| `multicluster.CephFSMirrorDirectoryAdditionStatus` | `cephfs.MirrorDirectoryAdditionStatus` |
| `multicluster.CephFSMirrorDirectoryRemoval` | `cephfs.MirrorDirectoryRemoval` |
| `multicluster.CephFSMirrorDirectoryRemovalDaemonStatus` | `cephfs.MirrorDirectoryRemovalDaemonStatus` |
| `multicluster.CephFSMirrorDirectoryRemovalStatus` | `cephfs.MirrorDirectoryRemovalStatus` |
| `multicluster.CephFSMirrorDirectoryStatus` | `cephfs.MirrorDirectoryStatus` |
| `multicluster.CephFSMirrorOriginalProcessStatus` | `cephfs.MirrorOriginalProcessStatus` |
| `multicluster.CephFSMirrorPeerRemoval` | `cephfs.MirrorPeerRemoval` |
| `multicluster.CephFSMirrorPeerRemovalDaemonStatus` | `cephfs.MirrorPeerRemovalDaemonStatus` |
| `multicluster.CephFSMirrorPeerRemovalStatus` | `cephfs.MirrorPeerRemovalStatus` |
| `multicluster.CephFSMirrorProcessBindingStatus` | `cephfs.MirrorProcessBindingStatus` |
| `multicluster.CephFSMirrorProcessQuiescenceAcknowledgment` | `cephfs.MirrorProcessQuiescenceAcknowledgment` |
| `multicluster.CephFSMirrorProcessQuiescenceStatus` | `cephfs.MirrorProcessQuiescenceStatus` |
| `multicluster.CephFSMirrorSnapshot` | `cephfs.MirrorSnapshot` |
| `multicluster.RunCephFSMirror` | `cephfs.RunMirror` |

### rgw

| 이전 | 현재 |
| --- | --- |
| `ceph.Gateways` | `rgw.Gateways` |
| `ceph.GatewaysContext` | `rgw.GatewaysContext` |
| `ceph.RGWAccount` | `rgw.Account` |
| `ceph.RGWAccountConfig` | `rgw.AccountConfig` |
| `ceph.RGWAccountInfo` | `rgw.AccountInfo` |
| `ceph.RGWAdminCapability` | `rgw.AdminCapability` |
| `ceph.RGWConfig` | `rgw.Config` |
| `ceph.RGWContainer` | `rgw.Gateway` |
| `ceph.RGWPlacement` | `rgw.Placement` |
| `ceph.RGWPlacementConfig` | `rgw.PlacementConfig` |
| `ceph.RGWPlacementState` | `rgw.PlacementState` |
| `ceph.RGWQuota` | `rgw.Quota` |
| `ceph.RGWStorageClassConfig` | `rgw.StorageClassConfig` |
| `ceph.RGWTLSConfig` | `rgw.TLSConfig` |
| `ceph.RGWUser` | `rgw.User` |
| `ceph.RGWUserConfig` | `rgw.UserConfig` |
| `ceph.RGWUserInfo` | `rgw.UserInfo` |
| `ceph.RGWUserPlacementConfig` | `rgw.UserPlacementConfig` |
| `ceph.RGWUserUsage` | `rgw.UserUsage` |
| `ceph.RemoveRGW` | `rgw.Remove` |
| `ceph.StartRGW` | `rgw.Start` |
| `ceph.WithRGW` | `rgw.WithGateways` |
| `ceph.WithRGWImage` | `rgw.WithImage` |
| `multicluster.RGWBucketSyncPolicyStatus` | `rgw.BucketSyncPolicyStatus` |
| `multicluster.RGWBucketSyncStatus` | `rgw.BucketSyncStatus` |
| `multicluster.RGWDataSyncStatus` | `rgw.DataSyncStatus` |
| `multicluster.RGWMetadataSyncStatus` | `rgw.MetadataSyncStatus` |
| `multicluster.RGWMultisite` | `rgw.Multisite` |
| `multicluster.RGWMultisiteConfig` | `rgw.MultisiteConfig` |
| `multicluster.RGWSyncAllowed` | `rgw.SyncAllowed` |
| `multicluster.RGWSyncBucketIdentity` | `rgw.SyncBucketIdentity` |
| `multicluster.RGWSyncBucketSelector` | `rgw.SyncBucketSelector` |
| `multicluster.RGWSyncEnabled` | `rgw.SyncEnabled` |
| `multicluster.RGWSyncFlowConfig` | `rgw.SyncFlowConfig` |
| `multicluster.RGWSyncForbidden` | `rgw.SyncForbidden` |
| `multicluster.RGWSyncGroup` | `rgw.SyncGroup` |
| `multicluster.RGWSyncGroupConfig` | `rgw.SyncGroupConfig` |
| `multicluster.RGWSyncGroupStatus` | `rgw.SyncGroupStatus` |
| `multicluster.RGWSyncObjectTag` | `rgw.SyncObjectTag` |
| `multicluster.RGWSyncPipeConfig` | `rgw.SyncPipeConfig` |
| `multicluster.RGWSyncPolicyScope` | `rgw.SyncPolicyScope` |
| `multicluster.RGWSyncStatus` | `rgw.SyncStatus` |
| `multicluster.RGWTopologyConfig` | `rgw.TopologyConfig` |
| `multicluster.RGWZone` | `rgw.Zone` |
| `multicluster.RGWZoneConfig` | `rgw.ZoneConfig` |
| `multicluster.RGWZonegroup` | `rgw.Zonegroup` |
| `multicluster.RGWZonegroupConfig` | `rgw.ZonegroupConfig` |
| `multicluster.RunRGWMultisite` | `rgw.RunMultisite` |
| `multicluster.RunRGWTopology` | `rgw.RunTopology` |

### rbd

| 이전 | 현재 |
| --- | --- |
| `ceph.CreateRBDNamespace` | `rbd.CreateNamespace` |
| `ceph.InitRBDPool` | `rbd.InitPool` |
| `ceph.ListRBDNamespaces` | `rbd.ListNamespaces` |
| `ceph.RBDNamespace` | `rbd.Namespace` |
| `ceph.RemoveRBDNamespace` | `rbd.RemoveNamespace` |
| `ceph.WithRBDPools` | `rbd.WithPools` |
| `multicluster.ExportRBDBackup` | `rbd.ExportBackup` |
| `multicluster.ExportRBDIncremental` | `rbd.ExportIncremental` |
| `multicluster.RBDMirror` | `rbd.Mirror` |
| `multicluster.RBDMirrorConfig` | `rbd.MirrorConfig` |
| `multicluster.RBDMirrorDaemon` | `rbd.MirrorDaemon` |
| `multicluster.RBDMirrorDaemonStatus` | `rbd.MirrorDaemonStatus` |
| `multicluster.RBDMirrorImageStatus` | `rbd.MirrorImageStatus` |
| `multicluster.RBDMirrorMode` | `rbd.MirrorMode` |
| `multicluster.RBDMirrorModeJournal` | `rbd.MirrorModeJournal` |
| `multicluster.RBDMirrorModeSnapshot` | `rbd.MirrorModeSnapshot` |
| `multicluster.RBDMirrorNamespace` | `rbd.MirrorNamespace` |
| `multicluster.RBDMirrorNamespaceState` | `rbd.MirrorNamespaceState` |
| `multicluster.RBDMirrorPolicies` | `rbd.MirrorPolicies` |
| `multicluster.RBDMirrorPoolReplayerStatus` | `rbd.MirrorPoolReplayerStatus` |
| `multicluster.RBDMirrorReceiverDaemonStatus` | `rbd.MirrorReceiverDaemonStatus` |
| `multicluster.RBDMirrorReceiverStatus` | `rbd.MirrorReceiverStatus` |
| `multicluster.RBDMirrorScope` | `rbd.MirrorScope` |
| `multicluster.RBDMirrorScopeImage` | `rbd.MirrorScopeImage` |
| `multicluster.RBDMirrorScopePool` | `rbd.MirrorScopePool` |
| `multicluster.RestoreRBDBackup` | `rbd.RestoreBackup` |
| `multicluster.RestoreRBDIncremental` | `rbd.RestoreIncremental` |
| `multicluster.RunRBDMirror` | `rbd.RunMirror` |

## 실행 검증

`TestServicePackages`는 다음을 확인한다.

| subtest | 확인 |
| --- | --- |
| rbd_run_default_pool | `rbd.Run` 기본 pool `rbd`의 application이 `rbd`뿐이고 clean 뒤 librbd image write/read |
| cephfs_run_default_filesystem | `cephfs.Run` 기본 filesystem `tc-cephfs`의 MDS 준비와 libcephfs file write/read |
| rgw_run_default_gateway | `rgw.Run` 기본 gateway의 test user로 서명한 S3 bucket·object put/get |
| combined_bridge, combined_host | `ceph.Run`에 `rbd.WithPools`, `cephfs.WithFilesystems`, `rgw.WithGateways`를 함께 전달한 한 클러스터에서 RBD, CephFS, S3 I/O를 번갈아 수행하고 앞 단계 image·file을 다시 검증, 세 서비스의 pool 존재 확인 |

2026-10-10 macOS ARM64 Docker Desktop(Linux ARM64 VM, 메모리 4 GiB)에서 기본
digest 고정 Quay Ceph 20.2.4 이미지로 실행한 결과는 5개 subtest 모두 PASS, 전체
193.46초였다. 조합 클러스터는 bridge/host 모두 pool 10개로 RBD image 2개, CephFS
file 2개, S3 object 1개를 검증했다. CI에서는 `Ceph short`의 `service_packages`
batch로 실행한다.

```sh
CGO_ENABLED=0 go test -tags=integration,features -count=1 -v -timeout=60m -run '^TestServicePackages$' ./internal/integration
```
