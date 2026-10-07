# 기능 범위: Operation과 Check

이 모듈은 테스트할 Ceph 서버 환경을 만들고, 장애·복원 조건을 조절하고,
그 상태를 assertion으로 확인하는 fixture다. 검증 대상 애플리케이션의 객체·image·파일
I/O는 소비자 클라이언트가 수행한다. 일반 운영 자동화나 운영용 Ceph 관리 SDK를
목표로 확장하지 않는다.

**Operation**은 fixture 준비, 장애 주입, 명시적 복구·설정 복원·정리에 가치가
있다. Pool 정책·quota, Cephx caps, namespace·subvolume·RGW 사용자도 테스트의
사전 조건을 만드는 기능이다. **Check**는 Docker·native CLI·admin socket으로
상태를 읽어 Go Ceph SDK/cgo 없이 assertion 근거를 제공한다. testcontainers-go와
Docker Go client는 기본 Go 의존성으로 사용한다. 이미지 안의 native CLI·library
요구사항은 유지한다. 현재 상태 질의와
보존된 설정·소유 descriptor·접속 정보 조회를 구분한다.

| 기능 축 | Operation: fixture 조건 준비·장애·복원 | Check: 확인하는 범위 |
| --- | --- | --- |
| 클러스터·MON·MGR·OSD | 초기 구성, 노드 증감·교체, cold bootstrap, owned cleanup | quorum, manager/module 준비, OSD 상태·flag, PG clean |
| Pool·placement·Cephx | pool·replica·quota·CRUSH 조건, 제한된 client caps, 임시 설정 복원 | native pool ID·정책·quota, caps, blocklist·설정 조회 |
| CephFS MDS | FS/pool 구성, active·standby/replay 조절, cold 첫 기동, stopped/last MDS 교체 | FSMap·논리적 rank/GID·owned 상태, 요청한 MDS capacity 준비 |
| CephFS client 조건 | 추가 data pool·layout, subvolume/group·snapshot·clone, pin·quiesce·권한 | native 목록·info·clone 상태·pin/quiesce 상태·authorized clients |
| RGW client 조건 | gateway, user/account/caps·quota, placement/storage class, native TLS 설정 | endpoint와 native identity·placement·quota/caps 설정 |
| RBD mirror | pool/namespace/peer 구성, daemon 증감·재기동·link 장애·재bootstrap | policy, daemon socket, receiver discovery/election, image replay attribution |
| CephFS mirror | peer/path 구성, typed intent와 제거, 명시적 interrupted-removal 승인 | directory 배정, exact snapshot 관측, cycle/replayer 해제, original process 종료 관측 |
| RGW multisite | zone/zonegroup, period와 sync 정책·flow·pipe, link 장애·복원 | metadata/data sync, bucket marker catch-up, 정책 준비 |
| 진단·연결 | client/container/image 연결과 raw CLI 제어 접점 | bounded native 상태·Docker logs·partial diagnostics, 보존된 소유 목록 |
| RBD archive | 복구 테스트의 export/import·incremental baseline 준비에 조건부 유용 | archive와 복원 payload를 소비자 assertion으로 확인; 운영 backup scheduler는 목표 밖 |

Status는 관측한 범위, Wait는 해당 API의 특정 predicate와 context를 나타낸다.
Ready가 전체 HEALTH_OK, 특정 bytes·snapshot 전달, process 전체 종료를 뜻하지
않는다. 오류와 partial 결과를 함께 보고, 과거에 보존한 값은 새로운 native
proof로 사용하지 않는다. 각 original identity·generation·ownership guard의
범위는 해당 API 계약을 따른다. MDS의 native 이름/GID와 Docker CID는 별도
관측이며 논리적 Owned 표지만으로 CID→GID process binding을 주장하지 않는다.

아래 Check 목록은 구현된 공개 관측 기능이다. Ceph가 제공하는 모든 Check를
이미 구현했다는 뜻은 아니다. 보존된 descriptor/접속 accessor도 유용하지만
현재 daemon 준비나 실제 I/O의 증거로 대신하지 않는다. Endpoint 조회는 경로에
따라 Docker inspect/port resolution을 사용할 수 있으며 native health 검사는 아니다.

현재 typed pool/user/account 조회는 policy·quota·caps/identity 범위다. 실제
사용량 통계를 반환하는 typed Check로 확대해 읽지 않는다. Raw CLI로 추가 상태를
질의할 수 있으며, 향후 Check gap은 typed 사용량·상태 query 관점에서 정리할 수
있다. 현재 목록의 분류와 아직 구현하지 않은 Check 확장은 구분한다.

Raw CLI 접점은 argv에 따라 조회와 변경 모두 가능하다. 이미지·client·customizer
옵션 및 안전한 문자열 표현도 공개 surface에 포함되지만 native Check로 세지
않는다. Backup/export/restore는 recovery fixture에서 조건부 가치가 있으므로
자동 삭제하지 않는다. Caller 소유 stream의 blocking I/O, partial archive와
baseline snapshot, 복원 bytes 검증은 별도 책임이다.

## 현재 공개 callable 수

| 분류 | ceph | multicluster | 합계 |
| --- | ---: | ---: | ---: |
| Fixture Operation | 89 | 46 | 135 |
| Check: native 질의/Wait 또는 보존 정보 조회 | 71 | 33 | 104 |
| 연결·raw CLI·customizer 접점 | 10 | 5 | 15 |
| 조건부 archive helper | 0 | 4 | 4 |
| 로컬 문자열 표현 | 14 | 2 | 16 |
| 전체 | 184 | 90 | **274** |

집계는 패키지 자체의 공개 함수와 공개 receiver의 공개 method다. Test/Example,
private receiver의 exported-name method, dependency가 승격하는 container method,
타입·상수·구조체 field는 이 274개에 포함하지 않는다. 아래 목록에서 각 callable을
한 번씩 나열하고 source에 연결한다. Config/result 타입과 option 계약은 따로 읽는다.

실행 검증은 [fixture 범위와 native 기록](CLUSTER_SCENARIOS.md),
[기능 coverage](CLIENT_FIXTURE_COVERAGE.md),
[CI 선택 범위](CI_FIXTURES.md)와
각 focused 계약 문서를 따른다. 선언·호출 이름의 lexical reference는 새 PASS나
모든 method의 native coverage 증거가 아니다. 이 inventory 감사는 Go/Docker를
실행하지 않았고, 과거 source의 결과를 현재 전체 surface 인증으로 합산하지 않는다.

## 전체 callable 목록

<!-- callables:begin -->
### Fixture Operation (135개)

| source | 공개 callable |
| --- | --- |
| [ceph/auth.go](../ceph/auth.go) | [Container.CreateClient](../ceph/auth.go#L101) · [Container.DeleteClient](../ceph/auth.go#L219) |
| [ceph/auth_policy.go](../ceph/auth_policy.go) | [Container.UpdateClientCaps](../ceph/auth_policy.go#L25) |
| [ceph/ceph.go](../ceph/ceph.go) | [Run](../ceph/ceph.go#L85) · [Container.AddOSD](../ceph/ceph.go#L408) · [Container.AddOSDWithConfig](../ceph/ceph.go#L415) · [Container.RemoveOSD](../ceph/ceph.go#L523)<br>[Container.Terminate](../ceph/ceph.go#L741) |
| [ceph/cephfs.go](../ceph/cephfs.go) | [Container.StartCephFS](../ceph/cephfs.go#L92) · [Container.StartCephFSWithConfig](../ceph/cephfs.go#L105) · [CephFSContainer.ScaleMDS](../ceph/cephfs.go#L247) |
| [ceph/cephfs_authorization.go](../ceph/cephfs_authorization.go) | [CephFSContainer.AuthorizeSubvolume](../ceph/cephfs_authorization.go#L67) · [CephFSContainer.DeauthorizeSubvolume](../ceph/cephfs_authorization.go#L396) · [CephFSContainer.EvictSubvolumeClients](../ceph/cephfs_authorization.go#L462) |
| [ceph/cephfs_clone_lifecycle.go](../ceph/cephfs_clone_lifecycle.go) | [CephFSContainer.CancelSubvolumeClone](../ceph/cephfs_clone_lifecycle.go#L116) · [CephFSContainer.RemovePartialSubvolumeClone](../ceph/cephfs_clone_lifecycle.go#L187) |
| [ceph/cephfs_data_pools.go](../ceph/cephfs_data_pools.go) | [CephFSContainer.AddDataPool](../ceph/cephfs_data_pools.go#L229) · [CephFSContainer.RemoveUnusedDataPool](../ceph/cephfs_data_pools.go#L362) |
| [ceph/cephfs_mds_replacement.go](../ceph/cephfs_mds_replacement.go) | [CephFSContainer.AddMDSReplacement](../ceph/cephfs_mds_replacement.go#L40) |
| [ceph/cephfs_pin.go](../ceph/cephfs_pin.go) | [CephFSContainer.TemporarySubvolumePin](../ceph/cephfs_pin.go#L96) · [CephFSContainer.TemporarySubvolumeGroupPin](../ceph/cephfs_pin.go#L104) · [CephFSPinOverride.Restore](../ceph/cephfs_pin.go#L182) |
| [ceph/cephfs_quiesce.go](../ceph/cephfs_quiesce.go) | [CephFSContainer.QuiesceSubvolumes](../ceph/cephfs_quiesce.go#L64) · [CephFSQuiesce.Release](../ceph/cephfs_quiesce.go#L159) |
| [ceph/cephfs_snapshot.go](../ceph/cephfs_snapshot.go) | [CephFSContainer.CreateSubvolumeSnapshot](../ceph/cephfs_snapshot.go#L131) · [CephFSContainer.RemoveSubvolumeSnapshot](../ceph/cephfs_snapshot.go#L270) · [CephFSContainer.CloneSubvolumeSnapshot](../ceph/cephfs_snapshot.go#L320) |
| [ceph/cephfs_stopped_mds.go](../ceph/cephfs_stopped_mds.go) | [CephFSContainer.RemoveStoppedMDS](../ceph/cephfs_stopped_mds.go#L81) |
| [ceph/cephfs_subvolume.go](../ceph/cephfs_subvolume.go) | [CephFSContainer.CreateSubvolumeGroup](../ceph/cephfs_subvolume.go#L282) · [CephFSContainer.CreateSubvolume](../ceph/cephfs_subvolume.go#L323) · [CephFSContainer.ResizeSubvolumeGroup](../ceph/cephfs_subvolume.go#L525) · [CephFSContainer.ResizeSubvolume](../ceph/cephfs_subvolume.go#L557)<br>[CephFSContainer.RemoveSubvolumeGroup](../ceph/cephfs_subvolume.go#L591) · [CephFSContainer.RemoveSubvolume](../ceph/cephfs_subvolume.go#L639) |
| [ceph/client_monitor_config.go](../ceph/client_monitor_config.go) | [Container.RefreshClientMonitorConfig](../ceph/client_monitor_config.go#L44) |
| [ceph/composition.go](../ceph/composition.go) | [WithPools](../ceph/composition.go#L12) · [WithCephFS](../ceph/composition.go#L26) · [WithRGW](../ceph/composition.go#L43) |
| [ceph/config.go](../ceph/config.go) | [Container.TemporaryConfig](../ceph/config.go#L73) · [ConfigOverride.Restore](../ceph/config.go#L127) |
| [ceph/fencing.go](../ceph/fencing.go) | [Container.TemporaryBlocklist](../ceph/fencing.go#L64) · [BlocklistOverride.Restore](../ceph/fencing.go#L115) |
| [ceph/mgr_modules.go](../ceph/mgr_modules.go) | [Container.TemporaryMGRModule](../ceph/mgr_modules.go#L60) · [MGRModuleOverride.Restore](../ceph/mgr_modules.go#L121) |
| [ceph/network.go](../ceph/network.go) | [Container.InterruptNetwork](../ceph/network.go#L96) · [InterruptNetwork](../ceph/network.go#L142) · [NetworkInterruption.Restore](../ceph/network.go#L199) |
| [ceph/options.go](../ceph/options.go) | [WithMonitorCount](../ceph/options.go#L42) · [WithManagerCount](../ceph/options.go#L53) · [WithNoInitialManagers](../ceph/options.go#L70) · [WithHostNetwork](../ceph/options.go#L90)<br>[WithSeparateClusterNetwork](../ceph/options.go#L100) · [WithNoInitialOSDs](../ceph/options.go#L165) · [WithOSDCount](../ceph/options.go#L173) · [WithInitialOSDs](../ceph/options.go#L187)<br>[WithDefaultCRUSHRoot](../ceph/options.go#L217) · [WithPoolDefaults](../ceph/options.go#L230) · [WithOSDBlockSize](../ceph/options.go#L242) |
| [ceph/osd_policy.go](../ceph/osd_policy.go) | [Container.SetOSDIn](../ceph/osd_policy.go#L57) · [Container.TemporaryOSDFlag](../ceph/osd_policy.go#L132) · [OSDFlagOverride.Restore](../ceph/osd_policy.go#L175) |
| [ceph/pool.go](../ceph/pool.go) | [Container.CreatePool](../ceph/pool.go#L62) |
| [ceph/pool_policy.go](../ceph/pool_policy.go) | [Container.SetPoolQuota](../ceph/pool_policy.go#L64) · [Container.SetPoolReplication](../ceph/pool_policy.go#L104) |
| [ceph/rbd.go](../ceph/rbd.go) | [Container.InitRBDPool](../ceph/rbd.go#L41) · [Container.CreateRBDNamespace](../ceph/rbd.go#L71) · [Container.RemoveRBDNamespace](../ceph/rbd.go#L131) |
| [ceph/rgw.go](../ceph/rgw.go) | [Container.StartRGW](../ceph/rgw.go#L53) · [Container.RemoveRGW](../ceph/rgw.go#L61) · [Container.StartRGWWithConfig](../ceph/rgw.go#L89) |
| [ceph/rgw_admin.go](../ceph/rgw_admin.go) | [RGWContainer.CreateUser](../ceph/rgw_admin.go#L258) · [RGWContainer.SetUserQuota](../ceph/rgw_admin.go#L469) · [RGWContainer.SetBucketQuota](../ceph/rgw_admin.go#L475) · [RGWContainer.SuspendUser](../ceph/rgw_admin.go#L526)<br>[RGWContainer.RemoveUser](../ceph/rgw_admin.go#L555) |
| [ceph/rgw_placement.go](../ceph/rgw_placement.go) | [RGWContainer.CreatePlacement](../ceph/rgw_placement.go#L139) · [RGWContainer.ApplyPlacement](../ceph/rgw_placement.go#L279) · [RGWContainer.ReloadPlacement](../ceph/rgw_placement.go#L377) |
| [ceph/rgw_tenants_accounts.go](../ceph/rgw_tenants_accounts.go) | [RGWContainer.CreateAccount](../ceph/rgw_tenants_accounts.go#L134) · [RGWContainer.CreateAccountRootUser](../ceph/rgw_tenants_accounts.go#L318) · [RGWContainer.SetAccountQuota](../ceph/rgw_tenants_accounts.go#L328) · [RGWContainer.SetAccountBucketQuota](../ceph/rgw_tenants_accounts.go#L334)<br>[RGWContainer.RemoveAccount](../ceph/rgw_tenants_accounts.go#L387) |
| [ceph/rgw_user_placement.go](../ceph/rgw_user_placement.go) | [RGWContainer.SetUserPlacement](../ceph/rgw_user_placement.go#L67) |
| [ceph/topology.go](../ceph/topology.go) | [Container.AddMonitor](../ceph/topology.go#L147) · [Container.RemoveMonitor](../ceph/topology.go#L261) · [Container.RefreshMonitorConfig](../ceph/topology.go#L338) · [Container.AddManager](../ceph/topology.go#L674)<br>[Container.RemoveManager](../ceph/topology.go#L755) |
| [multicluster/cephfs.go](../multicluster/cephfs.go) | [CephFSMirrorDaemon.Terminate](../multicluster/cephfs.go#L60) · [RunCephFSMirror](../multicluster/cephfs.go#L133) · [CephFSMirror.AddDaemon](../multicluster/cephfs.go#L494) · [CephFSMirror.RemoveDaemon](../multicluster/cephfs.go#L567)<br>[CephFSMirror.Terminate](../multicluster/cephfs.go#L606) · [CephFSMirror.AttachManagers](../multicluster/cephfs.go#L629) · [CephFSMirror.AddDirectory](../multicluster/cephfs.go#L829) · [CephFSMirror.RemoveDirectory](../multicluster/cephfs.go#L865)<br>[CephFSMirror.RebalanceDirectories](../multicluster/cephfs.go#L916) · [CephFSMirror.RemovePeer](../multicluster/cephfs.go#L1081) · [CephFSMirror.RebootstrapPeer](../multicluster/cephfs.go#L1123) |
| [multicluster/cephfs_connection_refresh.go](../multicluster/cephfs_connection_refresh.go) | [CephFSMirror.RefreshMonitorConfig](../multicluster/cephfs_connection_refresh.go#L24) · [CephFSMirror.RefreshPeerMonitorConfig](../multicluster/cephfs_connection_refresh.go#L87) |
| [multicluster/cephfs_directory_addition.go](../multicluster/cephfs_directory_addition.go) | [CephFSMirror.BeginDirectoryAddition](../multicluster/cephfs_directory_addition.go#L43) |
| [multicluster/cephfs_directory_removal.go](../multicluster/cephfs_directory_removal.go) | [CephFSMirror.BeginDirectoryRemoval](../multicluster/cephfs_directory_removal.go#L50) |
| [multicluster/cephfs_peer_removal.go](../multicluster/cephfs_peer_removal.go) | [CephFSMirror.BeginPeerRemoval](../multicluster/cephfs_peer_removal.go#L76) |
| [multicluster/cephfs_process_quiescence_acknowledgment.go](../multicluster/cephfs_process_quiescence_acknowledgment.go) | [CephFSMirrorPeerRemoval.AcknowledgeProcessQuiescence](../multicluster/cephfs_process_quiescence_acknowledgment.go#L22) · [CephFSMirrorDirectoryRemoval.AcknowledgeProcessQuiescence](../multicluster/cephfs_process_quiescence_acknowledgment.go#L38) |
| [multicluster/network.go](../multicluster/network.go) | [RBDMirror.InterruptPeerLink](../multicluster/network.go#L24) · [CephFSMirror.InterruptPeerLink](../multicluster/network.go#L47) · [RGWMultisite.InterruptZoneLink](../multicluster/network.go#L70) |
| [multicluster/rbd.go](../multicluster/rbd.go) | [RBDMirrorDaemon.Terminate](../multicluster/rbd.go#L142) · [RunRBDMirror](../multicluster/rbd.go#L191) · [RBDMirror.AddDaemon](../multicluster/rbd.go#L265) · [RBDMirror.RemoveDaemon](../multicluster/rbd.go#L329)<br>[RBDMirror.Rebootstrap](../multicluster/rbd.go#L374) · [RBDMirror.EnableImage](../multicluster/rbd.go#L596) · [RBDMirror.Terminate](../multicluster/rbd.go#L658) |
| [multicluster/rbd_connection_refresh.go](../multicluster/rbd_connection_refresh.go) | [RBDMirror.RefreshMonitorConfig](../multicluster/rbd_connection_refresh.go#L27) |
| [multicluster/rgw.go](../multicluster/rgw.go) | [RunRGWMultisite](../multicluster/rgw.go#L62) · [RGWMultisite.PullSourcePeriod](../multicluster/rgw.go#L482) · [RGWMultisite.PullDestinationPeriod](../multicluster/rgw.go#L494) · [RGWMultisite.Terminate](../multicluster/rgw.go#L504) |
| [multicluster/rgw_sync_period.go](../multicluster/rgw_sync_period.go) | [RGWMultisite.ApplySyncGroup](../multicluster/rgw_sync_period.go#L20) |
| [multicluster/rgw_sync_policy.go](../multicluster/rgw_sync_policy.go) | [RGWMultisite.CreateSyncGroup](../multicluster/rgw_sync_policy.go#L125) · [RGWMultisite.CreateSyncFlow](../multicluster/rgw_sync_policy.go#L183) · [RGWMultisite.RemoveSyncFlow](../multicluster/rgw_sync_policy.go#L204) · [RGWMultisite.CreateSyncPipe](../multicluster/rgw_sync_policy.go#L229)<br>[RGWMultisite.SetSyncPipePrefix](../multicluster/rgw_sync_policy.go#L265) · [RGWMultisite.RemoveSyncPipe](../multicluster/rgw_sync_policy.go#L298) · [RGWMultisite.SetSyncGroupStatus](../multicluster/rgw_sync_policy.go#L317) · [RGWMultisite.RemoveSyncGroup](../multicluster/rgw_sync_policy.go#L336) |
| [multicluster/rgw_topology.go](../multicluster/rgw_topology.go) | [RunRGWTopology](../multicluster/rgw_topology.go#L80) · [RGWMultisite.AddZone](../multicluster/rgw_topology.go#L304) |
| [multicluster/rgw_zonegroups.go](../multicluster/rgw_zonegroups.go) | [RGWMultisite.AddZonegroup](../multicluster/rgw_zonegroups.go#L46) · [RGWMultisite.RemoveZone](../multicluster/rgw_zonegroups.go#L116) |

### Check: 현재 상태 질의·policy/process 관측·Wait (62개)

| source | 공개 callable |
| --- | --- |
| [ceph/auth_policy.go](../ceph/auth_policy.go) | [Container.ClientCapabilities](../ceph/auth_policy.go#L12) |
| [ceph/cephfs.go](../ceph/cephfs.go) | [CephFSContainer.MDSStatus](../ceph/cephfs.go#L629) · [CephFSContainer.WaitReady](../ceph/cephfs.go#L649) |
| [ceph/cephfs_authorization.go](../ceph/cephfs_authorization.go) | [CephFSContainer.SubvolumeAuthorizedClients](../ceph/cephfs_authorization.go#L196) |
| [ceph/cephfs_data_pools.go](../ceph/cephfs_data_pools.go) | [CephFSContainer.DataPools](../ceph/cephfs_data_pools.go#L184) |
| [ceph/cephfs_pin.go](../ceph/cephfs_pin.go) | [CephFSContainer.SubvolumePinPolicy](../ceph/cephfs_pin.go#L78) · [CephFSContainer.SubvolumeGroupPinPolicy](../ceph/cephfs_pin.go#L86) |
| [ceph/cephfs_quiesce.go](../ceph/cephfs_quiesce.go) | [CephFSQuiesce.Status](../ceph/cephfs_quiesce.go#L139) |
| [ceph/cephfs_snapshot.go](../ceph/cephfs_snapshot.go) | [CephFSContainer.SubvolumeSnapshots](../ceph/cephfs_snapshot.go#L109) · [CephFSContainer.SubvolumeSnapshotInfo](../ceph/cephfs_snapshot.go#L173) · [CephFSContainer.SubvolumeCloneStatus](../ceph/cephfs_snapshot.go#L462) · [CephFSContainer.WaitForSubvolumeClone](../ceph/cephfs_snapshot.go#L481) |
| [ceph/cephfs_subvolume.go](../ceph/cephfs_subvolume.go) | [CephFSContainer.SubvolumeGroups](../ceph/cephfs_subvolume.go#L367) · [CephFSContainer.Subvolumes](../ceph/cephfs_subvolume.go#L378) · [CephFSContainer.SubvolumeGroupInfo](../ceph/cephfs_subvolume.go#L391) · [CephFSContainer.SubvolumeInfo](../ceph/cephfs_subvolume.go#L406) |
| [ceph/config.go](../ceph/config.go) | [Container.Configuration](../ceph/config.go#L59) |
| [ceph/diagnostics.go](../ceph/diagnostics.go) | [Container.CollectDiagnostics](../ceph/diagnostics.go#L85) |
| [ceph/fencing.go](../ceph/fencing.go) | [Container.BlocklistEntries](../ceph/fencing.go#L45) |
| [ceph/mgr_modules.go](../ceph/mgr_modules.go) | [Container.MGRModules](../ceph/mgr_modules.go#L29) · [Container.WaitMGRModuleReady](../ceph/mgr_modules.go#L187) |
| [ceph/osd_policy.go](../ceph/osd_policy.go) | [Container.OSDStates](../ceph/osd_policy.go#L29) · [Container.OSDFlags](../ceph/osd_policy.go#L99) · [Container.WaitForPGClean](../ceph/osd_policy.go#L307) |
| [ceph/pool_policy.go](../ceph/pool_policy.go) | [Container.Pools](../ceph/pool_policy.go#L38) · [Container.PoolStatus](../ceph/pool_policy.go#L48) |
| [ceph/rbd.go](../ceph/rbd.go) | [Container.ListRBDNamespaces](../ceph/rbd.go#L107) |
| [ceph/rgw_admin.go](../ceph/rgw_admin.go) | [RGWContainer.UserInfo](../ceph/rgw_admin.go#L457) |
| [ceph/rgw_placement.go](../ceph/rgw_placement.go) | [RGWContainer.PlacementStatus](../ceph/rgw_placement.go#L250) |
| [ceph/rgw_tenants_accounts.go](../ceph/rgw_tenants_accounts.go) | [RGWContainer.AccountInfo](../ceph/rgw_tenants_accounts.go#L299) |
| [ceph/status.go](../ceph/status.go) | [Container.Status](../ceph/status.go#L34) · [Container.WaitForClean](../ceph/status.go#L48) |
| [ceph/topology.go](../ceph/topology.go) | [Container.QuorumStatus](../ceph/topology.go#L125) · [Container.WaitForQuorum](../ceph/topology.go#L136) · [Container.ManagerStatus](../ceph/topology.go#L857) |
| [multicluster/cephfs.go](../multicluster/cephfs.go) | [CephFSMirror.PeerIDs](../multicluster/cephfs.go#L1033) |
| [multicluster/cephfs_directory_addition.go](../multicluster/cephfs_directory_addition.go) | [CephFSMirrorDirectoryAddition.Status](../multicluster/cephfs_directory_addition.go#L289) |
| [multicluster/cephfs_directory_removal.go](../multicluster/cephfs_directory_removal.go) | [CephFSMirrorDirectoryRemoval.Status](../multicluster/cephfs_directory_removal.go#L374) · [CephFSMirrorDirectoryRemoval.WaitReleased](../multicluster/cephfs_directory_removal.go#L480) |
| [multicluster/cephfs_directory_status.go](../multicluster/cephfs_directory_status.go) | [CephFSMirror.DirectoryStatus](../multicluster/cephfs_directory_status.go#L49) · [CephFSMirror.WaitDirectoryReady](../multicluster/cephfs_directory_status.go#L203) · [CephFSMirror.WaitSnapshotSynced](../multicluster/cephfs_directory_status.go#L218) |
| [multicluster/cephfs_peer_removal.go](../multicluster/cephfs_peer_removal.go) | [CephFSMirrorPeerRemoval.Status](../multicluster/cephfs_peer_removal.go#L238) · [CephFSMirrorPeerRemoval.WaitDrained](../multicluster/cephfs_peer_removal.go#L331) |
| [multicluster/cephfs_process_quiescence.go](../multicluster/cephfs_process_quiescence.go) | [CephFSMirrorPeerRemoval.ProcessQuiescence](../multicluster/cephfs_process_quiescence.go#L286) · [CephFSMirrorDirectoryRemoval.ProcessQuiescence](../multicluster/cephfs_process_quiescence.go#L298) |
| [multicluster/rbd.go](../multicluster/rbd.go) | [RBDMirrorDaemon.Status](../multicluster/rbd.go#L121) |
| [multicluster/rbd_image_status.go](../multicluster/rbd_image_status.go) | [RBDMirror.ImageStatus](../multicluster/rbd_image_status.go#L38) · [RBDMirror.WaitReplayReady](../multicluster/rbd_image_status.go#L228) |
| [multicluster/rbd_namespace.go](../multicluster/rbd_namespace.go) | [RBDMirror.PolicyStatus](../multicluster/rbd_namespace.go#L43) |
| [multicluster/rbd_namespace_binding.go](../multicluster/rbd_namespace_binding.go) | [RBDMirror.BindNamespace](../multicluster/rbd_namespace_binding.go#L61) · [RBDMirrorNamespace.ReceiverStatus](../multicluster/rbd_namespace_binding.go#L100) · [RBDMirrorNamespace.WaitReceiverReady](../multicluster/rbd_namespace_binding.go#L113) |
| [multicluster/rbd_namespace_image_status.go](../multicluster/rbd_namespace_image_status.go) | [RBDMirrorNamespace.ImageStatus](../multicluster/rbd_namespace_image_status.go#L18) · [RBDMirrorNamespace.WaitReplayReady](../multicluster/rbd_namespace_image_status.go#L30) |
| [multicluster/rbd_receiver_status.go](../multicluster/rbd_receiver_status.go) | [RBDMirror.ReceiverStatus](../multicluster/rbd_receiver_status.go#L66) · [RBDMirror.WaitReceiverReady](../multicluster/rbd_receiver_status.go#L78) |
| [multicluster/rgw_sync_bucket_status.go](../multicluster/rgw_sync_bucket_status.go) | [RGWMultisite.BucketSyncStatus](../multicluster/rgw_sync_bucket_status.go#L45) · [RGWMultisite.WaitBucketSyncReady](../multicluster/rgw_sync_bucket_status.go#L66) |
| [multicluster/rgw_sync_policy_ready.go](../multicluster/rgw_sync_policy_ready.go) | [RGWMultisite.WaitBucketSyncPolicyReady](../multicluster/rgw_sync_policy_ready.go#L38) |
| [multicluster/rgw_sync_status.go](../multicluster/rgw_sync_status.go) | [RGWMultisite.SyncStatus](../multicluster/rgw_sync_status.go#L48) · [RGWMultisite.WaitSyncReady](../multicluster/rgw_sync_status.go#L66) |

### Check: 보존된 정보·소유 목록·접속 정보 (42개)

| source | 공개 callable |
| --- | --- |
| [ceph/auth.go](../ceph/auth.go) | [ClientConfig.Name](../ceph/auth.go#L45) · [ClientConfig.User](../ceph/auth.go#L49) · [ClientConfig.KeyringPath](../ceph/auth.go#L52) · [ClientConfig.ConnectionConfig](../ceph/auth.go#L64) |
| [ceph/ceph.go](../ceph/ceph.go) | [Container.ControlImage](../ceph/ceph.go#L233) · [Container.NetworkName](../ceph/ceph.go#L315) · [Container.UsesHostNetwork](../ceph/ceph.go#L326) · [Container.PublicAddress](../ceph/ceph.go#L329)<br>[Container.ConnectionConfig](../ceph/ceph.go#L338) · [Container.ManagerContainer](../ceph/ceph.go#L365) · [Container.OSDs](../ceph/ceph.go#L682) · [Container.ServiceContainers](../ceph/ceph.go#L695) |
| [ceph/cephfs.go](../ceph/cephfs.go) | [CephFSContainer.MDSs](../ceph/cephfs.go#L211) |
| [ceph/cephfs_quiesce.go](../ceph/cephfs_quiesce.go) | [CephFSQuiesce.ID](../ceph/cephfs_quiesce.go#L51) |
| [ceph/client_monitor_config.go](../ceph/client_monitor_config.go) | [Container.MonitorBootstrapAddresses](../ceph/client_monitor_config.go#L20) |
| [ceph/composition.go](../ceph/composition.go) | [Container.Gateways](../ceph/composition.go#L55) · [Container.Filesystems](../ceph/composition.go#L68) |
| [ceph/fencing.go](../ceph/fencing.go) | [BlocklistOverride.Address](../ceph/fencing.go#L42) |
| [ceph/network.go](../ceph/network.go) | [Container.ClusterNetworkName](../ceph/network.go#L26) · [Container.HasSeparateClusterNetwork](../ceph/network.go#L34) |
| [ceph/placement.go](../ceph/placement.go) | [OSDContainer.Placement](../ceph/placement.go#L24) |
| [ceph/rbd.go](../ceph/rbd.go) | [RBDNamespace.Name](../ceph/rbd.go#L31) · [RBDNamespace.PoolName](../ceph/rbd.go#L34) |
| [ceph/rgw.go](../ceph/rgw.go) | [RGWContainer.DaemonEndpoint](../ceph/rgw.go#L366) · [RGWContainer.S3Endpoint](../ceph/rgw.go#L402) |
| [ceph/rgw_admin.go](../ceph/rgw_admin.go) | [RGWUser.ID](../ceph/rgw_admin.go#L67) · [RGWUser.Credentials](../ceph/rgw_admin.go#L76) |
| [ceph/rgw_tenants_accounts.go](../ceph/rgw_tenants_accounts.go) | [RGWAccount.ID](../ceph/rgw_tenants_accounts.go#L68) |
| [ceph/rgw_tls.go](../ceph/rgw_tls.go) | [RGWContainer.S3SecureEndpoint](../ceph/rgw_tls.go#L57) |
| [ceph/topology.go](../ceph/topology.go) | [Container.ControlContainer](../ceph/topology.go#L59) · [Container.Monitors](../ceph/topology.go#L82) · [Container.Managers](../ceph/topology.go#L97) |
| [ceph/topology_snapshot_context.go](../ceph/topology_snapshot_context.go) | [Container.ConnectionConfigContext](../ceph/topology_snapshot_context.go#L15) · [Container.ManagersContext](../ceph/topology_snapshot_context.go#L38) · [Container.GatewaysContext](../ceph/topology_snapshot_context.go#L56) · [Container.ControlContainerContext](../ceph/topology_snapshot_context.go#L75) |
| [multicluster/cephfs.go](../multicluster/cephfs.go) | [CephFSMirror.Daemons](../multicluster/cephfs.go#L473) |
| [multicluster/cephfs_process_quiescence.go](../multicluster/cephfs_process_quiescence.go) | [CephFSMirrorDaemon.ProcessObserverBindingStatus](../multicluster/cephfs_process_quiescence.go#L45) |
| [multicluster/rbd.go](../multicluster/rbd.go) | [RBDMirror.Daemons](../multicluster/rbd.go#L250) |
| [multicluster/rgw_sync_policy.go](../multicluster/rgw_sync_policy.go) | [RGWSyncGroup.ID](../multicluster/rgw_sync_policy.go#L101) |
| [multicluster/rgw_topology.go](../multicluster/rgw_topology.go) | [RGWMultisite.Zones](../multicluster/rgw_topology.go#L241) |
| [multicluster/rgw_zonegroups.go](../multicluster/rgw_zonegroups.go) | [RGWMultisite.Zonegroups](../multicluster/rgw_zonegroups.go#L17) |

### 연결·raw CLI·customizer 접점 (15개)

| source | 공개 callable |
| --- | --- |
| [ceph/auth.go](../ceph/auth.go) | [Container.WithClientIdentity](../ceph/auth.go#L191) |
| [ceph/ceph.go](../ceph/ceph.go) | [Container.WithClient](../ceph/ceph.go#L374) · [Container.Ceph](../ceph/ceph.go#L397) |
| [ceph/options.go](../ceph/options.go) | [Option.Customize](../ceph/options.go#L83) · [WithHostAddress](../ceph/options.go#L111) · [WithOSDImage](../ceph/options.go#L125) · [WithRGWImage](../ceph/options.go#L137)<br>[WithMDSImage](../ceph/options.go#L149) · [WithStartupTimeout](../ceph/options.go#L254) |
| [ceph/rgw_admin.go](../ceph/rgw_admin.go) | [RGWContainer.Admin](../ceph/rgw_admin.go#L122) |
| [multicluster/rbd.go](../multicluster/rbd.go) | [RBDMirror.SourceRBD](../multicluster/rbd.go#L571) · [RBDMirror.DestinationRBD](../multicluster/rbd.go#L580) |
| [multicluster/rgw.go](../multicluster/rgw.go) | [RGWMultisite.SourceAdmin](../multicluster/rgw.go#L465) · [RGWMultisite.DestinationAdmin](../multicluster/rgw.go#L471) |
| [multicluster/rgw_topology.go](../multicluster/rgw_topology.go) | [RGWMultisite.ZoneAdmin](../multicluster/rgw_topology.go#L271) |

### 조건부 archive helper (4개)

| source | 공개 callable |
| --- | --- |
| [multicluster/rbd_backup.go](../multicluster/rbd_backup.go) | [ExportRBDBackup](../multicluster/rbd_backup.go#L19) · [ExportRBDIncremental](../multicluster/rbd_backup.go#L25) · [RestoreRBDBackup](../multicluster/rbd_backup.go#L34) · [RestoreRBDIncremental](../multicluster/rbd_backup.go#L40) |

### 로컬 문자열 표현 (16개)

| source | 공개 callable |
| --- | --- |
| [ceph/auth.go](../ceph/auth.go) | [ClientConfig.String](../ceph/auth.go#L55) · [ClientConfig.GoString](../ceph/auth.go#L58) |
| [ceph/config.go](../ceph/config.go) | [ConfigOverride.String](../ceph/config.go#L51) · [ConfigOverride.GoString](../ceph/config.go#L54) |
| [ceph/rgw_admin.go](../ceph/rgw_admin.go) | [RGWUser.String](../ceph/rgw_admin.go#L83) · [RGWUser.GoString](../ceph/rgw_admin.go#L84) |
| [ceph/rgw_placement.go](../ceph/rgw_placement.go) | [RGWPlacement.String](../ceph/rgw_placement.go#L61) · [RGWPlacement.GoString](../ceph/rgw_placement.go#L62) |
| [ceph/rgw_tenants_accounts.go](../ceph/rgw_tenants_accounts.go) | [RGWAccount.String](../ceph/rgw_tenants_accounts.go#L75) · [RGWAccount.GoString](../ceph/rgw_tenants_accounts.go#L76) |
| [ceph/rgw_tls.go](../ceph/rgw_tls.go) | [RGWTLSConfig.String](../ceph/rgw_tls.go#L28) · [RGWTLSConfig.GoString](../ceph/rgw_tls.go#L29) · [RGWContainer.String](../ceph/rgw_tls.go#L32) · [RGWContainer.GoString](../ceph/rgw_tls.go#L33) |
| [multicluster/rgw_sync_policy.go](../multicluster/rgw_sync_policy.go) | [RGWSyncGroup.String](../multicluster/rgw_sync_policy.go#L107) · [RGWSyncGroup.GoString](../multicluster/rgw_sync_policy.go#L108) |
<!-- callables:end -->

## Config·타입·option 계약

Option 함수는 callable 목록에 포함한다. Config/result 구조체의 field 수백 개는
이 문서에 다시 나열하지 않는다. 전체 schema는 아래 source에서 확인한다.

<!-- schemas:begin -->
공개 타입은 `ceph` 77개, `multicluster` 51개다. 다음 source 묶음에서 config/result 전체 field와 각 주석 계약을 읽는다.

| source | 타입·schema |
| --- | --- |
| [ceph/auth.go](../ceph/auth.go) | [ClientCaps](../ceph/auth.go#L24) · [ClientConfig](../ceph/auth.go#L34) |
| [ceph/ceph.go](../ceph/ceph.go) | [Container](../ceph/ceph.go#L36) · [OSDContainer](../ceph/ceph.go#L71) |
| [ceph/cephfs.go](../ceph/cephfs.go) | [CephFSConfig](../ceph/cephfs.go#L24) · [MDSContainer](../ceph/cephfs.go#L41) · [CephFSContainer](../ceph/cephfs.go#L52) · [MDSStatus](../ceph/cephfs.go#L71)<br>[CephFSMDSStatus](../ceph/cephfs.go#L81) |
| [ceph/cephfs_authorization.go](../ceph/cephfs_authorization.go) | [CephFSSubvolumeAuthorizationConfig](../ceph/cephfs_authorization.go#L16) · [CephFSSubvolumeAuthorization](../ceph/cephfs_authorization.go#L26) · [CephFSSubvolumeAuthorizedClient](../ceph/cephfs_authorization.go#L41) |
| [ceph/cephfs_data_pools.go](../ceph/cephfs_data_pools.go) | [CephFSDataPoolState](../ceph/cephfs_data_pools.go#L16) · [CephFSDataPool](../ceph/cephfs_data_pools.go#L25) |
| [ceph/cephfs_pin.go](../ceph/cephfs_pin.go) | [CephFSPinType](../ceph/cephfs_pin.go#L16) · [CephFSPinSetting](../ceph/cephfs_pin.go#L29) · [CephFSPinPolicy](../ceph/cephfs_pin.go#L40) · [CephFSPinOverride](../ceph/cephfs_pin.go#L56) |
| [ceph/cephfs_quiesce.go](../ceph/cephfs_quiesce.go) | [CephFSQuiesceConfig](../ceph/cephfs_quiesce.go#L18) · [CephFSQuiesceState](../ceph/cephfs_quiesce.go#L24) · [CephFSQuiesce](../ceph/cephfs_quiesce.go#L37) |
| [ceph/cephfs_snapshot.go](../ceph/cephfs_snapshot.go) | [CephFSSubvolumeSnapshot](../ceph/cephfs_snapshot.go#L18) · [CephFSSnapshotPendingClone](../ceph/cephfs_snapshot.go#L25) · [CephFSSubvolumeSnapshotInfo](../ceph/cephfs_snapshot.go#L32) · [CephFSCloneConfig](../ceph/cephfs_snapshot.go#L43)<br>[CephFSSubvolumeClone](../ceph/cephfs_snapshot.go#L51) · [CephFSSubvolumeCloneStatus](../ceph/cephfs_snapshot.go#L59) |
| [ceph/cephfs_subvolume.go](../ceph/cephfs_subvolume.go) | [CephFSSubvolumeGroupConfig](../ceph/cephfs_subvolume.go#L19) · [CephFSSubvolumeConfig](../ceph/cephfs_subvolume.go#L30) · [CephFSSubvolumeGroup](../ceph/cephfs_subvolume.go#L42) · [CephFSSubvolume](../ceph/cephfs_subvolume.go#L53)<br>[CephFSSubvolumeGroupInfo](../ceph/cephfs_subvolume.go#L65) · [CephFSSubvolumeInfo](../ceph/cephfs_subvolume.go#L78) |
| [ceph/config.go](../ceph/config.go) | [ConfigSetting](../ceph/config.go#L19) · [ConfigEntry](../ceph/config.go#L25) · [ConfigOverride](../ceph/config.go#L38) |
| [ceph/diagnostics.go](../ceph/diagnostics.go) | [DiagnosticsConfig](../ceph/diagnostics.go#L33) · [DiagnosticsContainer](../ceph/diagnostics.go#L46) · [DiagnosticArtifact](../ceph/diagnostics.go#L55) · [DiagnosticsReport](../ceph/diagnostics.go#L68) |
| [ceph/fencing.go](../ceph/fencing.go) | [BlocklistEntry](../ceph/fencing.go#L20) · [BlocklistOverride](../ceph/fencing.go#L31) |
| [ceph/mgr_modules.go](../ceph/mgr_modules.go) | [MGRModuleState](../ceph/mgr_modules.go#L19) · [MGRModuleOverride](../ceph/mgr_modules.go#L42) |
| [ceph/network.go](../ceph/network.go) | [NetworkPlane](../ceph/network.go#L17) · [NetworkInterruption](../ceph/network.go#L81) |
| [ceph/options.go](../ceph/options.go) | [Option](../ceph/options.go#L80) |
| [ceph/osd_policy.go](../ceph/osd_policy.go) | [OSDState](../ceph/osd_policy.go#L20) · [OSDFlagOverride](../ceph/osd_policy.go#L116) |
| [ceph/placement.go](../ceph/placement.go) | [OSDConfig](../ceph/placement.go#L16) |
| [ceph/pool.go](../ceph/pool.go) | [PoolConfig](../ceph/pool.go#L18) · [ErasureCodeConfig](../ceph/pool.go#L39) · [Pool](../ceph/pool.go#L48) |
| [ceph/pool_policy.go](../ceph/pool_policy.go) | [PoolQuota](../ceph/pool_policy.go#L16) · [PoolState](../ceph/pool_policy.go#L24) |
| [ceph/rbd.go](../ceph/rbd.go) | [RBDNamespace](../ceph/rbd.go#L15) |
| [ceph/rgw.go](../ceph/rgw.go) | [RGWContainer](../ceph/rgw.go#L24) · [RGWConfig](../ceph/rgw.go#L43) |
| [ceph/rgw_admin.go](../ceph/rgw_admin.go) | [RGWUserConfig](../ceph/rgw_admin.go#L25) · [RGWUser](../ceph/rgw_admin.go#L37) · [RGWQuota](../ceph/rgw_admin.go#L90) · [RGWAdminCapability](../ceph/rgw_admin.go#L96)<br>[RGWUserInfo](../ceph/rgw_admin.go#L105) |
| [ceph/rgw_placement.go](../ceph/rgw_placement.go) | [RGWStorageClassConfig](../ceph/rgw_placement.go#L25) · [RGWPlacementConfig](../ceph/rgw_placement.go#L34) · [RGWPlacement](../ceph/rgw_placement.go#L51) · [RGWPlacementState](../ceph/rgw_placement.go#L67) |
| [ceph/rgw_tenants_accounts.go](../ceph/rgw_tenants_accounts.go) | [RGWAccountConfig](../ceph/rgw_tenants_accounts.go#L47) · [RGWAccount](../ceph/rgw_tenants_accounts.go#L55) · [RGWAccountInfo](../ceph/rgw_tenants_accounts.go#L81) |
| [ceph/rgw_tls.go](../ceph/rgw_tls.go) | [RGWTLSConfig](../ceph/rgw_tls.go#L24) |
| [ceph/rgw_user_placement.go](../ceph/rgw_user_placement.go) | [RGWUserPlacementConfig](../ceph/rgw_user_placement.go#L18) |
| [ceph/status.go](../ceph/status.go) | [Status](../ceph/status.go#L11) |
| [ceph/topology.go](../ceph/topology.go) | [MonitorContainer](../ceph/topology.go#L26) · [ManagerContainer](../ceph/topology.go#L32) · [QuorumStatus](../ceph/topology.go#L109) · [ManagerStatus](../ceph/topology.go#L847) |
| [multicluster/cephfs.go](../multicluster/cephfs.go) | [CephFSMirrorConfig](../multicluster/cephfs.go#L26) · [CephFSMirrorDaemon](../multicluster/cephfs.go#L48) · [CephFSMirror](../multicluster/cephfs.go#L82) |
| [multicluster/cephfs_directory_addition.go](../multicluster/cephfs_directory_addition.go) | [CephFSMirrorDirectoryAddition](../multicluster/cephfs_directory_addition.go#L16) · [CephFSMirrorDirectoryAdditionStatus](../multicluster/cephfs_directory_addition.go#L27) |
| [multicluster/cephfs_directory_removal.go](../multicluster/cephfs_directory_removal.go) | [CephFSMirrorDirectoryRemoval](../multicluster/cephfs_directory_removal.go#L18) · [CephFSMirrorDirectoryRemovalStatus](../multicluster/cephfs_directory_removal.go#L30) · [CephFSMirrorDirectoryRemovalDaemonStatus](../multicluster/cephfs_directory_removal.go#L38) |
| [multicluster/cephfs_directory_status.go](../multicluster/cephfs_directory_status.go) | [CephFSMirrorSnapshot](../multicluster/cephfs_directory_status.go#L18) · [CephFSMirrorDirectoryStatus](../multicluster/cephfs_directory_status.go#L32) |
| [multicluster/cephfs_peer_removal.go](../multicluster/cephfs_peer_removal.go) | [CephFSMirrorPeerRemoval](../multicluster/cephfs_peer_removal.go#L22) · [CephFSMirrorPeerRemovalStatus](../multicluster/cephfs_peer_removal.go#L43) · [CephFSMirrorPeerRemovalDaemonStatus](../multicluster/cephfs_peer_removal.go#L51) |
| [multicluster/cephfs_process_quiescence.go](../multicluster/cephfs_process_quiescence.go) | [CephFSMirrorProcessBindingStatus](../multicluster/cephfs_process_quiescence.go#L38) · [CephFSMirrorProcessQuiescenceStatus](../multicluster/cephfs_process_quiescence.go#L270) · [CephFSMirrorOriginalProcessStatus](../multicluster/cephfs_process_quiescence.go#L279) |
| [multicluster/cephfs_process_quiescence_acknowledgment.go](../multicluster/cephfs_process_quiescence_acknowledgment.go) | [CephFSMirrorProcessQuiescenceAcknowledgment](../multicluster/cephfs_process_quiescence_acknowledgment.go#L13) |
| [multicluster/rbd.go](../multicluster/rbd.go) | [RBDMirrorMode](../multicluster/rbd.go#L25) · [RBDMirrorConfig](../multicluster/rbd.go#L35) · [RBDMirror](../multicluster/rbd.go#L66) · [RBDMirrorDaemon](../multicluster/rbd.go#L88)<br>[RBDMirrorDaemonStatus](../multicluster/rbd.go#L102) · [RBDMirrorPoolReplayerStatus](../multicluster/rbd.go#L109) |
| [multicluster/rbd_image_status.go](../multicluster/rbd_image_status.go) | [RBDMirrorImageStatus](../multicluster/rbd_image_status.go#L22) |
| [multicluster/rbd_namespace.go](../multicluster/rbd_namespace.go) | [RBDMirrorScope](../multicluster/rbd_namespace.go#L18) · [RBDMirrorNamespaceState](../multicluster/rbd_namespace.go#L29) · [RBDMirrorPolicies](../multicluster/rbd_namespace.go#L36) |
| [multicluster/rbd_namespace_binding.go](../multicluster/rbd_namespace_binding.go) | [RBDMirrorNamespace](../multicluster/rbd_namespace_binding.go#L17) |
| [multicluster/rbd_receiver_status.go](../multicluster/rbd_receiver_status.go) | [RBDMirrorReceiverStatus](../multicluster/rbd_receiver_status.go#L21) · [RBDMirrorReceiverDaemonStatus](../multicluster/rbd_receiver_status.go#L36) |
| [multicluster/rgw.go](../multicluster/rgw.go) | [RGWMultisiteConfig](../multicluster/rgw.go#L23) · [RGWMultisite](../multicluster/rgw.go#L33) |
| [multicluster/rgw_sync_bucket_status.go](../multicluster/rgw_sync_bucket_status.go) | [RGWSyncBucketIdentity](../multicluster/rgw_sync_bucket_status.go#L17) · [RGWBucketSyncStatus](../multicluster/rgw_sync_bucket_status.go#L24) |
| [multicluster/rgw_sync_pipe.go](../multicluster/rgw_sync_pipe.go) | [RGWSyncBucketSelector](../multicluster/rgw_sync_pipe.go#L21) · [RGWSyncObjectTag](../multicluster/rgw_sync_pipe.go#L26) |
| [multicluster/rgw_sync_policy.go](../multicluster/rgw_sync_policy.go) | [RGWSyncPolicyScope](../multicluster/rgw_sync_policy.go#L25) · [RGWSyncGroupStatus](../multicluster/rgw_sync_policy.go#L30) · [RGWSyncGroupConfig](../multicluster/rgw_sync_policy.go#L38) · [RGWSyncFlowConfig](../multicluster/rgw_sync_policy.go#L48)<br>[RGWSyncPipeConfig](../multicluster/rgw_sync_policy.go#L62) · [RGWSyncGroup](../multicluster/rgw_sync_policy.go#L80) |
| [multicluster/rgw_sync_policy_ready.go](../multicluster/rgw_sync_policy_ready.go) | [RGWBucketSyncPolicyStatus](../multicluster/rgw_sync_policy_ready.go#L17) |
| [multicluster/rgw_sync_status.go](../multicluster/rgw_sync_status.go) | [RGWMetadataSyncStatus](../multicluster/rgw_sync_status.go#L17) · [RGWDataSyncStatus](../multicluster/rgw_sync_status.go#L28) · [RGWSyncStatus](../multicluster/rgw_sync_status.go#L35) |
| [multicluster/rgw_topology.go](../multicluster/rgw_topology.go) | [RGWZoneConfig](../multicluster/rgw_topology.go#L19) · [RGWZonegroupConfig](../multicluster/rgw_topology.go#L27) · [RGWTopologyConfig](../multicluster/rgw_topology.go#L39) · [RGWZone](../multicluster/rgw_topology.go#L50)<br>[RGWZonegroup](../multicluster/rgw_topology.go#L58) |
<!-- schemas:end -->
