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
| 클러스터·MON·MGR·OSD | 초기 구성, 노드 증감·교체, cold bootstrap, daemon·client 컨테이너 일시정지, replica 읽기 오류 주입·deep scrub·repair, owned cleanup | quorum, native health code·원인·mute, manager/module 준비·service URL, OSD 상태·flag, PG clean, scrub 불일치 object |
| Pool·placement·Cephx | pool·replica·quota·CRUSH 조건, 실행 중 PG 수·placement 변경, 제한된 client caps, 임시 설정 복원 | native pool ID·정책·quota·사용량·PG target 도달, caps, blocklist·설정 조회 |
| CephFS MDS | FS/pool 구성, active·standby/replay 조절, cold 첫 기동, stopped/last MDS 교체 | FSMap·논리적 rank/GID·owned 상태, 요청한 MDS capacity 준비 |
| CephFS client 조건 | 추가 data pool·layout, subvolume/group·snapshot·clone, pin·quiesce·권한, client session timeout | native 목록·info·clone 상태·pin/quiesce 상태·authorized clients·client session |
| RGW client 조건 | gateway, user/account/caps·quota, placement/storage class, native TLS 설정 | endpoint와 native identity·placement·quota/caps 설정·user/account 저장량 |
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

Pool/user/account의 policy·quota·caps/identity 조회와 사용량 조회는 별도 Check다.
`PoolUsage`는 monitor가 보고한 pool 통계, `UserUsage`는 RGW의 user 또는 account
집계 범위를 반환한다. 비동기 통계의 갱신과 실제 I/O 성공은 각 계약에 따라
별도로 확인한다. Raw CLI로 추가 상태를 질의할 수 있다.

`HealthDetails`는 원래 bootstrap FSID와 조회 전후 native FSID를 확인하고
MON의 health code·원인·mute를 반환한다. MGR·storage 준비나 HEALTH_OK를
요구하지 않으며, 세 번의 native 읽기는 비원자적이다. 오류·취소 시 zero
snapshot을 반환한다. 공식 role 이미지의 focused native 검증과 전체 CI는 별도이며, mute와 severity,
반환 message의 비밀 정보 가능성은 [HealthDetails 계약](HEALTH_DETAILS.md)을 따른다.

`PoolPGs`는 원래 FSID와 조회 앞뒤의 pool identity를 확인하고 reported
PG 상태·vector·primary·epoch·signed counters를 보존한다. Native PGReady와
stats-invalid flag로 실제 데이터 가시성이나 최신 매핑을 추론하지 않는다.
Unknown·빈 vector·EC NONE 슬롯 및 오류 zero-result 계약은
[PoolPGs 문서](POOL_PGS.md)를 따른다.

Raw CLI 접점은 argv에 따라 조회와 변경 모두 가능하다. 이미지·client·customizer
옵션 및 안전한 문자열 표현도 공개 surface에 포함되지만 native Check로 세지
않는다. Backup/export/restore는 recovery fixture에서 조건부 가치가 있으므로
자동 삭제하지 않는다. Caller 소유 stream의 blocking I/O, partial archive와
baseline snapshot, 복원 bytes 검증은 별도 책임이다.

## 현재 공개 callable 수

| 분류 | ceph | cephfs | rgw | rbd | 합계 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fixture Operation | 54 | 49 | 36 | 14 | 153 |
| Check: native 질의/Wait 또는 보존 정보 조회 | 49 | 33 | 20 | 17 | 119 |
| 연결·raw CLI·customizer 접점 | 8 | 1 | 5 | 2 | 16 |
| 조건부 archive helper | 0 | 0 | 0 | 4 | 4 |
| 로컬 문자열 표현 | 4 | 0 | 12 | 0 | 16 |
| 전체 | 115 | 83 | 73 | 37 | **308** |

집계는 공개 패키지 네 개(`ceph`, `cephfs`, `rgw`, `rbd`)의 공개 함수와 공개 타입의 공개 method다. 서비스 패키지의 타입은 `internal/cluster`·`internal/multicluster` 구현 타입의 alias이므로 method와 줄 anchor는 구현 선언을 가리킨다. Test/Example,
private receiver의 exported-name method, dependency가 승격하는 container method,
타입·상수·구조체 field는 이 308개에 포함하지 않는다. 아래 목록에서 각 callable을
한 번씩 나열하고 source에 연결한다. Config/result 타입과 option 계약은 따로 읽는다.
[internal/apiinventory](../internal/apiinventory/inventory_test.go)의 단위 테스트가
`go/ast`로 읽은 공개 callable·타입과 이 문서의 목록, 분류별 개수, 요약표, 줄
anchor를 비교한다. `make test`에 포함되며 Docker를 실행하지 않는다.

실행 검증은 [fixture 범위와 native 기록](CLUSTER_SCENARIOS.md),
[기능 coverage](CLIENT_FIXTURE_COVERAGE.md),
[CI 선택 범위](CI_FIXTURES.md)와
각 focused 계약 문서를 따른다. 선언·호출 이름의 lexical reference는 새 PASS나
모든 method의 native coverage 증거가 아니다. 이 inventory 감사는 Go/Docker를
실행하지 않았고, 과거 source의 결과를 현재 전체 surface 인증으로 합산하지 않는다.

## 전체 callable 목록

<!-- callables:begin -->
### Fixture Operation (153개)

| source | 공개 callable |
| --- | --- |
| [cephfs/run.go](../cephfs/run.go) | [cephfs.Run](../cephfs/run.go#L18) |
| [internal/cluster/auth.go](../internal/cluster/auth.go) | [ceph.Container.CreateClient](../internal/cluster/auth.go#L101) · [ceph.Container.DeleteClient](../internal/cluster/auth.go#L219) |
| [internal/cluster/auth_policy.go](../internal/cluster/auth_policy.go) | [ceph.Container.UpdateClientCaps](../internal/cluster/auth_policy.go#L25) |
| [internal/cluster/ceph.go](../internal/cluster/ceph.go) | [ceph.Run](../internal/cluster/ceph.go#L90) · [ceph.Container.AddOSD](../internal/cluster/ceph.go#L425) · [ceph.Container.AddOSDWithConfig](../internal/cluster/ceph.go#L432) · [ceph.Container.RemoveOSD](../internal/cluster/ceph.go#L549)<br>[ceph.Container.Terminate](../internal/cluster/ceph.go#L775) |
| [internal/cluster/cephfs.go](../internal/cluster/cephfs.go) | [cephfs.Filesystem.ScaleMDS](../internal/cluster/cephfs.go#L250) |
| [internal/cluster/cephfs_authorization.go](../internal/cluster/cephfs_authorization.go) | [cephfs.Filesystem.AuthorizeSubvolume](../internal/cluster/cephfs_authorization.go#L67) · [cephfs.Filesystem.DeauthorizeSubvolume](../internal/cluster/cephfs_authorization.go#L396) · [cephfs.Filesystem.EvictSubvolumeClients](../internal/cluster/cephfs_authorization.go#L462) |
| [internal/cluster/cephfs_clone_lifecycle.go](../internal/cluster/cephfs_clone_lifecycle.go) | [cephfs.Filesystem.CancelSubvolumeClone](../internal/cluster/cephfs_clone_lifecycle.go#L116) · [cephfs.Filesystem.RemovePartialSubvolumeClone](../internal/cluster/cephfs_clone_lifecycle.go#L187) |
| [internal/cluster/cephfs_data_pools.go](../internal/cluster/cephfs_data_pools.go) | [cephfs.Filesystem.AddDataPool](../internal/cluster/cephfs_data_pools.go#L229) · [cephfs.Filesystem.RemoveUnusedDataPool](../internal/cluster/cephfs_data_pools.go#L362) |
| [internal/cluster/cephfs_mds_replacement.go](../internal/cluster/cephfs_mds_replacement.go) | [cephfs.Filesystem.AddMDSReplacement](../internal/cluster/cephfs_mds_replacement.go#L40) |
| [internal/cluster/cephfs_pin.go](../internal/cluster/cephfs_pin.go) | [cephfs.Filesystem.TemporarySubvolumePin](../internal/cluster/cephfs_pin.go#L96) · [cephfs.Filesystem.TemporarySubvolumeGroupPin](../internal/cluster/cephfs_pin.go#L104) · [cephfs.PinOverride.Restore](../internal/cluster/cephfs_pin.go#L182) |
| [internal/cluster/cephfs_quiesce.go](../internal/cluster/cephfs_quiesce.go) | [cephfs.Filesystem.QuiesceSubvolumes](../internal/cluster/cephfs_quiesce.go#L64) · [cephfs.Quiesce.Release](../internal/cluster/cephfs_quiesce.go#L159) |
| [internal/cluster/cephfs_session.go](../internal/cluster/cephfs_session.go) | [cephfs.Filesystem.TemporarySessionTimeouts](../internal/cluster/cephfs_session.go#L100) · [cephfs.SessionTimeoutsOverride.Restore](../internal/cluster/cephfs_session.go#L125) |
| [internal/cluster/cephfs_snapshot.go](../internal/cluster/cephfs_snapshot.go) | [cephfs.Filesystem.CreateSubvolumeSnapshot](../internal/cluster/cephfs_snapshot.go#L131) · [cephfs.Filesystem.RemoveSubvolumeSnapshot](../internal/cluster/cephfs_snapshot.go#L270) · [cephfs.Filesystem.CloneSubvolumeSnapshot](../internal/cluster/cephfs_snapshot.go#L320) |
| [internal/cluster/cephfs_stopped_mds.go](../internal/cluster/cephfs_stopped_mds.go) | [cephfs.Filesystem.RemoveStoppedMDS](../internal/cluster/cephfs_stopped_mds.go#L81) |
| [internal/cluster/cephfs_subvolume.go](../internal/cluster/cephfs_subvolume.go) | [cephfs.Filesystem.CreateSubvolumeGroup](../internal/cluster/cephfs_subvolume.go#L282) · [cephfs.Filesystem.CreateSubvolume](../internal/cluster/cephfs_subvolume.go#L323) · [cephfs.Filesystem.ResizeSubvolumeGroup](../internal/cluster/cephfs_subvolume.go#L525) · [cephfs.Filesystem.ResizeSubvolume](../internal/cluster/cephfs_subvolume.go#L557)<br>[cephfs.Filesystem.RemoveSubvolumeGroup](../internal/cluster/cephfs_subvolume.go#L591) · [cephfs.Filesystem.RemoveSubvolume](../internal/cluster/cephfs_subvolume.go#L639) |
| [internal/cluster/client_monitor_config.go](../internal/cluster/client_monitor_config.go) | [ceph.Container.RefreshClientMonitorConfig](../internal/cluster/client_monitor_config.go#L44) |
| [internal/cluster/composition.go](../internal/cluster/composition.go) | [ceph.WithPools](../internal/cluster/composition.go#L12) · [cephfs.WithFilesystems](../internal/cluster/composition.go#L26) · [rgw.WithGateways](../internal/cluster/composition.go#L43) |
| [internal/cluster/config.go](../internal/cluster/config.go) | [ceph.Container.TemporaryConfig](../internal/cluster/config.go#L76) · [ceph.ConfigOverride.Restore](../internal/cluster/config.go#L141) |
| [internal/cluster/config_file.go](../internal/cluster/config_file.go) | [ceph.WithConfigFile](../internal/cluster/config_file.go#L48) |
| [internal/cluster/fencing.go](../internal/cluster/fencing.go) | [ceph.Container.TemporaryBlocklist](../internal/cluster/fencing.go#L64) · [ceph.BlocklistOverride.Restore](../internal/cluster/fencing.go#L115) |
| [internal/cluster/full_ratios.go](../internal/cluster/full_ratios.go) | [ceph.Container.TemporaryFullRatios](../internal/cluster/full_ratios.go#L78) · [ceph.FullRatiosOverride.Restore](../internal/cluster/full_ratios.go#L122) |
| [internal/cluster/mgr_modules.go](../internal/cluster/mgr_modules.go) | [ceph.Container.TemporaryMGRModule](../internal/cluster/mgr_modules.go#L60) · [ceph.MGRModuleOverride.Restore](../internal/cluster/mgr_modules.go#L121) |
| [internal/cluster/network.go](../internal/cluster/network.go) | [ceph.Container.InterruptNetwork](../internal/cluster/network.go#L96) · [ceph.InterruptNetwork](../internal/cluster/network.go#L142) · [ceph.NetworkInterruption.Restore](../internal/cluster/network.go#L199) |
| [internal/cluster/options.go](../internal/cluster/options.go) | [ceph.WithMessengerMode](../internal/cluster/options.go#L49) · [ceph.WithMonitorCount](../internal/cluster/options.go#L61) · [ceph.WithManagerCount](../internal/cluster/options.go#L72) · [ceph.WithNoInitialManagers](../internal/cluster/options.go#L89)<br>[ceph.WithHostNetwork](../internal/cluster/options.go#L109) · [ceph.WithSeparateClusterNetwork](../internal/cluster/options.go#L119) · [ceph.WithNoInitialOSDs](../internal/cluster/options.go#L184) · [ceph.WithOSDCount](../internal/cluster/options.go#L192)<br>[ceph.WithInitialOSDs](../internal/cluster/options.go#L206) · [ceph.WithDefaultCRUSHRoot](../internal/cluster/options.go#L236) · [ceph.WithPoolDefaults](../internal/cluster/options.go#L249) · [ceph.WithOSDBlockSize](../internal/cluster/options.go#L264)<br>[ceph.WithOSDInMemoryStorage](../internal/cluster/options.go#L284) |
| [internal/cluster/osd_policy.go](../internal/cluster/osd_policy.go) | [ceph.Container.SetOSDIn](../internal/cluster/osd_policy.go#L57) · [ceph.Container.TemporaryOSDFlag](../internal/cluster/osd_policy.go#L132) · [ceph.OSDFlagOverride.Restore](../internal/cluster/osd_policy.go#L175) |
| [internal/cluster/pause.go](../internal/cluster/pause.go) | [ceph.Container.PauseContainer](../internal/cluster/pause.go#L43) · [ceph.ContainerPause.Resume](../internal/cluster/pause.go#L109) |
| [internal/cluster/pool.go](../internal/cluster/pool.go) | [ceph.Container.CreatePool](../internal/cluster/pool.go#L62) |
| [internal/cluster/pool_policy.go](../internal/cluster/pool_policy.go) | [ceph.Container.SetPoolQuota](../internal/cluster/pool_policy.go#L70) · [ceph.Container.SetPoolReplication](../internal/cluster/pool_policy.go#L110) |
| [internal/cluster/pool_removal.go](../internal/cluster/pool_removal.go) | [ceph.Container.RemovePool](../internal/cluster/pool_removal.go#L30) |
| [internal/cluster/pool_relocation.go](../internal/cluster/pool_relocation.go) | [ceph.Container.SetPoolPGCount](../internal/cluster/pool_relocation.go#L31) · [ceph.Container.SetPoolPlacement](../internal/cluster/pool_relocation.go#L179) |
| [internal/cluster/rgw_admin.go](../internal/cluster/rgw_admin.go) | [rgw.Gateway.CreateUser](../internal/cluster/rgw_admin.go#L258) · [rgw.Gateway.SetUserQuota](../internal/cluster/rgw_admin.go#L469) · [rgw.Gateway.SetBucketQuota](../internal/cluster/rgw_admin.go#L475) · [rgw.Gateway.SuspendUser](../internal/cluster/rgw_admin.go#L526)<br>[rgw.Gateway.RemoveUser](../internal/cluster/rgw_admin.go#L555) |
| [internal/cluster/rgw_placement.go](../internal/cluster/rgw_placement.go) | [rgw.Gateway.CreatePlacement](../internal/cluster/rgw_placement.go#L139) · [rgw.Gateway.ApplyPlacement](../internal/cluster/rgw_placement.go#L279) · [rgw.Gateway.ReloadPlacement](../internal/cluster/rgw_placement.go#L377) |
| [internal/cluster/rgw_tenants_accounts.go](../internal/cluster/rgw_tenants_accounts.go) | [rgw.Gateway.CreateAccount](../internal/cluster/rgw_tenants_accounts.go#L134) · [rgw.Gateway.CreateAccountRootUser](../internal/cluster/rgw_tenants_accounts.go#L318) · [rgw.Gateway.SetAccountQuota](../internal/cluster/rgw_tenants_accounts.go#L328) · [rgw.Gateway.SetAccountBucketQuota](../internal/cluster/rgw_tenants_accounts.go#L334)<br>[rgw.Gateway.RemoveAccount](../internal/cluster/rgw_tenants_accounts.go#L387) |
| [internal/cluster/rgw_user_placement.go](../internal/cluster/rgw_user_placement.go) | [rgw.Gateway.SetUserPlacement](../internal/cluster/rgw_user_placement.go#L67) |
| [internal/cluster/scrub.go](../internal/cluster/scrub.go) | [ceph.Container.InjectObjectDataError](../internal/cluster/scrub.go#L66) · [ceph.Container.DeepScrubPG](../internal/cluster/scrub.go#L124) · [ceph.Container.RepairPG](../internal/cluster/scrub.go#L133) |
| [internal/cluster/services.go](../internal/cluster/services.go) | [cephfs.Start](../internal/cluster/services.go#L29) · [cephfs.Remove](../internal/cluster/services.go#L66) · [rgw.Start](../internal/cluster/services.go#L51) · [rgw.Remove](../internal/cluster/services.go#L77) · [rbd.InitPool](../internal/cluster/services.go#L108)<br>[rbd.CreateNamespace](../internal/cluster/services.go#L120) · [rbd.RemoveNamespace](../internal/cluster/services.go#L144) · [rbd.WithPools](../internal/cluster/services.go#L155) |
| [internal/cluster/topology.go](../internal/cluster/topology.go) | [ceph.Container.AddMonitor](../internal/cluster/topology.go#L146) · [ceph.Container.RemoveMonitor](../internal/cluster/topology.go#L263) · [ceph.Container.RefreshMonitorConfig](../internal/cluster/topology.go#L369) · [ceph.Container.AddManager](../internal/cluster/topology.go#L705)<br>[ceph.Container.RemoveManager](../internal/cluster/topology.go#L786) |
| [internal/multicluster/cephfs.go](../internal/multicluster/cephfs.go) | [cephfs.MirrorDaemon.Terminate](../internal/multicluster/cephfs.go#L60) · [cephfs.RunMirror](../internal/multicluster/cephfs.go#L133) · [cephfs.Mirror.AddDaemon](../internal/multicluster/cephfs.go#L494) · [cephfs.Mirror.RemoveDaemon](../internal/multicluster/cephfs.go#L567)<br>[cephfs.Mirror.Terminate](../internal/multicluster/cephfs.go#L606) · [cephfs.Mirror.AttachManagers](../internal/multicluster/cephfs.go#L629) · [cephfs.Mirror.AddDirectory](../internal/multicluster/cephfs.go#L829) · [cephfs.Mirror.RemoveDirectory](../internal/multicluster/cephfs.go#L865)<br>[cephfs.Mirror.RebalanceDirectories](../internal/multicluster/cephfs.go#L916) · [cephfs.Mirror.RemovePeer](../internal/multicluster/cephfs.go#L1081) · [cephfs.Mirror.RebootstrapPeer](../internal/multicluster/cephfs.go#L1123) |
| [internal/multicluster/cephfs_connection_refresh.go](../internal/multicluster/cephfs_connection_refresh.go) | [cephfs.Mirror.RefreshMonitorConfig](../internal/multicluster/cephfs_connection_refresh.go#L24) · [cephfs.Mirror.RefreshPeerMonitorConfig](../internal/multicluster/cephfs_connection_refresh.go#L87) |
| [internal/multicluster/cephfs_directory_addition.go](../internal/multicluster/cephfs_directory_addition.go) | [cephfs.Mirror.BeginDirectoryAddition](../internal/multicluster/cephfs_directory_addition.go#L43) |
| [internal/multicluster/cephfs_directory_removal.go](../internal/multicluster/cephfs_directory_removal.go) | [cephfs.Mirror.BeginDirectoryRemoval](../internal/multicluster/cephfs_directory_removal.go#L50) |
| [internal/multicluster/cephfs_peer_removal.go](../internal/multicluster/cephfs_peer_removal.go) | [cephfs.Mirror.BeginPeerRemoval](../internal/multicluster/cephfs_peer_removal.go#L76) |
| [internal/multicluster/cephfs_process_quiescence_acknowledgment.go](../internal/multicluster/cephfs_process_quiescence_acknowledgment.go) | [cephfs.MirrorPeerRemoval.AcknowledgeProcessQuiescence](../internal/multicluster/cephfs_process_quiescence_acknowledgment.go#L22) · [cephfs.MirrorDirectoryRemoval.AcknowledgeProcessQuiescence](../internal/multicluster/cephfs_process_quiescence_acknowledgment.go#L38) |
| [internal/multicluster/network.go](../internal/multicluster/network.go) | [rbd.Mirror.InterruptPeerLink](../internal/multicluster/network.go#L24) · [cephfs.Mirror.InterruptPeerLink](../internal/multicluster/network.go#L47) · [rgw.Multisite.InterruptZoneLink](../internal/multicluster/network.go#L70) |
| [internal/multicluster/rbd.go](../internal/multicluster/rbd.go) | [rbd.MirrorDaemon.Terminate](../internal/multicluster/rbd.go#L142) · [rbd.RunMirror](../internal/multicluster/rbd.go#L191) · [rbd.Mirror.AddDaemon](../internal/multicluster/rbd.go#L265) · [rbd.Mirror.RemoveDaemon](../internal/multicluster/rbd.go#L329)<br>[rbd.Mirror.Rebootstrap](../internal/multicluster/rbd.go#L374) · [rbd.Mirror.EnableImage](../internal/multicluster/rbd.go#L600) · [rbd.Mirror.Terminate](../internal/multicluster/rbd.go#L662) |
| [internal/multicluster/rbd_connection_refresh.go](../internal/multicluster/rbd_connection_refresh.go) | [rbd.Mirror.RefreshMonitorConfig](../internal/multicluster/rbd_connection_refresh.go#L27) |
| [internal/multicluster/rgw.go](../internal/multicluster/rgw.go) | [rgw.RunMultisite](../internal/multicluster/rgw.go#L62) · [rgw.Multisite.PullSourcePeriod](../internal/multicluster/rgw.go#L482) · [rgw.Multisite.PullDestinationPeriod](../internal/multicluster/rgw.go#L494) · [rgw.Multisite.Terminate](../internal/multicluster/rgw.go#L504) |
| [internal/multicluster/rgw_sync_period.go](../internal/multicluster/rgw_sync_period.go) | [rgw.Multisite.ApplySyncGroup](../internal/multicluster/rgw_sync_period.go#L20) |
| [internal/multicluster/rgw_sync_policy.go](../internal/multicluster/rgw_sync_policy.go) | [rgw.Multisite.CreateSyncGroup](../internal/multicluster/rgw_sync_policy.go#L125) · [rgw.Multisite.CreateSyncFlow](../internal/multicluster/rgw_sync_policy.go#L183) · [rgw.Multisite.RemoveSyncFlow](../internal/multicluster/rgw_sync_policy.go#L204) · [rgw.Multisite.CreateSyncPipe](../internal/multicluster/rgw_sync_policy.go#L229)<br>[rgw.Multisite.SetSyncPipePrefix](../internal/multicluster/rgw_sync_policy.go#L265) · [rgw.Multisite.RemoveSyncPipe](../internal/multicluster/rgw_sync_policy.go#L298) · [rgw.Multisite.SetSyncGroupStatus](../internal/multicluster/rgw_sync_policy.go#L317) · [rgw.Multisite.RemoveSyncGroup](../internal/multicluster/rgw_sync_policy.go#L336) |
| [internal/multicluster/rgw_topology.go](../internal/multicluster/rgw_topology.go) | [rgw.RunTopology](../internal/multicluster/rgw_topology.go#L80) · [rgw.Multisite.AddZone](../internal/multicluster/rgw_topology.go#L304) |
| [internal/multicluster/rgw_zonegroups.go](../internal/multicluster/rgw_zonegroups.go) | [rgw.Multisite.AddZonegroup](../internal/multicluster/rgw_zonegroups.go#L47) · [rgw.Multisite.RemoveZone](../internal/multicluster/rgw_zonegroups.go#L117) |
| [rbd/run.go](../rbd/run.go) | [rbd.Run](../rbd/run.go#L18) |
| [rgw/run.go](../rgw/run.go) | [rgw.Run](../rgw/run.go#L18) |

### Check: 현재 상태 질의·policy/process 관측·Wait (74개)

| source | 공개 callable |
| --- | --- |
| [internal/cluster/auth_policy.go](../internal/cluster/auth_policy.go) | [ceph.Container.ClientCapabilities](../internal/cluster/auth_policy.go#L12) |
| [internal/cluster/cephfs.go](../internal/cluster/cephfs.go) | [cephfs.Filesystem.MDSStatus](../internal/cluster/cephfs.go#L632) · [cephfs.Filesystem.WaitReady](../internal/cluster/cephfs.go#L652) |
| [internal/cluster/cephfs_authorization.go](../internal/cluster/cephfs_authorization.go) | [cephfs.Filesystem.SubvolumeAuthorizedClients](../internal/cluster/cephfs_authorization.go#L196) |
| [internal/cluster/cephfs_data_pools.go](../internal/cluster/cephfs_data_pools.go) | [cephfs.Filesystem.DataPools](../internal/cluster/cephfs_data_pools.go#L184) |
| [internal/cluster/cephfs_pin.go](../internal/cluster/cephfs_pin.go) | [cephfs.Filesystem.SubvolumePinPolicy](../internal/cluster/cephfs_pin.go#L78) · [cephfs.Filesystem.SubvolumeGroupPinPolicy](../internal/cluster/cephfs_pin.go#L86) |
| [internal/cluster/cephfs_quiesce.go](../internal/cluster/cephfs_quiesce.go) | [cephfs.Quiesce.Status](../internal/cluster/cephfs_quiesce.go#L139) |
| [internal/cluster/cephfs_session.go](../internal/cluster/cephfs_session.go) | [cephfs.Filesystem.SessionTimeouts](../internal/cluster/cephfs_session.go#L86) · [cephfs.Filesystem.Sessions](../internal/cluster/cephfs_session.go#L180) |
| [internal/cluster/cephfs_snapshot.go](../internal/cluster/cephfs_snapshot.go) | [cephfs.Filesystem.SubvolumeSnapshots](../internal/cluster/cephfs_snapshot.go#L109) · [cephfs.Filesystem.SubvolumeSnapshotInfo](../internal/cluster/cephfs_snapshot.go#L173) · [cephfs.Filesystem.SubvolumeCloneStatus](../internal/cluster/cephfs_snapshot.go#L462) · [cephfs.Filesystem.WaitForSubvolumeClone](../internal/cluster/cephfs_snapshot.go#L481) |
| [internal/cluster/cephfs_subvolume.go](../internal/cluster/cephfs_subvolume.go) | [cephfs.Filesystem.SubvolumeGroups](../internal/cluster/cephfs_subvolume.go#L367) · [cephfs.Filesystem.Subvolumes](../internal/cluster/cephfs_subvolume.go#L378) · [cephfs.Filesystem.SubvolumeGroupInfo](../internal/cluster/cephfs_subvolume.go#L391) · [cephfs.Filesystem.SubvolumeInfo](../internal/cluster/cephfs_subvolume.go#L406) |
| [internal/cluster/config.go](../internal/cluster/config.go) | [ceph.Container.Configuration](../internal/cluster/config.go#L59) |
| [internal/cluster/diagnostics.go](../internal/cluster/diagnostics.go) | [ceph.Container.CollectDiagnostics](../internal/cluster/diagnostics.go#L85) |
| [internal/cluster/fencing.go](../internal/cluster/fencing.go) | [ceph.Container.BlocklistEntries](../internal/cluster/fencing.go#L45) |
| [internal/cluster/full_ratios.go](../internal/cluster/full_ratios.go) | [ceph.Container.FullRatios](../internal/cluster/full_ratios.go#L36) |
| [internal/cluster/health_details.go](../internal/cluster/health_details.go) | [ceph.Container.HealthDetails](../internal/cluster/health_details.go#L55) |
| [internal/cluster/mgr_modules.go](../internal/cluster/mgr_modules.go) | [ceph.Container.MGRModules](../internal/cluster/mgr_modules.go#L29) · [ceph.Container.WaitMGRModuleReady](../internal/cluster/mgr_modules.go#L187) |
| [internal/cluster/mgr_services.go](../internal/cluster/mgr_services.go) | [ceph.Container.ManagerServices](../internal/cluster/mgr_services.go#L17) |
| [internal/cluster/object_watchers.go](../internal/cluster/object_watchers.go) | [ceph.Container.ObjectWatchers](../internal/cluster/object_watchers.go#L26) |
| [internal/cluster/osd_policy.go](../internal/cluster/osd_policy.go) | [ceph.Container.OSDStates](../internal/cluster/osd_policy.go#L29) · [ceph.Container.OSDFlags](../internal/cluster/osd_policy.go#L99) · [ceph.Container.WaitForPGClean](../internal/cluster/osd_policy.go#L307) |
| [internal/cluster/pool_pgs.go](../internal/cluster/pool_pgs.go) | [ceph.Container.PoolPGs](../internal/cluster/pool_pgs.go#L62) |
| [internal/cluster/pool_policy.go](../internal/cluster/pool_policy.go) | [ceph.Container.Pools](../internal/cluster/pool_policy.go#L44) · [ceph.Container.PoolStatus](../internal/cluster/pool_policy.go#L54) |
| [internal/cluster/pool_relocation.go](../internal/cluster/pool_relocation.go) | [ceph.Container.WaitForPoolPGCount](../internal/cluster/pool_relocation.go#L73) |
| [internal/cluster/pool_usage.go](../internal/cluster/pool_usage.go) | [ceph.Container.PoolUsage](../internal/cluster/pool_usage.go#L44) |
| [internal/cluster/rbd_image_clients.go](../internal/cluster/rbd_image_clients.go) | [rbd.ImageClients](../internal/cluster/rbd_image_clients.go#L56) |
| [internal/cluster/rgw_admin.go](../internal/cluster/rgw_admin.go) | [rgw.Gateway.UserInfo](../internal/cluster/rgw_admin.go#L457) |
| [internal/cluster/rgw_placement.go](../internal/cluster/rgw_placement.go) | [rgw.Gateway.PlacementStatus](../internal/cluster/rgw_placement.go#L250) |
| [internal/cluster/rgw_tenants_accounts.go](../internal/cluster/rgw_tenants_accounts.go) | [rgw.Gateway.AccountInfo](../internal/cluster/rgw_tenants_accounts.go#L299) |
| [internal/cluster/rgw_usage.go](../internal/cluster/rgw_usage.go) | [rgw.Gateway.UserUsage](../internal/cluster/rgw_usage.go#L35) |
| [internal/cluster/scrub.go](../internal/cluster/scrub.go) | [ceph.Container.PGInconsistencies](../internal/cluster/scrub.go#L224) |
| [internal/cluster/services.go](../internal/cluster/services.go) | [rbd.ListNamespaces](../internal/cluster/services.go#L130) |
| [internal/cluster/status.go](../internal/cluster/status.go) | [ceph.Container.Status](../internal/cluster/status.go#L34) · [ceph.Container.WaitForClean](../internal/cluster/status.go#L48) |
| [internal/cluster/topology.go](../internal/cluster/topology.go) | [ceph.Container.QuorumStatus](../internal/cluster/topology.go#L125) · [ceph.Container.WaitForQuorum](../internal/cluster/topology.go#L135) · [ceph.Container.ManagerStatus](../internal/cluster/topology.go#L888) |
| [internal/multicluster/cephfs.go](../internal/multicluster/cephfs.go) | [cephfs.Mirror.PeerIDs](../internal/multicluster/cephfs.go#L1033) |
| [internal/multicluster/cephfs_directory_addition.go](../internal/multicluster/cephfs_directory_addition.go) | [cephfs.MirrorDirectoryAddition.Status](../internal/multicluster/cephfs_directory_addition.go#L289) |
| [internal/multicluster/cephfs_directory_removal.go](../internal/multicluster/cephfs_directory_removal.go) | [cephfs.MirrorDirectoryRemoval.Status](../internal/multicluster/cephfs_directory_removal.go#L374) · [cephfs.MirrorDirectoryRemoval.WaitReleased](../internal/multicluster/cephfs_directory_removal.go#L480) |
| [internal/multicluster/cephfs_directory_status.go](../internal/multicluster/cephfs_directory_status.go) | [cephfs.Mirror.DirectoryStatus](../internal/multicluster/cephfs_directory_status.go#L49) · [cephfs.Mirror.WaitDirectoryReady](../internal/multicluster/cephfs_directory_status.go#L203) · [cephfs.Mirror.WaitSnapshotSynced](../internal/multicluster/cephfs_directory_status.go#L218) |
| [internal/multicluster/cephfs_peer_removal.go](../internal/multicluster/cephfs_peer_removal.go) | [cephfs.MirrorPeerRemoval.Status](../internal/multicluster/cephfs_peer_removal.go#L238) · [cephfs.MirrorPeerRemoval.WaitDrained](../internal/multicluster/cephfs_peer_removal.go#L331) |
| [internal/multicluster/cephfs_process_quiescence.go](../internal/multicluster/cephfs_process_quiescence.go) | [cephfs.MirrorPeerRemoval.ProcessQuiescence](../internal/multicluster/cephfs_process_quiescence.go#L286) · [cephfs.MirrorDirectoryRemoval.ProcessQuiescence](../internal/multicluster/cephfs_process_quiescence.go#L298) |
| [internal/multicluster/rbd.go](../internal/multicluster/rbd.go) | [rbd.MirrorDaemon.Status](../internal/multicluster/rbd.go#L121) |
| [internal/multicluster/rbd_image_status.go](../internal/multicluster/rbd_image_status.go) | [rbd.Mirror.ImageStatus](../internal/multicluster/rbd_image_status.go#L38) · [rbd.Mirror.WaitReplayReady](../internal/multicluster/rbd_image_status.go#L228) |
| [internal/multicluster/rbd_namespace.go](../internal/multicluster/rbd_namespace.go) | [rbd.Mirror.PolicyStatus](../internal/multicluster/rbd_namespace.go#L44) |
| [internal/multicluster/rbd_namespace_binding.go](../internal/multicluster/rbd_namespace_binding.go) | [rbd.Mirror.BindNamespace](../internal/multicluster/rbd_namespace_binding.go#L61) · [rbd.MirrorNamespace.ReceiverStatus](../internal/multicluster/rbd_namespace_binding.go#L100) · [rbd.MirrorNamespace.WaitReceiverReady](../internal/multicluster/rbd_namespace_binding.go#L113) |
| [internal/multicluster/rbd_namespace_image_status.go](../internal/multicluster/rbd_namespace_image_status.go) | [rbd.MirrorNamespace.ImageStatus](../internal/multicluster/rbd_namespace_image_status.go#L18) · [rbd.MirrorNamespace.WaitReplayReady](../internal/multicluster/rbd_namespace_image_status.go#L30) |
| [internal/multicluster/rbd_receiver_status.go](../internal/multicluster/rbd_receiver_status.go) | [rbd.Mirror.ReceiverStatus](../internal/multicluster/rbd_receiver_status.go#L66) · [rbd.Mirror.WaitReceiverReady](../internal/multicluster/rbd_receiver_status.go#L78) |
| [internal/multicluster/rgw_sync_bucket_status.go](../internal/multicluster/rgw_sync_bucket_status.go) | [rgw.Multisite.BucketSyncStatus](../internal/multicluster/rgw_sync_bucket_status.go#L46) · [rgw.Multisite.WaitBucketSyncReady](../internal/multicluster/rgw_sync_bucket_status.go#L67) |
| [internal/multicluster/rgw_sync_policy_ready.go](../internal/multicluster/rgw_sync_policy_ready.go) | [rgw.Multisite.WaitBucketSyncPolicyReady](../internal/multicluster/rgw_sync_policy_ready.go#L38) |
| [internal/multicluster/rgw_sync_status.go](../internal/multicluster/rgw_sync_status.go) | [rgw.Multisite.SyncStatus](../internal/multicluster/rgw_sync_status.go#L48) · [rgw.Multisite.WaitSyncReady](../internal/multicluster/rgw_sync_status.go#L66) |

### Check: 보존된 정보·소유 목록·접속 정보 (45개)

| source | 공개 callable |
| --- | --- |
| [internal/cluster/auth.go](../internal/cluster/auth.go) | [ceph.ClientConfig.Name](../internal/cluster/auth.go#L45) · [ceph.ClientConfig.User](../internal/cluster/auth.go#L49) · [ceph.ClientConfig.KeyringPath](../internal/cluster/auth.go#L52) · [ceph.ClientConfig.ConnectionConfig](../internal/cluster/auth.go#L64) |
| [internal/cluster/ceph.go](../internal/cluster/ceph.go) | [ceph.Container.ControlImage](../internal/cluster/ceph.go#L246) · [ceph.Container.NetworkName](../internal/cluster/ceph.go#L332) · [ceph.Container.UsesHostNetwork](../internal/cluster/ceph.go#L343) · [ceph.Container.PublicAddress](../internal/cluster/ceph.go#L346)<br>[ceph.Container.ConnectionConfig](../internal/cluster/ceph.go#L355) · [ceph.Container.ManagerContainer](../internal/cluster/ceph.go#L382) · [ceph.Container.OSDs](../internal/cluster/ceph.go#L713) · [ceph.Container.ServiceContainers](../internal/cluster/ceph.go#L726) |
| [internal/cluster/cephfs.go](../internal/cluster/cephfs.go) | [cephfs.Filesystem.MDSs](../internal/cluster/cephfs.go#L214) |
| [internal/cluster/cephfs_quiesce.go](../internal/cluster/cephfs_quiesce.go) | [cephfs.Quiesce.ID](../internal/cluster/cephfs_quiesce.go#L51) |
| [internal/cluster/client_monitor_config.go](../internal/cluster/client_monitor_config.go) | [ceph.Container.MonitorBootstrapAddresses](../internal/cluster/client_monitor_config.go#L20) |
| [internal/cluster/fencing.go](../internal/cluster/fencing.go) | [ceph.BlocklistOverride.Address](../internal/cluster/fencing.go#L42) |
| [internal/cluster/messenger.go](../internal/cluster/messenger.go) | [ceph.Container.MessengerMode](../internal/cluster/messenger.go#L22) |
| [internal/cluster/network.go](../internal/cluster/network.go) | [ceph.Container.ClusterNetworkName](../internal/cluster/network.go#L26) · [ceph.Container.HasSeparateClusterNetwork](../internal/cluster/network.go#L34) |
| [internal/cluster/placement.go](../internal/cluster/placement.go) | [ceph.OSDContainer.Placement](../internal/cluster/placement.go#L24) |
| [internal/cluster/rbd.go](../internal/cluster/rbd.go) | [rbd.Namespace.Name](../internal/cluster/rbd.go#L31) · [rbd.Namespace.PoolName](../internal/cluster/rbd.go#L34) |
| [internal/cluster/rbd_image_clients.go](../internal/cluster/rbd_image_clients.go) | [rbd.ImageClientStatus.ExclusiveOwner](../internal/cluster/rbd_image_clients.go#L41) |
| [internal/cluster/rgw.go](../internal/cluster/rgw.go) | [rgw.Gateway.DaemonEndpoint](../internal/cluster/rgw.go#L358) · [rgw.Gateway.S3Endpoint](../internal/cluster/rgw.go#L394) |
| [internal/cluster/rgw_admin.go](../internal/cluster/rgw_admin.go) | [rgw.User.ID](../internal/cluster/rgw_admin.go#L67) · [rgw.User.Credentials](../internal/cluster/rgw_admin.go#L76) |
| [internal/cluster/rgw_tenants_accounts.go](../internal/cluster/rgw_tenants_accounts.go) | [rgw.Account.ID](../internal/cluster/rgw_tenants_accounts.go#L68) |
| [internal/cluster/rgw_tls.go](../internal/cluster/rgw_tls.go) | [rgw.Gateway.S3SecureEndpoint](../internal/cluster/rgw_tls.go#L57) |
| [internal/cluster/services.go](../internal/cluster/services.go) | [cephfs.Filesystems](../internal/cluster/services.go#L39) · [rgw.Gateways](../internal/cluster/services.go#L87) · [rgw.GatewaysContext](../internal/cluster/services.go#L96) |
| [internal/cluster/topology.go](../internal/cluster/topology.go) | [ceph.Container.ControlContainer](../internal/cluster/topology.go#L59) · [ceph.Container.Monitors](../internal/cluster/topology.go#L82) · [ceph.Container.Managers](../internal/cluster/topology.go#L97) |
| [internal/cluster/topology_snapshot_context.go](../internal/cluster/topology_snapshot_context.go) | [ceph.Container.ConnectionConfigContext](../internal/cluster/topology_snapshot_context.go#L15) · [ceph.Container.ManagersContext](../internal/cluster/topology_snapshot_context.go#L38) · [ceph.Container.ControlContainerContext](../internal/cluster/topology_snapshot_context.go#L75) |
| [internal/cluster/version.go](../internal/cluster/version.go) | [ceph.Container.CephVersion](../internal/cluster/version.go#L18) |
| [internal/multicluster/cephfs.go](../internal/multicluster/cephfs.go) | [cephfs.Mirror.Daemons](../internal/multicluster/cephfs.go#L473) |
| [internal/multicluster/cephfs_process_quiescence.go](../internal/multicluster/cephfs_process_quiescence.go) | [cephfs.MirrorDaemon.ProcessObserverBindingStatus](../internal/multicluster/cephfs_process_quiescence.go#L45) |
| [internal/multicluster/rbd.go](../internal/multicluster/rbd.go) | [rbd.Mirror.Daemons](../internal/multicluster/rbd.go#L250) |
| [internal/multicluster/rgw_sync_policy.go](../internal/multicluster/rgw_sync_policy.go) | [rgw.SyncGroup.ID](../internal/multicluster/rgw_sync_policy.go#L101) |
| [internal/multicluster/rgw_topology.go](../internal/multicluster/rgw_topology.go) | [rgw.Multisite.Zones](../internal/multicluster/rgw_topology.go#L241) |
| [internal/multicluster/rgw_zonegroups.go](../internal/multicluster/rgw_zonegroups.go) | [rgw.Multisite.Zonegroups](../internal/multicluster/rgw_zonegroups.go#L18) |

### 연결·raw CLI·customizer 접점 (16개)

| source | 공개 callable |
| --- | --- |
| [internal/cluster/auth.go](../internal/cluster/auth.go) | [ceph.Container.WithClientIdentity](../internal/cluster/auth.go#L191) |
| [internal/cluster/ceph.go](../internal/cluster/ceph.go) | [ceph.Container.WithClient](../internal/cluster/ceph.go#L391) · [ceph.Container.Ceph](../internal/cluster/ceph.go#L414) |
| [internal/cluster/idle.go](../internal/cluster/idle.go) | [ceph.WithIdleEntrypoint](../internal/cluster/idle.go#L15) |
| [internal/cluster/options.go](../internal/cluster/options.go) | [ceph.Option.Customize](../internal/cluster/options.go#L102) · [ceph.WithHostAddress](../internal/cluster/options.go#L130) · [ceph.WithOSDImage](../internal/cluster/options.go#L144) · [rgw.WithImage](../internal/cluster/options.go#L156)<br>[cephfs.WithMDSImage](../internal/cluster/options.go#L168) · [ceph.WithStartupTimeout](../internal/cluster/options.go#L296) |
| [internal/cluster/rgw_admin.go](../internal/cluster/rgw_admin.go) | [rgw.Gateway.Admin](../internal/cluster/rgw_admin.go#L122) |
| [internal/multicluster/rbd.go](../internal/multicluster/rbd.go) | [rbd.Mirror.SourceRBD](../internal/multicluster/rbd.go#L575) · [rbd.Mirror.DestinationRBD](../internal/multicluster/rbd.go#L584) |
| [internal/multicluster/rgw.go](../internal/multicluster/rgw.go) | [rgw.Multisite.SourceAdmin](../internal/multicluster/rgw.go#L465) · [rgw.Multisite.DestinationAdmin](../internal/multicluster/rgw.go#L471) |
| [internal/multicluster/rgw_topology.go](../internal/multicluster/rgw_topology.go) | [rgw.Multisite.ZoneAdmin](../internal/multicluster/rgw_topology.go#L271) |

### 조건부 archive helper (4개)

| source | 공개 callable |
| --- | --- |
| [internal/multicluster/rbd_backup.go](../internal/multicluster/rbd_backup.go) | [rbd.ExportBackup](../internal/multicluster/rbd_backup.go#L19) · [rbd.ExportIncremental](../internal/multicluster/rbd_backup.go#L25) · [rbd.RestoreBackup](../internal/multicluster/rbd_backup.go#L34) · [rbd.RestoreIncremental](../internal/multicluster/rbd_backup.go#L40) |

### 로컬 문자열 표현 (16개)

| source | 공개 callable |
| --- | --- |
| [internal/cluster/auth.go](../internal/cluster/auth.go) | [ceph.ClientConfig.String](../internal/cluster/auth.go#L55) · [ceph.ClientConfig.GoString](../internal/cluster/auth.go#L58) |
| [internal/cluster/config.go](../internal/cluster/config.go) | [ceph.ConfigOverride.String](../internal/cluster/config.go#L51) · [ceph.ConfigOverride.GoString](../internal/cluster/config.go#L54) |
| [internal/cluster/rgw_admin.go](../internal/cluster/rgw_admin.go) | [rgw.User.String](../internal/cluster/rgw_admin.go#L83) · [rgw.User.GoString](../internal/cluster/rgw_admin.go#L84) |
| [internal/cluster/rgw_placement.go](../internal/cluster/rgw_placement.go) | [rgw.Placement.String](../internal/cluster/rgw_placement.go#L61) · [rgw.Placement.GoString](../internal/cluster/rgw_placement.go#L62) |
| [internal/cluster/rgw_tenants_accounts.go](../internal/cluster/rgw_tenants_accounts.go) | [rgw.Account.String](../internal/cluster/rgw_tenants_accounts.go#L75) · [rgw.Account.GoString](../internal/cluster/rgw_tenants_accounts.go#L76) |
| [internal/cluster/rgw_tls.go](../internal/cluster/rgw_tls.go) | [rgw.TLSConfig.String](../internal/cluster/rgw_tls.go#L28) · [rgw.TLSConfig.GoString](../internal/cluster/rgw_tls.go#L29) · [rgw.Gateway.String](../internal/cluster/rgw_tls.go#L32) · [rgw.Gateway.GoString](../internal/cluster/rgw_tls.go#L33) |
| [internal/multicluster/rgw_sync_policy.go](../internal/multicluster/rgw_sync_policy.go) | [rgw.SyncGroup.String](../internal/multicluster/rgw_sync_policy.go#L107) · [rgw.SyncGroup.GoString](../internal/multicluster/rgw_sync_policy.go#L108) |
<!-- callables:end -->

## Config·타입·option 계약

Option 함수는 callable 목록에 포함한다. Config/result 구조체의 field 수백 개는
이 문서에 다시 나열하지 않는다. 전체 schema는 아래 source에서 확인한다.

<!-- schemas:begin -->
공개 타입은 `ceph` 47개, `cephfs` 49개, `rgw` 38개, `rbd` 17개다. 서비스 패키지의 타입은 구현 타입의 alias이므로 아래 source에서 config/result 전체 field와 각 주석 계약을 읽는다.

| source | 타입·schema |
| --- | --- |
| [internal/cluster/auth.go](../internal/cluster/auth.go) | [ceph.ClientCaps](../internal/cluster/auth.go#L24) · [ceph.ClientConfig](../internal/cluster/auth.go#L34) |
| [internal/cluster/ceph.go](../internal/cluster/ceph.go) | [ceph.Container](../internal/cluster/ceph.go#L36) · [ceph.OSDContainer](../internal/cluster/ceph.go#L76) |
| [internal/cluster/cephfs.go](../internal/cluster/cephfs.go) | [cephfs.Config](../internal/cluster/cephfs.go#L24) · [cephfs.MDS](../internal/cluster/cephfs.go#L41) · [cephfs.Filesystem](../internal/cluster/cephfs.go#L52) · [cephfs.MDSStatus](../internal/cluster/cephfs.go#L72)<br>[cephfs.FilesystemStatus](../internal/cluster/cephfs.go#L82) |
| [internal/cluster/cephfs_authorization.go](../internal/cluster/cephfs_authorization.go) | [cephfs.SubvolumeAuthorizationConfig](../internal/cluster/cephfs_authorization.go#L16) · [cephfs.SubvolumeAuthorization](../internal/cluster/cephfs_authorization.go#L26) · [cephfs.SubvolumeAuthorizedClient](../internal/cluster/cephfs_authorization.go#L41) |
| [internal/cluster/cephfs_data_pools.go](../internal/cluster/cephfs_data_pools.go) | [cephfs.DataPoolState](../internal/cluster/cephfs_data_pools.go#L16) · [cephfs.DataPool](../internal/cluster/cephfs_data_pools.go#L25) |
| [internal/cluster/cephfs_pin.go](../internal/cluster/cephfs_pin.go) | [cephfs.PinType](../internal/cluster/cephfs_pin.go#L16) · [cephfs.PinSetting](../internal/cluster/cephfs_pin.go#L29) · [cephfs.PinPolicy](../internal/cluster/cephfs_pin.go#L40) · [cephfs.PinOverride](../internal/cluster/cephfs_pin.go#L56) |
| [internal/cluster/cephfs_quiesce.go](../internal/cluster/cephfs_quiesce.go) | [cephfs.QuiesceConfig](../internal/cluster/cephfs_quiesce.go#L18) · [cephfs.QuiesceState](../internal/cluster/cephfs_quiesce.go#L24) · [cephfs.Quiesce](../internal/cluster/cephfs_quiesce.go#L37) |
| [internal/cluster/cephfs_session.go](../internal/cluster/cephfs_session.go) | [cephfs.SessionTimeouts](../internal/cluster/cephfs_session.go#L23) · [cephfs.Session](../internal/cluster/cephfs_session.go#L33) · [cephfs.SessionTimeoutsOverride](../internal/cluster/cephfs_session.go#L48) |
| [internal/cluster/cephfs_snapshot.go](../internal/cluster/cephfs_snapshot.go) | [cephfs.SubvolumeSnapshot](../internal/cluster/cephfs_snapshot.go#L18) · [cephfs.SnapshotPendingClone](../internal/cluster/cephfs_snapshot.go#L25) · [cephfs.SubvolumeSnapshotInfo](../internal/cluster/cephfs_snapshot.go#L32) · [cephfs.CloneConfig](../internal/cluster/cephfs_snapshot.go#L43)<br>[cephfs.SubvolumeClone](../internal/cluster/cephfs_snapshot.go#L51) · [cephfs.SubvolumeCloneStatus](../internal/cluster/cephfs_snapshot.go#L59) |
| [internal/cluster/cephfs_subvolume.go](../internal/cluster/cephfs_subvolume.go) | [cephfs.SubvolumeGroupConfig](../internal/cluster/cephfs_subvolume.go#L19) · [cephfs.SubvolumeConfig](../internal/cluster/cephfs_subvolume.go#L30) · [cephfs.SubvolumeGroup](../internal/cluster/cephfs_subvolume.go#L42) · [cephfs.Subvolume](../internal/cluster/cephfs_subvolume.go#L53)<br>[cephfs.SubvolumeGroupInfo](../internal/cluster/cephfs_subvolume.go#L65) · [cephfs.SubvolumeInfo](../internal/cluster/cephfs_subvolume.go#L78) |
| [internal/cluster/config.go](../internal/cluster/config.go) | [ceph.ConfigSetting](../internal/cluster/config.go#L19) · [ceph.ConfigEntry](../internal/cluster/config.go#L25) · [ceph.ConfigOverride](../internal/cluster/config.go#L38) |
| [internal/cluster/diagnostics.go](../internal/cluster/diagnostics.go) | [ceph.DiagnosticsConfig](../internal/cluster/diagnostics.go#L33) · [ceph.DiagnosticsContainer](../internal/cluster/diagnostics.go#L46) · [ceph.DiagnosticArtifact](../internal/cluster/diagnostics.go#L55) · [ceph.DiagnosticsReport](../internal/cluster/diagnostics.go#L68) |
| [internal/cluster/fencing.go](../internal/cluster/fencing.go) | [ceph.BlocklistEntry](../internal/cluster/fencing.go#L20) · [ceph.BlocklistOverride](../internal/cluster/fencing.go#L31) |
| [internal/cluster/full_ratios.go](../internal/cluster/full_ratios.go) | [ceph.FullRatios](../internal/cluster/full_ratios.go#L17) · [ceph.FullRatioSnapshot](../internal/cluster/full_ratios.go#L24) · [ceph.FullRatiosOverride](../internal/cluster/full_ratios.go#L58) |
| [internal/cluster/health_details.go](../internal/cluster/health_details.go) | [ceph.HealthSnapshot](../internal/cluster/health_details.go#L21) · [ceph.HealthCheck](../internal/cluster/health_details.go#L29) · [ceph.HealthMute](../internal/cluster/health_details.go#L41) |
| [internal/cluster/messenger.go](../internal/cluster/messenger.go) | [ceph.MessengerMode](../internal/cluster/messenger.go#L8) |
| [internal/cluster/mgr_modules.go](../internal/cluster/mgr_modules.go) | [ceph.MGRModuleState](../internal/cluster/mgr_modules.go#L19) · [ceph.MGRModuleOverride](../internal/cluster/mgr_modules.go#L42) |
| [internal/cluster/network.go](../internal/cluster/network.go) | [ceph.NetworkPlane](../internal/cluster/network.go#L17) · [ceph.NetworkInterruption](../internal/cluster/network.go#L81) |
| [internal/cluster/object_watchers.go](../internal/cluster/object_watchers.go) | [ceph.ObjectWatcher](../internal/cluster/object_watchers.go#L17) |
| [internal/cluster/options.go](../internal/cluster/options.go) | [ceph.Option](../internal/cluster/options.go#L99) |
| [internal/cluster/osd_policy.go](../internal/cluster/osd_policy.go) | [ceph.OSDState](../internal/cluster/osd_policy.go#L20) · [ceph.OSDFlagOverride](../internal/cluster/osd_policy.go#L116) |
| [internal/cluster/pause.go](../internal/cluster/pause.go) | [ceph.ContainerPause](../internal/cluster/pause.go#L24) |
| [internal/cluster/placement.go](../internal/cluster/placement.go) | [ceph.OSDConfig](../internal/cluster/placement.go#L16) |
| [internal/cluster/pool.go](../internal/cluster/pool.go) | [ceph.PoolConfig](../internal/cluster/pool.go#L18) · [ceph.ErasureCodeConfig](../internal/cluster/pool.go#L39) · [ceph.Pool](../internal/cluster/pool.go#L48) |
| [internal/cluster/pool_pgs.go](../internal/cluster/pool_pgs.go) | [ceph.PoolPGSnapshot](../internal/cluster/pool_pgs.go#L17) · [ceph.PGState](../internal/cluster/pool_pgs.go#L31) · [ceph.PGStats](../internal/cluster/pool_pgs.go#L45) |
| [internal/cluster/pool_policy.go](../internal/cluster/pool_policy.go) | [ceph.PoolQuota](../internal/cluster/pool_policy.go#L16) · [ceph.PoolState](../internal/cluster/pool_policy.go#L26) |
| [internal/cluster/pool_relocation.go](../internal/cluster/pool_relocation.go) | [ceph.PoolPlacement](../internal/cluster/pool_relocation.go#L17) |
| [internal/cluster/pool_usage.go](../internal/cluster/pool_usage.go) | [ceph.PoolUsageSnapshot](../internal/cluster/pool_usage.go#L23) |
| [internal/cluster/rbd.go](../internal/cluster/rbd.go) | [rbd.Namespace](../internal/cluster/rbd.go#L15) |
| [internal/cluster/rbd_image_clients.go](../internal/cluster/rbd_image_clients.go) | [rbd.ImageWatcher](../internal/cluster/rbd_image_clients.go#L15) · [rbd.ImageLock](../internal/cluster/rbd_image_clients.go#L24) · [rbd.ImageClientStatus](../internal/cluster/rbd_image_clients.go#L34) |
| [internal/cluster/rgw.go](../internal/cluster/rgw.go) | [rgw.Gateway](../internal/cluster/rgw.go#L24) · [rgw.Config](../internal/cluster/rgw.go#L43) |
| [internal/cluster/rgw_admin.go](../internal/cluster/rgw_admin.go) | [rgw.UserConfig](../internal/cluster/rgw_admin.go#L25) · [rgw.User](../internal/cluster/rgw_admin.go#L37) · [rgw.Quota](../internal/cluster/rgw_admin.go#L90) · [rgw.AdminCapability](../internal/cluster/rgw_admin.go#L96)<br>[rgw.UserInfo](../internal/cluster/rgw_admin.go#L105) |
| [internal/cluster/rgw_placement.go](../internal/cluster/rgw_placement.go) | [rgw.StorageClassConfig](../internal/cluster/rgw_placement.go#L25) · [rgw.PlacementConfig](../internal/cluster/rgw_placement.go#L34) · [rgw.Placement](../internal/cluster/rgw_placement.go#L51) · [rgw.PlacementState](../internal/cluster/rgw_placement.go#L67) |
| [internal/cluster/rgw_tenants_accounts.go](../internal/cluster/rgw_tenants_accounts.go) | [rgw.AccountConfig](../internal/cluster/rgw_tenants_accounts.go#L47) · [rgw.Account](../internal/cluster/rgw_tenants_accounts.go#L55) · [rgw.AccountInfo](../internal/cluster/rgw_tenants_accounts.go#L81) |
| [internal/cluster/rgw_tls.go](../internal/cluster/rgw_tls.go) | [rgw.TLSConfig](../internal/cluster/rgw_tls.go#L24) |
| [internal/cluster/rgw_usage.go](../internal/cluster/rgw_usage.go) | [rgw.UserUsage](../internal/cluster/rgw_usage.go#L21) |
| [internal/cluster/rgw_user_placement.go](../internal/cluster/rgw_user_placement.go) | [rgw.UserPlacementConfig](../internal/cluster/rgw_user_placement.go#L18) |
| [internal/cluster/scrub.go](../internal/cluster/scrub.go) | [ceph.InconsistentObject](../internal/cluster/scrub.go#L18) · [ceph.InconsistentShard](../internal/cluster/scrub.go#L27) |
| [internal/cluster/status.go](../internal/cluster/status.go) | [ceph.Status](../internal/cluster/status.go#L11) |
| [internal/cluster/topology.go](../internal/cluster/topology.go) | [ceph.MonitorContainer](../internal/cluster/topology.go#L26) · [ceph.ManagerContainer](../internal/cluster/topology.go#L32) · [ceph.QuorumStatus](../internal/cluster/topology.go#L109) · [ceph.ManagerStatus](../internal/cluster/topology.go#L878) |
| [internal/multicluster/cephfs.go](../internal/multicluster/cephfs.go) | [cephfs.MirrorConfig](../internal/multicluster/cephfs.go#L26) · [cephfs.MirrorDaemon](../internal/multicluster/cephfs.go#L48) · [cephfs.Mirror](../internal/multicluster/cephfs.go#L82) |
| [internal/multicluster/cephfs_directory_addition.go](../internal/multicluster/cephfs_directory_addition.go) | [cephfs.MirrorDirectoryAddition](../internal/multicluster/cephfs_directory_addition.go#L16) · [cephfs.MirrorDirectoryAdditionStatus](../internal/multicluster/cephfs_directory_addition.go#L27) |
| [internal/multicluster/cephfs_directory_removal.go](../internal/multicluster/cephfs_directory_removal.go) | [cephfs.MirrorDirectoryRemoval](../internal/multicluster/cephfs_directory_removal.go#L18) · [cephfs.MirrorDirectoryRemovalStatus](../internal/multicluster/cephfs_directory_removal.go#L30) · [cephfs.MirrorDirectoryRemovalDaemonStatus](../internal/multicluster/cephfs_directory_removal.go#L38) |
| [internal/multicluster/cephfs_directory_status.go](../internal/multicluster/cephfs_directory_status.go) | [cephfs.MirrorSnapshot](../internal/multicluster/cephfs_directory_status.go#L18) · [cephfs.MirrorDirectoryStatus](../internal/multicluster/cephfs_directory_status.go#L32) |
| [internal/multicluster/cephfs_peer_removal.go](../internal/multicluster/cephfs_peer_removal.go) | [cephfs.MirrorPeerRemoval](../internal/multicluster/cephfs_peer_removal.go#L22) · [cephfs.MirrorPeerRemovalStatus](../internal/multicluster/cephfs_peer_removal.go#L43) · [cephfs.MirrorPeerRemovalDaemonStatus](../internal/multicluster/cephfs_peer_removal.go#L51) |
| [internal/multicluster/cephfs_process_quiescence.go](../internal/multicluster/cephfs_process_quiescence.go) | [cephfs.MirrorProcessBindingStatus](../internal/multicluster/cephfs_process_quiescence.go#L38) · [cephfs.MirrorProcessQuiescenceStatus](../internal/multicluster/cephfs_process_quiescence.go#L270) · [cephfs.MirrorOriginalProcessStatus](../internal/multicluster/cephfs_process_quiescence.go#L279) |
| [internal/multicluster/cephfs_process_quiescence_acknowledgment.go](../internal/multicluster/cephfs_process_quiescence_acknowledgment.go) | [cephfs.MirrorProcessQuiescenceAcknowledgment](../internal/multicluster/cephfs_process_quiescence_acknowledgment.go#L13) |
| [internal/multicluster/rbd.go](../internal/multicluster/rbd.go) | [rbd.MirrorMode](../internal/multicluster/rbd.go#L25) · [rbd.MirrorConfig](../internal/multicluster/rbd.go#L35) · [rbd.Mirror](../internal/multicluster/rbd.go#L66) · [rbd.MirrorDaemon](../internal/multicluster/rbd.go#L88)<br>[rbd.MirrorDaemonStatus](../internal/multicluster/rbd.go#L102) · [rbd.MirrorPoolReplayerStatus](../internal/multicluster/rbd.go#L109) |
| [internal/multicluster/rbd_image_status.go](../internal/multicluster/rbd_image_status.go) | [rbd.MirrorImageStatus](../internal/multicluster/rbd_image_status.go#L22) |
| [internal/multicluster/rbd_namespace.go](../internal/multicluster/rbd_namespace.go) | [rbd.MirrorScope](../internal/multicluster/rbd_namespace.go#L19) · [rbd.MirrorNamespaceState](../internal/multicluster/rbd_namespace.go#L30) · [rbd.MirrorPolicies](../internal/multicluster/rbd_namespace.go#L37) |
| [internal/multicluster/rbd_namespace_binding.go](../internal/multicluster/rbd_namespace_binding.go) | [rbd.MirrorNamespace](../internal/multicluster/rbd_namespace_binding.go#L17) |
| [internal/multicluster/rbd_receiver_status.go](../internal/multicluster/rbd_receiver_status.go) | [rbd.MirrorReceiverStatus](../internal/multicluster/rbd_receiver_status.go#L21) · [rbd.MirrorReceiverDaemonStatus](../internal/multicluster/rbd_receiver_status.go#L36) |
| [internal/multicluster/rgw.go](../internal/multicluster/rgw.go) | [rgw.MultisiteConfig](../internal/multicluster/rgw.go#L23) · [rgw.Multisite](../internal/multicluster/rgw.go#L33) |
| [internal/multicluster/rgw_sync_bucket_status.go](../internal/multicluster/rgw_sync_bucket_status.go) | [rgw.SyncBucketIdentity](../internal/multicluster/rgw_sync_bucket_status.go#L18) · [rgw.BucketSyncStatus](../internal/multicluster/rgw_sync_bucket_status.go#L25) |
| [internal/multicluster/rgw_sync_pipe.go](../internal/multicluster/rgw_sync_pipe.go) | [rgw.SyncBucketSelector](../internal/multicluster/rgw_sync_pipe.go#L21) · [rgw.SyncObjectTag](../internal/multicluster/rgw_sync_pipe.go#L26) |
| [internal/multicluster/rgw_sync_policy.go](../internal/multicluster/rgw_sync_policy.go) | [rgw.SyncPolicyScope](../internal/multicluster/rgw_sync_policy.go#L25) · [rgw.SyncGroupStatus](../internal/multicluster/rgw_sync_policy.go#L30) · [rgw.SyncGroupConfig](../internal/multicluster/rgw_sync_policy.go#L38) · [rgw.SyncFlowConfig](../internal/multicluster/rgw_sync_policy.go#L48)<br>[rgw.SyncPipeConfig](../internal/multicluster/rgw_sync_policy.go#L62) · [rgw.SyncGroup](../internal/multicluster/rgw_sync_policy.go#L80) |
| [internal/multicluster/rgw_sync_policy_ready.go](../internal/multicluster/rgw_sync_policy_ready.go) | [rgw.BucketSyncPolicyStatus](../internal/multicluster/rgw_sync_policy_ready.go#L17) |
| [internal/multicluster/rgw_sync_status.go](../internal/multicluster/rgw_sync_status.go) | [rgw.MetadataSyncStatus](../internal/multicluster/rgw_sync_status.go#L17) · [rgw.DataSyncStatus](../internal/multicluster/rgw_sync_status.go#L28) · [rgw.SyncStatus](../internal/multicluster/rgw_sync_status.go#L35) |
| [internal/multicluster/rgw_topology.go](../internal/multicluster/rgw_topology.go) | [rgw.ZoneConfig](../internal/multicluster/rgw_topology.go#L19) · [rgw.ZonegroupConfig](../internal/multicluster/rgw_topology.go#L27) · [rgw.TopologyConfig](../internal/multicluster/rgw_topology.go#L39) · [rgw.Zone](../internal/multicluster/rgw_topology.go#L50)<br>[rgw.Zonegroup](../internal/multicluster/rgw_topology.go#L58) |
<!-- schemas:end -->
