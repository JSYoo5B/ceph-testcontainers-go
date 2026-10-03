# Ceph client 테스트 fixture의 완료 기준

목표는 RADOS·RBD·CephFS·RGW client가 필요로 하는 서버 상태를 testcontainers로 준비하고, 장애·복구·복제 조건을 재현하는 것입니다. 공개 모듈은 go-ceph/cgo를 사용하지 않습니다. 서버 준비는 container의 CLI·파일로 수행하고, 실제 client 동작은 Linux container의 native library 또는 S3 SDK로 검증합니다.

참고 범위는 [go-ceph v0.41.0](https://github.com/ceph/go-ceph/tree/v0.41.0)의 rados, rbd, cephfs 및 admin package와 Ceph Tentacle의 서버 관리 API입니다. client의 object/image/file CRUD마다 동일한 Go wrapper를 만드는 것은 완료 조건이 아닙니다. 그 API를 테스트할 수 있도록 인증·pool·namespace·daemon·module·policy를 구성할 수 있는지가 조건입니다. client operation도 대표 동작을 실행해 서버 준비가 충분한지 확인합니다.

각 항목은 다음 네 조건을 모두 충족해야 완료로 표시합니다.

1. 공개 typed API 또는 그대로 실행 가능한 공개 API 조합 예제가 있습니다.
2. 명령 성공뿐 아니라 native 서버 상태·identity를 확인합니다.
3. 실제 client의 성공·거부·장애·복구 또는 복제 데이터를 확인합니다.
4. 원래 상태 복원 또는 owned resource/일회성 cluster 정리를 확인합니다.

tag compile·단위 테스트 통과만으로 완료로 표시하지 않습니다. `Ceph`, `RBD`, `Admin`, container `Exec` escape hatch가 있다는 사실만으로 미제공 항목을 닫지 않습니다. API 조합이 충분한 경우에도 해당 recipe와 실제 검증이 필요합니다. partial/lost reply를 처리할 수 없거나 외부 리소스를 adopt/삭제하는 구현은 완료 조건을 충족하지 못합니다.

## 제공 기준과 진행 상태

`완료`는 연결된 기존 PoC의 대표 조건을 충족한 항목입니다. 모든 parameter 조합을 실행했다는 뜻은 아닙니다. `검증 중`은 API·테스트 구현 후 실제 Docker 검증을 진행 중이며, `개발 중`과 `미완료`는 목표에 남아 있습니다. 현재 전체 목표는 진행 중입니다.

| ID | client 테스트에 필요한 서버 준비 | 공개 제공 경로 / 검증 기준 | 상태 |
| --- | --- | --- | --- |
| T01 | Linux native client 설정·key·namespace 연결 | `WithClient`, `WithClientIdentity`, `ConnectionConfig`; 별도 go-ceph 소비자 module에서 실제 RADOS/RBD/CephFS I/O | 완료: [Linux client](HOST_NETWORK_POC.md), `TestGoCephLinux` |
| T02 | 단일·독립 다중 cluster, MON quorum·MGR HA, public/cluster/peer network, 노드 lifecycle | `Run` topology options와 daemon API; advertised address, failover, 기존 데이터 유지 | 완료: [구성 기준](CLUSTER_SCENARIOS.md), [추가 topology](TOPOLOGY_EXTENSIONS.md) |
| T03 | replicated pool, CRUSH host/rack/root/device class, replica/quota 정책 | `CreatePool`, OSD placement, pool policy; 실제 분산 배치·read/write·quota 거부 | 완료: `TestHostFailureDomainPool`, `TestPoolPolicies` |
| T04 | EC RADOS/RBD/CephFS data pool | `CreatePool` EC/overwrite policy와 서비스별 pool 선택; 실제 데이터·metadata pool 분리 | 완료: `TestErasureCodedPools`, `TestCephFSAdditionalErasureCodedDataPool`; 새 동적 경로는 F05 |
| T05 | Cephx 읽기/쓰기/revoke와 RADOS namespace 격리 | owned `ClientIdentity` caps·key·client config; 두 principal의 허용·거부 | 완료: `TestClientIdentities`; subvolume 특화는 F06 |
| T06 | RADOS compound/xattr/omap/watch-notify/striper/object snapshot 테스트 환경 | 기존 pool·namespace·둘 이상의 client 조합 recipe; native compound atomicity, notification, snapshot/striped payload | 완료: `TestRADOSClientFixtures`, 아래 실행 recipe |
| T07 | OSD object class 실행 환경과 slim closure | OSD의 `rados-classes` 및 native `Exec`; `cls_hello` 실제 호출·결과 검증 | 완료: `TestRADOSClientFixtures`, 실제 `Hello, fixture!` 반환 |
| T08 | 정확한 client nonce fencing과 TTL | `TemporaryBlocklist`, `BlocklistEntries`, `Restore`; 같은 host의 다른 session 유지, ESHUTDOWN·해제·만료 | 완료: `TestClientFencing`, bridge/host |
| R01 | RBD metadata 초기화·namespace | `InitRBDPool`, `CreateRBDNamespace`; image/namespace 분리와 RO/RW client 효과 | 완료: `TestRBDNamespaces` |
| R02 | RBD image 기능을 테스트할 기본 구성 | R01 + client recipe: layering/clone/flatten, trash/migration/group, encryption 및 lock 테스트 | 검증 중: [client recipe](RBD_CLIENT_FIXTURES.md), `TestRBDClientFeatures` |
| R03 | MGR module membership·dependency·readiness·restore | `MGRModules`, `TemporaryMGRModule`, `WaitMGRModuleReady`; always-on/사용 중 보호·native command probe | 완료: `TestMGRModules`, bridge/host |
| R04 | RBD task queue와 mirror snapshot schedule | `rbd_support` + initialized pool/mirrored image; task 완료와 실제 자동 mirror snapshot·destination bytes | 완료: `TestMGRModules`, `TestRBDAutomaticSnapshotSchedule`, bridge/host |
| R05 | snapshot/journal mirror, peer/daemon HA, fanout/failback/split-brain/resync, backup | `multicluster` mirror/backup API + client checkpoint; peer fault 복구와 destination data | 완료: [다중 cluster](MULTICLUSTER_POC.md), topology 추가 검증 |
| R06 | pool 전체 mirror와 local/remote namespace mapping | pool mode의 journal image 자동 편입, ns-a→ns-b와 default namespace 조합, 다른 namespace 제외 | 완료: `TestMultiClusterRBDMirrorScopeAndNamespaces`, bridge/host 각 5조합 |
| F01 | named/multiple FS, multi-active MDS·standby/replay, failover | `WithCephFS`, `StartCephFSWithConfig`, `ScaleMDS`; rank/native map·POSIX 데이터 유지 | 완료: topology 검증 |
| F02 | group/subvolume quota·layout·RADOS namespace | owned provisioning API; native quota/layout와 client 쓰기 거부·격리 | 완료: `TestCephFSSubvolumes` |
| F03 | subvolume snapshot·async clone·wait 재시도 | owned snapshot/clone API; frozen bytes, quota·namespace 상속, pending source 보호 | 완료: `TestCephFSSubvolumeSnapshotsAndClones` |
| F04 | canceled/failed clone의 명시적 lifecycle | cancel·partial cleanup·pending source reference 해제, source bytes 보존, identity/readback | 완료: `TestCephFSCloneCancellationAndPartialCleanup`, bridge/host canceled·failed 모두 |
| F05 | 실행 중 FS data pool 추가와 provisioning 연동 | `AddDataPool`, `DataPools`, `RemoveUnusedDataPool`; replicated/EC에 subvolume·clone, native detach/replacement 거부 | 완료: `TestCephFSDynamicDataPools`, bridge/host |
| F06 | subvolume authorize/deauthorize·authorized list·session eviction | fresh 제한 principal; filesystem path와 OSD pool/namespace 모두 제한, RO/RW/revoke와 다른 subvolume 거부 | 완료: `TestCephFSSubvolumeClientAuthorization`, bridge/host |
| F07 | 일관된 checkpoint용 quiesce/release | owned set/version/members·deadline/TTL; 두 client의 쓰기 정지·해제·timeout 복구 | 완료: `TestCephFSQuiesceCheckpoints`, bridge/host |
| F08 | export/distributed/random pin | group/subvolume pin; multi-active MDS의 실제 subtree ownership과 정책 복원 | 완료: `TestCephFSPins`, bridge/host |
| F09 | subvolume/snapshot metadata와 retained snapshot provisioning | native metadata recipe·snapshot retention 후 source cleanup/recovery | 완료: `TestCephFSRetainedSnapshotAndMetadataRecipe`, bridge/host |
| F10 | snapshot mirror·directory peer·daemon 분산·backup | `multicluster` CephFS mirror/backup API; 실제 remote frozen bytes와 daemon fault 복구 | 완료: 기존 multicluster/topology PoC |
| G01 | RGW 사용자·AdminOps caps·quota·suspend·safe cleanup | owned user API; S3/AdminOps 허용·거부, quota, key 보존, bucket가 있는 user 삭제 거부 | 완료: `TestRGWUserAdministration` |
| G02 | named placement/storage class, replicated/EC data, realm activation | `CreatePlacement`, `ApplyPlacement`, `ReloadPlacement` + destination period pull; 실제 class와 RADOS pool payload | 완료: [fixture 확장](CLUSTER_FIXTURE_EXTENSIONS.md) |
| G03 | user default target/class와 placement tags | `SetUserPlacement`; 새 bucket 허용·거부·기존 bucket 유지·실제 선택 pool | 완료: `TestRGWUserPlacementPolicy`, host variant |
| G04 | tenant 및 account-root fixture/account quota | 같은 uid·bucket 이름의 tenant 격리, fresh account/root credentials·aggregate quota·cleanup | 완료: `TestRGWTenantsAndAccounts`, host variant |
| G05 | multisite selective replication의 owned 구성 | group/flow/pipe 구성·제거, bucket/prefix/tag 허용·거부, bucket/owner translation과 user mode | 검증 중: owned same-bucket/system-mode API 준비; translation/tag/user recipe는 추가 예정 |
| G06 | 기존 bucket 유지보수 조건 | 개별 bucket quota, reshard/queue·readiness; 실제 S3 payload 보존·quota 거부 | 완료: `TestRGWBucketMaintenance`, bridge/host |
| G07 | period 및 metadata/data/bucket sync 관측·bounded readiness | exact local committed period, 실제 destination checkpoint·bytes와 native sync 상태 조합 | 개발 중: `SyncStatus`, `WaitSyncReady` |
| G08 | S3 client 기능의 서버 조건 | versioning/multipart/lifecycle/object-lock/bucket policy/ACL client recipe와 필요한 daemon 옵션, 대표 동작 | 완료: `TestRGWS3ClientFeatures`, bridge/host; IAM role은 G09 |
| G09 | STS/Swift 및 암호화 backend 테스트 조건 | STS shared key와 role credential, Swift principal/endpoint, TLS·KMS endpoint 조합과 실제 consumer effect | 검증 중: `TestRGWNativeTLS` bridge/host 완료; protocol/backend recipe native 응답 보강 후 재실행 |
| G10 | realm/zonegroup/master 전환·recovery | `multicluster` zone/peer/period API; master failover 및 metadata/data 복구 | 완료: 기존 multicluster/topology PoC |
| G11 | AdminOps usage log와 bucket rate-limit 준비 | pre-start usage log·flush 조건, scoped admin caps·owned log trim과 별도 request rate 거부·복구 | 개발 중: 공개 설정·user API 조합 recipe |
| D01 | runtime 설정·OSD out/in·global 장애 flag·PG 복구 | `TemporaryConfig`, `SetOSDIn`, `TemporaryOSDFlag`, `WaitForPGClean`; native 상태와 I/O·restore | 완료: [fixture 확장](CLUSTER_FIXTURE_EXTENSIONS.md) |

## 책임과 실제 제한

단일 cluster의 pool·principal·module·FS/RGW policy는 `ceph`, cluster 사이의 peer·namespace mapping·realm/period 전달·선택적 복제는 `multicluster`가 담당합니다. 선택적 sync와 schedule처럼 기존 cluster 안에 서버 상태를 만드는 기능도 목적에 따라 해당 package에 둡니다. 실행 이미지에 필요한 native module/object class가 없으면 연결 옵션만으로 가능하다고 표시하지 않습니다.

CephFS additional pool 제거는 native FSMap에서 attachment를 해제합니다. native 명령은 POSIX file layout reference를 전부 검사하지 않으므로 `RemoveUnusedDataPool`은 helper가 사용한 적 없는 attachment, native provisioning 미사용, 모든 RADOS namespace의 빈 pool만 허용합니다. 외부 layout 설정·writer는 caller가 중지해야 합니다. detach는 pool·data·application tag를 삭제하지 않으며 남은 native tag 때문에 재등록에 별도 조치가 필요할 수 있습니다.

RGW CLI의 빈 user placement tags는 기존 tags를 비우지 않습니다. `SetUserPlacement`는 nil로 유지하거나 비어 있지 않은 목록으로 교체하고, 빈 목록 요청은 거부합니다. target 권한 취소는 matching tag를 nonmatching tag로 바꿔 확인합니다. 기존 bucket의 immutable placement와 object 권한을 이 설정의 효과로 혼동하지 않습니다. [RGW placement의 native 계약](https://docs.ceph.com/en/tentacle/radosgw/placement/).

MGR의 configured enabled와 active manager command readiness는 다릅니다. always-on module을 force-disable하여 복원을 가장하지 않습니다. readiness helper는 `rbd_support`와 `volumes`의 실제 CLI를 probe하며 다른 module은 소비할 command를 caller가 probe해야 합니다. [MGR 관리](https://docs.ceph.com/en/tentacle/mgr/administrator/).

NFS·SMB·NVMe-oF gateway, cephadm/systemd/LVM/실제 disk, kernel-only mount, hardware 성능·production upgrade 검증은 현재 네 역할 프로토콜 fixture와 구분합니다. 해당 gateway topology는 추가 daemon/image 계약이 필요한 별도 범위입니다. RADOS/RBD/CephFS/RGW client API의 서버 조건을 그 이유로 누락하지 않습니다.

## 조사와 실행 경로

새 대표 시나리오는 `make client-fixtures`로 순차 실행합니다. role 이미지 환경 변수는 [fixture 확장 문서](CLUSTER_FIXTURE_EXTENSIONS.md)의 실행 설정을 사용합니다.

RADOS client fixture의 [실행 가능한 public composition](../internal/integration/rados_client_fixtures_integration_test.go)은 `Run` → `CreatePool(Application: "rados")` → `WithClient`로 연결한 Linux consumer 두 개 → 각 consumer의 namespace 선택 순서입니다. 하나의 pool에 SDK에서 namespace를 선택하면 compound/xattr/omap/watch-notify/pool snapshot을 테스트할 수 있습니다. striper는 consumer의 `rados --striper`와 native libradosstriper가 추가로 필요하고 OSD role에는 실제 object class shared libraries가 있어야 합니다. 테스트는 다른 namespace가 섞이지 않는지와 compound 비교 실패 후 데이터 유지, 실제 notification, snapshot frozen bytes, 3개의 stripe object와 SHA256, `cls_hello` 결과 및 object cleanup까지 실행합니다. 명령을 구현한 새 CRUD wrapper는 필요하지 않습니다.

- [go-ceph admin 및 native consumer 소스](https://github.com/ceph/go-ceph/tree/v0.41.0): module/task/schedule, fencing, subvolume authorization/quiesce/pin/clone, RGW user/account 기준
- [CephFS volumes](https://docs.ceph.com/en/tentacle/cephfs/fs-volumes/): provisioning lifecycle의 기준
- [RBD mirroring](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/): pool scope·namespace 및 daemon 조건
- [RGW tenant](https://docs.ceph.com/en/tentacle/radosgw/multitenancy/), [account](https://docs.ceph.com/en/tentacle/radosgw/account/), [selective sync](https://docs.ceph.com/en/tentacle/radosgw/multisite-sync-policy/): server fixture 기준

기존 완료 항목은 연결된 문서의 실제 실행 기록을 사용하며 새 기능을 추가한 뒤 관계없는 모든 Docker 시나리오를 매번 재실행하지는 않습니다. 변경한 public composition 경로는 bridge/host 대표 실행으로 검증합니다. 단위·race·전체 tag compile/vet는 변경된 library의 회귀를 확인하며, Docker 로그와 native 관측 자료는 ignored `artifacts/`에 저장합니다.
