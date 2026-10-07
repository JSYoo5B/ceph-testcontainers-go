# Ceph client 테스트 fixture의 완료 기준

목표는 RADOS·RBD·CephFS·RGW client가 필요로 하는 서버 상태를 testcontainers로 준비하고, 장애·복구·복제 조건을 재현하는 것입니다. 공개 모듈은 go-ceph/cgo를 사용하지 않습니다. 서버 준비는 container의 CLI·파일로 수행하고, 실제 client 동작은 Linux container의 native library 또는 S3 SDK로 검증합니다.

기본 지원과 필수 검증 기준은 `ceph.DefaultImage`의 digest로 고정한 원본 Quay Ceph 20.2.4입니다. 프로젝트 사용이나 기본 검증을 위해 Ceph native build 또는 새 서버 이미지 생성을 요구하지 않습니다. [ceph-testcontainers-images](../../ceph-testcontainers-images/README.md)는 고정된 이미지 요구사항, 주어진 로컬 이미지의 quick/full checker, 공식 이미지 역할별 추출과 배포판 패키지 기반 역할 이미지를 제공합니다. 회사 패키지·native 패치·소비자 프로그램의 제작은 각 이미지 소유자의 책임입니다. 이 Go 프로젝트는 준비된 서버/소비자 이미지를 명시적으로 받아 구성·인증·bootstrap·entrypoint와 lifecycle을 제공합니다. 원본 서버에서 동작하지 않는 조건은 native 한계로 기록하며, custom image의 통과를 원본 이미지의 완료 증거로 채택하지 않습니다. Linux 개발 헤더·go-ceph 프로그램·cryptsetup 등 소비자 테스트의 의존성은 서버 이미지의 필수 요구사항이 아닙니다.

참고 범위는 [go-ceph v0.41.0](https://github.com/ceph/go-ceph/tree/v0.41.0)의 rados, rbd, cephfs 및 admin package와 Ceph Tentacle의 서버 관리 API입니다. client의 object/image/file CRUD마다 동일한 Go wrapper를 만드는 것은 완료 조건이 아닙니다. 그 API를 테스트할 수 있도록 인증·pool·namespace·daemon·module·policy를 구성할 수 있는지가 조건입니다. client operation도 대표 동작을 실행해 서버 준비가 충분한지 확인합니다.

2026-10-05의 검증 목표는 기존 토폴로지와 client용 서버 fixture를 합한 필수 상세 CI **101개 named test를 Linux native에서 실제 통과시키고**, 실패에서 확인한 문제를 공개 API와 fixture 구현에 반영하는 것이었습니다. Source `d9115f4`의 전체 CI는 terminal SUCCESS이며 상세 101개·선택된 child 121개·matrix 12개 조합·cleanup 22개를 확인했습니다. 101개는 Ceph runtime 90개·bootstrap 실패 cleanup 1개·Docker bridge SDK 2개·helper 검사 8개이며 child·matrix 반복을 distinct test나 native I/O 수로 더하지 않습니다. 고정된 이미지 요구사항을 유지하며 준비된 서버 이미지를 사용했습니다. 이전 `be58018`의 97개 PASS·RGW sync 2개 FAIL·translation 2개 미실행과 focused 3개 PASS의 strict source 검증 제한은 원문대로 보존합니다. 실제 source·상태·client 효과·복원·owned cleanup과 terminal 결과는 [전체 CI 완료 증거](CI_FIXTURES.md#d9115f4-전체-ci-완료)를 따릅니다. 이는 G05/G07의 알려진 native 한계까지 해결했다는 의미가 아닙니다.

각 항목은 다음 네 조건을 모두 충족해야 완료로 표시합니다.

1. 공개 typed API 또는 그대로 실행 가능한 공개 API 조합 예제가 있습니다.
2. 명령 성공뿐 아니라 native 서버 상태·identity를 확인합니다.
3. 실제 client의 성공·거부·장애·복구 또는 복제 데이터를 확인합니다.
4. 원래 상태 복원 또는 owned resource/일회성 cluster 정리를 확인합니다.

tag compile·단위 테스트 통과만으로 완료로 표시하지 않습니다. `Ceph`, `RBD`, `Admin`, container `Exec` escape hatch가 있다는 사실만으로 미제공 항목을 닫지 않습니다. API 조합이 충분한 경우에도 해당 recipe와 실제 검증이 필요합니다. partial/lost reply를 처리할 수 없거나 외부 리소스를 adopt/삭제하는 구현은 완료 조건을 충족하지 못합니다.

## 제공 기준과 진행 상태

`완료`는 연결된 기존 PoC의 대표 조건을 충족한 항목이며 모든 parameter 조합의 전수 검증을 뜻하지 않습니다. 기존 Quay-derived slim 및 Debian 실행 기록은 명시된 이미지 조합의 증거로 유지합니다. `부분 제공 / native 한계`는 API 구성과 일부 실제 동작은 검증했지만 원본 Quay에서 해당 전체 조건을 지원한다고 표시할 수 없는 항목입니다. `검증 중`은 실제 Docker 검증을 진행 중이며, `개발 중`과 `미완료`는 구현·검증에 남아 있습니다.

2026-10-04 G05/G07의 numeric priority와 ordinary-user source 권한 거부는 원본 Quay 20.2.4에서 실패했습니다. 두 native 패치를 적용한 Ubuntu Noble ARM64 RGW와 기존 Quay-derived control/OSD/MDS의 bridge/host 성공은 선택적 custom-image 검증으로 보존하며, 기본 이미지의 이 두 한계를 해결한 것으로 표시하지 않습니다. [원본 한계·선택적 패치 실행 범위](RGW_SYNC_POLICY.md)를 확인합니다.

| ID | client 테스트에 필요한 서버 준비 | 공개 제공 경로 / 검증 기준 | 상태 |
| --- | --- | --- | --- |
| T01 | Linux native client 설정·key·namespace 연결 | `WithClient`, `WithClientIdentity`, `ConnectionConfig`; 별도 go-ceph 소비자 module에서 실제 RADOS/RBD/CephFS I/O | 완료: [Linux client](HOST_NETWORK_POC.md), `TestGoCephLinux` |
| T02 | 단일·독립 다중 cluster, MON quorum·MGR HA, public/cluster/peer network, 노드 lifecycle | `Run` topology options와 daemon API; advertised address, failover, 기존 데이터 유지 | 완료: [구성 기준](CLUSTER_SCENARIOS.md), [추가 topology](TOPOLOGY_EXTENSIONS.md) |
| T03 | replicated pool, CRUSH host/rack/root/device class, replica/quota 정책 | `CreatePool`, OSD placement, pool policy; 실제 분산 배치·read/write·quota 거부 | 완료: 기존 배치 PoC와 `TestPoolPolicies`, bridge/host; 양수 native pool ID를 독립 OSD map과 교차 확인하며 replica·quota 거부/복구 후 identity·bytes 보존 |
| T04 | EC RADOS/RBD/CephFS data pool | `CreatePool` EC/overwrite policy와 서비스별 pool 선택; 실제 데이터·metadata pool 분리 | 완료: `TestErasureCodedPools`, `TestCephFSAdditionalErasureCodedDataPool`; 새 동적 경로는 F05 |
| T05 | Cephx 읽기/쓰기/revoke와 RADOS namespace 격리 | owned `ClientIdentity` caps·key·client config; 두 principal의 허용·거부 | 완료: `TestClientIdentities`; subvolume 특화는 F06 |
| T06 | RADOS compound/xattr/omap/watch-notify/striper/object snapshot 테스트 환경 | 기존 pool·namespace·둘 이상의 client 조합 recipe; native compound atomicity, notification, snapshot/striped payload | 완료: `TestRADOSClientFixtures`, 아래 실행 recipe |
| T07 | OSD object class 실행 환경과 slim closure | OSD의 `rados-classes` 및 native `Exec`; `cls_hello` 실제 호출·결과 검증 | 완료: `TestRADOSClientFixtures`, 실제 `Hello, fixture!` 반환 |
| T08 | 정확한 client nonce fencing과 TTL | `TemporaryBlocklist`, `BlocklistEntries`, `Restore`; 같은 host의 다른 session 유지, ESHUTDOWN·해제·만료 | 완료: `TestClientFencing`, bridge/host |
| R01 | RBD metadata 초기화·namespace | `InitRBDPool`, `CreateRBDNamespace`; image/namespace 분리와 RO/RW client 효과 | 완료: `TestRBDNamespaces`, bridge/host; RO open exact bytes, 기본 writable open·image create·직접 RADOS write·foreign namespace read의 native EPERM/EACCES, RW 유지·owned cleanup |
| R02 | RBD image 기능을 테스트할 기본 구성 | R01 + client recipe: layering/clone/flatten, trash/migration/group, encryption 및 lock 테스트 | 완료: [client recipe](RBD_CLIENT_FIXTURES.md), `TestRBDClientFeatures`, bridge/host |
| R03 | MGR module membership·dependency·readiness·restore | `MGRModules`, `TemporaryMGRModule`, `WaitMGRModuleReady`; always-on/사용 중 보호·native command probe | 완료: `TestMGRModules`, bridge/host |
| R04 | RBD task queue와 mirror snapshot schedule | `rbd_support` + initialized pool/mirrored image; task 완료와 실제 자동 mirror snapshot·destination bytes | 완료: `TestMGRModules`, `TestRBDAutomaticSnapshotSchedule`, bridge/host |
| R05 | snapshot/journal mirror, peer/daemon HA, fanout/failback/split-brain/resync, backup | `multicluster` mirror/backup API + client checkpoint; peer fault 복구와 destination data | 완료: [다중 cluster](MULTICLUSTER_POC.md), topology 추가 검증 |
| R06 | pool 전체 mirror와 local/remote namespace mapping | pool mode의 journal image 자동 편입, ns-a→ns-b와 default namespace 조합, 다른 namespace 제외 | 완료: `TestMultiClusterRBDMirrorScopeAndNamespaces`, bridge/host 각 5조합 |
| R07 | mirror image 상태·receiver 준비 대기 | `ImageStatus`, `WaitReplayReady`; 원래 pool/namespace/image identity, live owned instance와 deadline | 완료: 원본 Quay bridge/host journal failback·receiver 재시작, pool-named journal·image-snapshot-named mapping; 실제 bytes·제외·cleanup은 [별도 검증](MIRROR_OBSERVABILITY.md#rbd-검증) |
| F01 | named/multiple FS, multi-active MDS·standby/replay, failover | `WithCephFS`, `StartCephFSWithConfig`, `ScaleMDS`; rank/native map·POSIX 데이터 유지 | 완료: topology 검증 |
| F02 | group/subvolume quota·layout·RADOS namespace | owned provisioning API; native quota/layout와 client 쓰기 거부·격리 | 완료: `TestCephFSSubvolumes`, bridge/host; subvolume/group 각각 실제 EDQUOT 거부, 기존 bytes·독립 neighbor 보존, quota 확장 후 fresh session 쓰기 복구·owned cleanup |
| F03 | subvolume snapshot·async clone·wait 재시도 | owned snapshot/clone API; frozen bytes, quota·namespace 상속, pending source 보호 | 완료: `TestCephFSSubvolumeSnapshotsAndClones` |
| F04 | canceled/failed clone의 명시적 lifecycle | cancel·partial cleanup·pending source reference 해제, source bytes 보존, identity/readback | 완료: `TestCephFSCloneCancellationAndPartialCleanup`, bridge/host canceled·failed 모두 |
| F05 | 실행 중 FS data pool 추가와 provisioning 연동 | `AddDataPool`, `DataPools`, `RemoveUnusedDataPool`; replicated/EC에 subvolume·clone, native detach/replacement 거부 | 완료: `TestCephFSDynamicDataPools`, bridge/host; native pool ID 재생성 시 add/remove의 identity 거부, replacement sentinel·전체 FSMap pool IDs·기존 replicated/EC bytes 보존 및 owned cleanup |
| F06 | subvolume authorize/deauthorize·authorized list·session eviction | fresh 제한 principal; filesystem path와 OSD pool/namespace 모두 제한, RO/RW/revoke와 다른 subvolume 거부 | 완료: `TestCephFSSubvolumeClientAuthorization`, bridge/host |
| F07 | 일관된 checkpoint용 quiesce/release | owned set/version/members·deadline/TTL; 두 client의 쓰기 정지·해제·timeout 복구 | 완료: `TestCephFSQuiesceCheckpoints`, bridge/host; QUIESCED·release·EXPIRED와 별도 SIGSTOP acquisition TIMEDOUT, 정확한 PID 재개·동일/fresh session 쓰기 복구·neighbor 보존·owned cleanup |
| F08 | export/distributed/random pin | group/subvolume pin; multi-active MDS의 실제 subtree ownership과 정책 복원 | 완료: `TestCephFSPins`, bridge/host |
| F09 | subvolume/snapshot metadata와 retained snapshot provisioning | native metadata recipe·snapshot retention 후 source cleanup/recovery | 완료: `TestCephFSRetainedSnapshotAndMetadataRecipe`, bridge/host |
| F10 | snapshot mirror·directory peer·daemon 분산·backup | `multicluster` CephFS mirror API와 archive/restore 공개 조합 recipe; 실제 remote frozen bytes와 daemon fault 복구 | 완료: 기존 multicluster/topology PoC |
| F11 | owned mirror directory의 상태·정확한 checkpoint 대기 | `DirectoryStatus`, `WaitDirectoryReady`, `WaitSnapshotSynced`; 원래 filesystem/peer와 live watcher, 독립 source ID·이름, partial member·deadline | 완료: 원본 Quay snapshot/backup bridge·host와 bridge HA/rebalance·zero-instance backlog·peer 단절, 별도 원문·strict cleanup; [관측 계약과 실행](MIRROR_OBSERVABILITY.md#cephfs-검증) |
| G01 | RGW 사용자·AdminOps caps·quota·suspend·safe cleanup | owned user API; S3/AdminOps 허용·거부, quota, key 보존, bucket가 있는 user 삭제 거부 | 완료: `TestRGWUserAdministration`와 host variant; ordinary user aggregate quota의 실제 403 QuotaExceeded, sibling 쓰기 유지·quota 복원 후 동일 bytes 복구·owned cleanup |
| G02 | named placement/storage class, replicated/EC data, realm activation | `CreatePlacement`, `ApplyPlacement`, `ReloadPlacement` + destination period pull; 실제 class와 RADOS pool payload | 완료: [fixture 확장](CLUSTER_FIXTURE_EXTENSIONS.md) |
| G03 | user default target/class와 placement tags | `SetUserPlacement`; 새 bucket 허용·거부·기존 bucket 유지·실제 선택 pool | 완료: `TestRGWUserPlacementPolicy`, host variant |
| G04 | tenant 및 account-root fixture/account quota | 같은 uid·bucket 이름의 tenant 격리, fresh account/root credentials·aggregate quota·cleanup | 완료: `TestRGWTenantsAndAccounts`, host variant |
| G05 | multisite selective replication의 owned 구성 | group/flow/pipe 구성·제거, bucket/prefix/tag 허용·거부, bucket/owner translation과 user mode | 부분 제공 / native 한계: unpatched 기본 lifecycle·single tag·owner/class·same-tenant system/user는 bridge/host 실제 bytes·제외·cleanup 통과. 원본 Quay의 numeric priority와 ordinary-user source deny/grant는 지원으로 표시하지 않음. 전체 translation 및 account-root/cross-tenant의 patched-image 성공은 [선택적 실행 증거](RGW_SYNC_POLICY.md)로 별도 보존 |
| G06 | 기존 bucket 유지보수 조건 | 개별 bucket quota, reshard/queue·readiness; 실제 S3 payload 보존·quota 거부 | 완료: `TestRGWBucketMaintenance`, bridge/host |
| G07 | period 및 metadata/data/bucket sync 관측·bounded readiness | exact local committed period, 실제 destination checkpoint·bytes와 native sync 상태 조합 | 부분 제공 / native 한계: unpatched 기본 lifecycle·single tag·owner/class·tenant의 exact period/group/bucket import·11 shard checkpoint·bytes를 bridge/host 확인. Observer의 오류·deadline guard unit/race 통과. Caught-up은 원본 Quay의 numeric priority나 ordinary-user source 거부 효과를 보장하지 않으며 해당 end-to-end 조건의 patched-image PASS는 선택적 증거 |
| G08 | S3 client 기능의 서버 조건 | versioning/multipart/lifecycle/object-lock/bucket policy/ACL client recipe와 필요한 daemon 옵션, 대표 동작 | 완료: `TestRGWS3ClientFeatures`, bridge/host; IAM role은 G09 |
| G09 | STS/Swift 및 암호화 backend 테스트 조건 | STS shared key와 role credential, Swift principal/endpoint, TLS·KMS endpoint 조합과 실제 consumer effect | 완료: `TestRGWNativeTLS`, [STS·Swift·Vault recipe](RGW_PROTOCOL_BACKENDS.md)의 `TestRGWProtocolBackends`, bridge/host |
| G10 | realm/zonegroup/master 전환·recovery | `multicluster` zone/peer/period API; master failover 및 metadata/data 복구 | 완료: 기존 multicluster/topology PoC |
| G11 | AdminOps usage log와 bucket rate-limit 준비 | pre-start usage log·flush 조건, scoped admin caps·owned log trim과 별도 request rate 거부·복구 | 완료: [실행 recipe](RGW_ADMIN_RECORDS.md), `TestRGWAdminRecordsAndRateLimit`, bridge/host |
| D01 | runtime 설정·OSD out/in·global 장애 flag·PG 복구 | `TemporaryConfig`, `SetOSDIn`, `TemporaryOSDFlag`, `WaitForPGClean`; native 상태와 I/O·restore | 완료: [fixture 확장](CLUSTER_FIXTURE_EXTENSIONS.md) |
| D02 | OSD 등록 identity와 삭제 실패 재시도 | `RemoveOSD`, `AddOSD`; 실제 purge 응답 유실·외부 UUID 거부·등록 개수 guard | 완료: 원본 Quay bridge/host `TestOSDRemovalLifecycle`, 8개 object 원문 및 owned cleanup; [삭제 계약](TOPOLOGY_EXTENSIONS.md#osd-삭제의-소유권과-재시도) |
| D03 | 전체 MON 교체 후 기존 daemon 재시작 | `AddMonitor`, `RemoveMonitor`, `RefreshMonitorConfig`; running/stopped 소유 config 갱신과 개별 설정 유지 | 완료: 원본 Quay bridge/host `TestMonitorRollingReplacement`, MGR/MDS 새 native GID·RADOS/S3 원문·owned cleanup; [bootstrap 계약](TOPOLOGY_EXTENSIONS.md#mon-교체-후-bootstrap-설정) |

## 책임과 실제 제한

단일 cluster의 pool·principal·module·FS/RGW policy는 `ceph`, cluster 사이의 peer·namespace mapping·realm/period 전달·선택적 복제는 `multicluster`가 담당합니다. 선택적 sync와 schedule처럼 기존 cluster 안에 서버 상태를 만드는 기능도 목적에 따라 해당 package에 둡니다. 실행 이미지에 필요한 native module/object class가 없으면 연결 옵션만으로 가능하다고 표시하지 않습니다.

CephFS additional pool 제거는 native FSMap에서 attachment를 해제합니다. native 명령은 POSIX file layout reference를 전부 검사하지 않으므로 `RemoveUnusedDataPool`은 helper가 사용한 적 없는 attachment, native provisioning 미사용, 모든 RADOS namespace의 빈 pool만 허용합니다. 외부 layout 설정·writer는 caller가 중지해야 합니다. detach는 pool·data·application tag를 삭제하지 않으며 남은 native tag 때문에 재등록에 별도 조치가 필요할 수 있습니다.

RGW CLI의 빈 user placement tags는 기존 tags를 비우지 않습니다. `SetUserPlacement`는 nil로 유지하거나 비어 있지 않은 목록으로 교체하고, 빈 목록 요청은 거부합니다. target 권한 취소는 matching tag를 nonmatching tag로 바꿔 확인합니다. 기존 bucket의 immutable placement와 object 권한을 이 설정의 효과로 혼동하지 않습니다. [RGW placement의 native 계약](https://docs.ceph.com/en/tentacle/radosgw/placement/).

MGR의 configured enabled와 active manager command readiness는 다릅니다. always-on module을 force-disable하여 복원을 가장하지 않습니다. readiness helper는 `rbd_support`와 `volumes`의 실제 CLI를 probe하며 다른 module은 소비할 command를 caller가 probe해야 합니다. [MGR 관리](https://docs.ceph.com/en/tentacle/mgr/administrator/).

NFS·SMB·NVMe-oF gateway, cephadm/systemd/LVM/실제 disk, kernel-only mount, hardware 성능·production upgrade 검증은 현재 네 역할 프로토콜 fixture와 구분합니다. 해당 gateway topology는 추가 daemon/image 계약이 필요한 별도 범위입니다. RADOS/RBD/CephFS/RGW client API의 서버 조건을 그 이유로 누락하지 않습니다.

## 조사와 실행 경로

Go의 필수 원본 Quay 경로는 기존 `make check`, `make scenario-default`, `make scenario-topology`, `make scenario-multicluster-topology`, `make scenario-topology-extensions`와 fixture profile 6개입니다. 현재 Cluster/CephFS/RADOS/RBD/RGW/RGW sync별 9/8/4/6/14/7개, fixture 이름 48개와 기본·토폴로지 55개·SDK 2개를 합해 105개를 선택합니다. 후속 OSD 삭제·MON rolling·multicluster bootstrap·constructor deadline 이름 4개는 아래 source `d9115f4`의 전체 CI 101개와 별도 증거로 추적합니다. Linux go-ceph 1개는 별도 소비자 검증이며, Go의 실행 target이 호출자가 준비한 client/runner 이미지만 소비합니다. 이미지 프로젝트의 독립 Python functional checker와 CI는 이 Go named test를 실행하지 않습니다. [CI fixture 전체 목록·조건](CI_FIXTURES.md)에 정확한 target과 named test를 기록합니다. 이전 source `be58018`의 [run 37226924156](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37226924156)에서는 RGW sync에서 prefix 변경 뒤 미래 객체가 404로 남는 실패 2개가 확인됐습니다. 성공한 필수 job 9개는 94개 PASS이며 실패 job의 PASS 3개를 더하면 97개입니다. Translation 2개는 첫 명령 실패로 미실행이고 관측한 parent/child skip은 0개입니다. 이후 policy import barrier를 수정한 `d9115f4`의 [전체 CI run 37240162309](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37240162309)는 상세 101개·필수 cleanup 22개까지 모두 PASS했으며 이전 실패와 별도 실행으로 기록합니다. 서버는 기존 원본 Quay를 직접 사용하며 [기존 topology 증거](CLUSTER_SCENARIOS.md)도 당시 source의 결과로 보존합니다.

`make cluster-features`와 `make client-fixtures`는 별도로 선택하는 내부 기능·client recipe입니다. 전체 client recipe에는 알려진 native 한계 회귀가 포함되므로 원본 Quay에서 전체 PASS를 보장하는 기본 suite로 표시하지 않습니다. 필수 RGW translation child와 strict optional native 회귀는 [CI 실행 기준](CI_FIXTURES.md)에서 구분합니다. 역할별 이미지를 선택하는 경우에는 [fixture 확장 문서](CLUSTER_FIXTURE_EXTENSIONS.md)의 환경 변수를 사용합니다. RBD encryption recipe는 cryptsetup과 같은 Ceph ABI의 native client library를 포함한 consumer image가 필요합니다. 공식 Quay 이미지 이외의 control을 쓰는 경우 [RBD 소비자 이미지 조건](RBD_CLIENT_FIXTURES.md#cryptsetup이-있는-native-client-이미지)을 확인한 준비된 이미지를 `CEPH_TEST_RBD_CLIENT_IMAGE`로 지정합니다. 이 consumer 전용 의존성이 공개 Go module이나 daemon role image에 추가되지는 않습니다.

RADOS client fixture의 [실행 가능한 public composition](../internal/integration/rados_client_fixtures_integration_test.go)은 `Run` → `CreatePool(Application: "rados")` → `WithClient`로 연결한 Linux consumer 두 개 → 각 consumer의 namespace 선택 순서입니다. 하나의 pool에 SDK에서 namespace를 선택하면 compound/xattr/omap/watch-notify/pool snapshot을 테스트할 수 있습니다. striper는 consumer의 `rados --striper`와 native libradosstriper가 추가로 필요하고 OSD role에는 실제 object class shared libraries가 있어야 합니다. 이 recipe는 같은 namespace의 두 client로 compound 비교 실패 후 데이터 유지, 실제 notification, snapshot frozen bytes, 3개의 stripe object와 SHA256, `cls_hello` 결과 및 object cleanup까지 실행합니다. 다른 namespace의 격리와 권한 거부는 T05의 `TestClientIdentities`에서 별도로 검증합니다. 명령을 구현한 새 CRUD wrapper는 필요하지 않습니다.

- [go-ceph admin 및 native consumer 소스](https://github.com/ceph/go-ceph/tree/v0.41.0): module/task/schedule, fencing, subvolume authorization/quiesce/pin/clone, RGW user/account 기준
- [CephFS volumes](https://docs.ceph.com/en/tentacle/cephfs/fs-volumes/): provisioning lifecycle의 기준
- [RBD mirroring](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/): pool scope·namespace 및 daemon 조건
- [RGW tenant](https://docs.ceph.com/en/tentacle/radosgw/multitenancy/), [account](https://docs.ceph.com/en/tentacle/radosgw/account/), [selective sync](https://docs.ceph.com/en/tentacle/radosgw/multisite-sync-policy/): server fixture 기준

기존 완료 항목은 연결된 문서의 실제 실행 기록을 사용하며 새 기능을 추가한 뒤 관계없는 모든 Docker 시나리오를 매번 재실행하지는 않습니다. 변경한 public composition 경로는 bridge/host 대표 실행으로 검증합니다. 단위·race·전체 tag compile/vet는 변경된 library의 회귀를 확인하며, Docker 로그와 native 관측 자료는 ignored `artifacts/`에 저장합니다.
