# 테스트용 클러스터 구성 목표와 검증

이 문서는 완료된 토폴로지 단계의 기준과 증거를 기록합니다. 이후 클러스터 내부 리소스·정책 API의 제공 범위와 검증은 [CLUSTER_INTERNAL_FEATURES.md](CLUSTER_INTERNAL_FEATURES.md)에 정리합니다.

목표는 **클라이언트 테스트에 필요한 Ceph 토폴로지를 testcontainers로 생성하고, 구성 요소의 추가·교체·중단·복구와 클러스터 간 연결이 가능한지** 확인하는 것입니다. CRUSH rule·EC·pool 정책·권한과 개별 RADOS/RBD/CephFS/S3 기능은 후속 확장으로 둡니다. 이들 기능의 제공 여부는 완료 조건에 포함하지 않습니다. go-ceph와 다른 native client의 읽기·쓰기는 구성의 연결성을 확인하는 증거로 사용합니다.

공개 모듈은 CLI/파일로 제어하며 cgo에 의존하지 않습니다. go-ceph 소비자 테스트는 별도 Linux 전용 모듈에 둡니다. 기본 bridge에서는 클러스터별 전용 네트워크를 생성하고, 애플리케이션은 `WithClient`로 해당 네트워크에 연결합니다. host mode에서는 서로 다른 FSID·키와 자동 선택 MON/RGW 포트를 사용합니다. RADOS/RBD/CephFS 클라이언트는 MON뿐 아니라 광고된 OSD/MDS 주소에도 도달해야 합니다.

## 구성별 제공 상태

2026-10-03 작업 중 기록입니다. “구현”과 실제 Docker PoC 통과를 구분합니다. 이 문서는 검증 결과에 맞춰 갱신합니다.

| 구성 | 생성·변경 API | 실제 검증 상태 |
|---|---|---|
| 빠른 단일 클러스터 | `Run`, `WithOSDCount`, `AddOSD`, `RemoveOSD` | 기존 PoC 통과. 기본 MON 1/MGR 1/OSD 2, sparse BlueStore. Linux go-ceph의 RADOS/RBD/CephFS 통신도 통과 |
| MON quorum / MGR standby | `WithMonitorCount(3)`, `WithManagerCount(2)`, `Monitors`, `Managers`, `AddMonitor`, `RemoveMonitor`, 독립 `ControlContainer` | bridge/host 통과. MON 중단·재시작·3→4→3 교체 동안 같은 native RADOS 연결 유지. quorum 상실 시 새 인증 실패, 복구 후 기존/새 연결 성공. active MGR 변경 및 volumes 명령 확인 |
| 실행 중 MGR 증감 | `AddManager`, `RemoveManager` | bridge 통과. 1→2→1→2→1에서 active 제거·standby 승격, 교체 standby 추가·제거 후 native MGR 명령과 실제 map 확인. 마지막 candidate 제거 거부 |
| CephFS active/standby/replay | `WithCephFS`, `StartCephFSWithConfig`, `CephFSConfig` | standalone standby-replay 장애·승격·재가입 통과. 요청한 rank와 standby의 실제 FSMap 확인 |
| 다중 active MDS / 여러 filesystem | `WithCephFS`의 이름·active/standby 수·MDS affinity | Run에서 두 filesystem, 2 active + 1 standby 구성 통과. rank1 중단·승격 후 통신과 다른 filesystem의 독립성 확인 |
| 실행 중 MDS 증감 | `CephFSContainer.ScaleMDS` | bridge 통과. 일반 standby의 1/0→2/1→1/1→1/0과 replay follower의 1/1→2/1→1/1→1/0 확인. rank handoff·남는 standby 제거, 실제 FSMap/컨테이너 수 확인. 같은 filesystem/pool ID와 기존 파일, 다른 filesystem MDS의 GID 유지 |
| RGW 단독 / 같은 zone의 여러 gateway | `WithRGW`, `StartRGWWithConfig`, `Gateways`, `RemoveRGW` | bridge/host 통과. 초기 Run의 gateway, 동적 추가, 2→1→2 제거·교체, 중단·재시작 뒤 같은 zone의 기존 데이터 확인 |
| 독립 클러스터 2개 동시 사용 | 독립 `Run`과 `WithClient`/`ConnectionConfig` | bridge/host 통과. 동일 pool/object 이름의 서로 다른 데이터, 양쪽 OSD 교체, Linux go-ceph의 실제 session 확인 |
| RGW multisite 2 zone | `multicluster.RunRGWMultisite` | bridge/host 통과. host의 자동 선택 gateway endpoint를 양쪽 최종 period에서 확인. 양방향 연결·gateway 중단/복구·metadata master 전환/복귀 |
| RGW multisite 여러 zone | `RunRGWTopology`, `RGWTopologyConfig.Zones/MetadataMaster`, `AddZone`, `Zones`, `ZoneAdmin` | 3 zone bridge/host 통과. 초기 MON a 제거 후 남은 quorum으로 구성, 입력 순서와 다른 metadata master 지정, 실제 zone ID/endpoint, secondary 중단·재가입과 각 zone의 통신, master gateway 중단 후 보조 zone의 독립 읽기 확인 |
| RBD snapshot mirror / journal mirror | `multicluster.RunRBDMirror`, `RBDMirrorConfig.Mode` | snapshot은 bridge/host 통과. journal의 receiver 재시작·A→B→A 구성도 통과 |
| RBD 3 cluster fanout | 기존 pair API를 A→B, A→C로 조합 | bridge 통과. 서로 다른 FSID/네트워크/키, 실제 tx/rx peer graph, 한 receiver 중단 중 다른 receiver 유지, 재시작 후 catch-up. source MON/OSD와 두 receiver 중단 후 B/C의 독립 읽기 확인 |
| RBD backup/restore | 전체·증분 export/restore API | 실제 source 중단 후 복원·새 session 통과 |
| CephFS snapshot mirror / archive restore | `multicluster.RunCephFSMirror`, `AttachManagers`, directory/peer 변경 | bridge/host 통과. 초기 MGR a 제거 후 b active/c standby로 생성, d 추가·연결 준비, b 제거 후 c 승격·peer 재등록과 새 snapshot 도달 확인. 기존 snapshot 및 양쪽 클러스터 유지, fixture가 추가한 attachment만 cleanup. 기존 daemon 재시작·OSD 교체·source 중단 사례도 통과 |

기존 세부 결과와 로그 경로는 [MULTICLUSTER_POC.md](MULTICLUSTER_POC.md), [HOST_NETWORK_POC.md](HOST_NETWORK_POC.md), [SERVICES_POC.md](SERVICES_POC.md)에 있습니다. 위 “통과”는 각 케이스의 최종 실행 결과이며 전체 테스트가 한 번의 명령에서 모두 통과했다는 뜻은 아닙니다.

이번 토폴로지 실행은 Ceph 20.2.4 역할별 slim 이미지와 Docker Desktop의 Linux ARM64 엔진에서 수행했습니다. 주요 로그는 로컬 `artifacts/cluster-core-topologies-final.log`와 `artifacts/cluster-peer-gateway-topologies-final.log`입니다. 앞의 core 실행은 665.794초, peer/gateway 실행은 808.041초에 통과했습니다. host RGW multisite 514.54초, RBD fanout 144.49초, 같은 zone RGW lifecycle bridge/host 148.55초입니다. fanout의 source/receiver 중단은 통신 경로의 독립성 확인이며 물리 장애 내성이나 전체 네트워크 partition 검증으로 확대 해석하지 않습니다.

후속 실행의 증거는 다음과 같습니다. 시간은 해당 로그의 실제 결과이며 서로 다른 실행을 하나의 전체 suite 통과로 합치지 않습니다.

| 로그 | 결과 |
|---|---|
| `artifacts/cluster-manager-lifecycle-final2.log` | MGR lifecycle 18.95초, 명령 전체 19.409초 PASS |
| `artifacts/cluster-mds-scale-final.log` | 일반 standby MDS scale 명령 전체 93.742초 PASS |
| `artifacts/cluster-mds-replay-scale-final.log` | standby-replay MDS scale 명령 전체 99.372초 PASS |
| `artifacts/cluster-topology-review-regression.log` | MON/MGR 및 RGW의 bridge 회귀 검증 276.464초 PASS |
| `artifacts/go-ceph-linux-topology-regression/integration.log` | Linux go-ceph 소비자 테스트 bridge/host 233.04초 PASS |
| `artifacts/go-ceph-linux-topology-regression/summary.json` | `passed: true`, `remaining_owned_container_ids: []`. 이 runner가 추적한 컨테이너의 잔여 리소스 0개 |
| `artifacts/cluster-rgw-three-zone-final2.log` | host 3 zone PASS, bridge 3 zone FAIL. 명령 전체는 FAIL |
| `artifacts/cluster-rgw-three-zone-bridge-final3.log` | 불필요한 새 gateway 재시작 제거 후 bridge 3 zone 414.97초 PASS |
| `artifacts/cluster-topology-final-validation.log` | 초기 metadata shard 준비 조건을 추가한 최종 RGW host 597.39초 / bridge 313.82초 개별 PASS. 같은 명령의 CephFS MGR 준비 전환 검사 88.20초 FAIL로 명령 전체는 FAIL; 해당 대기는 수정 후 별도 재검증 |
| `artifacts/cluster-cephfs-manager-topology-final2.log` | MGR 준비 대기 수정 후 CephFS MGR topology bridge 123.05초 / host 117.96초, 명령 전체 241.241초 PASS |

### 마지막 구성 수정과 검증

- **RGW 3 zone**: bridge의 HTTP 403 실패 뒤 새 gateway의 불필요한 초기 재시작을 제거하고, 실행 중 secondary의 모든 metadata shard가 incremental 상태인지 확인합니다. 이 수정으로 bridge/host 검증이 완료됐습니다. 이전 실패의 Ceph 내부 원인을 확정한 것은 아닙니다.
- **CephFS mirror와 MGR HA의 결합**: 초기 MGR 포인터 의존을 제거하고 현재 owned 후보 전체의 peer network를 준비합니다. `AttachManagers`와 `RebootstrapPeer`가 새 후보를 재조정하며 MGR 모듈 활성화 중 실제 active 준비를 기다립니다. 초기 MGR 제거 후 mirror 생성과 standby 승격 뒤 peer 재등록이 bridge/host에서 통과했습니다.

최종 실행 뒤 Docker의 running/stopped container 목록은 비어 있었고 testcontainers가 만든 임시 network도 남지 않았습니다. 기존 `kind` network는 유지했습니다. CephFS 검증은 mirror 종료 후 현재 MGR의 owned attachment가 제거됐으며 양쪽 cluster control이 계속 동작하는지도 확인했습니다. 불확실한 network connect·disconnect, 삭제된 MGR, Docker client close 실패의 cleanup 재시도는 별도 단위 테스트에서 확인했습니다.

## 우선순위와 완료 판단

첫 단계는 **daemon 역할별 수와 active/standby, 노드 추가·제거·교체, 네트워크와 포트, 독립 cluster 및 peer/zone 연결 그래프**를 public 구성 API로 만드는 것입니다. native/HTTP 읽기·쓰기는 구성된 노드가 실제로 통신하는지 확인하는 최소 증거로 사용합니다. 클라이언트의 모든 기능과 데이터 정책을 검증하는 것은 이 작업의 목표가 아닙니다.

이번 단계는 다음 대표 구성의 생성과 변경을 완료 기준으로 삼습니다. 숫자와 정책 조합의 전수 검증 대신 실제 map과 연결을 확인합니다.

1. 기본 단일 클러스터와 3 MON quorum, active/standby MGR, 여러 OSD를 요청한 수와 identity로 생성합니다.
2. MON/MGR/OSD의 추가·제거·교체, filesystem별 multi-active MDS와 standby/replay 증감, 같은 zone의 여러 RGW 생명주기를 확인합니다.
3. bridge/host에서 독립 클러스터를 함께 사용하고, RGW 2/3 zone과 RBD pair/fanout, CephFS pair의 peer 그래프가 실제로 통신합니다.
4. 초기 MON/MGR을 제거한 뒤에도 남은 quorum과 active MGR로 구성·연결 API를 사용할 수 있으며 중단·승격·재가입 뒤 연결이 복구됩니다.
5. 성공과 부분 생성 실패에서 fixture가 소유한 컨테이너·네트워크 연결을 정리할 수 있습니다. cleanup 증거의 범위도 해당 실행의 owned resource로 명시합니다.

단순히 `HEALTH_OK`인지보다 요청한 토폴로지가 만들어졌는지가 중요합니다. bootstrap에 필요한 최소 pool/auth 설정은 생성 코드에 포함하지만 정책 조합의 전수 검증은 하지 않습니다.

위 첫 대표 토폴로지에 이어 RGW 여러 zonegroup·zone 탈퇴, 복수 mirror daemon, 별도의 cluster/public network와 제한된 endpoint 단절·복구의 구성 API와 대표 PoC를 완료했습니다. 5 MON quorum, 분리 네트워크, RBD/CephFS daemon 증감·HA, RGW group 추가·zone 탈퇴와 세 서비스의 peer endpoint 복구가 통과했습니다. CephFS 20.2.4의 자동 증설 재분배 오류와 검증된 명시적 재분배 경로는 [TOPOLOGY_EXTENSIONS.md](TOPOLOGY_EXTENSIONS.md)에 구분합니다. 숫자 조합 전체나 모든 네트워크 장애를 검증했다는 의미는 아닙니다. CRUSH rule, EC profile, pool replica 정책, namespace·권한·layout 등은 별도 후속 기능 과제입니다. 이미 작성한 정책 API와 PoC는 유지하되 토폴로지 작업의 완료 기준으로 삼지 않습니다.

CRUSH host/rack은 같은 Docker 엔진에서 만든 논리적 배치 도메인입니다. 실제 물리 노드나 디스크의 장애 내성을 입증하지 않습니다. CSI가 필요로 하는 pool·identity·filesystem·MDS 구성은 이 fixture의 범위에 들어가지만, krbd/NBD, kernel CephFS/FUSE, Kubernetes NodeStage/NodePublish는 별도 Linux/Kubernetes harness의 구성 과제로 둡니다.

## 후속 기능 작업의 기록

이번 조사 중 작성한 `CreatePool`/`WithPools`, `WithPoolDefaults`, CRUSH placement 및 default-root 옵션, EC 설정, `CreateClient`/`WithClientIdentity`는 후속 기능 확장의 출발점입니다. EC RADOS/RBD와 제한된 identity의 bridge/host PoC는 통과했습니다. 이들은 토폴로지 단계의 완료 조건과 별개였으며, 다음 내부 설정 단계에서 pool 정책·caps 변경·RBD namespace·CephFS subvolume·RGW 사용자 API를 확장합니다. [내부 설정 API](CLUSTER_INTERNAL_FEATURES.md)

CephFS는 native mirror module의 filesystem당 single peer 제한을 따릅니다. 같은 filesystem의 A→B/C fanout은 API 일반화만으로 제공할 수 있는 구성으로 분류하지 않습니다. 여러 독립 filesystem을 각각 다른 peer에 연결하는 방식은 별도 구성으로 검토합니다. [CephFS mirror peer 제한](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/#mirroring-module)

## 클라이언트 요구를 참고한 근거

- [go-ceph의 native library와 build tag 계약](https://github.com/ceph/go-ceph/blob/v0.41.0/README.md): 공개 모듈 의존성으로 넣지 않고 Linux 소비자 fixture에서 실제 통신을 확인합니다.
- [MON quorum](https://docs.ceph.com/en/tentacle/rados/configuration/mon-config-ref/#monitor-quorum), [MON 추가·제거](https://docs.ceph.com/en/tentacle/rados/operations/add-or-rm-mons/), [MGR HA](https://docs.ceph.com/en/tentacle/mgr/administrator/#high-availability): 공유 monmap과 다수결, standby promotion을 구성 기준으로 사용합니다.
- [MDS standby](https://docs.ceph.com/en/tentacle/cephfs/standby/), [multi-MDS](https://docs.ceph.com/en/tentacle/cephfs/multimds/): active rank, standby/replay, filesystem별 daemon affinity와 rank handoff를 구성 기준으로 사용합니다.
- [RBD mirroring](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [RGW multisite](https://docs.ceph.com/en/tentacle/radosgw/multisite/): 클러스터 자체와 peer/zone 연결은 별도 패키지에서 구성합니다.
- [AWS SDK endpoint 설정](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-endpoints.html), [MinIO client 옵션](https://github.com/minio/minio-go/blob/master/api.go): RGW 소비자에는 연결 가능한 HTTP endpoint와 테스트 자격 증명이 필요합니다. SDK의 모든 기능을 모듈 자체가 구현할 필요는 없습니다.
