# 단일 클러스터와 다중 클러스터 시나리오 API

`ceph/`의 `ceph` 패키지는 단일 일회성 클러스터, `multicluster/`의 `multicluster` 패키지는 기존 클러스터 사이의 zone/peer 연결과 mirroring 구성을 다룹니다. 현재 우선순위는 역할별 수·active/standby·네트워크·노드 생명주기와 연결 그래프입니다. 데이터 읽기·쓰기는 이 구성이 통신하는지 확인하는 증거로 사용하며, 정책·클라이언트 기능과 백업 편의 API의 확장은 별도 후속 과제로 둡니다.

루트의 `go.mod` 하나를 유지하며 의존 방향은 `multicluster → ceph`입니다. 단일 클러스터의 bootstrap 스크립트는 `ceph/internal/scripts/`에 두고 `ceph` 패키지에 embed합니다. `ceph.Run`에는 다른 클러스터와의 관계를 섞지 않습니다. 일반 RGW/RBD/CephFS 테스트에서는 복제용 컨테이너를 실행하지 않습니다.

이 문서는 현재 구현의 구성·수명 계약을 설명합니다. API 제공과 실제 Docker 검증은 구분하며, 토폴로지 확장의 진행 상태와 실행 근거는 [TOPOLOGY_EXTENSIONS.md](TOPOLOGY_EXTENSIONS.md)를 따릅니다.

## 책임과 수명

| 계층 | API | 소유 리소스 |
| --- | --- | --- |
| 단일 클러스터 | `ceph.Run`, 역할별 초기 옵션·추가/제거 API, `ScaleMDS` | MON, MGR, OSD, 필요한 RGW/MDS, 자체 Ceph network와 독립 CLI control |
| RGW multisite | `RunRGWMultisite`, `RunRGWTopology`, `AddZonegroup`, `AddZone`, `RemoveZone` | zone마다 gateway·관리 client, bridge mode의 공통 HTTP network |
| RBD snapshot/journal mirror | `multicluster.RunRBDMirror`, `AddDaemon`, `RemoveDaemon` | 관리 client 2개, destination의 `rbd-mirror` daemon들 |
| CephFS snapshot mirror | `multicluster.RunCephFSMirror`, `AddDaemon`, `RemoveDaemon`, `AttachManagers` | source의 `cephfs-mirror` daemon들, owned MGR 후보에 추가한 remote network 연결 |
| RBD 백업·복원 | `ExportRBDBackup`, `ExportRBDIncremental`, `RestoreRBDBackup`, `RestoreRBDIncremental` | CLI archive의 byte 전달과 임시 파일 정리. 보관처·pool·client는 호출자 소유 |
| CephFS archive PoC | 테스트의 JSON/base64 fixture helper | 파일 tree를 별도 경로에 복원. 범용 backup API로 제공하지 않음 |

각 연결은 기존 `*ceph.Container`를 참조합니다. FSID, OSD, 키, 클러스터 네트워크를 공유하지 않습니다. RBD pool과 CephFS/MDS는 호출자가 먼저 만듭니다. RGW multisite는 realm/zone을 기동 전에 구성해야 하므로 standalone `StartRGW`를 실행하지 않은 새 클러스터를 사용합니다.

클러스터 cleanup을 먼저 등록하고 연결 cleanup을 나중에 등록합니다. LIFO로 연결 → 클러스터 순서로 종료합니다. 오류와 함께 non-nil 연결이 반환돼도 cleanup이 필요합니다. Mirror의 embedded `Container`는 초기 daemon 하나의 호환 handle입니다. `Stop`/`Start`는 그 daemon만 제어하며 초기 daemon 제거 뒤에는 nil이 됩니다. 현재 구성원은 `Daemons()`로 조회합니다. `Terminate`는 연결이 소유한 추가 컨테이너와 네트워크 연결을 제거하고 클러스터나 데이터를 삭제하지 않습니다. Ceph에 쓴 realm, peer, auth, directory policy는 일회성 클러스터에 남습니다. CephFS의 `RemoveDirectory`/`RemovePeer`와 RBD의 peer 제어는 호출자가 명시적으로 선택하는 구성 변경이며 `Terminate`가 자동으로 수행하지 않습니다. CephFS의 별도 `BeginDirectoryRemoval`·`WaitReleased`는 [원래 cycle 해제](CEPHFS_DIRECTORY_REMOVAL.md), `BeginPeerRemoval`·`WaitDrained`는 [원래 peer worker 종료](CEPHFS_PEER_REMOVAL.md)를 확인하는 retained receipt입니다. 원래 process가 바뀌거나 daemon이 제거된 뒤의 retained raw observer·`ProcessQuiescence`는 [별도 관측 계약](CEPHFS_PROCESS_QUIESCENCE.md)을 따르며 기존 receipt completion·pending gate를 바꾸지 않습니다. 원래 daemon을 모두 `RemoveDaemon`으로 제거한 뒤 `AcknowledgeProcessQuiescence`의 새 증거로 별도 승인 상태를 기록하고 구성을 재개할 수 있습니다. [복구 승인 계약](CEPHFS_PROCESS_ACKNOWLEDGMENT.md)을 따릅니다. 기존 클러스터 전체의 원래 상태 복원도 계약에 포함하지 않습니다.

RBD의 image별 replay 상태와 bounded readiness는 `ImageStatus`·`WaitReplayReady`로 관측합니다. Original pool/namespace/image identity와 실제 소유 receiver를 확인하며, 특정 데이터의 복제 완료는 별도 client 검증으로 유지합니다. [Mirror 관측 계약](MIRROR_OBSERVABILITY.md)을 따릅니다.

## 사용 예

두 공개 패키지는 다음 경로로 import합니다.

```go
import (
    "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
    "github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
)
```

```go
source, err := ceph.Run(ctx, controlImage, ceph.WithOSDImage(osdImage))
if source != nil { testcontainers.CleanupContainer(t, source) }
if err != nil { t.Fatal(err) }

destination, err := ceph.Run(ctx, controlImage, ceph.WithOSDImage(osdImage))
if destination != nil { testcontainers.CleanupContainer(t, destination) }
if err != nil { t.Fatal(err) }

multisite, err := multicluster.RunRGWMultisite(ctx, rgwImage,
    multicluster.RGWMultisiteConfig{
        Source: source, Destination: destination, ControlImage: controlImage,
    })
if multisite != nil {
    t.Cleanup(func() {
        cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
        defer cancel()
        if err := multisite.Terminate(cleanupCtx); err != nil { t.Error(err) }
    })
}
if err != nil { t.Fatal(err) }
endpoint, err := multisite.Destination.S3Endpoint(ctx)
if err != nil { t.Fatal(err) }
// endpoint, AccessKey, SecretKey, Region을 path-style S3 client에 전달합니다.
```

### 여러 RGW zone 구성

`RunRGWTopology`는 두 개 이상의 독립된 새 클러스터를 하나의 realm에 연결합니다. 단일 zonegroup은 `Zones`/`Zonegroup`/`MetadataMaster`로 지정합니다. `MetadataMaster`를 생략하면 입력의 첫 zone이 초기 metadata master가 됩니다. 모든 클러스터는 같은 network mode를 사용해야 합니다. `ControlImage`는 RGW image와 별도로 관리 CLI image를 선택하며 생략하면 RGW image를 사용합니다.

아래 예제의 `clusterA/B/C`는 각각 `ceph.Run`으로 만들고 cleanup을 등록한 클러스터입니다. multisite에서 사용할 클러스터에는 standalone RGW를 먼저 실행하지 않습니다.

```go
topology, err := multicluster.RunRGWTopology(ctx, rgwImage,
    multicluster.RGWTopologyConfig{
        Realm: "application", Zonegroup: "us-east-1", MetadataMaster: "a",
        ControlImage: controlImage,
        Zones: []multicluster.RGWZoneConfig{
            {Name: "b", Cluster: clusterB},
            {Name: "c", Cluster: clusterC},
            {Name: "a", Cluster: clusterA},
        },
    })
if topology != nil {
    t.Cleanup(func() {
        cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
        defer cancel()
        if err := topology.Terminate(cleanupCtx); err != nil { t.Error(err) }
    })
}
if err != nil { t.Fatal(err) }

for _, zone := range topology.Zones() {
    endpoint, err := zone.Gateway.S3Endpoint(ctx)
    if err != nil { t.Fatal(err) }
    // endpoint와 Gateway.AccessKey/SecretKey/Region으로 S3 client를 생성합니다.
    _ = endpoint
}
period, err := topology.ZoneAdmin(ctx, "c", "period", "get", "--format", "json")
if err != nil { t.Fatal(err) }
_ = period
```

반환 타입은 기존 `*RGWMultisite`입니다. 호환 필드 `Source`는 초기 metadata master의 gateway, `Destination`은 나머지 입력 중 첫 zone의 gateway입니다. 이후 master를 전환해도 이 필드가 재지정되지는 않습니다. 전체 zone은 `Zones()`로 조회합니다.

여러 region은 위 호출의 config 대신 `Zonegroups`를 지정합니다. `Zones`/`Zonegroup`/`MetadataMaster`와 함께 사용할 수 없습니다. 각 `RGWZonegroupConfig`는 하나 이상의 zone을 가져야 하며 `MasterZone`의 기본값은 해당 group의 첫 zone입니다. `MasterZonegroup`의 기본값은 첫 group이고, 그 group의 local master가 realm metadata master가 됩니다. zone 이름과 storage cluster는 전체 topology에서 서로 달라야 합니다.

```go
config := multicluster.RGWTopologyConfig{
    Realm: "application", MasterZonegroup: "us", ControlImage: controlImage,
    Zonegroups: []multicluster.RGWZonegroupConfig{
        {Name: "us", MasterZone: "a", Zones: []multicluster.RGWZoneConfig{
            {Name: "a", Cluster: clusterA},
            {Name: "b", Cluster: clusterB},
        }},
        {Name: "eu", MasterZone: "c", Zones: []multicluster.RGWZoneConfig{
            {Name: "c", Cluster: clusterC},
        }},
    },
}
_ = config // RunRGWTopology(ctx, rgwImage, config)에 전달합니다.
```

realm의 user·bucket metadata는 zonegroup 사이에서 공유하며 object data는 각 zonegroup 안에서 복제합니다. 여러 zonegroup을 하나의 object-data fanout으로 취급하지 않습니다.

| API/필드 | 계약 |
|---|---|
| `Zones()` | 이름순 `[]RGWZone` 복사본. `Name`, native `ID`, `Zonegroup`, `PeerEndpoint`, `Gateway`를 포함하며 실패한 부분 zone도 나타날 수 있음 |
| `Zonegroups()` | 이름순 `[]RGWZonegroup` snapshot. `Name`, native `ID`, `MasterZone`, 이름순 `Zones`를 포함. direct native promotion 뒤 master 표시는 다음 topology 작업에서 갱신 |
| `RGWZone.PeerEndpoint` | gateway와 관리 CLI 사이의 주소. bridge의 HTTP network 또는 Docker host namespace에서 사용 |
| `RGWZone.Gateway.S3Endpoint(ctx)` | 애플리케이션의 HTTP 주소. bridge mode에서는 현재 host published port를 조회 |
| `ZoneAdmin(ctx, name, args...)` | 해당 owned zone의 CLI에서 native `radosgw-admin` 실행. private system credentials는 descriptor에 노출하지 않음 |
| `AddZone(ctx, image, RGWZoneConfig, opts...)` | owned zonegroup에 독립된 새 클러스터를 추가. `RGWZoneConfig.Zonegroup` 생략 시 초기 master zonegroup 사용. 현재 committed period의 metadata master와 등록 zone의 identity/endpoint 확인 |
| `AddZonegroup(ctx, image, RGWZonegroupConfig, opts...)` | 하나 이상의 fresh zone으로 새 non-master zonegroup 추가. `MasterZone`으로 local master를 선택하며 realm metadata master는 유지 |
| `RemoveZone(ctx, name)` | owned non-master zone의 membership을 committed period에서 제거·전파한 뒤 gateway·관리 client 종료. 현재 realm/group master와 group의 마지막 zone은 제거 거부 |

`AddZone`은 각 site의 활성 period를 갱신하고, 실행 중 secondary의 초기 metadata full sync 완료를 확인한 뒤 기존 gateway에 최종 period를 적용합니다. 새 bridge gateway는 이미 최종 period로 시작하므로 불필요하게 재시작하지 않습니다. 중단해 둔 gateway는 중단 상태를 유지합니다. Docker가 bridge mode의 published port를 바꿀 수 있으므로 **추가 완료 뒤 기존 zone을 포함한 모든 S3 client의 endpoint를 `S3Endpoint`로 다시 조회**합니다. 직접 `Stop`/`Start`한 gateway도 재시작 뒤 다시 조회합니다. `PeerEndpoint`와 애플리케이션 endpoint는 서로 다른 용도입니다.

추가 클러스터는 호출자가 cleanup을 소유하고, gateway·관리 client·HTTP network는 기존 topology fixture가 소유합니다. `AddZone`/`AddZonegroup`이 non-nil 부분 결과와 오류를 반환하면 fixture를 `Terminate`하여 정리합니다. 같은 이름을 다시 추가하거나 외부 zone을 대신 소유하는 방식으로 재시도하지 않습니다. `ZoneAdmin`으로 period/zone을 변경하거나 metadata master를 전환하는 동안 topology 변경 API를 동시에 실행하지 않습니다. 외부 master 전환을 완료한 뒤 추가하거나, 이전 master gateway를 중단한 상태로 유지합니다. 생성·추가의 성공은 최신 데이터까지 동기화됐다는 뜻은 아니므로 필요한 최소 I/O를 별도로 관측합니다.

`RemoveZone`은 realm·local zone 설정·user·pool·object를 storage cluster에 남깁니다. 탈퇴한 이름과 cluster를 fresh zone으로 재사용하지 않습니다. period 전파나 runtime cleanup의 부분 실패는 같은 이름으로 재시도할 수 있으며, 진행 중 탈퇴를 마쳐야 다른 zone을 추가·제거할 수 있습니다. master 탈퇴는 먼저 native 명령으로 master를 전환해야 합니다. group 전체 제거 API는 제공하지 않습니다. 추가·탈퇴 후에는 남은 gateway의 `S3Endpoint`도 다시 조회합니다.

기존 대표 구성의 검증은 [CLUSTER_SCENARIOS.md](CLUSTER_SCENARIOS.md), 여러 zonegroup·daemon·peer network 확장의 검증은 [TOPOLOGY_EXTENSIONS.md](TOPOLOGY_EXTENSIONS.md)를 따릅니다.

### RBD와 CephFS 연결

RBD pool을 초기화한 뒤에는 다음과 같이 연결합니다. 대상 image마다 `rbd mirror image enable POOL/IMAGE snapshot`과 `rbd mirror image snapshot POOL/IMAGE`을 따로 실행합니다. 연결 API의 성공이 데이터 동기화 완료나 자동 failover를 의미하지는 않습니다.

```go
mirror, err := multicluster.RunRBDMirror(ctx, controlImage,
    multicluster.RBDMirrorConfig{
        Source: source, Destination: destination, Pool: "images", DaemonCount: 2,
    })
if mirror != nil { testcontainers.CleanupContainer(t, mirror) }
if err != nil { t.Fatal(err) }
```

CephFS는 양쪽에서 `StartCephFS`를 호출한 뒤 연결합니다. 지정한 directory의 snapshot을 복제합니다.

```go
mirror, err := multicluster.RunCephFSMirror(ctx, controlImage,
    multicluster.CephFSMirrorConfig{
        Source: source, Destination: destination,
        SourceFilesystem: sourceFS.FilesystemName,
        DestinationFilesystem: destinationFS.FilesystemName,
        Directories: []string{"/application"},
        DaemonCount: 2,
    })
if mirror != nil { testcontainers.CleanupContainer(t, mirror) }
if err != nil { t.Fatal(err) }
```

CephFS mirror는 초기 MGR a의 존재 대신 현재 native map의 실행 중 owned active MGR을 확인합니다. MGR 모듈 활성화·승격 중에는 최대 90초간 준비를 기다리며 호출자 context를 따릅니다. bridge에서는 standby와 중단 상태를 포함한 현재 owned MGR 후보 모두를 destination network에 연결합니다. host mode에는 추가 attachment가 없습니다.

실행 중 `source.AddManager`로 새 후보를 추가하면 `mirror.AttachManagers(ctx)`로 미리 연결을 준비합니다. `RebootstrapPeer`도 peer token을 import하기 전에 자동으로 이를 재조정합니다. cleanup은 fixture가 추가한 연결만 제거하며, 호출자가 미리 연결한 network와 source MGR 컨테이너의 수명은 유지합니다.

### Mirror daemon 증감과 관측

두 config의 `DaemonCount`는 0이면 기본 1개이고 음수는 거부합니다. `NoInitialDaemons: true`와 count 0을 함께 사용하면 최초 mirror daemon 없이 정책을 구성한 뒤 `AddDaemon`으로 명시적으로 시작할 수 있습니다. True와 positive count의 조합은 runtime 접근 전에 거부합니다. RBD는 설정용 CLI 2개를 계속 소유하며 CephFS는 bridge MGR 연결을 소유할 수 있습니다. [최초 daemon 없는 구성 계약](NO_INITIAL_MIRROR_DAEMONS.md)을 따릅니다. 초기 daemon 이름은 `a`부터 `z`, 이후 `node-27` 순서입니다. 복수 daemon은 같은 peer와 filesystem/pool을 담당하며 native assignment와 failover는 비동기로 진행됩니다. socket 준비나 컨테이너 수만으로 복제·HA 완료를 판단하지 않습니다.

| API | 계약 |
|---|---|
| RBD/CephFS `Daemons()` | 이름순 owned daemon slice의 복사본. 중단·부분 시작한 구성원 포함. 외부 fixture의 daemon은 발견하지 않음 |
| `AddDaemon(ctx, name, opts...)` | 기존 peer와 데이터 정책을 유지하며 process 추가. constructor customizer 다음에 해당 호출의 customizer 적용. non-nil 부분 결과도 fixture가 소유 |
| `RemoveDaemon(ctx, name, opts...)` | owned process 종료·inventory 제거. 마지막 daemon 제거도 허용하며 이후 `AddDaemon`으로 재개. 실패하면 membership을 유지하여 재시도 |
| RBD daemon `Status(ctx)` | native admin socket의 `PoolReplayers` 조회. `Pool`, `Peer`, `State`, `InstanceID`, `LeaderInstanceID`, `Leader`, `Instances`로 해당 pool의 election/membership 관측. process 중단 또는 socket 부재 시 오류 |
| CephFS `RebalanceDirectories(ctx)` | fixture가 소유한 directory 정책을 명시적으로 제거·release 대기·재등록. 현재 구성원 전부 실행 중이어야 하며 중단한 구성원은 먼저 제거 |
| RBD `ImageStatus(ctx, name)`, `WaitReplayReady(ctx, name)` | 원래 pool/namespace/image pair와 실행 중 owned receiver의 exact native instance 확인. 특정 write/checkpoint의 완료는 별도 client에서 검증 |
| CephFS `DirectoryStatus(ctx, path)`, `WaitDirectoryReady(ctx, path)` | 현재 owned directory의 원래 filesystem/peer, MGR assignment·live owner 및 다른 unavailable member 문제 관측 |
| CephFS `WaitSnapshotSynced(ctx, path, CephFSMirrorSnapshot)` | source client에서 독립적으로 읽은 snapshot ID·이름과 정확한 native last_synced 일치 대기. Destination bytes·retention은 별도 확인 |

관측별 deadline·identity·partial readiness와 실제 실행 증거는 [mirror 관측 계약](MIRROR_OBSERVABILITY.md)을 따릅니다.

RBD daemon은 각각 다른 Ceph client를 사용하며 pool receiving peer와 native leader election/image assignment를 공유합니다. CephFS daemon은 동일한 source client와 single peer를 공유합니다. filesystem당 peer 하나 제한은 그대로이므로 여러 daemon이 같은 filesystem의 A→B/C fanout을 만들지는 않습니다.

CephFS `RebalanceDirectories`는 `AddDaemon` 뒤 기존 directory가 자동 재분배되지 않을 때 호출할 수 있습니다. 작업 중 해당 경로의 복제가 잠시 멈추며 파일·snapshot·peer·auth는 유지합니다. caller가 별도로 등록한 정책과 다른 filesystem은 건드리지 않습니다. 부분 CLI 실패에도 목표 directory 목록을 유지하므로 새 context로 재시도할 수 있습니다. 이 명시적 정책 재등록과 native daemon 장애 복구는 서로 다른 작업입니다.

### Peer endpoint 단절·복구

| API | 제거하는 bridge endpoint | 유지하는 경로·정책 |
|---|---|---|
| RBD `InterruptPeerLink(ctx, daemonName)` | receiver daemon의 source public network | destination/pool-election endpoint, peer/image 정책 |
| CephFS `InterruptPeerLink(ctx, daemonName)` | source daemon의 destination public network | source filesystem/MGR registration, directory/peer 정책 |
| RGW `InterruptZoneLink(ctx, zoneName)` | gateway의 multisite HTTP network | local Ceph/S3 endpoint, realm/period membership |

각 API는 owned 구성원만 선택하고 `*ceph.NetworkInterruption`을 반환합니다. 중단은 해당 endpoint 전체에 적용하며 daemon을 종료·재시작하지 않습니다. 개별 zone 쌍의 packet filtering이나 지연·손실 주입은 제공하지 않습니다. host mode에는 분리할 Docker bridge endpoint가 없어 거부합니다.

`cut.Restore(ctx)`는 원래 IP·alias·gateway priority로 재연결하며 재시도할 수 있습니다. 외부에서 다른 endpoint로 재연결했다면 덮어쓰지 않습니다. disconnect의 transport 오류는 실제 적용 여부를 확정하지 못하므로 **오류와 함께 반환된 non-nil handle도 복구 대상으로 유지**합니다. fixture가 복구를 cleanup에 등록하므로 fixture 종료 이전에 직접 복구할 수 있고, 복구 실패나 이미 제거된 daemon의 정리도 재시도합니다. endpoint 복구와 실제 backlog catch-up 완료는 별도로 관측합니다.

## 백업·복원과 관계 변경

RBD archive는 mirroring 연결 없이 전달할 수 있습니다. Native format-2 full export와 snapshot baseline 기반 incremental export를 사용하며, 실제 보관처는 호출자가 선택합니다.

```go
var full bytes.Buffer
if err := multicluster.ExportRBDBackup(ctx, sourceClient, "images/application", &full); err != nil {
    t.Fatal(err)
}
if err := multicluster.RestoreRBDBackup(ctx, destinationClient, "images/restored", bytes.NewReader(full.Bytes())); err != nil {
    t.Fatal(err)
}
```

`ExportRBDIncremental`은 추가로 baseline snapshot 이름을 받고 `RestoreRBDIncremental`은 대상 image에 그 baseline이 있는지 Ceph CLI가 검증합니다. Reader/Writer를 helper가 닫지 않으며, container archive와 restore용 private host spool은 정리합니다. Host에서는 byte 전달만 수행하고 Ceph client/native linking은 사용하지 않습니다. 이미지 삭제나 pool 생성은 helper가 자동으로 수행하지 않습니다.

Archive 전송은 read 사이에서 context 취소를 확인합니다. 호출자가 제공한 Reader/Writer의 blocking I/O 자체를 중단해야 한다면 해당 구현도 취소를 지원해야 합니다. Incremental restore는 baseline의 존재를 검사하지만, 같은 이름의 snapshot 내용까지 인증하는 backup manifest를 제공하지는 않습니다.

RBD `ReceiverStatus`·`WaitReceiverReady`는 configured pool/namespace에서 image 없이도 exact owned daemon cohort의 발견·election 합의를 확인합니다. HA survivor 이름을 명시할 수 있고, 대기는 원래 peer generation·handles/CIDs를 고정합니다. [Receiver 준비 상태 계약](RBD_RECEIVER_READINESS.md)을 따릅니다.

RBD의 `SourceRBD`/`DestinationRBD`는 연결이 소유한 관리 client에서 명령을 실행하며 `Rebootstrap`은 제거한 receiving peer를 다시 등록합니다. 왕복 전환에는 `SourceSite`/`DestinationSite`를 각 클러스터의 고정 이름으로 지정하고 반대 방향 연결에서도 그 이름을 유지합니다. 한 방향 연결의 daemon들은 destination에서 실행됩니다. 반대 방향 연결을 추가하면 반대 클러스터에도 receiving daemon이 필요합니다. Primary demote/promote, writer fencing, 복제 완료 대기, split-brain의 authoritative image 선택과 `resync`는 시나리오가 명시적으로 수행합니다.

CephFS `BeginDirectoryAddition`은 daemon이 없는 구성에서도 native 요청 전 intent를 보존하고 같은 receipt의 fresh Begin으로 소유권을 확정합니다. `Status`는 등록 정책·소유권 관측만 수행합니다. [응답 유실·미적용 요청과 generation 계약](CEPHFS_DIRECTORY_ADDITION.md)을 따릅니다.

CephFS의 `AddDirectory`/`RemoveDirectory`는 복제 directory 정책을, `PeerIDs`/`RemovePeer`/`RebootstrapPeer`는 동일한 source/destination 사이의 연결을 제어합니다. 제거한 정책이 daemon에 반영될 때까지 확인하고 다음 snapshot을 생성해야 합니다. API 성공과 실제 데이터 동기화 완료는 별도로 관측합니다. Destination에 이미 복제된 데이터나 snapshot을 peer 제거 시 삭제하지 않습니다.

RGW의 `SourceAdmin`/`DestinationAdmin`은 realm/zone별 제어 명령을 실행합니다. `PullSourcePeriod`/`PullDestinationPeriod`는 native `realm pull`로 상대 site의 realm과 current committed period를 가져오고 활성 period 포인터·realm epoch·local zonegroup을 갱신합니다. Native `period pull`만으로는 활성 realm이나 local 구성이 바뀌지 않으므로 이 복귀 절차에 충분하지 않습니다. 정책 변경과 master 승격에는 실제 Ceph CLI와 gateway 재시작을 사용합니다. 새 master로 전환하기 전 metadata 동기화와 이전 writer fencing은 시나리오가 명시적으로 수행합니다. Realm/current period 반영은 해당 버전의 [rgw_zone.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_zone.cc)를 기준으로 구성했습니다.

Ceph 20.2.4의 승격 PoC는 `period update`와 `period commit`을 나눠 실행합니다. 합친 명령에서는 이미 바뀐 local zonegroup으로 driver를 초기화하여 metadata master의 빈 sync 상태를 승격 검사에 전달할 수 있습니다. 별도 commit은 이전 current period를 기준으로 검사를 수행합니다. 승격 전 native metadata status의 period·realm epoch·incremental shard와 master 비교 결과를 확인하며 `--yes-i-really-mean-it`으로 검사를 우회하지 않습니다. 근거: 해당 버전의 [radosgw-admin](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/radosgw-admin/radosgw-admin.cc), [metadata sync](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_sync.cc), [period 승격 검사](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_period.cc).

각 데이터 경로와 서비스별 함수는 같은 Go package에 두되 [rgw.go](../multicluster/rgw.go), [rgw_topology.go](../multicluster/rgw_topology.go), [rgw_zonegroups.go](../multicluster/rgw_zonegroups.go), [rbd.go](../multicluster/rbd.go), [cephfs.go](../multicluster/cephfs.go), [network.go](../multicluster/network.go), [rbd_backup.go](../multicluster/rbd_backup.go)로 구현 책임을 구분합니다. 세부 시나리오와 관측 결과는 [PoC 보고서](MULTICLUSTER_POC.md)와 [토폴로지 확장 진행 기록](TOPOLOGY_EXTENSIONS.md)에 기록합니다.

## Testcontainers와 이미지의 경계

단일 기술 module의 진입점은 기존 `ceph.Run(ctx, image, opts...)`입니다. 다중 클러스터 연결 API는 그 위의 구성 helper로 두며 image를 명시하고, `ContainerCustomizer`를 daemon에 마지막으로 적용하고, 부분 생성 결과를 반환하고, 소유한 리소스를 cleanup합니다. 별개의 공식 Testcontainers module로 등록한 것은 아닙니다. [Testcontainers Go module 지침](https://golang.testcontainers.org/modules/)의 Run·composition·customizer·SDK dependency 원칙을 기반으로 복수 리소스의 수명을 명시한 추가 API입니다.

이미지는 기존 다섯 역할을 유지합니다. `control`은 MON/MGR/CLI에 더해 `rbd-mirror`와 `cephfs-mirror`를 제공하고 `all`도 포함합니다. `osd`/`rgw`/`mds`에는 mirror daemon을 넣지 않습니다. 이미지에 포함해도 일반 클러스터에서 자동으로 실행하지 않으며 각각의 연결 API에서만 실행합니다.

`multicluster`는 복수 클러스터를 사용하는 시나리오의 상위 패키지입니다. 공통 Ceph federation control plane을 뜻하지 않습니다. RGW는 realm/zone의 HTTP multisite, RBD는 destination이 source MON/OSD에서 읽는 snapshot mirror, CephFS는 source daemon이 destination에 쓰는 snapshot mirror입니다. RBD의 새 receiving peer는 rx-only로 등록하고, 반대 방향 연결에서 재사용하는 tx-only peer는 기존 송신을 유지하도록 rx-tx로 확장합니다. CephFS bootstrap에서는 source MGR의 원격 filesystem 검사도 필요하므로 추가 네트워크 연결을 API가 관리합니다. [RGW](https://docs.ceph.com/en/tentacle/radosgw/multisite/), [RBD](https://docs.ceph.com/en/tentacle/rbd/rbd-mirroring/), [CephFS](https://docs.ceph.com/en/tentacle/cephfs/cephfs-mirroring/)의 기능과 제약을 각각 적용합니다.
