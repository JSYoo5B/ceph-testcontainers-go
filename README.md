# ceph-testcontainers-go

Ceph와 통신하는 애플리케이션을 테스트하기 위한 실험적 testcontainers-go 모듈입니다. 실제 Ceph 데몬을 작은 일회성 클러스터로 실행하고, 컨테이너 내부 CLI로 구성과 상태를 제어합니다. 공개 Go 모듈에는 `go-ceph`, 호스트 `librados`, cgo 의존성을 넣지 않습니다. 실제 go-ceph 소비자 검증은 별도 테스트 모듈에서 Linux 전용으로 실행합니다.

기본 구성은 MON 1개, MGR 1개, OSD 2개를 각각 별도 컨테이너로 실행합니다. MON quorum·MGR standby, OSD 수, 여러 filesystem과 active/standby MDS, 이름별 RGW와 클러스터 사이의 peer/zone 연결을 선택할 수 있습니다. RBD는 별도 데몬 없이 OSD pool을 사용합니다. OSD마다 1 GiB sparse BlueStore 파일을 사용합니다. Ceph 데몬에 privileged 모드, 호스트 디스크, LVM, Docker 소켓, systemd가 필요하지 않습니다. testcontainers 자체와 Ryuk은 Docker 엔진 접근이 필요합니다.

자료 조사와 판단 근거는 [RESEARCH.md](docs/RESEARCH.md), 클러스터 실행 결과는 [POC.md](docs/POC.md), RGW·RBD·CephFS 검증은 [SERVICES_POC.md](docs/SERVICES_POC.md)에 정리했습니다. 이미지 요구사항·검사·역할 이미지 생성은 별도 [ceph-testcontainers-images](../ceph-testcontainers-images/README.md)에서 관리합니다. 이 프로젝트는 주어진 이미지를 Go에서 실행하고 클러스터를 구성합니다.

제공 API는 테스트 환경을 준비·변경·복원하는 operation과 실제 상태를 관측하는 check로 구분합니다. 토폴로지·pool·자격 증명·장애 조건을 준비하는 operation을 제공하며, 일반 운영 자동화를 확장 목표로 삼지 않습니다. Check는 Docker와 컨테이너 내부 CLI를 통해 assertion의 근거를 제공하고, 공개 Go 패키지에 Ceph 클라이언트 SDK·cgo 의존성을 요구하지 않습니다. 상태 관측은 토폴로지 구성과 별개의 주요 제공 책임입니다.

기능별 구분과 현재 공개 API 전체 목록은 [Operation과 Check](docs/API_CAPABILITIES.md)에 있습니다. Native 질의와 보유 descriptor 조회, 각 Wait가 확인하는 조건을 구분해 assertion에 사용합니다. Context 취소·부분 결과 보존과 호출자 소유 채널·콜백 조합은 [Go 대기 사용법](docs/GO_WAITS.md)을 따릅니다.

`HealthDetails(ctx)`는 원래 FSID를 확인하고 MON이 보고한 health code·원인·mute를 읽습니다. MGR나 storage 준비 전에도 사용할 수 있으며, 실패하면 zero snapshot과 출력이 제거된 error를 반환합니다. 의미와 실제 검증 범위는 [HealthDetails 계약](docs/HEALTH_DETAILS.md)에 있습니다.

`PoolPGs(ctx, poolName)`는 원래 FSID와 pool ID를 확인하고 보고된 PG 상태·up/acting·primary·epoch·signed 통계를 읽습니다. `pg_ready`, unknown/빈 매핑, EC NONE 슬롯과 보고 지연의 의미는 [PoolPGs 계약](docs/POOL_PGS.md)을 따릅니다.

역할별 daemon 수·active/standby·네트워크·peer/zone 토폴로지와 노드 추가·제거·교체·복구 API를 제공합니다. 구성별 제공 범위와 원본 Quay 이미지의 필수 검증 상태는 [CLUSTER_SCENARIOS.md](docs/CLUSTER_SCENARIOS.md)에 기록합니다. 이어서 클라이언트 테스트의 사전 조건을 만드는 pool 정책·quota, Cephx caps, RBD namespace, CephFS subvolume, RGW 사용자 관리 API를 제공합니다. 사용법과 검증 범위는 [CLUSTER_INTERNAL_FEATURES.md](docs/CLUSTER_INTERNAL_FEATURES.md)에 있습니다.

여러 zonegroup·zone 탈퇴, mirror daemon 증감·HA, public/cluster 네트워크 분리와 선택적 peer 연결 단절의 제공 범위와 검증 결과는 [TOPOLOGY_EXTENSIONS.md](docs/TOPOLOGY_EXTENSIONS.md)에 있습니다. `make topology-extensions`로 해당 대표 시나리오를 다시 실행합니다.

client 테스트를 위한 서버 fixture의 전체 제공 기준과 항목별 검증 결과는 [CLIENT_FIXTURE_COVERAGE.md](docs/CLIENT_FIXTURE_COVERAGE.md)에서 추적합니다. 각 항목은 공개 구성 경로, native 상태, 실제 client 효과, 복원·정리까지 확인해야 완료로 표시합니다.

기본 이미지는 digest로 고정한 원본 Quay Ceph 20.2.4이며, 필수 Go CI는 같은 release의 공식 역할 이미지로 실행합니다. 클러스터를 사용하기 위해 Ceph source를 빌드하거나 새 서버 이미지를 만들 필요가 없습니다. RGW selective replication의 numeric priority와 ordinary-user source 권한 거부는 원본 이미지에서 확인한 native 한계이며 기본 지원으로 표시하지 않습니다. API로 해당 정책을 저장할 수 있다는 사실과 실제 복제 효과를 구분합니다. [지원 범위와 실행 증거](docs/RGW_SYNC_POLICY.md)를 확인합니다.

## 프로젝트 구성

공개 API는 `ceph/`와 `multicluster/`에 나란히 두고, 루트의 `go.mod` 하나로 관리합니다. 각 패키지의 단위 테스트·godoc 예제는 구현 옆에 둡니다. Docker로 실행하는 통합 테스트와 PoC는 공개 API를 사용하는 별도 테스트 패키지로 모았습니다.

```text
go.mod                 두 공개 패키지를 관리하는 단일 Go module
ceph/                  단일 클러스터 API, 단위 테스트와 사용 예
ceph/internal/scripts/ ceph 패키지에 embed하는 bootstrap 스크립트
multicluster/          클러스터 사이의 구성·복제·백업 API
internal/integration/  단일·다중 클러스터의 Docker 통합 테스트와 PoC
docs/                  설계·조사·검증 기록
```

단위 테스트는 구현 옆에 유지하고, 새 Docker 시나리오는 `internal/integration`에 추가합니다. 단일·다중 클러스터 테스트가 공통 fixture를 사용하므로 같은 패키지에 두고 build tag로 실행 범위를 선택합니다.

## 요구사항

- Go 1.25 이상
- Linux 컨테이너를 실행할 수 있는 Docker 엔진
- 공식 Ceph 이미지와 Ryuk 이미지를 받을 수 있는 환경

기본 이미지는 Ceph Tentacle 20.2.4의 amd64/arm64 manifest digest로 고정되어 있습니다. macOS ARM64 + Docker Desktop에서 실행을 검증했습니다. `CGO_ENABLED=0`으로 테스트할 수 있습니다.

## 사용 예

```go
package integration_test

import (
    "testing"

    ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
    "github.com/testcontainers/testcontainers-go"
)

func TestTopology(t *testing.T) {
    ctx := t.Context()
    cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithOSDCount(2))
    if cluster != nil {
        testcontainers.CleanupContainer(t, cluster)
    }
    if err != nil {
        t.Fatal(err)
    }

    osd, err := cluster.AddOSD(ctx)
    if err != nil {
        t.Fatal(err)
    }
    if err := cluster.RemoveOSD(ctx, osd.ID); err != nil {
        t.Fatal(err)
    }
}
```

모듈 경로는 `github.com/jsyoo5b/ceph-testcontainers-go`이며 원격 저장소의 `main`에서 관리합니다. 아직 버전 릴리스 tag는 만들지 않았습니다. 같은 체크아웃에서 테스트를 실행하거나 소비 프로젝트에서 로컬 `replace`를 사용할 수 있습니다.

### 초기 클러스터 구성

`WithMessengerMode(ceph.MessengerV2Secure)`로 v2 secure 전용 bootstrap을 선택할 수 있습니다.
노드·client 설정 상속, legacy 포트 미노출과 실제 연결 검증 범위는
[Messenger secure 구성 계약](docs/MESSENGER_SECURE.md)을 따릅니다.

`Run`에서 daemon 수, filesystem별 active/standby MDS, 이름별 gateway를 함께 선택합니다. `WithCephFS`와 `WithRGW`는 초기 역할 구성을, `StartCephFSWithConfig`, `StartRGWWithConfig`, `AddMonitor`, `AddManager`, `AddOSD`는 실행 중의 추가 구성을 담당합니다. pool 설정은 `WithPools`/`CreatePool`로 적용하고, 생성 후 정책은 `SetPoolReplication`/`SetPoolQuota`로 변경할 수 있습니다. 실행 중 PG split·merge와 CRUSH placement 이동은 `SetPoolPGCount`·`WaitForPoolPGCount`·`SetPoolPlacement`로 만들며 [PG 재배치 계약](docs/POOL_RELOCATION.md)을 따릅니다.

```go
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
    ceph.WithMonitorCount(3),
    ceph.WithManagerCount(2),
    ceph.WithOSDCount(3),
    ceph.WithCephFS(ceph.CephFSConfig{
        Name: "app-fs", ActiveMDS: 2, StandbyMDS: 1,
    }),
    ceph.WithRGW(
        ceph.RGWConfig{Name: "gateway-a"},
        ceph.RGWConfig{Name: "gateway-b", SkipUserCreation: true},
    ),
)
if cluster != nil {
    testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
    t.Fatal(err)
}
// cluster.Monitors(), cluster.Managers(), cluster.OSDs(), cluster.Filesystems(), cluster.Gateways()
```

`ceph.WithNoInitialOSDs()`를 선택하면 초기 OSD 없이 MON/MGR부터 구성하고 `AddOSD`·`AddOSDWithConfig`로 storage를 추가할 수 있습니다. 초기 user pool·CephFS·RGW를 함께 요청할 수 없으며, storage 이전의 health와 데이터 준비는 구분합니다. 기본 `Run`의 OSD 2개와 마지막 owned OSD 제거 보호는 유지합니다. [최초 OSD 없는 bootstrap 계약](docs/NO_INITIAL_OSDS.md)을 확인합니다.

`ceph.WithOSDInMemoryStorage(maxBytes)`는 해당 cluster의 모든 OSD 저장소를 크기 제한이 있는 shared tmpfs named volume에 둡니다. 첫 OSD가 생길 때 control-image keeper를 함께 만들고 모든 OSD의 Stop/Start 동안 데이터를 보존하며 `Terminate`에서 함께 정리합니다. 기본 sparse-file 저장소는 유지하며 [용량과 수명 계약](docs/OSD_MEMORY_STORAGE.md)을 따릅니다. 개별 `make scenario-osd-memory`와 기존 bootstrap CI 묶음에서 [메모리 OSD 토폴로지 fixture](docs/CI_FIXTURES.md#메모리-기반-osd-fixture)를 검사합니다.

`ceph.WithOSDBlockSize(bytes)`의 기본 크기는 1 GiB이며 최소 64 MiB를 허용합니다. 작은 OSD의 기능 검사는 첫 OSD 추가 전에 기존 `TemporaryConfig`로 benchmark skip을 명시적으로 준비하며 scheduler는 그대로 둡니다. 옵션이 scheduler나 benchmark 설정을 자동으로 바꾸지는 않습니다. `make scenario-small-osds`와 기존 bootstrap CI 묶음의 [작은 OSD fixture](docs/CI_FIXTURES.md#작은-osd-fixture), [용량과 준비 조건](docs/SMALL_OSD_STORAGE.md)을 따릅니다. 전체 native 검증 결과는 해당 실행 receipt로 확인해야 합니다.

`ceph.WithNoInitialManagers()`는 초기 MGR 없이 MON quorum과 owned OSD up/in을 확인하고 반환합니다. 양수 OSD와 초기 pool 구성은 유지하며, 초기 CephFS·RGW는 첫 `AddManager` 뒤 명시적으로 구성합니다. `WithNoInitialOSDs()`와 함께 선택하면 MON-only 단계로 시작합니다. MGR 통계·module·clean 준비는 별도로 확인하며 기본 MGR 1개와 마지막 MGR 제거 보호를 바꾸지 않습니다. [최초 MGR 없는 bootstrap 계약](docs/NO_INITIAL_MANAGERS.md)을 따릅니다.

각 daemon handle의 `Stop`/`Start`로 장애를 주입합니다. 프로세스를 끝내지 않고 응답만 멈추려면 `PauseContainer`로 컨테이너를 일시정지하고 `ContainerPause.Resume`으로 되살립니다. Host network에서도 동작하며 [일시정지 계약](docs/CONTAINER_PAUSE.md)을 따릅니다. Replica 읽기 오류를 주입해 scrub 불일치와 repair를 재현하려면 `InjectObjectDataError`, `DeepScrubPG`, `RepairPG`, `PGInconsistencies`를 사용하며 [scrub 불일치 계약](docs/SCRUB_INCONSISTENCY.md)을 따릅니다. MGR prometheus exporter의 활성화와 failover 뒤 URL 추적은 `ManagerServices`와 [prometheus recipe](docs/MGR_PROMETHEUS.md)를 따릅니다. 초기 생성과 이후 변경 모두 같은 클러스터가 cleanup을 소유합니다. MON 여러 개를 선택하면 별도 CLI control container가 있어 첫 MON이 정지해도 남은 quorum을 통해 관리할 수 있습니다. `WaitForQuorum`은 현재 monmap의 다수결을, filesystem의 `WaitReady`는 요청한 active rank와 standby 수를 확인합니다.

`ceph.CephFSConfig{NoInitialMDS: true}`는 원래 filesystem과 pool을 먼저 구성하고 MDS auth·container·customizer 없이 반환합니다. Active/standby count는 생략하고 replay는 false로 두며, 같은 descriptor의 첫 `ScaleMDS(ctx, 1, 0)`으로 기동합니다. Cold 상태의 embedded `Container`는 nil이고 `WaitReady`는 client 준비를 성공으로 표시하지 않습니다. [최초 MDS 없는 filesystem 계약](docs/NO_INITIAL_MDS.md)을 따릅니다.

`RemoveMonitor`는 quorum을, `RemoveManager`는 다른 실행 중 candidate의 승격 가능성을 확인한 뒤 해당 노드를 제거합니다. `CephFSContainer.ScaleMDS(ctx, active, standby)`는 같은 filesystem에서 rank handoff 후 남는 standby를 제거하며 pool과 파일을 유지합니다. `RemoveRGW`는 gateway만 제거하므로 같은 zone의 다른 gateway나 교체 노드가 기존 데이터를 계속 제공합니다. multisite의 gateway 주소를 바꿀 때는 period endpoint도 함께 변경해야 합니다.

## 애플리케이션 연결

`cluster.WithClient()`를 애플리케이션 컨테이너의 `testcontainers.Run` 옵션으로 전달하면 클러스터 네트워크 연결과 `/etc/ceph/ceph.conf`, `/etc/ceph/ceph.client.admin.keyring` 복사를 함께 처리합니다. 해당 컨테이너에는 사용하려는 Ceph 클라이언트가 있어야 합니다. 이 옵션이 클라이언트 패키지를 설치하지는 않습니다.

```go
client, err := testcontainers.Run(ctx, ceph.DefaultImage,
    cluster.WithClient(),
    ceph.WithIdleEntrypoint(),
)
if client != nil {
    testcontainers.CleanupContainer(t, client)
}
if err != nil {
    t.Fatal(err)
}
// client.Exec(ctx, []string{"rados", "-p", "my-pool", "ls"}, ...)
```

`ceph.WithIdleEntrypoint()`는 명령을 실행할 대기용 컨테이너를 `sleep infinity`로 띄우되 Docker init 아래에서 실행합니다. PID 1인 `sleep`은 SIGTERM을 무시하므로 init 없이 띄우면 정지할 때마다 Docker 기본 timeout 10초를 기다립니다. 애플리케이션 컨테이너는 클러스터보다 나중에 cleanup을 등록하여 먼저 종료합니다. `WithClient`가 복사하는 admin 키는 신뢰할 수 있는 테스트 컨테이너용입니다. 자격 증명에는 현재 테스트 클러스터 전체에 대한 권한이 있습니다.

Ceph 클라이언트는 MON에서 받은 OSD 주소로 직접 접속합니다. 기본 bridge 모드에서는 애플리케이션 컨테이너에 `WithClient()`를 적용하여 광고된 주소에 접근합니다. MON의 `MappedPort`만으로 macOS/Windows 호스트 프로세스에서 RADOS/RBD/CephFS 전체에 연결할 수 있다고 가정하면 안 됩니다. 근거: [Ceph 네트워크 문서](https://docs.ceph.com/en/tentacle/rados/configuration/network-config-ref/).

`WithSeparateClusterNetwork()`는 public bridge 외에 OSD replication·recovery용 bridge를 생성합니다. OSD만 두 network에 연결되고 `WithClient`와 MON/MGR/MDS/RGW는 public bridge를 사용합니다. `NetworkName()`과 `ClusterNetworkName()`으로 각각 조회합니다. `InterruptNetwork(ctx, container, ceph.PublicNetworkPlane)` 또는 `ceph.ClusterNetworkPlane`으로 한 endpoint를 끊고 반환된 handle의 `Restore(ctx)`로 원래 IP를 복구할 수 있습니다. host mode와 분리 옵션은 함께 사용할 수 없습니다. 계약과 검증 범위는 [TOPOLOGY_EXTENSIONS.md](docs/TOPOLOGY_EXTENSIONS.md)에 있습니다.

### Host network와 네이티브 클라이언트

Linux Docker 호스트에서 실행하는 네이티브 RADOS 애플리케이션에는 host network 모드를 선택할 수 있습니다. MON/MGR/OSD와 `WithClient()`로 연결하는 컨테이너가 Docker daemon의 host 네트워크를 사용하고, Ceph가 광고하는 주소로 직접 통신합니다. 기본 public 주소는 `127.0.0.1`이며 원격 Docker를 사용하는 경우 클라이언트가 도달할 수 있는 Docker 호스트 주소를 지정합니다.

```go
cluster, err := ceph.Run(ctx, ceph.DefaultImage,
    ceph.WithHostNetwork(),
    // ceph.WithHostAddress("192.0.2.10"), // 원격 Linux Docker 호스트의 실제 주소
)
if cluster != nil {
    testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
    t.Fatal(err)
}
config, keyring, err := cluster.ConnectionConfig()
if err != nil {
    t.Fatal(err)
}
// 신뢰할 수 있는 테스트 애플리케이션에 config와 keyring을 전달합니다.
// keyring은 이 테스트 클러스터의 admin 자격 증명입니다.
_, _ = config, keyring
```

`UsesHostNetwork()`와 `PublicAddress()`로 선택된 모드를 조회합니다. `ConnectionConfig()`는 Ceph 설정과 admin keyring의 복사본을 반환합니다. host network에서는 Docker port publishing이 적용되지 않으므로 모든 데몬의 광고 주소와 실제 listener가 클라이언트에서 도달 가능해야 합니다. MON/RGW 포트는 Docker 호스트 안에서 열린 socket으로 예약한 뒤 실제 daemon 기동 직전에 해제합니다. 이 사이에 포트를 빼앗겨 실제 bind 오류가 발생하면 최대 5회 시도하며, MON은 새 컨테이너와 monmap으로 재구성합니다. MGR/OSD/MDS는 Ceph의 동적 포트 선택을 사용합니다.

host network 모드의 `Run`은 클러스터를 만들기 전에 실행 환경을 검증합니다. 먼저 Docker 엔진이 Linux 컨테이너를 실행하는지 확인합니다. 첫 MON이 기동하면 테스트 프로세스에서 광고된 MON 주소로 직접 접속해 msgr2 banner를 받는지 확인하며, 이 검사는 MGR/OSD 생성 전에 최대 5초 동안 재시도합니다. 실패하면 `ceph.ErrHostNetworkUnavailable`을 감싼 오류를 반환하므로 `errors.Is`로 구분할 수 있습니다. 오류 메시지에는 Docker 환경에 맞는 원인 안내가 들어갑니다. 대표적인 원인은 Docker Desktop의 host networking 비활성화, Docker Desktop VM 내부 주소를 `WithHostAddress`로 지정한 경우, 원격 Docker에서 기본 주소 `127.0.0.1`을 사용한 경우, bridge 컨테이너 안의 테스트 프로세스에서 host 모드 클러스터를 실행한 경우입니다. `WithHostAddress`에 Docker 호스트에 없는 주소를 지정하면 MON 생성 전에 같은 오류로 실패합니다. 검사 대상은 첫 MON의 v2 endpoint입니다. 이후 추가하는 MON/RGW와 Ceph가 동적으로 고르는 MGR/OSD/MDS 포트의 도달 여부는 따로 검사하지 않습니다.

`make hostnetwork`는 두 클러스터의 동시 기동·서로 다른 MON/MGR/OSD endpoint·같은 pool/object 이름의 데이터 분리·양쪽 OSD `2 → 3 → 2` 변경 후 Python `librados` I/O와 실제 MON 포트 충돌 후 재시도를 검사합니다. RGW는 기본 포트 7480을 점유한 상태에서 서로 다른 endpoint를 만들고 signed S3 데이터를 비교합니다. RBD snapshot/clone과 CephFS 파일 I/O도 OSD 교체 전후에 검사합니다. 이 테스트는 `integration,hostnetwork` 태그로 분리되어 기본 `make integration`에 추가되지 않습니다.

`make hostnetwork-multicluster`는 host-mode 클러스터 간 RBD snapshot mirror, CephFS mirror·backup, RGW multisite를 검사합니다. RGW는 자동 선택한 daemon endpoint를 양쪽 최종 period에 반영하고 양방향 연결·gateway 재시작·metadata master 전환을 검증했습니다. 실행 결과와 검증 범위는 [CLUSTER_SCENARIOS.md](docs/CLUSTER_SCENARIOS.md)와 [HOST_NETWORK_POC.md](docs/HOST_NETWORK_POC.md)에 기록합니다.

Docker Desktop의 host networking은 4.34 이상에서 설정으로 활성화하는 기능이며 Linux Engine과 네트워크 동작이 다릅니다. 이 설정이 꺼져 있으면 macOS 프로세스가 광고 주소에 도달하지 못하므로 host 모드의 `Run`이 위 검증에서 실패합니다. 현재 macOS 환경의 Python RADOS 검증은 Desktop Linux VM의 host 네트워크 안에 있는 별도 클라이언트 컨테이너에서 수행합니다. macOS native Ceph 클라이언트의 인증된 RADOS I/O 자체를 검증한 결과는 아닙니다. RGW 테스트는 호스트 Go HTTP 클라이언트의 endpoint 도달 가능 여부를 별도로 확인하고, 연결되면 signed S3 읽기·쓰기를 추가 검증합니다. 연결되지 않으면 VM에서 통과한 범위와 호스트 HTTP 미검증 상태를 각각 로그에 남깁니다. `CEPH_TEST_HOST_HTTP_REQUIRED=1`이면 호스트 HTTP 연결 실패도 테스트 실패로 처리합니다. 기본 suite timeout은 40분이며 `HOSTNETWORK_TIMEOUT`으로 바꿀 수 있습니다. [Docker host network 지원 범위](https://docs.docker.com/engine/network/drivers/host/), [Testcontainers networking](https://golang.testcontainers.org/features/networking/)을 참고합니다.

Docker Desktop host networking을 활성화한 뒤에는 macOS Go 프로세스에서 두 RGW의 signed S3 읽기·쓰기·삭제와 MON/MGR/OSD의 실제 광고 포트에 대한 TCP 연결이 통과했습니다. OSD 추가·삭제 후 새 포트도 도달했습니다. `CEPH_TEST_HOST_TCP_REQUIRED=1`을 지정하면 이 직접 TCP 검사를 클러스터 기동과 각 OSD 변경 전후에 필수로 실행합니다. go-ceph 연동 테스트의 실행 지원 범위는 Linux로 한정합니다.

```sh
make hostnetwork
# Docker Desktop host networking 활성화 후 호스트 직접 접근을 필수로 검사
CEPH_TEST_HOST_HTTP_REQUIRED=1 CEPH_TEST_HOST_TCP_REQUIRED=1 make hostnetwork
make hostnetwork-multicluster
# 다른 역할별 이미지는 기존 CEPH_TEST_IMAGE/CEPH_TEST_OSD_IMAGE 등의 변수를 사용합니다.
```

### RGW / S3

```go
rgw, err := cluster.StartRGW(ctx)
if err != nil {
    t.Fatal(err) // 부분 생성 RGW도 cluster cleanup이 정리합니다.
}
endpoint, err := rgw.S3Endpoint(ctx)
if err != nil {
    t.Fatal(err)
}
// S3 SDK: endpoint, rgw.AccessKey, rgw.SecretKey, rgw.Region
// path-style bucket addressing을 사용합니다.
_ = endpoint
```

RGW는 HTTP endpoint를 publish하므로 호스트 Go 프로세스에서 일반 S3 클라이언트를 사용할 수 있습니다. 검증 테스트는 표준 라이브러리의 HTTP와 SigV4 서명을 사용합니다. `StartRGW`의 기본 구성은 gateway 1개와 일반 S3 테스트 사용자 1개입니다. `WithRGW`/`StartRGWWithConfig`로 이름별 gateway를 추가하고 `RemoveRGW`로 제거할 수 있습니다. 같은 zone의 gateway들은 같은 저장 상태를 사용합니다.

### RBD / CephFS

RBD metadata pool은 `InitRBDPool`로 초기화하고 `CreateRBDNamespace`로 분리할 수 있습니다. image 생성·읽기·쓰기·snapshot은 소비자 librbd 또는 클라이언트 컨테이너의 `rbd` CLI로 수행합니다. 별도의 RBD 서버 컨테이너는 필요하지 않습니다. 실제 데이터와 snapshot/clone 검증은 [rbd_integration_test.go](internal/integration/rbd_integration_test.go)에 있습니다.

```go
fs, err := cluster.StartCephFS(ctx)
if err != nil {
    t.Fatal(err)
}
// WithClient()로 연결한 클라이언트에서 fs.FilesystemName을 사용합니다.
// fs.MetadataPool과 fs.DataPool도 조회할 수 있습니다.
_ = fs
```

기본 CephFS 생성은 `tc-cephfs` 파일시스템, metadata/data 풀, MDS 1개를 생성하고 rank 0의 `up:active`를 기다립니다. 검증에는 공식 이미지 내부의 Python `libcephfs` 바인딩을 사용했습니다. 이 네이티브 라이브러리는 Linux 컨테이너 안에만 있으며 Go 호스트의 cgo 의존성을 추가하지 않습니다. RBD kernel mapping과 CephFS kernel/FUSE mount는 이번 검증 범위에 포함하지 않습니다.

### Linux go-ceph 연동 테스트

`make goceph-linux`는 미리 준비한 Linux client와 runner 이미지를 받아 testcontainers 클러스터와 go-ceph 소비자 테스트를 실행합니다. Probe 소스와 별도 Go 모듈은 이 프로젝트에 유지합니다. 소비자는 probe 실행 파일·Linux native 라이브러리가 있는 client와 검증할 Go checkout의 integration binary가 있는 runner를 준비합니다. 이 두 소비자 이미지는 Ceph 역할 이미지 계약과 별개이며 이미지 프로젝트의 산출물이나 CI 검사를 전제하지 않습니다. 자세한 실행 계약은 [IMAGE_COMPATIBILITY.md](docs/IMAGE_COMPATIBILITY.md)를 따릅니다.

bridge/host 각각 두 클러스터를 함께 실행하여 CephX·FSID, 같은 이름의 RADOS object/RBD image/CephFS file 분리, 새 연결에서 전체 데이터 비교, RBD snapshot 불변성, 각 OSD `2 → 3 → 2` 후 읽기·쓰기와 삭제를 검사합니다. host 모드에는 `ConnectionConfig()`를 사용하는 Linux 프로세스 검증도 포함합니다. 클러스터 제어에는 기존 CLI API를 사용하며 데이터 I/O는 Go의 go-ceph API로 수행합니다.

```sh
CEPH_TEST_GOCEPH_CLIENT_IMAGE=ceph-testcontainers-goceph:20.2.4-client \
CEPH_TEST_GOCEPH_RUNNER_IMAGE=ceph-testcontainers-goceph:20.2.4-runner \
make goceph-linux

# CLI로 이미지를 직접 지정할 수도 있습니다.
python3 internal/integration/goceph/run.py \
  --client-image ceph-testcontainers-goceph:20.2.4-client \
  --runner-image ceph-testcontainers-goceph:20.2.4-runner
```

두 이미지는 로컬 Docker 엔진에 존재해야 하며 harness는 이미지를 생성하거나 내려받지 않습니다. Docker socket을 runner에 연결하고 host network를 사용하므로 로컬 Linux Docker Engine 또는 host networking을 켠 Docker Desktop이 필요합니다. Python 3.9 이상과 Docker CLI가 필요하며 호스트에 Go/Ceph 개발 라이브러리를 설치하지 않습니다. macOS/Windows에서도 native I/O는 Docker의 Linux 환경에서 수행합니다. 기존 결과와 범위는 [Linux go-ceph 검증 기록](docs/HOST_NETWORK_POC.md#linux-go-ceph-연동-검증)에 정리합니다.

## API

| API | 역할 |
| --- | --- |
| `Run(ctx, image, opts...)` | 클러스터 부트스트랩, 요청한 초기 MGR 활성화, 초기 owned OSD up/in 확인 |
| `WithMonitorCount(n)` / `WithManagerCount(n)` | 초기 MON quorum 후보와 active/standby MGR 수 |
| `WithOSDCount(n)` | 초기 OSD 수, 기본 2개 |
| `WithNoInitialOSDs()` | MON/MGR부터 시작하고 이후 명시적 OSD 추가 |
| `WithNoInitialManagers()` | 초기 MGR 없이 MON/OSD로 시작하고 이후 명시적 MGR 추가 |
| `WithCephFS(configs...)` / `WithRGW(configs...)` | 초기 filesystem별 active/standby/replay MDS와 이름별 gateway 구성 |
| `WithOSDBlockSize(bytes)` | OSD sparse 파일 크기, 기본 1 GiB·최소 64 MiB, [작은 OSD 준비 조건](docs/SMALL_OSD_STORAGE.md) |
| `WithStartupTimeout(duration)` | 부트스트랩 및 개별 토폴로지 작업 제한, 기본 3분 |
| `WithHostNetwork()` | 모든 daemon과 클라이언트의 Docker host network, MON/RGW 자동 포트 선택 |
| `WithHostAddress(address)` | host mode의 실제 bind·광고 IPv4 주소, 기본 `127.0.0.1` |
| `UsesHostNetwork()` / `PublicAddress()` | 네트워크 모드와 광고 주소 조회 |
| `ConnectionConfig()` | native client에 전달할 설정·admin keyring 복사본 |
| `WithOSDImage(image)` | 초기 OSD와 이후 추가 OSD의 이미지 선택 |
| `WithRGWImage(image)` | `StartRGW`의 이미지 선택 |
| `WithMDSImage(image)` | `StartCephFS`의 MDS 이미지 선택 |
| `AddMonitor(ctx, name)` / `RemoveMonitor(ctx, name)` | 같은 monmap의 MON 추가·제거 |
| `AddManager(ctx, name)` / `RemoveManager(ctx, name)` | MGR candidate 추가·제거와 standby 승격 확인 |
| `Monitors()` / `Managers()` | 이름순 소유 daemon handle 목록 |
| `QuorumStatus(ctx)` / `ManagerStatus(ctx)` | native quorum·active/standby map 조회 |
| `ControlContainer()` / `ControlImage()` | 남은 quorum으로 관리하는 CLI handle / 설정된 control 이미지 |
| `AddOSD(ctx)` | OSD 등록, 포맷, 컨테이너 실행, up/in 확인 |
| `RemoveOSD(ctx, id)` | drain → safe-to-destroy → stop → down → purge → 컨테이너 제거 |
| `RefreshMonitorConfig(ctx)` | 현재 quorum의 MON 주소를 소유 데몬들의 설정 파일에 다시 반영 |
| `OSDs()` | ID 순서로 정렬한 소유 OSD 목록 |
| `OSDs()[i].Stop/Start` | 해당 데몬의 정지/재시작을 통한 장애 주입 |
| `Ceph(ctx, args...)` | 제어 컨테이너에서 CLI 실행, stdout 반환 |
| `Status(ctx)` | readiness에 필요한 상태 JSON 일부 |
| `CollectDiagnostics(ctx, config)` | 부분 생성·정지·종료 상태도 포함하는 제한된 진단 report 수집 |
| `WaitForClean(ctx)` | 소유 OSD up/in, MGR 활성, 모든 PG active+clean 대기 |
| `NetworkName()` / `WithClient()` | 애플리케이션 컨테이너 연결 |
| `StartRGW(ctx)` / `RGWContainer.S3Endpoint(ctx)` | S3 gateway 기동, 테스트 자격 증명 및 호스트 HTTP endpoint |
| `StartRGWWithConfig(ctx, config, opts...)` / `RemoveRGW(ctx, name)` | 이름별 gateway 추가·제거, zone의 기존 데이터 유지 |
| `StartCephFS(ctx, opts...)` | 풀·파일시스템 생성, MDS 기동 및 active 대기 |
| `StartCephFSWithConfig(ctx, config, opts...)` / `CephFSContainer.ScaleMDS(ctx, active, standby)` | filesystem 구성과 실행 중 MDS 수 변경 |
| `CephFSConfig.NoInitialMDS` | 최초 MDS 없이 FS/pool 생성; 첫 명시적 `ScaleMDS(ctx, 1, 0)` 전 embedded handle은 nil |
| `CephFSContainer.RemoveStoppedMDS(ctx, daemon)` | native 전역에서 원래 이름이 사라진 stopped original CID만 제거; auth·desired capacity 유지, [명시적 replacement 계약](docs/CEPHFS_STOPPED_MDS.md) |
| `CephFSContainer.AddMDSReplacement(ctx, originalStopped)` | original 1 active / 0 standby의 failed rank에 새 indexed MDS를 추가한 뒤 별도 original CID retire; [last-MDS 계약](docs/CEPHFS_LAST_MDS_REPLACEMENT.md) |
| `Filesystems()` / `Gateways()` | 이름순 소유 filesystem·gateway descriptor 목록 |
| `ServiceContainers()` | 소유 RGW/MDS 컨테이너 조회 |
| `ManagerContainer()` | 초기 MGR의 호환 handle. active 조회와 전체 후보에는 `ManagerStatus`·`Managers` 사용 |
| `Terminate(ctx)` | 소유 데몬과 네트워크 정리 |

일반 `testcontainers.With*` 옵션은 MON 컨테이너에 적용합니다. `WithOSDCount` 등의 모듈 옵션은 클러스터 설정에 적용합니다. 일반 옵션으로 MON의 이미지, 네트워크, 시작 명령, 내부 경로를 교체하면 부트스트랩 계약이 깨질 수 있습니다. 추가 MGR/OSD에 대한 임의 옵션 전파는 현재 구현하지 않았습니다.

`Run`의 성공은 클러스터 제어와 OSD 등록 준비를 의미합니다. 일반 애플리케이션용 풀은 호출자가 생성하며, RGW와 CephFS는 시작할 때 필요한 풀을 생성합니다. 작은 테스트를 위해 기본 PG는 8개, autoscaler는 off, PGP는 PG에 맞춰 자동 설정합니다. 풀을 만든 뒤, 혹은 토폴로지 변경 이후에는 `WaitForClean`으로 데이터 배치 완료를 기다릴 수 있습니다. 하나의 OSD만 사용할 경우 복제 수를 1로 설정해야 해당 풀의 `active+clean`을 기대할 수 있습니다.

RGW/MDS는 클러스터가 소유하므로 별도 cleanup 등록이 필요하지 않습니다. `cluster.Terminate`는 이 서비스들을 OSD보다 먼저 종료합니다. 오류와 함께 반환된 서비스도 클러스터 cleanup으로 정리합니다.

마지막으로 등록된 소유 OSD의 제거는 거부합니다. `RemoveOSD`는 각 단계에서 등록 UUID를 확인하여 같은 번호의 외부 replacement를 거부합니다. Purge 응답이 유실되면 handle을 유지하며 새 context로 재시도할 수 있습니다. 이미 완료된 purge는 Docker 정리만 재시도합니다. 이 정리가 끝나기 전에는 `AddOSD`를 거부합니다. 복제 수나 잔여 용량 때문에 안전한 이동이 불가능하면 timeout으로 끝나며 이미 out/reweight된 OSD를 자동으로 in 상태로 되돌리지는 않습니다. 외부 OSD 등록·교체는 삭제와 동시에 실행하지 않아야 합니다. [삭제 계약과 검증](docs/TOPOLOGY_EXTENSIONS.md#osd-삭제의-소유권과-재시도)을 따릅니다.

실패 진단은 cleanup 전에 별도의 짧은 background context로 `cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{})`를 호출합니다. 반환된 report는 `json.MarshalIndent`로 저장할 수 있고 일부 조회가 실패해도 artifact와 오류를 함께 보존합니다. Mirror/client 추가, 시간·출력 제한, 비밀 값 마스킹과 JSON 저장 예시는 [진단 snapshot](docs/DIAGNOSTICS.md)을 따릅니다. 수집은 클러스터를 변경하거나 종료하지 않습니다.

## 실행

태그 없이 `go test ./...`를 실행하면 호스트 단위 테스트가 실행됩니다. `make test-all`은 `CGO_ENABLED=0`과 `all` build tag로 일반 전체 suite를 실행하며 Docker와 준비된 Ceph 이미지가 필요합니다. Linux go-ceph와 알려진 native regression은 각각 `goceph`, `native_regression`을 추가해야 선택됩니다.

```sh
make test
make check
make test-all

# Docker 실행 없이 source-owned CI 목록과 실제 compiled coverage 확인
python3 .github/scripts/tag_scenarios.py plan --verify --output artifacts/tag-plan.json
```

PR 검증은 `Code checks`, `Docker checks`, `Ceph short`, `Ceph topology`, `Ceph multicluster`, `Ceph recovery` workflow로 나눕니다. `Code checks` 안에서도 unit·race·static·tag coverage를 독립 job으로 표시합니다. Runtime 결과는 유형과 batch 이름으로 표시하며 compile → environment → native → cleanup 중 실제 실패한 단계의 summary와 annotation을 남깁니다. 알려진 native regression은 별도 수동 `Native regressions` workflow로 실행합니다.

CI는 소스의 `ci_code`, `ci_environment`, `ci_short`, `ci_topology`, `ci_multicluster`, `ci_recovery`와 `ci_batch_*`를 읽어 자동으로 batch를 구성합니다. 각 runtime workflow는 자기 category만 선택하며, 전체 compiled coverage는 `Code checks`에서 확인합니다. 같은 파일에 Test를 추가하거나 새 batch 파일을 추가할 때 workflow의 테스트 이름 목록을 수정하지 않습니다. `all`은 category 조건을 우회하므로 `all,ci,ci_short`는 선택 필터가 아닙니다. Workflow 구분과 category/batch 실행은 [TEST_TAGS.md](docs/TEST_TAGS.md)의 planner/runner를 사용합니다.

기존 이름 기반 Make target과 `-run`은 수동 진단 경로로 유지합니다. `check`는 단위·race·vet와 tag 컴파일만 수행하며 Docker를 실행하지 않습니다. 아래 target은 준비된 서버 이미지를 직접 실행합니다.

```sh
make scenario-default
make scenario-topology
make scenario-multicluster-topology
make scenario-cephfs-removal
make scenario-rbd-receivers
make scenario-mirror-initial-daemons
make scenario-rbd-namespaces
make scenario-rbd-namespace-observation
make scenario-storage-bootstrap
make scenario-osd-memory
make scenario-small-osds
make scenario-manager-bootstrap
make scenario-mds-bootstrap
make scenario-mds-replacement
make scenario-topology-extensions
```

`scenario-default`는 기본 서비스·노드 lifecycle과 cleanup을, `scenario-topology`는 MON/MGR/MDS/RGW의 구성·변경을 검사합니다. `scenario-multicluster-topology`는 독립 cluster와 RGW zone·RBD/CephFS peer 그래프를, `scenario-topology-extensions`는 분리 네트워크·복수 mirror daemon·zonegroup/zone lifecycle·단절 복구를 검사합니다. `scenario-cephfs-removal`은 retained peer/directory receipt와 원래 process·watcher 관측, 명시적 승인 뒤 새 daemon/peer/path 복구와 응답 유실을 보존하는 directory 등록·재등록을 별도 시간 예산에서 검사합니다. Control/OSD/RGW/MDS 이미지 override 네 개는 각 profile에서 해제하며 mirror는 클러스터의 control 이미지를 사용합니다. 대표 범위와 기존 slim 결과·새 원본 실행 결과는 [CLUSTER_SCENARIOS.md](docs/CLUSTER_SCENARIOS.md)에서 구분합니다. `topology-smoke`는 빠른 일부 검사입니다.

O source에서 확인한 필수 compiled 선택은 N source의 115개에 `TestMultiClusterRBDNamespaceImageObservation` 한 이름만 추가한 116개입니다. 기본 14개, 필수 internal 114개와 SDK 전체 11개 중 선택 2개를 확인했고 기존 selector는 유지했습니다. 원래 owner와 별도로 retained namespace view의 `ImageStatus`·`WaitReplayReady`를 위한 `scenario-rbd-namespace-observation`을 추가했습니다. 첫 scoped native FAIL은 보존하며, 두 OSD·두 replica와 기존 strict health 조건을 유지한 원본 Quay Linux ARM64 bridge/host 재실행은 3개 RUN/PASS·package886.778초·자체 cleanup의 새 container/network0개를 확인했습니다. 18개 ready 관측과 18개 2MiB byte 기록·12개 source checkpoint ID는 구분하며, 이 focused 결과를 전체116개 CI나 다른 image/platform 조합의 새 성공으로 표시하지 않습니다. [Scoped image 계약과 검증 상태](docs/RBD_NAMESPACE_IMAGE_OBSERVATION.md)를 따릅니다.

`scenario-mds-bootstrap`은 `TestNoInitialMDSTopology` 한 parent를 별도 Go 50분·job 60분 예산에서 선택합니다. Cold bootstrap 추가 당시의 필수 compiled 선택은 O source의 실제 116개에 이 이름만 추가한 117개였습니다. 기본 14개, 필수 internal 115개와 SDK 전체 11개 중 선택 2개를 확인했고 기존 selector는 유지했습니다. 두 OSD·두 replica, cold target과 ready sibling의 original identity·가용성·첫 MDS·fresh nonce I/O 및 엄격한 health/cleanup을 순차 bridge/host에서 검사하도록 구성했습니다. 첫 native는 bridge 기능 관측과 자체 raw removal을 완료했지만 fallback의 중복 client 제거로 package 103.921초 FAIL했습니다. Host는 시작하지 않았고 별도 outer cleanup은 새 리소스 0개였습니다. 성공한 explicit 제거 receipt만 fallback에서 건너뛰는 좁은 수정 뒤 원본 Quay Linux ARM64의 bridge/host 재실행은 3개 RUN/PASS·package 203.141초로 완료했습니다. 2개 cold zero/첫 active/가용성 관측과 10개 128 KiB byte 기록(독립 nonce dataset 4개), final strict HEALTH_OK·required module closure, 자체 outer cleanup의 새 container/network 0개를 확인했습니다. 256개 runtime source와 고정 image policy가 유지됐으며 새 117개 전체 CI나 다른 image/platform의 성공으로 합산하지 않습니다. 같은 source의 기존 bridge-only ordinary MDS scale/standby-replay도 별도 2개 RUN/PASS·202.702초와 자체 cleanup 0개로 확인했으며 [현재 계약과 검증 상태](docs/NO_INITIAL_MDS.md)를 따릅니다. O116·N115·M114와 과거 전체 CI101 결과는 해당 source의 증거로 유지합니다.

`scenario-mds-replacement`는 `TestStoppedMDSRetirementTopology`를 독립 Go 50분·job 60분 예산에서 선택합니다. Q 추가 당시 필수 compiled 선택은 P의 117개에 이 parent만 추가한 118개였으며 기본 14개·필수 internal 116개·SDK 전체 11개 중 선택 2개와 기존 selector를 확인했습니다. `RemoveStoppedMDS`는 전역 native 이름 부재와 건강한 owned rank를 확인한 뒤 원래 stopped CID만 retire하고, 기존 `ScaleMDS(ctx, 1, 1)`로 새 standby를 구성하도록 합니다. 원본 Quay Linux ARM64의 독립 bridge/host 실행은 3개 RUN/PASS·package 207.328초, 20개 128 KiB byte 기록(독립 dataset 8개), strict health/modules와 자체 outer cleanup의 새 리소스 0개를 확인했습니다. 같은 source의 기존 cold-MDS 회귀도 별도 3개 RUN/PASS·201.502초·자체 cleanup 0개로 완료했습니다. 이름/GID와 Docker CID의 관측은 별도이며 CID-to-GID process binding·replay/foreign-standby native 변형·전체 118개 CI나 다른 이미지/platform의 성공을 추가로 주장하지 않습니다. [계약과 실제 실행 범위](docs/CEPHFS_STOPPED_MDS.md)를 따릅니다.

이전 이름 기반 CI 구성에는 `scenario-cluster-fixtures`, `scenario-cephfs-fixtures`, `scenario-rados-fixtures`, `scenario-rbd-fixtures`, `scenario-rgw-fixtures`, `scenario-rgw-sync-fixtures`의 6개 추가 profile이 있었으며 당시 각각 9/8/4/6/14/7개, 총 48개 이름이었습니다. 이 이름 기반 target은 수동 실행에 유지합니다. Manager bootstrap 추가 시점에는 기본·토폴로지 55개, 별도 CephFS 제거·재등록 복구 5개, RBD receiver 1개, 최초 daemon 없는 mirror 1개, 공유 RBD namespace 1개, 최초 OSD 없는 bootstrap 1개, 최초 MGR 없는 bootstrap 1개, fixture 48개와 Docker bridge SDK 2개를 합한 실제 distinct top-level test 선택이 115개였습니다. storage bootstrap까지 실제 선택한 M source의 114개에 `TestNoInitialManagerTopology` 한 이름만 추가한 compiled 목록을 확인했습니다. 새 manager bootstrap의 원본 Quay Linux ARM64 bridge/host focused 실행은 7개 RUN/PASS·141.986초·자체 cleanup의 새 리소스 0개를 확인했으며, 이 N source의 115개 전체 CI 성공으로 표시하지 않습니다. 공유 namespace 관측 시점의 113개와 과거 전체 CI PASS도 각각의 source 증거로 유지합니다. `scenario-multicluster-topology` 20개와 `scenario-cephfs-removal` 5개는 겹치지 않습니다. 후속 `TestOSDRemovalLifecycle`, `TestMonitorRollingReplacement`, `TestMultiClusterMonitorBootstrapRefresh`, `TestMultiClusterTopologySnapshotsHonorBusyOwners`, `TestMultiClusterCephFSPeerRemovalDrain`, `TestMultiClusterCephFSDirectoryRemovalRelease`, `TestMultiClusterCephFSOriginalProcessQuiescence`, `TestMultiClusterCephFSOriginalProcessQuiescenceRecovery`, `TestMultiClusterCephFSDirectoryAdditionIntent`, `TestMultiClusterRBDReceiverReadiness`, `TestMultiClusterNoInitialMirrorDaemons`, `TestMultiClusterRBDNamespaceBinding`, `TestNoInitialOSDTopology`, `TestNoInitialManagerTopology`, `TestMultiClusterRBDNamespaceImageObservation`은 아래 `d9115f4`의 전체 CI 증거 101개에 포함되지 않으며 각 로컬 실행 증거를 별도로 기록합니다. `scenario-goceph-linux`는 호출자가 준비한 client/runner 이미지로 별도 실행하는 선택 target입니다. 이미지 프로젝트 CI는 자체 이미지 검사기를 실행하며 Go integration이나 go-ceph를 실행하지 않습니다.

Source `d9115f4`의 [전체 CI run 37240162309](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37240162309)는 terminal SUCCESS입니다. 상세 job 10개의 101개 named test와 선택된 child 121개가 모두 RUN/PASS했고 parent/child FAIL·SKIP은 0개였습니다. 101개는 Ceph runtime 90개·bootstrap 실패 cleanup 1개·Docker bridge SDK 2개·helper 검사 8개입니다. 공식·Debian·Ubuntu의 12개 native 이미지 조합도 각각 대표 9개를 통과했고 상세·matrix cleanup artifact 22개에서 새 container/network 0개를 확인했습니다. Matrix 반복이나 child 수를 distinct native I/O 수로 더하지 않습니다.

이전 `be58018`의 [run 37226924156](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37226924156)은 97개 PASS·RGW sync 2개 FAIL·translation 2개 미실행으로 종료됐으며, 그 실패와 로컬 focused 실행의 strict source 검증 제한은 그대로 보존합니다. 알려진 RGW numeric priority·ordinary-user source 권한 거부는 계속 native 한계입니다. 정확한 source·named test·소비자 조건·원본 로그·cleanup 증거는 [CI 완료 기록](docs/CI_FIXTURES.md#d9115f4-전체-ci-완료)을 따릅니다.

RGW 공개망 우선순위 수정까지 포함한 `3f79a78`의 [Linux AMD64 CI](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37173593510)에서 원본 Quay 기본 14개와 topology 38개 전체, Docker bridge SDK 회귀 2개가 통과했습니다. 새 서버 이미지 빌드 없이 실행했으며, 기본 14개에는 native/runtime 11개와 signer helper 3개가 포함됩니다. Docker Desktop의 peer 단절 중 공개 포트 경로 한계와 이전 실행은 구성별 기록에 구분합니다.

`make integration`, `make topology`, `make topology-extensions`, `make cluster-features`, `make client-fixtures`는 선택한 이미지 환경 변수를 사용하는 기존 별도 실행 경로로 유지합니다. 전체 client recipe에는 알려진 원본 서버 한계와 consumer 도구 조건이 있으므로 기본 Quay suite 전체 통과로 해석하지 않습니다.

각 인터페이스만 실행할 수도 있습니다.

```sh
CGO_ENABLED=0 go test -tags=integration -run '^TestRGWS3$' -count=1 -v -timeout=15m ./internal/integration
CGO_ENABLED=0 go test -tags=integration -run '^TestRBDLifecycle$' -count=1 -v -timeout=15m ./internal/integration
CGO_ENABLED=0 go test -tags=integration -run '^TestCephFSFilesystem$' -count=1 -v -timeout=15m ./internal/integration
```

통합 테스트는 `all` 또는 기존 `integration`/capability build tag로 선택합니다. Docker가 없을 때 조용히 skip하지 않으므로 PoC 실행 여부를 분명하게 알 수 있습니다. 다른 이미지로 같은 시나리오를 시험하려면:

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:YOUR_VERSION make integration
```

Docker Desktop 소켓을 자동으로 찾지 못하는 환경에서는 현재 Docker context에 맞는 `DOCKER_HOST`를 지정합니다. Ryuk에는 VM 내부 소켓 경로가 필요할 수 있습니다.

```sh
export DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
make integration
```

## 이미지 선택

기본 `DefaultImage`는 digest로 고정한 원본 Quay Ceph 이미지입니다. 준비된 `all` 이미지 하나를 재사용하거나, 호환되는 `control`, `osd`, `rgw`, `mds` 이미지를 지정합니다. `control`은 MON/MGR, CLI·Python client, RBD/CephFS mirror daemon과 multisite 관리 도구를 모두 포함합니다. MON/MGR와 mirror는 같은 이미지를 사용하더라도 각각 별도 컨테이너로 실행하며, 요청한 daemon만 시작합니다.

```go
cluster, err := ceph.Run(ctx, "ceph-testcontainers:official-20.2.4-control",
    ceph.WithOSDImage("ceph-testcontainers:official-20.2.4-osd"),
    ceph.WithRGWImage("ceph-testcontainers:official-20.2.4-rgw"),
    ceph.WithMDSImage("ceph-testcontainers:official-20.2.4-mds"),
)
```

생략한 역할은 `Run`의 이미지로 실행하므로 그 이미지에도 생략한 역할의 구성요소가 있어야 합니다. 추가 OSD에도 같은 OSD 설정을 적용합니다. 한 이미지 방식은 `Run(ctx, "ceph-testcontainers:official-20.2.4-all", ...)`로 사용합니다. 위 태그는 미리 준비한 이미지의 예시이며 회사 이미지나 다른 registry reference도 선택할 수 있습니다.

고정된 [이미지 요구사항](../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)은 다섯 역할의 실행 계약 하나를 정의합니다. 이전 `base`/`multicluster` 단계 구분은 사용하지 않으며 mirror도 `control`의 필수 구성입니다. `control`은 Python native clients, librbd 암호화·libcryptsetup, `cryptsetup` 실행 파일과 `rados --striper`·libradosstriper를 제공하고, `osd`는 `hello`·`lock` object class를 제공합니다. `all`은 네 역할의 합집합입니다. 이미지의 배포판·패키지 이름·빌드 방법·label·생성 manifest는 Go API의 조건이 아닙니다. 이미지 요구사항·검사·역할 이미지 생성은 [이미지 프로젝트](../ceph-testcontainers-images/README.md), 공식 이미지에서의 역할 추출은 [ROLE_IMAGES.md](../ceph-testcontainers-images/docs/ROLE_IMAGES.md)를 따릅니다. Go 모듈은 런타임에 설정·키·bootstrap과 entrypoint를 제공하며 이미지를 빌드하거나 패키지를 설치하지 않습니다.

이미지 프로젝트의 `quick`은 구성요소의 존재·로딩을, `full`은 자체 Docker harness의 기본 서비스·다중 클러스터·RBD 암호화/rekey·object class·striper를 포함한 11개 시나리오를 검사합니다. Go 모듈의 토폴로지·fixture·SDK 전체 검증과는 별개입니다. `make image-compatibility`는 준비된 이미지로 Go API 대표 9개 시나리오를 실행하며 역할별 환경 변수도 유지합니다. 사용 예와 역할별 명령 실행 위치, 추가 소비자 도구의 조건은 [IMAGE_COMPATIBILITY.md](docs/IMAGE_COMPATIBILITY.md)에 정리합니다.

필수 Go CI의 Ceph 시나리오는 준비된 공식 역할 이미지 네 개로 Linux AMD64에서 실행하고, 실제 image ID·digest·platform과 native assertion·cleanup을 기록합니다. 공식·Debian·Ubuntu, `all`/역할 조합과 AMD64/ARM64의 quick/full 이미지 검증은 이미지 프로젝트 CI가 담당하며, 자동 12개 Go 호환성 matrix는 실행하지 않습니다. 이미지 검사기의 다른 platform PASS를 해당 platform의 Go 모듈 연결 검증과 동일하게 해석하지 않습니다.

`make image-matrix IMAGE_VARIANT=debian IMAGE_LAYOUT=roles`와 `make image-compatibility`는 준비된 이미지로 대표 9개를 검사하는 수동 경로로 유지합니다. [수동 입력 선택과 이전 matrix 증거](docs/IMAGE_COMPATIBILITY.md#공식debianubuntu-이미지-matrix)를 따르며 `ceph.DefaultImage`의 원본 Quay 기본값은 유지합니다.

Linux go-ceph client/runner와 `CEPH_TEST_VAULT_IMAGE`로 선택하는 외부 Vault는 별도 fixture 입력이며, 추가 SDK·서비스 도구를 Ceph 서버 이미지 조건에 넣지 않습니다.

## 다중 클러스터 구성과 PoC

단일 클러스터 구성은 `ceph` 패키지에서, 기존 클러스터 사이의 zone/peer 연결은 별도 `multicluster` 패키지에서 관리합니다. `RunRGWMultisite`, `RunRBDMirror`, `RunCephFSMirror`는 기존 두 클러스터를 받아 추가 데몬·client·네트워크 연결을 소유합니다. `RunRGWTopology`는 두 개 이상의 클러스터를 하나의 realm/zonegroup에 연결하며 `RGWTopologyConfig.Zones`와 `MetadataMaster`로 zone 배치와 초기 master를 선택합니다. 연결을 먼저 종료하고 클러스터를 나중에 종료합니다. 연결의 `Terminate`는 클러스터나 데이터를 삭제하지 않으며 Ceph 내부 peer/auth 등의 설정은 일회성 클러스터에 남깁니다. [API 구성과 사용 예](docs/MULTICLUSTER_API.md)를 확인합니다.

RGW의 `Zones()`는 이름순 zone descriptor를, `ZoneAdmin(ctx, name, args...)`은 해당 zone의 관리 CLI를 제공합니다. `AddZone`으로 독립된 새 클러스터를 추가하면 활성 period와 secondary의 초기 metadata 준비를 확인한 뒤 기존 gateway에 최종 period를 적용합니다. **추가 완료나 gateway 재시작 뒤에는 각 `Gateway.S3Endpoint(ctx)`를 다시 조회**하여 S3 client를 갱신합니다. bridge mode의 host published port는 재시작 때 바뀔 수 있습니다. `PeerEndpoint`는 gateway 간 연결용 주소입니다.

CephFS mirror는 현재 owned MGR 후보에 peer network를 준비합니다. 새 MGR를 추가하면 `AttachManagers(ctx)`로 연결을 재조정할 수 있고 `RebootstrapPeer`도 자동으로 이를 확인합니다. 초기 MGR를 제거해도 남은 active와 standby로 연결 구성을 계속 사용할 수 있습니다.

`make multicluster`는 독립된 두 개 또는 세 개의 클러스터에서 다음 구성을 순차 검증합니다.

- RGW 2/3 zone, gateway 중단·복구, metadata master A → B → A 전환
- RBD snapshot/journal mirror pair와 3 cluster fanout, receiver 중단·복구, peer 제거·재등록과 A → B → A 전환
- CephFS snapshot mirror pair, daemon 재시작, peer 제거·재등록, OSD 교체와 source 정지 후 독립 읽기

기존 suite에는 RGW 선택 복제 정책, RBD split-brain·전체/증분 archive 복원과 CephFS archive PoC도 포함되어 있습니다. 이들 기능의 확장은 토폴로지 작업의 완료 조건에서 제외합니다.

일반 단일 클러스터 테스트와 별도로 `integration,multicluster` build tag를 사용합니다. 전체 suite timeout은 기본 60분이며 `MULTICLUSTER_TIMEOUT`으로 바꿀 수 있습니다. 전용 `rbd-mirror`·`cephfs-mirror` 데몬은 `control`과 `all`의 필수 구성입니다. 통합 테스트는 두 연결 API에 source 클러스터의 `ControlImage()`를 전달하며 mirror 전용 이미지 환경 변수는 사용하지 않습니다. RGW multisite 관리 client는 각 클러스터의 `ControlImage()`를 기본으로 사용하며 `ControlImage` 설정으로 공용 이미지를 명시할 수 있습니다. RBD archive helper는 Go Reader/Writer로 byte를 전달하며 Go 호스트의 cgo나 kernel mount는 필요하지 않습니다.

전환은 writer fencing·동기화 완료 확인·명시적 승격을 수행하는 계획된 절차입니다. RBD split-brain resync는 선택하지 않은 branch를 폐기합니다. CephFS native mirror의 user xattr 차이는 계속 관측되므로 완전한 metadata 보존으로 해석하지 않습니다. 구성과 케이스별 실제 결과는 [MULTICLUSTER_POC.md](docs/MULTICLUSTER_POC.md)를 확인합니다.

전체 MON 교체 뒤 caller client와 multicluster link의 명시적 local/remote 주소 갱신은 [bootstrap 재연결 계약](docs/MON_BOOTSTRAP_REFRESH.md)을 따릅니다.

RBD image와 CephFS directory의 상태 조회·bounded 준비 대기는 [mirror 관측 계약](docs/MIRROR_OBSERVABILITY.md)을 따릅니다. RBD readiness와 CephFS의 exact source snapshot 완료를 구분하며, destination의 실제 데이터는 client에서 별도로 확인합니다. CephFS 디렉터리 제거는 `BeginDirectoryRemoval`의 retained receipt와 `Status`·`WaitReleased`로 원래 live cohort의 sync cycle 해제를 확인할 수 있습니다. [제거·재시도 계약](docs/CEPHFS_DIRECTORY_REMOVAL.md)을 따르며 기존 `RemoveDirectory`는 정책 요청 API입니다.

CephFS peer 제거는 `BeginPeerRemoval`로 원래 peer·daemon cohort를 보존하고 `Status`·`WaitDrained`로 같은 live session의 replayer 종료를 확인할 수 있습니다. 응답 유실과 재시도, 기존 정책 요청 API 및 적용 조건은 [peer 제거 계약](docs/CEPHFS_PEER_REMOVAL.md)을 따릅니다. 원래 process가 중지·재시작·제거된 뒤에는 선택적 raw Docker observer와 receipt의 `ProcessQuiescence`로 원래 task·watcher 종료를 별도로 관측합니다. [원래 process 관측 계약](docs/CEPHFS_PROCESS_QUIESCENCE.md)을 따르며 이 read-only 결과는 pending 제거 gate를 변경하지 않습니다. 원래 daemon을 모두 명시적으로 제거한 뒤 `AcknowledgeProcessQuiescence`로 새 증거를 확인하고 구성을 재개하는 방법은 [복구 승인 계약](docs/CEPHFS_PROCESS_ACKNOWLEDGMENT.md)을 따릅니다.

기존 topology·lifecycle 호출의 owner gate와 cleanup·network mutex 대기, context snapshot 및 CephFS setup/scale·provisioning·인증 admission은 caller context를 따릅니다. 취소된 대기자는 후속 native 변경 없이 반환하고 같은 fixture를 새 context로 재시도할 수 있습니다. 이미 생성된 identity/descriptor는 추적을 유지합니다. 적용 범위와 남은 customizer·publication 경로는 [잠금 대기 계약](docs/TOPOLOGY_CONTEXT.md)을 확인합니다.

## 현재 범위

상태 assertion용으로 [Pool 사용량](docs/POOL_USAGE_CHECK.md)과
[RGW 사용자·account 저장량](docs/RGW_USER_USAGE.md)을 조회할 수 있습니다.
Docker 안의 native CLI를 사용하며 Go Ceph SDK/cgo는 필요하지 않습니다.
비동기 통계와 account 집계 범위를 구분합니다.

MON quorum 상실·복구와 교체, MGR standby 승격, 여러 filesystem의 multi-active MDS·standby/replay 증감, 여러 RGW와 독립 클러스터·mirror/multisite를 구성할 수 있습니다. 기존 역할별 slim 이미지의 PoC에서는 RGW 3 zone과 초기 MGR 제거·standby 승격 후 CephFS mirror 재연결까지 bridge/host에서 검증했습니다. 원본 Quay 이미지의 실행 결과와 각 로그는 [CLUSTER_SCENARIOS.md](docs/CLUSTER_SCENARIOS.md)를 따릅니다. 서버 측 pool·Cephx·namespace·subvolume·사용자 정책은 [CLUSTER_INTERNAL_FEATURES.md](docs/CLUSTER_INTERNAL_FEATURES.md)에 정리합니다.

CephFS subvolume snapshot·비동기 clone, RGW placement·storage class, 임시 중앙 config·OSD flag와 in/out 제어도 제공합니다. 클라이언트 테스트에 필요한 서버 조건을 준비하고 원래 설정을 복원하는 API입니다. `make cluster-feature-extensions`로 실제 Linux 클라이언트와 함께 검증하며, 사용법과 복원·부분 실패 계약은 [CLUSTER_FIXTURE_EXTENSIONS.md](docs/CLUSTER_FIXTURE_EXTENSIONS.md)를 확인합니다.

5 MON, 여러 zonegroup·zone 탈퇴, 여러 mirror daemon, public/cluster 네트워크 분리와 선택적 endpoint 단절·복구 API도 제공합니다. 기존 역할별 slim 검증과 원본 Quay 필수 실행은 구성별 기록에서 구분합니다. OSD 컨테이너 1개를 테스트상의 저장 노드 1개로 취급하며 물리 호스트 장애 내성을 입증하지 않습니다. 객체·image·파일 CRUD와 프로토콜 기능 검증은 소비자 클라이언트가 수행합니다. kernel mapping/mount와 동일 daemon data directory를 재사용하는 전체 복원은 별도 harness 과제입니다.

CephFS `BeginDirectoryAddition`은 daemon이 없는 구성에서도 추가 intent를 보존합니다. 응답 유실 뒤 fresh Begin으로 소유권을 확정하며 `Status`는 관측만 수행합니다. [등록·재시도 계약](docs/CEPHFS_DIRECTORY_ADDITION.md)을 따릅니다.

`scenario-rbd-receivers`는 image 없는 RBD receiver의 namespace 발견·leader/member 합의와 장애·교체·zero inventory 복구를 검증합니다. [RBD receiver 관측 계약](docs/RBD_RECEIVER_READINESS.md)을 따릅니다.

RBD와 CephFS mirror는 `NoInitialDaemons`로 최초 daemon 없이 정책을 구성한 뒤 `AddDaemon`으로 시작할 수 있습니다. [최초 daemon 없는 구성 계약](docs/NO_INITIAL_MIRROR_DAEMONS.md)을 따릅니다.

RBD `BindNamespace`는 이미 설정된 여러 namespace mapping을 한 owner의 pool·peer·daemon 집합으로 관측합니다. View는 읽기 전용이며 추가 컨테이너나 cleanup 책임을 만들지 않습니다. [공유 namespace 계약](docs/RBD_NAMESPACE_BINDING.md)을 따르며 `make scenario-rbd-namespaces`로 검증합니다.

Retained RBD namespace view에서도 `view.ImageStatus(ctx, "volume")`와 `view.WaitReplayReady(ctx, "volume")`를 사용할 수 있습니다. Source image의 실제 snapshot/journal mode를 따르며, 한 live owned receiver의 replay 준비와 실제 bytes·checkpoint 전달을 구분합니다. 원래 owner의 configured mode·mapping은 유지합니다. [사용법·대기 중 cohort 계약·검증 상태](docs/RBD_NAMESPACE_IMAGE_OBSERVATION.md)를 따르며 `make scenario-rbd-namespace-observation`으로 독립 실행합니다.

`scenario-last-mds-replacement`는 `TestLastMDSReplacementTopology`를 별도 Go 50분·job 60분 예산으로 선택합니다. Last-MDS 추가 당시 required compiled 선택은 Q의 118개에 이 parent만 추가한 실제 119개였으며 기본 14개·필수 internal 117개·SDK 전체 11개 중 선택 2개를 유지합니다. Sole original 1 active / 0 standby의 stopped 등록 거부·caller fail·새 worker 추가·Q retire와 원래 additional-pool/sibling bytes를 검사합니다. 원본 Quay Linux ARM64의 독립 bridge/host 실행은 3개 RUN/PASS·package 223.931초, 24개 128 KiB full-reader 기록(독립 dataset 8개), strict failed-target health와 final HEALTH_OK/module closure·자체 cleanup의 새 리소스 0개를 확인했습니다. Native name/GID와 original Docker CID는 별도 관측이며 [계약과 실제 실행 범위](docs/CEPHFS_LAST_MDS_REPLACEMENT.md)를 따릅니다. Partial/lost reply/race·damaged/replay 변형과 전체 119개 CI·다른 image/platform의 성공은 추가로 주장하지 않습니다. Q118·P117 및 과거 focused/전체 CI101 결과는 해당 source의 기록으로 보존합니다. 당시 같은 262개 source의 기존 Q stopped-MDS 회귀도 별도 3개 RUN/PASS·212.955초, P cold-MDS 회귀는 별도 3개 RUN/PASS·197.593초와 각각 자체 cleanup 새 리소스 0개로 완료했습니다. 이 값들은 이전 Q의 source 259·207.328/201.502초나 P의 source 256·203.141/202.702초를 대체하지 않습니다.
