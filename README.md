# ceph-testcontainers-go

Ceph와 통신하는 애플리케이션을 테스트하기 위한 실험적 testcontainers-go 모듈입니다. 실제 Ceph 데몬을 작은 일회성 클러스터로 실행하고, 컨테이너 내부 CLI로 구성과 상태를 제어합니다. 공개 Go 모듈에는 `go-ceph`, 호스트 `librados`, cgo 의존성을 넣지 않습니다. 실제 go-ceph 소비자 검증은 별도 테스트 모듈에서 Linux 전용으로 실행합니다.

현재 PoC는 MON 1개, MGR 1개, 기본 OSD 2개를 각각 별도 컨테이너로 실행합니다. RGW와 CephFS용 MDS는 필요할 때 추가합니다. RBD는 별도 데몬 없이 OSD 풀을 사용합니다. OSD마다 1 GiB sparse BlueStore 파일을 사용합니다. Ceph 데몬에 privileged 모드, 호스트 디스크, LVM, Docker 소켓, systemd가 필요하지 않습니다. testcontainers 자체와 Ryuk은 Docker 엔진 접근이 필요합니다.

자료 조사와 판단 근거는 [RESEARCH.md](docs/RESEARCH.md), 클러스터 실행 결과는 [POC.md](docs/POC.md), RGW·RBD·CephFS 검증은 [SERVICES_POC.md](docs/SERVICES_POC.md)에 정리했습니다. 경량화의 초기 결과는 [SLIM_IMAGE_POC.md](docs/SLIM_IMAGE_POC.md), 현재 역할별 빌드와 검증은 [SLIM_IMAGE_AUTOMATION.md](docs/SLIM_IMAGE_AUTOMATION.md), 큰 구성요소와 분리 효과는 [COMPONENT_SIZE_ANALYSIS.md](docs/COMPONENT_SIZE_ANALYSIS.md)를 확인합니다.

## 프로젝트 구성

공개 API는 `ceph/`와 `multicluster/`에 나란히 두고, 루트의 `go.mod` 하나로 관리합니다. 각 패키지의 단위 테스트·godoc 예제는 구현 옆에 둡니다. Docker로 실행하는 통합 테스트와 PoC는 공개 API를 사용하는 별도 테스트 패키지로 모았습니다.

```text
go.mod                 두 공개 패키지를 관리하는 단일 Go module
ceph/                  단일 클러스터 API, 단위 테스트와 사용 예
ceph/internal/scripts/ ceph 패키지에 embed하는 bootstrap 스크립트
multicluster/          클러스터 사이의 구성·복제·백업 API
internal/integration/  단일·다중 클러스터의 Docker 통합 테스트와 PoC
image/slim/            역할별 이미지 빌드·분석 도구
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

모듈 경로는 배포 전 프로젝트 이름으로 설정되어 있으며 아직 원격 저장소나 릴리스를 만들지는 않았습니다. 같은 체크아웃에서 테스트를 실행하거나 소비 프로젝트에서 로컬 `replace`를 사용할 수 있습니다.

## 애플리케이션 연결

`cluster.WithClient()`를 애플리케이션 컨테이너의 `testcontainers.Run` 옵션으로 전달하면 클러스터 네트워크 연결과 `/etc/ceph/ceph.conf`, `/etc/ceph/ceph.client.admin.keyring` 복사를 함께 처리합니다. 해당 컨테이너에는 사용하려는 Ceph 클라이언트가 있어야 합니다. 이 옵션이 클라이언트 패키지를 설치하지는 않습니다.

```go
client, err := testcontainers.Run(ctx, ceph.DefaultImage,
    cluster.WithClient(),
    testcontainers.WithEntrypoint("sleep"),
    testcontainers.WithCmd("infinity"),
)
if client != nil {
    testcontainers.CleanupContainer(t, client)
}
if err != nil {
    t.Fatal(err)
}
// client.Exec(ctx, []string{"rados", "-p", "my-pool", "ls"}, ...)
```

애플리케이션 컨테이너는 클러스터보다 나중에 cleanup을 등록하여 먼저 종료합니다. `WithClient`가 복사하는 admin 키는 신뢰할 수 있는 테스트 컨테이너용입니다. 자격 증명에는 현재 테스트 클러스터 전체에 대한 권한이 있습니다.

Ceph 클라이언트는 MON에서 받은 OSD 주소로 직접 접속합니다. 기본 bridge 모드에서는 애플리케이션 컨테이너에 `WithClient()`를 적용하여 광고된 주소에 접근합니다. MON의 `MappedPort`만으로 macOS/Windows 호스트 프로세스에서 RADOS/RBD/CephFS 전체에 연결할 수 있다고 가정하면 안 됩니다. 근거: [Ceph 네트워크 문서](https://docs.ceph.com/en/tentacle/rados/configuration/network-config-ref/).

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

`make hostnetwork`는 두 클러스터의 동시 기동·서로 다른 MON/MGR/OSD endpoint·같은 pool/object 이름의 데이터 분리·양쪽 OSD `2 → 3 → 2` 변경 후 Python `librados` I/O와 실제 MON 포트 충돌 후 재시도를 검사합니다. RGW는 기본 포트 7480을 점유한 상태에서 서로 다른 endpoint를 만들고 signed S3 데이터를 비교합니다. RBD snapshot/clone과 CephFS 파일 I/O도 OSD 교체 전후에 검사합니다. 이 테스트는 `integration,hostnetwork` 태그로 분리되어 기본 `make integration`에 추가되지 않습니다.

`make hostnetwork-multicluster`는 host-mode 클러스터 간 RBD snapshot mirror와 CephFS mirror·backup을 검사합니다. 현재 RGW multisite는 bridge-mode 클러스터에서 제공하며 host mode는 구성 변경 전에 거절합니다. 실행 결과와 검증 범위는 [HOST_NETWORK_POC.md](docs/HOST_NETWORK_POC.md)에 기록합니다.

Docker Desktop의 host networking은 4.34 이상에서 설정으로 활성화하는 기능이며 Linux Engine과 네트워크 동작이 다릅니다. 현재 macOS 환경의 Python RADOS 검증은 Desktop Linux VM의 host 네트워크 안에 있는 별도 클라이언트 컨테이너에서 수행합니다. macOS native Ceph 클라이언트의 인증된 RADOS I/O 자체를 검증한 결과는 아닙니다. RGW 테스트는 호스트 Go HTTP 클라이언트의 endpoint 도달 가능 여부를 별도로 확인하고, 연결되면 signed S3 읽기·쓰기를 추가 검증합니다. 연결되지 않으면 VM에서 통과한 범위와 호스트 HTTP 미검증 상태를 각각 로그에 남깁니다. `CEPH_TEST_HOST_HTTP_REQUIRED=1`이면 호스트 HTTP 연결 실패도 테스트 실패로 처리합니다. 기본 suite timeout은 40분이며 `HOSTNETWORK_TIMEOUT`으로 바꿀 수 있습니다. [Docker host network 지원 범위](https://docs.docker.com/engine/network/drivers/host/), [Testcontainers networking](https://golang.testcontainers.org/features/networking/)을 참고합니다.

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

RGW는 HTTP endpoint를 publish하므로 호스트 Go 프로세스에서 일반 S3 클라이언트를 사용할 수 있습니다. 검증 테스트는 표준 라이브러리의 HTTP와 SigV4 서명을 사용합니다. 클러스터당 RGW 1개와 일반 S3 테스트 사용자 1개를 생성합니다.

### RBD / CephFS

RBD는 클라이언트 컨테이너에서 `rbd pool init`, `rbd create/import/export` 등 CLI로 제어합니다. 별도의 RBD 서버 컨테이너는 필요하지 않습니다. 실제 데이터와 snapshot/clone 검증은 [rbd_integration_test.go](internal/integration/rbd_integration_test.go)에 있습니다.

```go
fs, err := cluster.StartCephFS(ctx)
if err != nil {
    t.Fatal(err)
}
// WithClient()로 연결한 클라이언트에서 fs.FilesystemName을 사용합니다.
// fs.MetadataPool과 fs.DataPool도 조회할 수 있습니다.
_ = fs
```

CephFS는 `tc-cephfs` 파일시스템, metadata/data 풀, MDS 1개를 생성하고 rank 0의 `up:active`를 기다립니다. 검증에는 공식 이미지 내부의 Python `libcephfs` 바인딩을 사용했습니다. 이 네이티브 라이브러리는 Linux 컨테이너 안에만 있으며 Go 호스트의 cgo 의존성을 추가하지 않습니다. RBD kernel mapping과 CephFS kernel/FUSE mount는 이번 검증 범위에 포함하지 않습니다.

### Linux go-ceph 연동 테스트

`make goceph-linux`는 Linux runner 안에서 testcontainers 클러스터를 구성하고, 별도 모듈의 go-ceph v0.41.0 클라이언트를 실제로 컴파일·실행합니다. Ceph 20.2.4 공개 헤더와 같은 버전의 native 라이브러리, `CGO_ENABLED=1`, `-tags tentacle`을 사용합니다. macOS/Windows에서 이 명령을 실행하더라도 Go 테스트와 native I/O는 Docker의 Linux 환경 안에서 수행합니다.

bridge/host 각각 두 클러스터를 함께 실행하여 CephX·FSID, 같은 이름의 RADOS object/RBD image/CephFS file 분리, 새 연결에서 전체 데이터 비교, RBD snapshot 불변성, 각 OSD `2 → 3 → 2` 후 읽기·쓰기와 삭제를 검사합니다. host 모드에는 `ConnectionConfig()`를 사용하는 Linux 프로세스 검증도 포함합니다. 클러스터 제어에는 기존 CLI API를 사용하며 데이터 I/O는 Go의 go-ceph API로 수행합니다.

```sh
make goceph-linux
# 이미 만든 20.2.4 slim 이미지로 실행하려면 역할 이미지 변수를 지정하고:
python3 internal/integration/goceph/run.py \
  --client-base-image ceph-testcontainers:20.2.4-control
```

Docker socket을 runner에 연결하고 host network를 사용하므로 로컬 Linux Docker Engine 또는 host networking을 켠 Docker Desktop이 필요합니다. Python 3.9 이상과 named build context를 지원하는 BuildKit이 필요하며 호스트에 Go/Ceph 개발 라이브러리를 설치하지 않습니다. 현재 fixture의 native 빌드 버전은 20.2.4로 고정합니다. 결과와 범위는 [Linux go-ceph 검증 기록](docs/HOST_NETWORK_POC.md#linux-go-ceph-연동-검증)에 정리합니다.

## API

| API | 역할 |
| --- | --- |
| `Run(ctx, image, opts...)` | 클러스터 부트스트랩, MGR 활성화, 초기 OSD up/in 확인 |
| `WithOSDCount(n)` | 초기 OSD 수, 기본 2개 |
| `WithOSDBlockSize(bytes)` | OSD sparse 파일 크기, 기본/최소 1 GiB |
| `WithStartupTimeout(duration)` | 부트스트랩 및 개별 토폴로지 작업 제한, 기본 3분 |
| `WithHostNetwork()` | 모든 daemon과 클라이언트의 Docker host network, MON/RGW 자동 포트 선택 |
| `WithHostAddress(address)` | host mode의 실제 bind·광고 IPv4 주소, 기본 `127.0.0.1` |
| `UsesHostNetwork()` / `PublicAddress()` | 네트워크 모드와 광고 주소 조회 |
| `ConnectionConfig()` | native client에 전달할 설정·admin keyring 복사본 |
| `WithOSDImage(image)` | 초기 OSD와 이후 추가 OSD의 이미지 선택 |
| `WithRGWImage(image)` | `StartRGW`의 이미지 선택 |
| `WithMDSImage(image)` | `StartCephFS`의 MDS 이미지 선택 |
| `AddOSD(ctx)` | OSD 등록, 포맷, 컨테이너 실행, up/in 확인 |
| `RemoveOSD(ctx, id)` | drain → safe-to-destroy → stop → down → purge → 컨테이너 제거 |
| `OSDs()` | ID 순서로 정렬한 소유 OSD 목록 |
| `OSDs()[i].Stop/Start` | 해당 데몬의 정지/재시작을 통한 장애 주입 |
| `Ceph(ctx, args...)` | 제어 컨테이너에서 CLI 실행, stdout 반환 |
| `Status(ctx)` | readiness에 필요한 상태 JSON 일부 |
| `WaitForClean(ctx)` | 소유 OSD up/in, MGR 활성, 모든 PG active+clean 대기 |
| `NetworkName()` / `WithClient()` | 애플리케이션 컨테이너 연결 |
| `StartRGW(ctx)` / `RGWContainer.S3Endpoint(ctx)` | S3 gateway 기동, 테스트 자격 증명 및 호스트 HTTP endpoint |
| `StartCephFS(ctx, opts...)` | 풀·파일시스템 생성, MDS 기동 및 active 대기 |
| `ServiceContainers()` | 소유 RGW/MDS 컨테이너 조회 |
| `ManagerContainer()` | 소유 MGR 조회, 검사·장애 주입 및 테스트 네트워크 연결 |
| `Terminate(ctx)` | 소유 데몬과 네트워크 정리 |

일반 `testcontainers.With*` 옵션은 MON 컨테이너에 적용합니다. `WithOSDCount` 등의 모듈 옵션은 클러스터 설정에 적용합니다. 일반 옵션으로 MON의 이미지, 네트워크, 시작 명령, 내부 경로를 교체하면 부트스트랩 계약이 깨질 수 있습니다. 추가 MGR/OSD에 대한 임의 옵션 전파는 현재 구현하지 않았습니다.

`Run`의 성공은 클러스터 제어와 OSD 등록 준비를 의미합니다. 일반 애플리케이션용 풀은 호출자가 생성하며, RGW와 CephFS는 시작할 때 필요한 풀을 생성합니다. 작은 테스트를 위해 기본 PG는 8개, autoscaler는 off, PGP는 PG에 맞춰 자동 설정합니다. 풀을 만든 뒤, 혹은 토폴로지 변경 이후에는 `WaitForClean`으로 데이터 배치 완료를 기다릴 수 있습니다. 하나의 OSD만 사용할 경우 복제 수를 1로 설정해야 해당 풀의 `active+clean`을 기대할 수 있습니다.

RGW/MDS는 클러스터가 소유하므로 별도 cleanup 등록이 필요하지 않습니다. `cluster.Terminate`는 이 서비스들을 OSD보다 먼저 종료합니다. 오류와 함께 반환된 서비스도 클러스터 cleanup으로 정리합니다.

마지막 OSD의 제거는 거부합니다. 복제 수나 잔여 용량 때문에 안전한 이동이 불가능하면 `RemoveOSD`는 timeout으로 끝납니다. 이미 out/reweight된 OSD를 자동으로 in 상태로 되돌리지는 않습니다. CLI로 상태를 확인하고 재시도하거나 테스트 클러스터 전체를 종료합니다.

## 실행

```sh
make test
make vet
make integration
```

각 인터페이스만 실행할 수도 있습니다.

```sh
CGO_ENABLED=0 go test -tags=integration -run '^TestRGWS3$' -count=1 -v -timeout=15m ./internal/integration
CGO_ENABLED=0 go test -tags=integration -run '^TestRBDLifecycle$' -count=1 -v -timeout=15m ./internal/integration
CGO_ENABLED=0 go test -tags=integration -run '^TestCephFSFilesystem$' -count=1 -v -timeout=15m ./internal/integration
```

통합 테스트는 `integration` build tag로 분리했습니다. Docker가 없을 때 조용히 skip하지 않으므로 PoC 실행 여부를 분명하게 알 수 있습니다. 다른 이미지로 같은 시나리오를 시험하려면:

```sh
CEPH_TEST_IMAGE=quay.io/ceph/ceph:YOUR_VERSION make integration
```

Docker Desktop 소켓을 자동으로 찾지 못하는 환경에서는 현재 Docker context에 맞는 `DOCKER_HOST`를 지정합니다. Ryuk에는 VM 내부 소켓 경로가 필요할 수 있습니다.

```sh
export DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
make integration
```

## 경량 이미지

원본 Ceph RPM 이미지 또는 로컬 Debian 패키지 묶음을 입력하여 `control`, `osd`, `rgw`, `mds`, `all`의 다섯 로컬 이미지를 자동으로 빌드합니다. 입력의 Ceph 바이너리와 설치된 의존성을 선별하고, 라이선스·Python 바인딩·OSD 동적 플러그인·MGR core module을 보존합니다. `control`에는 MON/MGR, 클라이언트 도구, `rbd-mirror`·`cephfs-mirror`를 함께 넣으며, MON과 MGR는 기존처럼 별도 컨테이너로 실행합니다. `all`은 모든 역할의 기능을 포함합니다.

```sh
make slim-images         # 원본 pull, 다섯 이미지 빌드와 smoke test
make slim-images-verify  # 위 과정 + 혼합 이미지 및 all 이미지 전체 통합 테스트
make slim-images-multicluster # 위 과정 + 독립 두 클러스터의 복제/백업 검증
```

Makefile의 기본 원본은 digest로 고정한 Ceph 20.2.4입니다. `CEPH_SOURCE_IMAGE`로 다른 원본을 지정할 수 있으며, 출력 tag는 원본의 실제 Ceph 버전에서 정합니다. 기본 repository에서는 `ceph-testcontainers:20.2.4-control` 등의 tag가 만들어집니다. 이미지 빌드에는 Python 3.9 이상, Docker API 1.49 이상과 호환 CLI, `ADD --link`를 지원하는 BuildKit/buildx가 필요하고, Go는 통합 테스트를 선택할 때 사용합니다.

회사에서 빌드한 `.deb` 패키지는 호환되는 Debian/Ubuntu 기반 이미지와 함께 지정합니다. Ceph 패키지의 의존 라이브러리도 같은 빌드의 `.deb` 묶음에 포함하며, 일반 배포판 의존성은 기반 이미지의 APT 저장소에서 설치합니다. 입력 파일의 SHA256·패키지명·버전·architecture와 설치 결과를 기록하고, 제공하지 않은 Ceph 패키지가 저장소에서 보충되면 실패합니다.

```sh
python3 image/slim/build.py \
  --deb-packages /path/to/company-build/*.deb \
  --base-image ubuntu:24.04 \
  --tag company-patch-001 \
  --integration
```

출력은 `company-patch-001-control/osd/rgw/mds/all`입니다. 동일한 Ceph 버전의 서로 다른 패치 빌드는 `--tag`로 구분합니다. 패키지 디렉터리는 `--deb-directory` 또는 `make slim-images-deb CEPH_DEB_DIRECTORY=...`로 전달할 수 있습니다. Ubuntu 24.04 ARM64의 공개 Ceph 19.2.3 `.deb` 21개로 다섯 이미지의 smoke와 혼합/all 단일 클러스터 전체 suite를 통과했습니다. 동일 버전의 수정된 파일 재설치도 별도 검증했습니다. 재현 절차와 입력 조건은 [Debian 패키지 이미지 빌드](docs/DEBIAN_IMAGE_AUTOMATION.md)를 따릅니다.

앞의 예제에서 `Run` 호출을 다음처럼 바꾸면 역할별 이미지를 사용합니다. 이미지 옵션을 생략한 역할은 `Run`의 이미지로 실행하며, 새로 추가하는 OSD에도 같은 OSD 이미지 설정을 적용합니다.

```go
cluster, err := ceph.Run(ctx, "ceph-testcontainers:20.2.4-control",
    ceph.WithOSDImage("ceph-testcontainers:20.2.4-osd"),
    ceph.WithRGWImage("ceph-testcontainers:20.2.4-rgw"),
    ceph.WithMDSImage("ceph-testcontainers:20.2.4-mds"),
)
```

기존 방식은 `Run(ctx, "ceph-testcontainers:20.2.4-all", ...)`로 유지할 수 있습니다. `DefaultImage`는 공식 Quay 이미지입니다. 빌드는 registry에 push하지 않으며, 결과·manifest·단계별 로그는 실행마다 새 `artifacts/slim-UTC-UUID/` 디렉터리에 저장합니다.

Linux ARM64에서 mirror 추가 이전 혼합/all 이미지의 전체 통합 테스트를 통과했고, mirror 포함 다섯 이미지의 smoke test를 다시 통과했습니다. 로컬 Docker `Size`는 공식 이미지 2,042,985,596 bytes, mirror를 포함한 `control` 676,867,079 bytes, `all` 895,036,802 bytes입니다. 공통 layer는 `all`을 포함해 재사용합니다. 이 수치를 이미지별로 합쳐 물리 디스크 사용량이나 다운로드 크기로 해석하지 않으며, daemon RAM 감소를 보장하지 않습니다. AMD64와 다른 원본 버전은 별도 검증이 필요합니다.

명령 옵션, 공유 layer, 측정값과 metadata 보존 범위는 [자동화 기록](docs/SLIM_IMAGE_AUTOMATION.md)에 있습니다. 초기 여섯 역할 분석의 `mon-mgr`와 `client`를 이번 구현에서 `control`로 합친 판단은 [이미지 구성 분석](docs/IMAGE_LAYOUT.md)과 함께 볼 수 있습니다. 기존 단일 slim 실험의 `make slim-image`, `make slim-smoke`, `make slim-integration`도 유지하며, 당시 결과는 [SLIM_IMAGE_POC.md](docs/SLIM_IMAGE_POC.md)에 기록했습니다.

## 다중 클러스터 구성과 PoC

단일 클러스터 구성은 `ceph` 패키지에서, 기존 클러스터 사이의 multisite 구성·정책, mirroring, 백업·복원은 별도 `multicluster` 패키지에서 관리합니다. `RunRGWMultisite`, `RunRBDMirror`, `RunCephFSMirror`는 기존 두 클러스터를 받아 추가 데몬·client·네트워크 연결을 소유합니다. 연결을 먼저 종료하고 클러스터를 나중에 종료합니다. 백업·복원은 별도 archive helper로 실행합니다. 연결의 `Terminate`는 클러스터나 데이터를 삭제하지 않으며 Ceph 내부 peer/auth 등의 설정은 일회성 클러스터에 남깁니다. [API 구성과 사용 예](docs/MULTICLUSTER_API.md)를 확인합니다.

`make multicluster`는 독립된 두 클러스터에서 다음 시나리오를 순차 검증합니다.

- RGW 양방향 복제·outage, bucket/prefix·방향 선택 정책과 live 변경, metadata master A → B → A 전환과 새 user/bucket 생성·복제
- RBD 전체/증분 archive 복원, snapshot mirror·계획된 A → B → A 전환, split-brain 감지·명시적 resync, peer 제거·재등록
- CephFS snapshot mirror·directory/peer 제거·재등록, archive 복원, mirror restart·삭제 전파·OSD 교체·source 정지 후 읽기

일반 단일 클러스터 테스트와 별도로 `integration,multicluster` build tag를 사용합니다. 전체 suite timeout은 기본 60분이며 `MULTICLUSTER_TIMEOUT`으로 바꿀 수 있습니다. 전용 `rbd-mirror`·`cephfs-mirror` 데몬의 기본 이미지는 테스트 control 이미지이며, `CEPH_TEST_MIRROR_IMAGE`로 별도 지정할 수도 있습니다. 두 데몬은 slim `control`과 `all`에 포함되어 있습니다. RBD archive helper는 Go Reader/Writer로 byte를 전달하며 Go 호스트의 cgo나 kernel mount는 필요하지 않습니다.

전환은 writer fencing·동기화 완료 확인·명시적 승격을 수행하는 계획된 절차입니다. RBD split-brain resync는 선택하지 않은 branch를 폐기합니다. CephFS native mirror의 user xattr 차이는 계속 관측되므로 완전한 metadata 보존으로 해석하지 않습니다. 구성과 케이스별 실제 결과는 [MULTICLUSTER_POC.md](docs/MULTICLUSTER_POC.md)를 확인합니다.

## 현재 범위

OSD 추가·삭제와 장애 주입, Cephx 인증, 실제 RADOS 객체 I/O를 확인했습니다. RGW/S3, RBD 이미지 및 snapshot/clone, MDS를 통한 CephFS 파일 I/O도 확인했습니다. 단일 서비스 테스트는 OSD `2 → 3 → 2` 변경 후 기존 데이터를 비교합니다. 클러스터 간 PoC의 검증 범위는 위 보고서를 따릅니다. MON/MGR 수 변경, quorum 장애, MDS failover, kernel mapping/mount, 동일 daemon data directory를 재사용하는 전체 클러스터 복원은 후속 검증 대상입니다. OSD 컨테이너 1개를 테스트상의 저장 노드 1개로 취급하며, 여러 OSD를 묶는 호스트 모델은 없습니다. 이 PoC의 OSD failure domain은 `osd`입니다.
