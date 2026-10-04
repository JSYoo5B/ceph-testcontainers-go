# Host network와 자동 포트 PoC

실행일: 2026-10-02, Asia/Seoul. MON만 publish하는 방식으로는 접근할 수 없는 native RADOS/RBD/CephFS 클라이언트를 위해 모든 daemon을 Docker host network에 배치했습니다. 같은 Docker 호스트의 두 클러스터가 포트를 수동 지정하지 않고 동작하는지 검증했습니다.

## 제공 API

```go
cluster, err := ceph.Run(ctx, image, ceph.WithHostNetwork())
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
// 별도 native client에 config와 keyring을 전달합니다.
_, _ = config, keyring
```

`WithClient()`로 만든 애플리케이션 컨테이너도 host network와 이 클러스터의 자격 증명을 사용합니다. 기본 광고 주소는 `127.0.0.1`입니다. 원격 Linux Docker 호스트에 연결하는 클라이언트는 그 호스트에 실제로 존재하며 도달 가능한 IPv4 주소를 `WithHostAddress(address)`에 지정합니다. `ConnectionConfig()`는 부트스트랩이 완료된 설정과 admin keyring의 복사본을 반환합니다.

| 역할 | bind 및 주소 구성 |
| --- | --- |
| MON | v2/v1 포트 두 개를 기동 전에 선택하여 config와 monmap에 기록 |
| MGR / OSD / MDS | Ceph의 동적 포트 선택 후 map에 주소 등록 |
| RGW | 빈 포트 하나를 선택, bind 주소와 `S3Endpoint()`에 반영 |
| CLI / native client | MON에서 받은 실제 daemon 주소로 직접 접속 |

MON/RGW 포트 선택은 Go 프로세스의 네트워크가 아닌 Docker daemon의 host namespace에서 수행합니다. 짧게 실행되는 control 이미지의 Python 프로세스가 `bind(address, 0)`한 socket을 보유한 상태에서 대상 daemon 컨테이너를 생성하고, 예약 컨테이너를 종료한 직후 daemon을 시작합니다. Linux native 라이브러리는 Go 호스트에 링크하지 않습니다.

예약 해제와 daemon bind를 원자적으로 처리할 수는 없습니다. Daemon이 종료되었고 로그에서 실제 `EADDRINUSE`를 확인한 경우에만 최대 5회 시도합니다. MON은 실패한 컨테이너를 제거하고 새 포트·monmap·store를 사용합니다. 다른 초기화 오류는 호출자에게 반환하고 부분 생성 자원은 cluster cleanup이 소유합니다. RGW readiness는 선택 포트의 socket이 해당 RGW 프로세스 소유인지 확인한 뒤 HTTP 응답을 확인합니다.

## 검증 환경과 결과

- macOS ARM64, Docker Desktop 4.93.0 / Engine 29.8.1 / Linux ARM64 VM
- Docker VM: 4 vCPU, 약 3916 MiB RAM
- Ceph 20.2.4 역할별 slim 이미지: `control`, `osd`, `rgw`, `mds`
- testcontainers-go v0.44.0, `CGO_ENABLED=0`
- 기본 topology: 클러스터마다 MON 1개, MGR 1개, OSD 2개

| 테스트 | 실제 확인 사항 | 결과 / 관측 시간 |
| --- | --- | --- |
| 두 클러스터 동시 기동 | 서로 다른 FSID·keyring·MON/MGR/OSD endpoint, 같은 pool/object 이름의 데이터 분리, 양쪽 OSD `2 → 3 → 2` 후 fresh librados session I/O | PASS / 76.89초 |
| MON 포트 충돌 | 예약 해제 직후 별도 프로세스가 첫 포트를 점유, 실제 bind 실패, 두 번째 MON 컨테이너의 config·monmap·native status 확인 | PASS / 18.54초 |
| RGW | 기본 7480 점유 중 자동 선택된 두 HTTP endpoint, 같은 bucket/object 이름의 signed PUT/GET/DELETE와 데이터 분리 | PASS / 83.79초 |
| RBD | image I/O, snapshot·clone 분리, OSD 교체 후 bytes 보존, flatten·삭제 | PASS / 45.54초 |
| CephFS | MDS active, 파일 I/O·rename·unlink, OSD 교체 후 새 libcephfs session에서 데이터 확인 | PASS / 47.05초 |
| RBD mirror | 실제 `rbd-mirror`, 두 checkpoint의 전체 8 MiB 비교, demote/promote, source 중단 후 destination 읽기·쓰기 | PASS / 95.04초 |
| CephFS mirror + backup | 실제 `cephfs-mirror`, restart·삭제·directory/peer 재등록, destination OSD 교체, source 중단 후 mirrored snapshot과 archive restore 읽기 | PASS / 185.82초 |

각 시간은 캐시된 이미지로 수행한 한 번의 실행에서 cleanup을 포함한 관측값입니다. 위 결과는 여러 Go 실행에서 얻었으며 단일 suite 실행이나 성능 보장을 의미하지 않습니다. Native CephFS mirror의 user xattr 차이는 기존 [검증 기록](MULTICLUSTER_POC.md)과 동일하게 별도로 보고합니다.

기존 bridge 모드의 `TestClusterLifecycle`(64.55초), `TestBootstrapFailureCleanup`(0.37초), `TestRGWS3`(70.53초)도 다시 실행하여 통과했습니다. 마지막 S3 테스트는 macOS Go HTTP 클라이언트의 mapped endpoint 접근을 검증합니다. `CGO_ENABLED=0 go test ./...`, 전체 integration/hostnetwork/multicluster 태그의 compile·vet, 수정한 shell script의 `sh -n`, `git diff --check`도 통과했습니다. 성공·실패 시도 로그에 기록된 145개 컨테이너 ID를 Docker 목록과 비교한 결과 잔여 컨테이너는 0개였습니다. 최종 개별 케이스 결과는 로컬 `summary.json`에 기록합니다.

첫 실행에서 IP만 지정한 `public_bind_addr`가 선택된 MON 포트를 기본 포트로 덮어쓰는 문제가 발견됐습니다. 이 override를 제거하여 monmap의 전체 주소 벡터로 bind하도록 수정했습니다. 근거는 [Ceph 20.2.4 MON 구현](https://github.com/ceph/ceph/blob/v20.2.4/src/ceph_mon.cc#L773-L779)입니다.

S3 테스트의 첫 PUT은 Python urllib가 서명하지 않은 `Content-Type`을 자동 추가하여 거부됐습니다. 테스트 클라이언트를 `http.client` 직접 전송으로 수정하고 실제 HTTP socket을 이용해 header·body 보존을 확인했습니다. Ceph의 해당 검증은 [SigV4 구현](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_auth_s3.cc#L714-L726)에 있습니다.

## 실행과 범위

```sh
export CEPH_TEST_IMAGE=ceph-testcontainers:official-20.2.4-control
export CEPH_TEST_OSD_IMAGE=ceph-testcontainers:official-20.2.4-osd
export CEPH_TEST_RGW_IMAGE=ceph-testcontainers:official-20.2.4-rgw
export CEPH_TEST_MDS_IMAGE=ceph-testcontainers:official-20.2.4-mds
make hostnetwork

# 위 이미지 변수와 동일하게, mirror image 기본값은 control 이미지입니다.
make hostnetwork-multicluster
```

로컬 실행 로그는 git에서 제외된 `artifacts/host-network-poc/`에 보존합니다. `make hostnetwork`는 기본 bridge integration과 분리되며, mirror 검증에는 추가 `multicluster` 태그를 사용합니다.

Docker host mode는 port publishing을 사용하지 않습니다. Linux Engine에서는 호스트의 네트워크 namespace를 공유하며, Docker Desktop은 별도 설정으로 활성화하는 layer 4 기능입니다. Native librados/libcephfs I/O 검증은 Docker Desktop **Linux VM의 host namespace에 있는 클라이언트 컨테이너**에서 수행했습니다. macOS native RADOS I/O를 검증했다는 의미는 아닙니다. [Docker 문서](https://docs.docker.com/engine/network/drivers/host/)

최초 실행에서 RGW의 VM 내부 endpoint는 `http://localhost:58835`와 `http://localhost:59565`였고 signed S3 I/O가 성공했습니다. 당시 macOS Go 프로세스의 직접 HTTP 연결은 두 endpoint 모두 connection refused였습니다. 이후 사용자가 Docker Desktop host networking을 활성화한 상태에서 아래와 같이 호스트 직접 연결을 다시 검증했습니다. `CEPH_TEST_HOST_HTTP_REQUIRED=1`이면 호스트 HTTP 연결 실패를 테스트 실패로 처리합니다.

현재 RGW multisite 구성은 bridge 모드에서 제공합니다. Host-mode RGW multisite는 변경 전에 명시적으로 거절합니다. Host 모드의 대량 클러스터 실행, 원격 Docker/NIC 주소, 여러 MON의 quorum, macOS native RADOS 전체 경로는 이번 검증에 포함하지 않습니다.

## Docker Desktop host networking 활성화 후 재검증

같은 날 사용자가 Docker Desktop의 host networking을 활성화한 후 macOS Go 프로세스의 직접 연결을 필수 조건으로 다시 실행했습니다. Agent는 Docker Desktop 설정을 추가로 변경하거나 재시작하지 않았습니다.

| 검사 | 결과 |
| --- | --- |
| macOS Go → 두 RGW의 signed S3 GET/PUT/DELETE | PASS, 83.11초; `http://localhost:57397`, `http://localhost:56173` |
| macOS Go → 두 클러스터의 실제 MON/MGR/OSD 광고 주소 | 모든 TCP 연결 PASS |
| 각 OSD 추가 전·추가 후·기존 OSD 삭제 후 macOS 포트 도달 | 모두 PASS; 새로 선택된 OSD 포트 포함 |
| 별도 Linux 클라이언트의 인증된 RADOS I/O와 클러스터 분리 | PASS; 위 TCP 검사를 포함한 테스트 전체 80.80초 |

재현 시에는 기존 이미지 환경 변수에 다음 옵션을 추가합니다.

```sh
CEPH_TEST_HOST_HTTP_REQUIRED=1 CEPH_TEST_HOST_TCP_REQUIRED=1 make hostnetwork
```

`CEPH_TEST_HOST_TCP_REQUIRED=1`은 실제 광고된 IP와 포트를 그대로 사용하며 각 연결을 3초로 제한합니다. TCP 연결 성공은 CephX 인증이나 macOS native librados/go-ceph I/O를 뜻하지 않습니다. 해당 데이터 I/O는 Linux 클라이언트 컨테이너에서 별도로 확인합니다.

이 재검증 범위에서는 host networking 활성화 외에 추가 Docker Desktop 설정이 필요하지 않았습니다. 실행 로그와 최종 결과는 git에서 제외된 `artifacts/host-network-desktop-enabled/`에 남깁니다.

## Linux go-ceph 연동 검증

go-ceph를 사용하는 소비자 테스트는 Linux 전용으로 제공합니다. 클러스터를 생성하는 공개 모듈의 `go.mod`에는 go-ceph를 추가하지 않고, `internal/integration/goceph/probe`의 별도 모듈에서 v0.41.0을 고정합니다. Linux runner의 testcontainers 코드는 `CGO_ENABLED=0`, 데이터 I/O를 수행하는 별도 Go 프로세스는 `CGO_ENABLED=1`, `-tags tentacle`로 빌드합니다. [go-ceph의 native 의존성과 release tag](https://github.com/ceph/go-ceph/blob/v0.41.0/README.md)를 따릅니다.

다음 빌드 설명과 2026-10-02 결과는 이전 test-only 소비자 제작 도구의 실험 기록입니다. 당시 build stage는 digest로 고정한 Quay Ceph 20.2.4 이미지의 GCC·native 라이브러리를 사용했습니다. 이 이미지에 개발 헤더가 없어 같은 Ceph v20.2.4 공개 소스의 `rados/librados.h`, `rados/rados_types.h`, `rbd/librbd.h`, `rbd/features.h`, `cephfs/libcephfs.h`, `cephfs/ceph_ll_client.h`를 가져오고 임시 linker symlink로 기존 SONAME 라이브러리에 링크했습니다. 런타임에는 선택한 Ceph 20.2.4 이미지와 컴파일된 바이너리만 사용하여 호스트의 Ceph 설치·헤더·cgo 환경을 요구하지 않았습니다. 현재 이미지 프로젝트는 개발 헤더나 소비자 프로그램을 제작하지 않으며, 이 Go 프로젝트의 harness도 이미지를 빌드하지 않습니다.

테스트 전체가 Docker Desktop의 Linux ARM64 VM에서 실행됩니다. Runner는 Docker socket을 통해 형제 컨테이너를 생성하고 Linux host network에 배치합니다. bridge 케이스의 클라이언트는 각 전용 네트워크에 `WithClient()`로 연결합니다. host 케이스는 같은 옵션으로 연결한 클라이언트 외에도 `ConnectionConfig()`를 임시 파일에 저장하여 runner의 Linux 프로세스에서 직접 연결합니다. 후자는 Docker daemon과 같은 network namespace에서 실행되며, macOS native 실행을 뜻하지 않습니다.

각 네트워크 모드에서 두 클러스터를 동시 부트스트랩합니다. 클러스터마다 MON 1개, MGR 1개, OSD 2개, MDS 1개를 사용하며 서로 다른 FSID와 CephX admin keyring을 확인합니다. 구성·상태 조회에는 기존 컨테이너 CLI를 사용하고 다음 데이터 검증은 모두 Go의 go-ceph API로 실행합니다.

| 검증 | 데이터와 API 범위 |
| --- | --- |
| 인증·클러스터 선택 | CephX 필수 설정, admin keyring, 실제 연결 후 FSID 일치 |
| RADOS | 동일 object 이름에 클러스터별 64 KiB 데이터, 전체 읽기 비교, 새 object 쓰기·읽기·삭제 |
| RBD | 동일 image 이름에 클러스터별 8 MiB 데이터, 전체 읽기 비교, head 변경 후 baseline snapshot 불변성, 삭제 |
| CephFS | 동일 file 경로에 클러스터별 96 KiB 데이터, userspace libcephfs mount, 전체 읽기·새 파일 쓰기·fsync·삭제 |
| OSD topology | 양쪽 클러스터 각각 `2 → 3 → 2`, 교체 후 양쪽에서 새 연결로 기존 데이터와 새 쓰기 검증 |
| 클러스터 분리 | 같은 pool/image/object/file 이름을 쓰면서 서로 다른 전체 bytes와 FSID 유지 |

각 probe는 별도 프로세스여서 이전 연결의 캐시에 의존하지 않습니다. Native MON/OSD/mount timeout은 30초이며 프로세스 전체에 75초 제한을 둡니다. RBD head를 변경하여 snapshot 불변성을 확인한 뒤 baseline으로 복원하므로 각 topology 단계에서 독립적으로 다시 비교할 수 있습니다.

2026-10-02 역할별 Ceph 20.2.4 slim 이미지, Go 1.27.1, go-ceph v0.41.0, testcontainers-go v0.44.0으로 실행했습니다. Linux Engine 29.8.1, ARM64, 4 vCPU, 3916 MiB 환경입니다.

| 케이스 | 결과 / 관측 시간 |
| --- | --- |
| bridge: 전용 네트워크 두 개와 `WithClient()`의 실제 go-ceph I/O | PASS / 106.67초 |
| host: 자동 MON 포트, `WithClient()`와 Linux 프로세스의 실제 go-ceph I/O | PASS / 108.41초 |
| 전체 `TestGoCephLinux`, 각 클러스터 OSD 교체와 cleanup 포함 | PASS / 215.09초 |

26개 별도 go-ceph 프로세스가 성공했습니다. 컨테이너 내부 probe 20개와 Linux runner의 직접 probe 6개이며, seed 4회·새 연결 verify 18회·삭제 확인 4회입니다. 모든 verify에서 RADOS 64 KiB, RBD 8 MiB, CephFS 96 KiB 전체 bytes를 비교하고 새 데이터를 읽기·쓰기했습니다. 같은 이름의 리소스를 쓰는 두 클러스터의 payload hash와 실제 FSID는 각 모드에서 서로 달랐습니다. 성공한 실행이 생성한 컨테이너 32개를 검사한 결과 잔여 컨테이너는 0개였습니다.

Linux native CGO 빌드와 Linux의 기존 `ceph`/`multicluster` 단위 테스트도 통과했습니다. macOS에서는 공개 모듈의 `CGO_ENABLED=0 go test ./...`, integration/goceph 태그 compile·vet, Python script compile, diff 검사를 통과했습니다. 위 native 기능을 위해 공개 API나 서버 구성을 추가 수정할 필요는 없었습니다.

초기 테스트 작성 중 공유 pool에 두 application label을 활성화하여 Ceph 확인 요구에 실패했고, `client_metadata_timeout`이라는 미지원 옵션을 설정하여 probe가 거절됐습니다. 테스트 pool의 label을 `rbd`로 정하고 지원되는 timeout만 사용한 뒤 위 전체 케이스가 통과했습니다. 초기 실패 두 번의 생성 자원도 모두 정리했습니다. 최종 로그와 이미지 inspect는 `artifacts/go-ceph-linux-poc-run3/`에 있습니다. 이미지 build 시간은 위 테스트 시간에 포함하지 않으며, 한 번의 관측값입니다.

현재 기본 실행은 Quay 20.2.4를 모든 서버 역할에 사용합니다. Client/runner 이미지는 호출자가 미리 준비하며, 아래 이미지 이름은 실제로 공개·배포된 이미지를 뜻하지 않는 입력 예시입니다. Client와 runner에는 같은 Ceph ABI로 빌드한 `/usr/local/bin/go-ceph-probe`와 runtime libraries가 필요합니다. Runner는 현재 Go checkout의 integration/goceph 태그 테스트를 Linux용으로 컴파일하고 그 binary를 entrypoint로 실행해야 합니다. [Go 실행 harness](../internal/integration/goceph/run.py)는 로컬 Linux 이미지의 immutable ID를 확인해 실행하며, [Go 소비자 recipe](../internal/integration/goceph_integration_test.go)가 probe 실행과 실제 데이터 I/O를 검증합니다. 이미지 프로젝트의 [요구사항과 checker](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)는 서버 이미지 계약을 확인하며 이 Go 소비자를 빌드하거나 해당 named test를 실행하지 않습니다. 아래 명령은 Go 프로젝트 루트에서 실행합니다.

```sh
CEPH_TEST_GOCEPH_CLIENT_IMAGE=my-company/ceph-go-client:20.2.4 \
CEPH_TEST_GOCEPH_RUNNER_IMAGE=my-company/ceph-go-runner:20.2.4 \
make goceph-linux
```

공식 이미지에서 [역할별 추출](../../ceph-testcontainers-images/docs/ROLE_IMAGES.md)한 서버를 선택할 수도 있습니다. 같은 release·ABI·architecture의 준비된 client/runner를 지정합니다. 이전 실험에서는 이 준비를 Go harness가 수행했으며, 현재 harness에는 빌드 기능이 없습니다.

```sh
export CEPH_TEST_IMAGE=ceph-testcontainers:official-20.2.4-control
export CEPH_TEST_OSD_IMAGE=ceph-testcontainers:official-20.2.4-osd
export CEPH_TEST_RGW_IMAGE=ceph-testcontainers:official-20.2.4-rgw
export CEPH_TEST_MDS_IMAGE=ceph-testcontainers:official-20.2.4-mds
python3 internal/integration/goceph/run.py \
  --client-image my-company/ceph-go-client:20.2.4 \
  --runner-image my-company/ceph-go-runner:20.2.4
```

로컬 Docker Engine의 `/var/run/docker.sock`에 접근할 수 있어야 합니다. Docker Desktop에서는 host networking을 활성화합니다. 실행에는 Python 3.9 이상과 Docker CLI가 필요합니다. 소비자 빌드 도구·개발 헤더·Go module 다운로드는 호출자의 이미지 준비 과정에서 처리합니다. `--docker`로 CLI 경로를, `--output-dir`로 새 결과 디렉터리를 지정합니다. 기본 결과는 `artifacts/go-ceph-linux-UTC-UUID/`이며 integration log, 실제 이미지 inspect, 시간과 cleanup 결과를 `summary.json`에 기록합니다. Ryuk의 정상 재접속 유예 시간 이후 생성한 컨테이너만 검사합니다.

현재 fixture의 native 빌드와 런타임 라이브러리는 Ceph 20.2.4로 맞춥니다. macOS native go-ceph, Linux AMD64·독립 bare-metal host, 제한된 client capability, RGW admin HTTP API, mirror/failover의 go-ceph 소비자 검증은 이번 케이스에 포함하지 않습니다. 기존 RGW/S3와 multicluster 시나리오의 CLI/Python 검증 결과는 앞의 기록을 따릅니다.
