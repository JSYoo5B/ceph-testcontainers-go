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
export CEPH_TEST_IMAGE=ceph-testcontainers:20.2.4-control
export CEPH_TEST_OSD_IMAGE=ceph-testcontainers:20.2.4-osd
export CEPH_TEST_RGW_IMAGE=ceph-testcontainers:20.2.4-rgw
export CEPH_TEST_MDS_IMAGE=ceph-testcontainers:20.2.4-mds
make hostnetwork

# 위 이미지 변수와 동일하게, mirror image 기본값은 control 이미지입니다.
make hostnetwork-multicluster
```

로컬 실행 로그는 git에서 제외된 `artifacts/host-network-poc/`에 보존합니다. `make hostnetwork`는 기본 bridge integration과 분리되며, mirror 검증에는 추가 `multicluster` 태그를 사용합니다.

Docker host mode는 port publishing을 사용하지 않습니다. Linux Engine에서는 호스트의 네트워크 namespace를 공유하며, Docker Desktop은 별도 설정으로 활성화하는 layer 4 기능입니다. 이번 native librados/libcephfs 검증은 Docker Desktop **Linux VM의 host namespace에 있는 클라이언트 컨테이너**에서 수행했습니다. macOS native RADOS 연결을 검증했다는 의미는 아닙니다. Docker Desktop 설정을 변경하거나 재시작하지 않았습니다. [Docker 문서](https://docs.docker.com/engine/network/drivers/host/)

RGW의 VM 내부 endpoint는 `http://localhost:58835`와 `http://localhost:59565`였고 signed S3 I/O가 성공했습니다. macOS Go 프로세스의 직접 HTTP 연결은 두 endpoint 모두 connection refused였으므로 호스트 직접 S3 검증은 통과 범위에 포함하지 않습니다. `CEPH_TEST_HOST_HTTP_REQUIRED=1`이면 이 상태를 테스트 실패로 처리합니다.

현재 RGW multisite 구성은 bridge 모드에서 제공합니다. Host-mode RGW multisite는 변경 전에 명시적으로 거절합니다. Host 모드의 대량 클러스터 실행, 원격 Docker/NIC 주소, 여러 MON의 quorum, macOS native RADOS 전체 경로는 이번 검증에 포함하지 않습니다.
