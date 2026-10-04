# RGW · RBD · CephFS 서비스 PoC

실행일: 2026-10-02, Asia/Seoul. 기본 MON/MGR/OSD 클러스터 위에서 각 서비스의 실제 데이터 접근과 기존 OSD 교체 후 데이터 보존을 검증했습니다. Go 제어 코드와 테스트는 `CGO_ENABLED=0`으로 실행하고, `go-ceph`나 S3 SDK 의존성을 추가하지 않았습니다.

## 실행 결과

| 서비스 | 통합 테스트 | 기록된 결과 | cleanup 포함 소요 시간 | 실제 접근 경로 |
| --- | --- | --- | --- | --- |
| RGW/S3 | `TestRGWS3` | PASS | 52.16초 | macOS 호스트 Go 프로세스 → 매핑된 HTTP 포트 → RGW |
| RBD | `TestRBDLifecycle` | PASS | 46.02초 | 별도 Linux 클라이언트 컨테이너의 `rbd` CLI → MON/OSD |
| CephFS | `TestCephFSFilesystem` | PASS | 47.01초 | 별도 Linux 클라이언트 컨테이너의 Python libcephfs → MON/MDS/OSD |

세 서비스의 OSD 추가·제거 완료 시점에는 모든 OSD가 up/in이고 모든 PG가 `active+clean`이었습니다. 로그에서 RGW는 PG 49개, RBD는 PG 9개, CephFS는 PG 17개와 `HEALTH_OK`를 확인했습니다. 시간은 이미지를 캐시한 단일 실행의 관측값이며 다운로드 시간이나 성능 보장을 포함하지 않습니다.

전역 PG 설정 수정 후 기존 `TestClusterLifecycle`(68.19초), `TestBootstrapFailureCleanup`(0.30초), CephFS, RBD를 다시 실행하여 모두 통과했습니다. 이 회귀 테스트 전체는 161.893초였습니다. `CGO_ENABLED=0` 단위 테스트, integration build tag를 포함한 vet, 셸 문법 검사, Linux/Windows amd64 교차 컴파일도 통과했습니다. RGW의 초기 실패 시도를 포함한 6개 testcontainers session을 조회하여 잔여 컨테이너와 네트워크가 없음을 확인했습니다.

## 환경과 공통 토폴로지

| 항목 | 값 |
| --- | --- |
| 호스트 | macOS ARM64 |
| Docker | Docker Desktop, Linux aarch64 VM |
| VM 자원 | 4 vCPU, 약 3916 MiB RAM |
| Ceph | Tentacle 20.2.4 |
| testcontainers-go | v0.44.0 |
| Go 실행 설정 | `CGO_ENABLED=0` |
| 기본 구성 | MON 1개 + MGR 1개 + OSD 2개, 각각 별도 컨테이너 |
| OSD 저장소 | 각 1 GiB의 컨테이너 내부 sparse BlueStore 파일 |
| 선택 서비스 | RGW는 게이트웨이 1개, CephFS는 MDS 1개 추가, RBD는 추가 데몬 없음 |

이미지는 `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`로 고정했습니다. 데몬과 클라이언트는 클러스터별 Docker 네트워크에 연결합니다. 풀은 테스트용 복제 수 2, `min_size=1`을 사용합니다.

세 테스트 모두 데이터를 먼저 기록한 뒤 `2 → 3 → 2`로 OSD 구성을 변경합니다. 새 OSD를 추가하고 clean 상태를 기다린 다음, **처음부터 데이터를 보유하던 기존 OSD**를 drain/purge하여 제거합니다. 추가한 새 OSD를 바로 삭제하는 테스트가 아닙니다. 제거 후 다시 clean 상태를 기다리고 기존 데이터의 바이트를 비교합니다.

4 GiB VM에서 세 서비스를 한꺼번에 실행하는 대신 테스트마다 독립 클러스터를 만들고 순차 실행합니다. 클러스터 `Terminate`는 선택 서비스, OSD, MGR, MON, 네트워크를 정리하며 부분 생성된 선택 서비스도 정리 대상으로 추적합니다.

## RGW/S3

`StartRGW`는 Beast HTTP frontend를 7480 포트로 실행하고, `radosgw-admin user create`로 일반 S3 사용자의 access/secret key를 생성합니다. `S3Endpoint`는 호스트에서 사용할 매핑된 HTTP endpoint를 반환합니다. 테스트 클라이언트는 Go 표준 라이브러리 `net/http`와 암호화 패키지로 AWS Signature V4 요청을 구성하며 path-style bucket 주소를 사용합니다. [Ceph S3 API 문서](https://docs.ceph.com/en/tentacle/radosgw/s3/)와 [RGW 관리 문서](https://docs.ceph.com/en/tentacle/radosgw/admin/)를 기준으로 구성했습니다.

검증 항목은 다음과 같습니다.

1. bucket 생성과 전체 bucket 목록 확인.
2. 96 KiB payload를 서로 다른 객체 4개에 signed PUT. 중첩 경로 형태의 key도 포함.
3. signed GET으로 모든 payload를 바이트 단위 비교하고 `ListObjectsV2` 목록 확인.
4. private 객체에 서명 없는 요청 및 잘못된 secret key 요청을 보내 각각 HTTP 403 확인.
5. OSD `2 → 3 → 2` 변경 후 기존 객체 읽기·목록을 다시 확인하고 새 객체 쓰기·읽기.
6. 객체 삭제, 삭제된 객체 GET의 HTTP 404, 빈 목록, bucket 삭제 확인.

RGW 데몬의 내부 CephX 연결은 이 PoC의 임시 admin keyring을 사용합니다. S3 사용자 자격 증명과 CephX admin 자격 증명은 별개입니다. 운영 배포의 RGW 전용 최소권한 CephX 설정은 검증 범위에 포함하지 않았습니다. [RGW 설정 문서](https://docs.ceph.com/en/tentacle/radosgw/config-ref/).

## RBD

별도 클라이언트 컨테이너에서 풀을 `rbd pool init`으로 초기화한 뒤 RBD format 2와 layering 기능을 사용했습니다. 다음 동작은 실제 RBD 이미지 데이터를 읽고 쓰는 CLI 작업입니다. [rbd 명령 문서](https://docs.ceph.com/en/tentacle/man/8/rbd/).

1. 빈 8 MiB 이미지 생성, JSON `info`에서 크기·format·layering 확인, 이미지 삭제와 빈 목록 확인.
2. 8 MiB 전체를 nonzero 패턴으로 채운 payload를 1 MiB object 크기로 import하고 export 결과를 전 바이트 비교. 여러 RADOS 객체에 실제 데이터가 기록됩니다.
3. `baseline` snapshot 생성·protect, snapshot에서 clone 생성.
4. 다른 8 MiB 패턴을 별도 이미지에 import하고 `export-diff`/`import-diff`로 원본 이미지의 데이터를 교체.
5. 원본은 새 패턴, snapshot과 clone은 이전 패턴을 유지하는지 확인.
6. OSD `2 → 3 → 2` 변경 후 원본·snapshot·clone 전체 바이트를 다시 비교.
7. clone을 flatten하고 부모 snapshot을 unprotect·삭제한 뒤 clone의 이전 패턴을 다시 확인. 마지막으로 모든 이미지를 삭제하고 빈 목록 확인.

snapshot/clone 데이터 분리와 flatten 이후 부모 의존성 제거까지 확인했습니다. [RBD snapshot과 clone 문서](https://docs.ceph.com/en/tentacle/rbd/rbd-snapshot/).

이 검증은 컨테이너 내부 userspace RBD CLI의 이미지 I/O입니다. `/dev/rbd`나 NBD device mapping, host kernel RBD module, RBD 이미지 위의 파일시스템 mount, QEMU/CSI 연동을 실행한 것은 아닙니다. 해당 경로가 필요한 소비자는 별도의 Linux 환경과 device/capability 조건으로 검증해야 합니다.

## CephFS

`StartCephFS`는 metadata/data 풀을 생성하고 `ceph fs new`로 `tc-cephfs` 파일시스템을 등록합니다. 공식 수동 배포 방식의 `mds.a` credentials로 MDS 1개를 시작하고 JSON filesystem map에서 rank 0의 `up:active` 상태를 기다립니다. MDS cache memory limit는 작은 테스트 VM에 맞춰 128 MiB로 설정했습니다. [CephFS 생성 문서](https://docs.ceph.com/en/tentacle/cephfs/createfs/)와 [MDS 수동 배포 문서](https://docs.ceph.com/en/tentacle/cephfs/add-remove-mds/)를 기준으로 구성했습니다.

클라이언트는 이미지에 이미 설치된 Python `cephfs` 모듈을 사용합니다. 검증 항목은 다음과 같습니다.

1. 이름을 지정한 파일시스템에 libcephfs handle/session을 연결하고 디렉터리 생성.
2. 서로 다른 약 64 KiB payload를 파일 16개에 write·fsync·close한 뒤 read 결과를 바이트 비교.
3. 각 파일 rename 후 기존 경로가 사라졌는지 확인. 임시 파일 쓰기·읽기·unlink와 삭제 확인.
4. OSD `2 → 3 → 2` 변경 후 MDS rank 0 `up:active` 확인.
5. 새 Python 프로세스와 libcephfs session에서 기존 파일 16개를 다시 읽고 비교. 이전 session의 파일 cache만으로 통과하는 것을 방지합니다.
6. 교체 후 새 디렉터리·파일 생성, 쓰기·읽기·rename, 빈 디렉터리 제거, 기존 파일 1개 unlink.
7. 세 번째 session에서 남은 기존 데이터, 새 파일 데이터, 파일·디렉터리 삭제가 보이는지 확인.

Python libcephfs는 MON/MDS/OSD와 통신하는 실제 userspace CephFS 클라이언트입니다. `mount()`라는 API 이름은 libcephfs handle을 연결하는 동작이며, OS mount table에 FUSE나 kernel filesystem을 붙인다는 뜻은 아닙니다. 이 테스트는 `/dev/fuse`, `CAP_SYS_ADMIN`, 추가 host kernel 지원을 사용하지 않습니다. [Python libcephfs API 문서](https://docs.ceph.com/en/tentacle/cephfs/api/libcephfs-py/).

Python 모듈과 native libcephfs의 `.so`는 Linux 컨테이너 내부에 있습니다. 호스트 Go 프로그램은 해당 `.so`를 링크하거나 cgo로 컴파일하지 않습니다. 컨테이너 native client를 사용하는 것과 Go 라이브러리의 host native dependency는 분리됩니다.

일반 앱이 mounted path에 `os.ReadFile` 등으로 접근하는 경로, FUSE/kernel mount, MDS standby/failover, 다수 active MDS, POSIX의 전체 동시성·locking semantics는 이번 검증에 포함하지 않았습니다.

## 서비스 부트스트랩에서 확인한 구성 문제

RGW에는 기본 CLI 데몬의 keyring 탐색과 다른 경로가 적용되어, `radosgw`와 `radosgw-admin` 모두 `--keyring /etc/ceph/ceph.client.admin.keyring`을 명시했습니다.

Tentacle의 익명 루트 GET은 HTTP 200으로 빈 bucket 목록을 반환했습니다. HTTP readiness는 200 또는 403을 허용하며, 실제 private 객체의 접근 제어는 위의 S3 테스트에서 별도로 확인합니다.

또한 RGW가 자동으로 만드는 풀은 호출 시 PG/PGP 값을 0으로 넘길 수 있습니다. 기존 전역 `pg_num=8`, `pgp_num=8`과 autoscale `on` 조합에서는 새 풀의 PG가 더 작은 값으로 선택되어 `pgp_num > pg_num` 검증에 걸리고 `ERANGE`가 발생했습니다. 이 분기는 [Ceph v20.2.4 OSDMonitor.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/OSDMonitor.cc#L7680-L7704)에서도 확인했습니다.

현재 테스트 구성은 전역 `osd pool default pg num=8`, `osd pool default pgp num=0`, `osd pool default pg autoscale mode=off`를 사용합니다. `pgp_num=0`은 생성 시 선택된 `pg_num`에 맞춰 결정되도록 하고, autoscale은 작은 테스트 풀의 PG 수를 일정하게 유지하기 위해 끕니다. 운영 클러스터의 권장 PG 정책을 제시하는 설정은 아닙니다.

## 재현

Docker를 실행한 상태에서 저장소 루트에서 각각 실행합니다. 테스트는 cluster와 client를 생성하고 정리하므로 사전 Ceph 배포가 필요하지 않습니다.

```sh
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=15m -run '^TestRGWS3$' ./internal/integration
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=15m -run '^TestRBDLifecycle$' ./internal/integration
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=15m -run '^TestCephFSFilesystem$' ./internal/integration
```

세 서비스 테스트를 한 번에 순차 실행할 수도 있습니다.

```sh
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=20m -run '^Test(RGWS3|RBDLifecycle|CephFSFilesystem)$' ./internal/integration
```

다른 이미지는 `CEPH_TEST_IMAGE` 환경 변수로 지정할 수 있지만, 위 결과는 고정된 Tentacle 20.2.4 이미지에 대한 결과입니다. `CGO_ENABLED=0 go test ./...`만 실행하면 Docker 통합 테스트는 실행하지 않습니다.

다른 이미지는 [고정 이미지 요구사항](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)을 충족한 상태로 준비합니다. 공식 바이너리의 경량 대안은 [역할별 추출 이미지](../../ceph-testcontainers-images/docs/ROLE_IMAGES.md)이며, Go 테스트는 준비된 역할 이미지를 환경 변수로 선택합니다. 이미지 프로젝트의 quick/full checker는 Go 테스트와 독립된 검증입니다. 이전 slim PoC의 성공은 당시 image ID·platform의 증거이며 현재 추출 결과의 자동 PASS로 처리하지 않습니다.

로컬 최종 로그는 `artifacts/poc-rgw.log`와 `artifacts/poc-services-regression.log`에 보관합니다. 최초 RBD 성공은 `artifacts/poc-rbd.log`, 최초 CephFS 성공과 RGW keyring 실패는 `artifacts/poc-rgw-cephfs.log`, RGW readiness 실패는 `artifacts/poc-rgw-readiness-failure.log`에 남아 있습니다. 자원 정리 조회 결과는 `artifacts/poc-resource-audit.json`입니다. `artifacts/`는 `.gitignore`에 포함되어 있으므로 저장소 clone에는 로그가 따라가지 않습니다. 필요하면 위 명령의 출력을 파일로 저장하여 재생성합니다.

## 판단과 남은 범위

RBD와 CephFS에서는 native client를 Linux 컨테이너에 넣고 Go가 CLI·파일·프로세스를 제어하는 방식으로, host cgo 없이 실제 서비스 데이터 접근과 기존 OSD 교체를 검증했습니다. RGW는 호스트의 일반 HTTP S3 클라이언트로 실제 객체 I/O와 기존 OSD 교체를 검증했습니다. 세 인터페이스 모두 이 fixture에서 소비할 수 있음을 확인했습니다.

공통으로 단일 MON/MGR와 작은 복제 풀을 쓰는 기능 검증 환경입니다. quorum 변경, production HA·성능·내구성, 다중 호스트 배포, 대량 병렬 실행, 완전한 취소/장애 복구, CephX 최소권한 설계, 각 서비스의 전체 API 호환성을 검증했다는 의미는 아닙니다. 필요한 앱의 실제 소비 경로에 맞춰 host S3 SDK, kernel/FUSE mount, CSI 또는 전용 클라이언트를 후속 테스트에 연결할 수 있습니다.
