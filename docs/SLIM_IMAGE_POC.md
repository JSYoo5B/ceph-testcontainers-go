# 경량 Ceph 이미지 PoC

실행일: 2026-10-02, Asia/Seoul. 공식 Quay 이미지에 들어 있는 Ceph 20.2.4 런타임을 선별하여 새 이미지를 만들고, 같은 클러스터·서비스 테스트를 실행합니다. Ceph 프로토콜을 흉내 내는 서버를 만들거나 다른 구현으로 바꾸는 방식은 아닙니다.

## 구성과 선택 이유

기준 이미지는 다음 digest로 고정합니다.

```text
quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
```

공식 [v20.2.4 Containerfile](https://github.com/ceph/ceph/blob/v20.2.4/container/Containerfile)은 CentOS Stream 9를 기본으로 사용합니다. 여러 배포 방식을 지원하기 위해 dashboard, disk prediction/scikit-learn, NFS Ganesha, iSCSI, mirror daemon, GCC, ceph-volume/LVM 등도 설치합니다. 이 PoC는 MON/MGR/OSD/RGW/MDS를 직접 실행하고 sparse BlueStore 파일을 쓰므로 그중 상당 부분을 사용하지 않습니다.

`image/slim/Dockerfile`의 assemble stage는 기준 이미지에서 `assemble.py`를 실행합니다. 설치된 RPM database를 대상으로 `dnf --cacheonly repoquery --installed --requires --resolve --recursive`를 호출하고, 선택한 root package와 설치된 의존성의 파일을 `/runtime-rootfs`에 복사합니다. 새 패키지를 다운로드하거나 Ceph를 다시 컴파일하지 않습니다. 원본 바이너리·glibc·Python ABI를 함께 유지하므로 다른 배포판의 라이브러리와 섞는 문제를 피할 수 있습니다.

root package는 `ceph-mon`, `ceph-mgr`, `ceph-osd`, `ceph-mds`, `ceph-radosgw`, `ceph-common`, `python3-cephfs`와 shell·기본 명령·CA·filesystem package입니다. root를 포함한 215개 RPM의 파일을 선택했습니다. 이는 실행에 필요한 모든 파일의 절대 최소 집합을 의미하지 않습니다. 패키지 의존성을 기준으로 여유 있게 선택한 런타임입니다. 최종 이미지에는 package 설치 단계를 실행하지 않고 payload를 복사합니다. 파일 목록에 기존 RPM database 등 원본의 생성 파일도 일부 포함되므로, 선별한 package version은 별도의 manifest로 조회합니다.

최종 stage는 `FROM scratch`에서 rootfs만 복사합니다. 큰 원본 이미지에 삭제 layer를 추가하는 방식과 달리, 사용하지 않는 원본 layer가 최종 이미지에 포함되지 않습니다. 원본 이미지와 build cache는 builder의 로컬 저장 공간에 별도로 남을 수 있습니다.

## 보존하는 런타임

파일은 symlink, hardlink, 실행 권한과 소유자를 보존합니다. `/bin`, `/lib`, `/lib64` 등의 경로와 `/etc/alternatives`를 유지하고, passwd/group, NSS 설정, loader cache, CA trust를 함께 복사합니다. 테스트가 쓰는 Ceph 설정·데이터·로그·socket 디렉터리와 `/tmp`도 생성합니다.

RPM 의존성과 파일 목록을 따라 복사하므로 Ceph Python binding, MGR core module, OSD object class와 plugin을 유지합니다. 특히 `ldd ceph-osd`만으로는 찾아낼 수 없는 `/usr/lib64/rados-classes`와 `/usr/lib64/ceph`의 동적 plugin을 놓치지 않는 것이 중요합니다. RBD/RGW는 이 object class를 통해 OSD에서 작업을 수행합니다. 관련 파일 위치와 package 구분은 [공식 RPM spec](https://github.com/ceph/ceph/blob/v20.2.4/ceph.spec.in#L1574-L1597)에 정의되어 있습니다.

MGR의 기본 동작도 유지합니다. Tentacle의 always-on module은 `crash`, `status`, `progress`, `balancer`, `devicehealth`, `orchestrator`, `rbd_support`, `volumes`, `pg_autoscaler`, `telemetry`입니다. 이 module을 제거하거나 필요한 Python dependency를 빠뜨리면 클러스터 health가 실패할 수 있습니다. [MGR always-on 정의](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/MgrMonitor.cc#L69-L96), [module load와 health 판정](https://github.com/ceph/ceph/blob/v20.2.4/src/mgr/PyModuleRegistry.cc#L400-L448).

일반 문서, man/info, Python bytecode cache는 제외합니다. RPM `%license` 파일은 별도로 보존하여 Ceph의 COPYING 등 배포물의 라이선스 정보를 남깁니다. 선택된 package version과 기준 image digest는 이미지 안의 `/usr/share/ceph-testcontainers/runtime-packages.txt`에 기록합니다.

Go 라이브러리나 host client에 native dependency를 추가하지 않습니다. Python 및 Ceph `.so`는 기존 PoC처럼 Linux 컨테이너 안에서만 사용합니다. 테스트용 CLI와 shell도 유지하므로 기존 `Container.Exec` 및 파일 전달 경로를 그대로 사용할 수 있습니다.

## 측정 결과

최종 ARM64 이미지의 측정값입니다. 이미지 ID는 `sha256:473cad8ece50fe82c468fc5270bb3d2237814f1733c48738ed7ca6002ed4eff6`입니다.

| 지표 | 공식 Quay 기준 | slim 최종 | 감소 |
| --- | ---: | ---: | ---: |
| 로컬 Docker image `Size` | 2,042,985,596 bytes | 879,425,100 bytes | 약 57.0% |
| image history의 unpacked layer 합계 | 1,531,740,160 bytes | 650,657,792 bytes | 약 57.5% |

두 지표는 Docker가 보고하는 서로 다른 값입니다. 각 열은 같은 조회 방식끼리 비교하며, `Size`를 registry 다운로드 크기라고 표현하지 않습니다. 기준 Quay ARM64 manifest의 압축 layer 합계는 511,223,262 bytes로 별도로 확인했습니다. slim 이미지의 registry 전송 크기는 아직 측정하지 않았고, 이미지를 registry에 publish하지 않았습니다.

크기가 큰 이미지를 여러 컨테이너에서 사용해도 공통 read-only image layer는 Docker가 공유합니다. OSD 3개를 시작한다고 공식 이미지 2 GB가 각각 세 번 복사되는 것은 아닙니다. 이번 감소는 최종 이미지의 저장 공간에 관한 결과이며 daemon의 RAM 감소를 보장하지 않습니다. 실제 Ceph 바이너리와 기본 프로세스 구성이 같으므로 런타임 메모리는 별도로 측정해야 합니다.

## 검증 결과

최종 이미지의 smoke test는 통과했습니다. `ceph`, MON/MGR/OSD/MDS, `rados`, `rbd`, `radosgw`, `radosgw-admin`의 version 출력과 Python `cephfs`, `rados`, `rbd`, `ceph_argparse`, `ceph_daemon` import를 확인했습니다. Ceph version 출력은 원본과 같은 20.2.4 및 commit prefix `7f7937`을 사용합니다. keyring 생성·읽기와 monmap 생성, Ceph COPYING 보존도 확인했습니다.

`CGO_ENABLED=0`으로 동일한 전체 테스트를 실행하여 모두 통과했습니다. 각 시간은 cleanup을 포함한 한 번의 관측값이며 이미지 다운로드나 반복 성능 측정은 포함하지 않습니다.

| 테스트 | 동일하게 확인한 동작 | 결과 |
| --- | --- | --- |
| `TestClusterLifecycle` | RADOS 쓰기·읽기, OSD restart, 반복 추가·제거 | PASS, 68.34초 |
| `TestBootstrapFailureCleanup` | MON 부트스트랩 실패 후 자원 정리 | PASS, 0.29초 |
| `TestRGWS3` | signed S3 객체 I/O·인증 거부·목록·삭제, 기존 OSD 교체 | PASS, 51.97초 |
| `TestRBDLifecycle` | 실제 이미지 데이터·snapshot·clone·flatten, 기존 OSD 교체 | PASS, 44.00초 |
| `TestCephFSFilesystem` | 파일 I/O·fsync·rename·삭제·새 session, 기존 OSD 교체 | PASS, 47.08초 |

단위 테스트도 포함한 최종 출력은 `ok ... 212.078s`입니다. RADOS와 각 서비스의 토폴로지 변경 완료 시점에 `HEALTH_OK`, 모든 OSD up/in, 모든 PG `active+clean`을 확인했습니다. 자료는 로컬 `artifacts/poc-slim-build-final.log`, `poc-slim-smoke-final.log`, `poc-slim-integration-final.log`, `poc-slim-size-final.json`에 기록했습니다. 이 디렉터리는 `.gitignore`에 포함되어 있어 복제 시 따라오지 않습니다.

초기 및 최종 실행의 Testcontainers session label로 종료 후 자원을 조회하여 잔여 컨테이너와 네트워크가 없음을 확인했습니다. 조회 결과는 `artifacts/poc-slim-resource-audit.json`입니다.

서비스별 I/O 범위는 [서비스 PoC 기록](SERVICES_POC.md)과 같습니다. 이미지 대체가 테스트 의미를 바꾸지 않도록 같은 테스트 코드를 사용하고, `CEPH_TEST_IMAGE` 값만 변경합니다.

## 재현

Docker가 실행된 상태에서 저장소 루트에서 진행합니다.

```sh
make slim-image
make slim-smoke
make slim-integration
```

기본 local tag는 `ceph-testcontainers:20.2.4-slim`입니다. 다른 tag는 세 명령 모두 `SLIM_IMAGE`로 지정합니다.

```sh
make slim-image SLIM_IMAGE=ceph-testcontainers:local-slim
make slim-smoke SLIM_IMAGE=ceph-testcontainers:local-slim
make slim-integration SLIM_IMAGE=ceph-testcontainers:local-slim
```

build의 `RUN` 단계는 `--network=none`으로 실행하며, 설치된 RPM을 offline으로 조회합니다. 최초 실행에는 기준 Quay 이미지를 가져와야 하므로 기준 이미지 pull까지 네트워크 없이 되는 것은 아닙니다. 기준 이미지가 로컬에 캐시되어 있으면 조립 단계에서 추가 package repository에 접속하지 않습니다.

개별 서비스만 다시 실행할 수도 있습니다.

```sh
CEPH_TEST_IMAGE=ceph-testcontainers:20.2.4-slim CGO_ENABLED=0 \
  go test -tags=integration -count=1 -v -timeout=20m \
  -run '^Test(RGWS3|RBDLifecycle|CephFSFilesystem)$' ./...
```

라이브러리의 `DefaultImage`는 공식 Quay 기준 이미지로 유지합니다. 소비자는 `Run(ctx, "ceph-testcontainers:20.2.4-slim", ...)`처럼 명시적으로 slim 이미지를 선택할 수 있습니다. 이 local tag는 사용자 registry에 공개한 이미지가 아니므로 다른 개발자나 CI에서는 먼저 같은 Dockerfile로 build해야 합니다.

## 적용 범위와 후속 선택

현재 목표는 이 저장소의 기능 테스트를 공식 이미지보다 작은 런타임에서 재현하는 것입니다. 모든 Ceph 기능이나 공식 `ceph/ceph` 이미지의 대체 가능성을 주장하지 않습니다. cephadm, dashboard, NFS/iSCSI, mirror, 실제 block device 준비, kernel/FUSE mount 경로는 이 PoC의 검증 범위에 포함하지 않습니다.

현재 실측 환경은 macOS ARM64 호스트의 Docker Desktop Linux aarch64 VM, 4 vCPU, 약 3916 MiB RAM입니다. AMD64 빌드와 실행은 별도 검증이 필요합니다. 같은 script가 다른 release/image의 RPM 구조에도 그대로 적용된다고 보장하지 않습니다. 기준 digest를 바꾸면 package manifest와 smoke/full integration 결과를 다시 확인해야 합니다.

한 이미지에 모든 검증 대상의 daemon과 client를 유지하여 기존 `Run` 및 `WithClient` 구조를 그대로 사용합니다. 큰 패키지·실제 파일, 서비스별 제외 효과와 공통 의존성 중복은 [구성요소 용량 분석](COMPONENT_SIZE_ANALYSIS.md)에 측정했습니다. RGW를 제외하는 효과는 약 97 MB지만 RGW 전용 환경에서 MDS를 제외하는 효과는 약 6.5 MB입니다. 더 줄일 때는 미사용 도구 선별을 먼저 검토하고, 역할별 이미지에는 공통 base를 공유하는 편이 적절합니다. 역할별 image를 받는 API 및 혼합 구성 테스트는 아직 구현하지 않았습니다.

공개 이미지 역할을 `mon-mgr`, `osd`, `rgw`, `mds`, `client`, `all`로 나누는 구체적인 파일 분할과 layer 효율은 [이미지 구성 분석](IMAGE_LAYOUT.md)에 정리했습니다.
