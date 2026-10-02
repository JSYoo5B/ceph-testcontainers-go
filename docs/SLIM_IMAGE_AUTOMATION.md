# 다섯 역할 Ceph 이미지 자동화

검증일: 2026-10-02, Asia/Seoul. [build.py](../image/slim/build.py)는 원본 Ceph RPM 이미지를 받아 `control`, `osd`, `rgw`, `mds`, `all`의 다섯 로컬 이미지를 조립하고 검증합니다. Ceph를 다시 컴파일하지 않고 원본 바이너리와 의존성을 사용합니다. Linux ARM64에서 역할별 smoke test, 혼합 이미지 클러스터, 기존 단일 이미지 방식의 전체 테스트를 통과했습니다.

## 역할과 Go API

| 출력 역할 | 포함하는 기능 | 컨테이너 사용 |
| --- | --- | --- |
| `control` | MON/MGR, MGR module, `ceph`·`ceph-authtool`·`monmaptool`, RADOS/RBD CLI, Python CephFS client | MON/MGR를 각각 실행하며 독립 테스트 client에도 사용 |
| `osd` | OSD와 storage 의존성·object class·동적 plugin | OSD당 컨테이너 하나, 초기 및 이후 추가 OSD에 적용 |
| `rgw` | RGW와 `radosgw-admin` | S3 gateway와 테스트 사용자 생성 |
| `mds` | MDS | CephFS를 시작할 때 실행 |
| `all` | 위 네 역할의 합집합 | 기존처럼 한 이미지를 모든 컨테이너에서 재사용 |

초기 [여섯 역할 분석](IMAGE_LAYOUT.md)의 `mon-mgr`와 `client`를 `control`로 묶었습니다. 현재 RPM closure에서 client 파일은 서버 공통에도 포함되므로 별도 client 이미지를 추가하는 효과가 작고, 같은 control 이미지로 I/O 도구를 사용할 수 있습니다. 이미지 통합은 컨테이너 통합과 별개이며, MON/MGR를 합쳐 실행하는 supervisor는 구현하지 않았습니다.

```go
cluster, err := ceph.Run(ctx, "ceph-testcontainers:20.2.4-control",
    ceph.WithOSDImage("ceph-testcontainers:20.2.4-osd"),
    ceph.WithRGWImage("ceph-testcontainers:20.2.4-rgw"),
    ceph.WithMDSImage("ceph-testcontainers:20.2.4-mds"),
)
if cluster != nil {
    testcontainers.CleanupContainer(t, cluster)
}
if err != nil {
    t.Fatal(err)
}
```

`Run`의 이미지가 MON/MGR와 생략한 역할의 기본 이미지입니다. `WithOSDImage`는 이후 `AddOSD`에도 적용하며, RGW/MDS는 각 시작 함수에서 지정된 이미지를 사용합니다. 빈 역할 이미지 옵션은 자원 생성 전에 거부합니다. 서로 호환되는 같은 Ceph 버전의 이미지를 사용합니다. 기존 `Run(ctx, "ceph-testcontainers:20.2.4-all", ...)` 호출도 그대로 동작합니다.

`WithClient()`는 네트워크와 설정·keyring을 전달하며 패키지를 설치하지 않습니다. 독립 client 컨테이너에는 `control` 또는 필요한 클라이언트가 있는 애플리케이션 이미지를 사용합니다. Go 호스트에 `go-ceph`나 cgo 의존성을 추가하지 않습니다.

## 실행

호스트에는 Python 3.9 이상, Docker API 1.49 이상을 지원하는 엔진과 호환 CLI, `ADD --link`를 지원하는 BuildKit/buildx가 필요합니다. 생성 이미지 조회에 사용하는 `docker image inspect --platform`의 API 요구사항입니다. [Docker inspect 문서](https://docs.docker.com/reference/cli/docker/image/inspect/). 이번 환경은 Docker Engine 29.8.1/buildx 0.37.1입니다. 이미지 조립에 Go는 필요하지 않습니다. `--integration` 또는 `--multicluster`를 선택할 때만 프로젝트의 Go 1.25 이상 환경이 필요하며 `CGO_ENABLED=0`으로 실행합니다.

```sh
make slim-images
make slim-images-verify
make slim-images-multicluster
```

첫 명령은 원본 pull → 다섯 이미지 빌드 → 다섯 smoke test를 실행합니다. 두 번째는 이어서 혼합 이미지 전체 suite와 `all` 이미지 전체 suite를 순서대로 실행합니다. 세 번째는 혼합 역할과 control mirror 이미지로 다중 클러스터 복제/백업 PoC를 실행합니다. 두 검증 옵션을 함께 지정할 수도 있습니다. 기본 원본은 [Makefile](../Makefile)의 고정 Ceph 20.2.4 digest이며, `CEPH_SOURCE_IMAGE`와 `SLIM_REPOSITORY`로 원본과 출력 repository를 바꿀 수 있습니다.

직접 실행할 때 `--source-image`는 필수입니다. tag나 digest를 받을 수 있습니다.

```sh
python3 image/slim/build.py \
  --source-image quay.io/ceph/ceph:v20.2.4 \
  --platform linux/arm64 \
  --integration
```

출력 tag는 원본의 `ceph --version`에서 읽은 실제 버전을 사용합니다. 기본 repository가 `ceph-testcontainers`이고 실제 버전이 20.2.4이면 `20.2.4-control`, `20.2.4-osd`, `20.2.4-rgw`, `20.2.4-mds`, `20.2.4-all` tag를 만듭니다. 입력 tag는 조회한 RepoDigest로 정규화하며 RepoDigest가 없으면 image ID를 기록합니다. 추출용 컨테이너는 조회 당시의 source image ID로 생성하므로 이후 입력 tag가 바뀌어도 같은 실행의 추출 대상은 고정됩니다. registry push는 하지 않습니다.

| 옵션 | 동작 |
| --- | --- |
| `--source-image IMAGE` | 필수 원본 Linux Ceph RPM 이미지 |
| `--repository NAME` | 로컬 출력 repository, 기본 `ceph-testcontainers` |
| `--tag PREFIX` | 실제 버전 대신 사용할 출력 tag prefix; manifest의 실제 Ceph 버전은 유지 |
| `--platform PLATFORM` | 한 실행의 platform 선택; 생략하면 조회한 source architecture 사용 |
| `--output-dir DIR` | 기록 디렉터리 지정; 없거나 비어 있어야 함 |
| `--skip-pull` | 로컬에 캐시된 source 사용 |
| `--skip-smoke` | smoke 생략; report에는 `skipped`로 기록 |
| `--integration` | 혼합 역할 이미지와 `all`의 Go 단일 클러스터 전체 suite 실행 |
| `--multicluster` | 혼합 역할과 control mirror 이미지로 복제/백업 suite 실행 |
| `--go-command PATH` | integration에 사용할 Go 실행 파일, 기본 `go` |
| `--keep-context` | 생성한 tar build context를 기록 디렉터리의 `context/`에도 보존 |

한 실행에서는 platform 하나만 빌드·load합니다. 자동 multi-architecture manifest는 생성하지 않습니다. source pull 이후 RPM 조회·조립, 이미지 build와 smoke는 네트워크 없이 실행하며 추가 package repository에서 설치하지 않습니다. 처음 원본을 받을 때는 네트워크가 필요합니다. Go integration에는 테스트 의존성과 Ryuk 이미지 등의 준비가 별도로 필요할 수 있습니다.

수동으로 혼합 suite를 다시 실행할 수도 있습니다.

```sh
CEPH_TEST_IMAGE=ceph-testcontainers:20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:20.2.4-osd \
CEPH_TEST_RGW_IMAGE=ceph-testcontainers:20.2.4-rgw \
CEPH_TEST_MDS_IMAGE=ceph-testcontainers:20.2.4-mds \
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=20m ./internal/integration
```

`CEPH_TEST_IMAGE`는 MON/MGR와 독립 client에 사용합니다. 빈 역할 환경 변수는 이 이미지로 fallback하므로, `CEPH_TEST_IMAGE`만 `all`로 지정하고 나머지 역할 변수는 설정하지 않으면 기존 단일 이미지 suite를 실행할 수 있습니다.

## 조립과 공유 layer

`control`과 `all`에는 `rbd-mirror`·`cephfs-mirror` RPM과 필요한 의존성을 추가합니다. 해당 데몬은 다른 세 역할에 없습니다. 실행은 multicluster API에서 선택하며 일반 클러스터 생성 시에는 기동하지 않습니다. 두 RPM 추가는 기존 215개에서 217개 package로, 논리 파일은 11,087,734 bytes 증가했습니다. 공통 그룹의 논리 용량과 OSD/RGW/MDS의 Docker `Size`는 이전과 같습니다. 같은 빌드에서 공유하는 layer DiffID를 확인하며, 추출 컨테이너의 hostname/hosts 등 동적 입력 때문에 서로 다른 빌드의 common DiffID가 항상 같다고 보장하지는 않습니다.

[package_roles.py](../image/slim/package_roles.py)는 설치된 RPM의 의존성·파일·license를 조사하고, 실제 파일 membership으로 겹치지 않는 tar 그룹을 만듭니다. `all`은 네 역할의 파일 합집합입니다. `ceph-common`의 보수적인 RPM closure를 따르므로 native daemon이 직접 사용하지 않는 Python/client 파일도 공통 그룹에 남습니다. 절대 최소 runtime을 계산한 결과는 아닙니다.

rootfs는 Linux source 컨테이너 안에서 조립하고 tar로 생성합니다. 호스트에는 tar 파일만 전달하며 rootfs 디렉터리를 `docker cp`로 옮기지 않습니다. tar 패키징은 조립 완료된 runtime의 숫자 UID/GID, mode, symlink, hardlink와 RPM license 파일을 보존하고 timestamp를 0으로 정규화합니다. assembler가 실행용 디렉터리를 0755, `/tmp`를 1777로 설정하는 부분도 있으므로 원본의 모든 디렉터리 mode를 그대로 복제한다는 의미는 아닙니다. 생성 Dockerfile은 같은 tar 그룹을 `ADD --link`로 재사용합니다. [Docker ADD --link 설명](https://docs.docker.com/reference/dockerfile/#add---link).

| 논리 파일 그룹 | 네 실행 역할 기준 membership | 일반 파일 용량 |
| --- | --- | ---: |
| `common` | `control`, `osd`, `rgw`, `mds` | 431,713,608 bytes |
| `shared-control-osd` | `control`, `osd` | 38,999 bytes |
| `control` | `control` | 45,704,489 bytes |
| `osd` | `osd` | 55,083,978 bytes |
| `rgw` | `rgw` | 97,028,970 bytes |
| `mds` | `mds` | 6,482,457 bytes |
| 합집합 `all` | 위 모든 그룹 | **636,052,501 bytes** |

이 용량은 hardlink inode를 중복 제거한 일반 파일의 논리 크기이며, directory·symlink·tar/image metadata와 생성 manifest를 제외합니다. `all`도 같은 payload 그룹을 사용합니다. 공유하는 각 그룹의 unpacked layer DiffID가 모든 사용 이미지에서 일치하는지 자동 검사했으며, 이번 빌드는 payload 6개와 역할별 manifest 5개로 **고유 DiffID 11개**를 확인했습니다. 공통 base만 공유한 뒤 각 역할 rootfs 전체를 다시 복사하는 방식의 중복을 피했습니다.

DiffID 일치는 동일한 unpacked layer 내용의 재사용을 확인합니다. 이 결과만으로 로컬 snapshot 저장 공간이나 압축 registry 전송량을 계산하지 않습니다. source 이미지와 build cache도 로컬에 별도로 남을 수 있습니다.

## 실측과 테스트 결과

원본은 Makefile의 고정 Quay digest이며, 실제 버전은 `20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964) tentacle`입니다. macOS ARM64의 Docker Desktop Linux ARM64 환경에서 확인했습니다.

| 이미지 | 로컬 Docker image `Size` |
| --- | ---: |
| 원본 Quay | 2,042,985,596 bytes |
| `ceph-testcontainers:20.2.4-control` | 676,867,079 bytes |
| `ceph-testcontainers:20.2.4-osd` | 690,348,258 bytes |
| `ceph-testcontainers:20.2.4-rgw` | 739,136,693 bytes |
| `ceph-testcontainers:20.2.4-mds` | 619,863,871 bytes |
| `ceph-testcontainers:20.2.4-all` | 895,036,802 bytes |

각 `Size`는 같은 Docker 조회 방식의 비교값입니다. 여러 이미지의 값을 더해 물리 디스크 사용량으로 해석하거나 다운로드 크기로 사용하지 않습니다. 앞의 논리 payload 용량과도 다른 지표입니다. 같은 Ceph 프로세스를 실행하므로 이 이미지 분리만으로 daemon RAM 감소를 약속하지 않습니다.

다섯 역할의 smoke test는 원본과 같은 release/commit, 실행 파일의 역할 경계, Python import, OSD plugin, keyring·monmap 생성, license·manifest와 조립 runtime 기준 Linux metadata probe를 확인했습니다. Linux root에서 실행한 패키징 self-test도 numeric UID 167, set-id mode 6751/2750, sticky mode 1777, symlink·hardlink와 동일 입력의 tar 재현성을 확인했습니다.

다음 단일 클러스터 suite는 mirror 추가 이전의 검증 기록입니다.

| 전체 Go suite | 결과 | 관측 실행 시간 |
| --- | --- | ---: |
| `control` + `osd` + `rgw` + `mds` 혼합 | 최상위 테스트 7개 PASS | 218.164초 |
| 모든 역할에 `all` 사용 | 최상위 테스트 7개 PASS | 219.857초 |

suite에는 단위 테스트와 RADOS lifecycle, 부트스트랩 실패 cleanup, RGW/S3, RBD snapshot/clone, CephFS I/O가 포함됩니다. OSD 중단·재시작과 추가·삭제 후 데이터 유지도 기존 시나리오로 확인했습니다. 시간은 cleanup을 포함한 한 번의 Go 실행 기록이며 다운로드·이미지 build 시간과 반복 성능 측정은 포함하지 않습니다. 두 실행 모두 `CGO_ENABLED=0`을 사용했습니다.

## 기록과 검증 경계

기본 출력은 실행마다 새로 만드는 `artifacts/slim-UTC-UUID/`입니다. `--output-dir`는 기존 파일이 없는 디렉터리에만 사용할 수 있습니다. 주요 기록은 다음과 같습니다.

- `build-report.json`: source digest/ID/platform, 실제 버전과 출력 tag/ID/Size, DiffID 공유, 검증별 상태와 오류
- `source-image.json`, `plan.json`: 원본 Docker inspect와 package·파일 그룹·역할 manifest 계획
- `Dockerfile.generated`, `assembly.log`, `build-ROLE.log`, `smoke-ROLE.log`: 조립·빌드·검증 근거
- `integration-mixed.log`, `integration-all.log`: `--integration` 실행 결과
- `integration-multicluster.log`: `--multicluster` 실행 결과
- `context/`: `--keep-context`를 지정했을 때의 tar 및 생성 Dockerfile

초기 단일 클러스터 검증은 로컬 `artifacts/slim-five-20.2.4/`, mirror 포함 빌드·smoke는 `artifacts/slim-mirror-20.2.4/`에 있습니다. 복제 API 실행 기록은 [다중 클러스터 PoC](MULTICLUSTER_POC.md)를 따릅니다. `artifacts/`는 git에 포함하지 않으므로 새 checkout에서는 자동화를 다시 실행해 기록을 생성합니다. 캐시에 없는 source를 `--skip-pull`로 지정한 실패 경로에서 report의 `failed`와 검증별 `not_run`도 확인했습니다. 실패나 생략을 검증별 상태에 구분하며, 기본 빌드 성공만으로 전체 integration이 수행됐다고 해석하지 않습니다. 종료 후 테스트가 소유한 컨테이너와 네트워크가 남지 않았음을 조회했습니다.

Python tar는 extended attribute를 직렬화하지 않습니다. capability·ACL·SELinux label은 보존 대상에서 제외되며, 현재 root로 실행하는 fixture의 동작을 검증했습니다. source의 전체 filesystem metadata나 non-root 실행 호환성을 보장하지 않습니다.

AMD64는 아직 실행하지 않았습니다. 현재 Ceph RPM package 이름·CLI 경로·version 형식을 기대하며, 필요한 root package·CLI·license가 없으면 실패합니다. 향후 버전이나 다른 배포판 이미지의 호환성은 보장하지 않습니다. 원본을 바꾸면 smoke와 전체 integration을 다시 확인합니다. cephadm, dashboard, NFS/iSCSI, 실제 block device 준비 및 kernel/FUSE mount의 대체 이미지를 검증한 범위도 아닙니다.

기존 단일 slim workflow와 당시 측정값은 [SLIM_IMAGE_POC.md](SLIM_IMAGE_POC.md)에 남아 있습니다. `make slim-image`, `make slim-smoke`, `make slim-integration`은 그 실험을 재현하는 기존 명령으로 유지합니다.
