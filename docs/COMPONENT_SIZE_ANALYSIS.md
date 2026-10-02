# Ceph 이미지의 큰 구성요소와 분리 효과

측정일: 2026-10-02, Asia/Seoul. Ceph 20.2.4 ARM64 공식 이미지와 [검증된 slim 이미지](SLIM_IMAGE_POC.md)를 기준으로 조사했습니다.

가장 먼저 제외할 것은 dashboard·머신러닝·개발 도구입니다. 이들은 이미 slim 이미지에서 제외했습니다. 남은 큰 패키지는 RGW와 ceph-common입니다. RGW를 선택적으로 배포하면 RBD/CephFS 전용 환경에서는 의미 있는 용량 절감이 가능하지만, RGW 전용 환경에서 MDS를 제외하는 효과는 작습니다. 더 작은 RGW fixture를 원한다면 패키지를 역할별로 나누는 것보다 패키지 안의 미사용 도구를 선별하는 작업이 먼저입니다.

## 측정 기준

기준 이미지는 다음 digest입니다.

```text
quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
```

`image/slim/analyze.py`를 기준 이미지 안에서 실행했습니다. 설치된 RPM 435개의 inventory를 읽고, slim assembler와 같은 root package·설치된 dependency closure·문서 제외·라이선스 보존·CA 및 기본 설정 파일을 적용합니다. 각 profile 안에서는 hardlink와 symlink target을 inode 기준으로 중복 계산하지 않습니다. RPM의 imported signing key 기록인 `gpg-pubkey`는 package count에서 제외합니다.

아래에서 MB는 1,000,000 bytes입니다. 원본 package 표는 RPM이 선언한 설치 용량이며, slim 및 profile 표는 원본 filesystem에 실제 존재하는 일반 파일의 논리적 크기입니다. Docker layer의 unpacked 크기, 압축 다운로드 크기, 실행 RAM과는 다릅니다. 전체 slim 선택 파일의 논리적 크기는 **624,964,767 bytes, 약 625.0 MB**입니다. 최종 Docker image history의 unpacked layer 합계는 약 650.7 MB이고 로컬 Docker `Size`는 약 879.4 MB입니다. 지표 차이와 이미지 실행 결과는 [slim PoC](SLIM_IMAGE_POC.md)를 따릅니다.

파일 목록은 RPM payload뿐 아니라 현재 존재하는 `%ghost` 파일도 포함합니다. 예를 들어 원본의 `/var/lib/rpm/rpmdb.sqlite` 약 28.8 MB가 복사 대상에 들어갑니다. 이 database는 원본의 package 상태를 나타내므로 최종 런타임에 선별한 package 목록은 별도 manifest를 기준으로 봐야 합니다. package 선언 용량과 실제 파일 크기가 다른 이유 중 하나입니다.

## 원본에서 큰 패키지

| 패키지 | RPM 선언 용량 | 현재 fixture에서의 판단 |
| --- | ---: | --- |
| `ceph-mgr-dashboard` | 152.0 MB | dashboard 미사용, slim에서 제외 |
| `ceph-radosgw` | 97.0 MB | RGW 데몬과 여러 독립 도구, slim에 보존 |
| `ceph-common` | 93.8 MB | 제어 CLI·client·검사/복구 도구가 함께 들어 있음 |
| `gcc` | 80.0 MB | 런타임에서 컴파일하지 않으므로 제외 |
| `python3-scipy` | 78.8 MB | slim dependency closure에 포함되지 않음 |
| `ceph-mgr-diskprediction-local` | 68.7 MB | disk prediction 미사용, 제외 |
| `ceph-osd` | 50.9 MB | 데이터 저장용 데몬 및 도구, 보존 |
| `python3-scikit-learn` | 47.0 MB | slim dependency closure에 포함되지 않음 |

dashboard, diskprediction-local, SciPy, scikit-learn 네 패키지의 선언 용량만 합쳐도 약 346.5 MB입니다. GCC까지 합치면 약 426.5 MB입니다. 이 합은 개별 package의 설치 용량이고 dependency까지 포함한 고유 layer 감소량은 아닙니다. 원본이 여러 배포 기능을 함께 제공하는 것은 [공식 Containerfile](https://github.com/ceph/ceph/blob/v20.2.4/container/Containerfile)에서 확인할 수 있습니다.

이런 기능을 별도 선택 이미지로 빼거나 제거하는 것이 가장 큰 첫 단계입니다. slim은 새 scratch stage에 필요한 파일만 복사하므로 사용하지 않는 원본 layer가 최종 이미지에 남지 않습니다. 최종 로컬 Docker `Size`는 **2,043.0 MB → 879.4 MB, 약 57% 감소**했고, RGW·RBD·CephFS와 OSD 변경 테스트가 모두 통과했습니다.

## slim에 남은 큰 패키지와 파일

| 패키지 | 보존된 일반 파일 용량 |
| --- | ---: |
| `ceph-radosgw` | 97.0 MB |
| `ceph-common` | 93.7 MB |
| `ceph-osd` | 50.9 MB |
| `libicu` | 34.0 MB |
| `rpm` | 29.5 MB, 원본 RPM DB 포함 |
| `librgw2` | 26.4 MB |
| `ceph-base` | 26.3 MB |
| `python3-libs` | 19.6 MB |
| `ceph-mon` | 17.5 MB |
| `libarrow` | 16.5 MB |

패키지 크기는 데몬 실행 파일의 크기와 다릅니다. `ceph-radosgw`의 실제 `radosgw`는 **25.7 MB**입니다. 다음 세 개의 독립 executable이 각각 약 23.6 MB이며, 합계 **70,755,256 bytes, 약 70.8 MB**를 차지합니다.

| 파일 | 용량 | 역할과 현재 테스트 사용 여부 |
| --- | ---: | --- |
| `/usr/bin/radosgw-es` | 23.585 MB | Elasticsearch query compiler 도구, 미사용 |
| `/usr/bin/radosgw-object-expirer` | 23.586 MB | 별도 object expiration processor, 미사용 |
| `/usr/bin/rgw-policy-check` | 23.584 MB | IAM policy parser 검사 도구, 미사용 |

각 executable은 [RGW CMake 정의](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/CMakeLists.txt)에서 별도로 빌드합니다. 용도는 [ES query 도구 소스](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_es_main.cc), [policy 검사 소스](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_polparser.cc)를 확인했습니다. object-expirer executable을 제외하더라도 RGW 내부의 expiration 기능을 제거하는 것은 아닙니다. 일반 `RGWRados`도 [내부 expirer thread를 시작](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/driver/rados/rgw_rados.cc#L1292-L1296)하므로 daemon의 core library와 의존성은 보존해야 합니다.

`ceph-common`의 큰 부분도 선택적인 도구입니다.

| 파일/구성 | 용량 | 현재 테스트 사용 여부 |
| --- | ---: | --- |
| `/usr/lib64/ceph/denc/denc-mod-*.so` | 합계 40.836 MB | `ceph-dencoder`용 플러그인, 미사용 |
| `/usr/bin/radosgw-admin` | 24.381 MB | RGW 사용자/키 생성에 사용 |
| CephFS data-scan/journal/table 도구 3개 | 합계 19.573 MB | 복구/검사 도구, 미사용 |
| `/usr/bin/rbd` | 3.481 MB | RBD 데이터·snapshot·clone 검증에 사용 |
| `/usr/bin/rados` | 0.675 MB | RADOS 객체 검증에 사용 |

`denc` 플러그인은 [ceph-dencoder CMake 정의](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/ceph-dencoder/CMakeLists.txt)와 [도구의 plugin loader](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/ceph-dencoder/ceph_dencoder.cc)를 기준으로 구분했습니다. OSD 런타임에서 사용하는 compressor, erasure-code, crypto plugin이나 `/usr/lib64/rados-classes`와는 용도가 다릅니다.

따라서 **RGW 독립 도구 3개 + denc 플러그인만 약 111.6 MB**, 전체 slim 선택 파일의 약 **17.9%**입니다. `/var/lib/rpm` database 약 28.8 MB와 복구 도구도 별도 검토할 수 있습니다. `%ghost` 파일 전체를 일괄 제외하면 loader cache나 CA 생성 파일까지 빠질 수 있으므로 경로와 실제 사용 여부를 따로 확인해야 합니다.

이들은 다음 경량화 후보로 확인한 파일입니다. 현재 검증된 slim 이미지에서는 아직 제거하지 않았으며, 제거 후의 크기와 I/O 동작을 검증한 결과로 표현하지 않습니다. `radosgw-admin`처럼 제어에 필요한 도구는 삭제하기보다 control/tools 이미지로 옮기는 선택이 가능합니다.

## 서비스별로 제외했을 때의 효과

아래 값은 실제 설치 package closure를 선별해 비교한 결과입니다. 역할별 이미지를 새로 빌드하거나 혼합 클러스터를 실행한 결과는 아닙니다.

| 필요한 클러스터 | 선택 파일 총량 | 전체 slim 대비 제외 용량 | 감소율 |
| --- | ---: | ---: | ---: |
| MON + MGR + OSD + RGW + MDS/client | 625.0 MB | — | — |
| RGW 전용: MON + MGR + OSD + RGW | 618.5 MB | MDS 6.5 MB | 1.04% |
| CephFS 전용: MON + MGR + OSD + MDS/client | 527.9 MB | RGW와 mailcap 97.0 MB | 15.53% |
| RADOS/RBD 전용: MON + MGR + OSD/client | 521.5 MB | RGW와 MDS 등 103.5 MB | 16.56% |

**RGW를 optional image로 분리하는 것은 RBD·CephFS 소비자에게 의미가 있습니다. MDS만 분리해서 RGW 소비자의 이미지 용량을 줄이는 효과는 작습니다.** RGW는 librados 위에서 동작하며 MDS가 필요하지 않습니다. [RGW 구조](https://docs.ceph.com/en/tentacle/radosgw/).

`ceph-base`가 같은 version의 `ceph-common`을 요구하고, 이 공통 package가 CLI·client binding과 많은 라이브러리를 함께 끌고 옵니다. 실제 closure에서는 RGW를 제외해도 `radosgw-admin`, `librgw2`, Python client 등이 공통 dependency로 남습니다. 그러므로 이 결과는 RPM 단위 분리의 효과이고 각 서비스의 절대 최소 런타임은 아닙니다. package 구분과 요구 관계는 [공식 RPM spec](https://github.com/ceph/ceph/blob/v20.2.4/ceph.spec.in)을 따릅니다.

## 역할마다 독립 이미지를 만들면 생기는 중복

| 역할 | 독립적으로 조립했을 때 선택 파일 총량 |
| --- | ---: |
| RADOS/client | 356.0 MB |
| MON/control | 449.2 MB |
| MGR | 448.8 MB |
| OSD | 486.8 MB |
| RGW | 528.7 MB |
| MDS | 438.2 MB |

여섯 역할에 모두 들어가는 공통 파일만 약 **356.0 MB**입니다. 각 역할의 rootfs를 독립적인 flatten 이미지로 만들면 파일 총합은 약 **2,707.9 MB**가 됩니다. 같은 파일을 한 번씩만 세는 union은 **625.0 MB**이므로, 공통 layer를 공유하지 않는 구성에서는 약 **2,082.9 MB**가 중복되는 모델입니다. 이 값은 파일 payload의 모델이며 Docker image size 실측은 아닙니다.

단일 slim 이미지는 여러 컨테이너가 같은 image layer를 공유합니다. 따라서 컨테이너 수에 이미지 크기를 곱해서 저장 공간을 계산하면 과대평가합니다. 서로 다른 role 이미지라도 동일한 공통 base layer를 쓰면 이 layer를 공유합니다. 파일 내용이 같다는 이유만으로 서로 다른 flatten layer가 자동 공유된다고 가정하면 안 됩니다. [Docker image layer 공유](https://docs.docker.com/engine/storage/drivers/#container-and-layers).

역할별 이미지를 만들 경우에는 **공통 runtime base + 역할별 추가 layer + 선택적인 control/tools 이미지**가 적절합니다. 같은 base digest와 Ceph version을 사용하고, role별 이미지의 합계·고유 layer 총량·전체 클러스터가 실제 pull하는 용량을 함께 비교해야 합니다. RADOS/client의 독점 파일은 이번 계산에서 0 bytes이므로 client를 별도 이미지로 만드는 것만으로 전체 로컬 용량이 줄지는 않습니다.

## RAM/CPU와 구성 최소화

이미지 안의 미사용 executable은 프로세스로 실행되지 않습니다. 미사용 MDS 파일을 제거했다고 그 용량만큼 RGW 클러스터 RAM이 줄어드는 것은 아닙니다. 현재 `StartRGW`만 호출한 fixture는 이미 MDS를 실행하지 않으며 별도 RBD daemon도 없습니다.

RAM/CPU를 줄이는 핵심은 실행 데몬과 OSD 수입니다. 기본 기능 I/O만 필요하다면 MON 1 + MGR 1 + OSD 1에 RGW 또는 MDS를 선택적으로 붙이는 profile을 검토할 수 있습니다. MGR은 client I/O의 엄밀한 필수 경로는 아니지만 없으면 runtime/metrics CLI가 막히거나 상태 정보가 갱신되지 않아 현재 readiness·PG 검사를 위해 유지하는 편이 좋습니다. [MGR_DOWN 동작](https://docs.ceph.com/en/tentacle/rados/operations/health-checks/#mgr-down).

1 OSD profile에는 `WithOSDCount(1)` 외에도 자동 생성 풀부터 `size=1`, `min_size=1`을 적용하는 설정이 필요합니다. 현재 기본값은 size 2입니다. `mon_allow_pool_size_one`은 이미 켜져 있지만, size 1 풀에는 `POOL_NO_REDUNDANCY` 경고가 발생하므로 해당 예상 경고를 명시적으로 처리해야 합니다. [20.2.4 health 판정](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc#L7497-L7516). 이 최소 profile은 아직 구현·측정하지 않았습니다. 복제·장애·OSD drain을 검증하는 기존 2개 이상 OSD 구성과 목적을 구분해야 합니다.

현재 판단 순서는 다음과 같습니다.

1. 검증된 단일 slim 이미지를 사용합니다. 불필요한 dashboard·ML·개발 도구를 제외한 큰 절감은 이미 달성했습니다.
2. 더 줄일 필요가 있으면 미사용 executable·denc·package database를 선별하고 같은 전체 테스트로 다시 검증합니다.
3. RBD/CephFS 전용 소비자가 많다면 RGW를 optional layer/image로 분리합니다. RGW 전용 소비자에게 MDS 분리는 우선순위가 낮습니다.
4. 여러 역할을 나눌 때는 공통 base를 공유하고, RAM 절감은 별도의 데몬/OSD profile 실험으로 측정합니다.

## 재현과 자료

기준 이미지를 먼저 캐시한 후 저장소 루트에서 실행합니다. 분석 컨테이너는 네트워크를 사용하지 않습니다.

```sh
CEPH_SOURCE_IMAGE=quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
mkdir -p artifacts
docker run --rm -i --network=none \
  -e SOURCE_IMAGE="$CEPH_SOURCE_IMAGE" --entrypoint python3 "$CEPH_SOURCE_IMAGE" - \
  < image/slim/analyze.py > artifacts/poc-component-sizes.json
```

JSON에는 profile별 package 목록과 라이선스 파일, 선택 파일 크기, package/file 상위 목록, 서비스 제외 용량과 role 중복 모델이 들어 있습니다. `unvalidated_trimming_candidates`에는 denc·RGW 독립 도구·CephFS 복구 도구·RPM database의 전체 파일 목록과 합계도 기록합니다. 별도 JSON은 `artifacts/`에 저장하여 git에 포함하지 않습니다. 최종 이미지 build·smoke·전체 통합 테스트 결과는 [SLIM_IMAGE_POC.md](SLIM_IMAGE_POC.md), 실제 서비스 테스트 범위는 [SERVICES_POC.md](SERVICES_POC.md)를 따릅니다.
