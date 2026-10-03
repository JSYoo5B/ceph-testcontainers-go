# 로컬 Debian 패키지로 Ceph 이미지 빌드

[build.py](../image/slim/build.py)는 회사에서 패치한 Ceph `.deb` 묶음과 호환되는 Debian/Ubuntu 기반 이미지를 받아 `control`, `osd`, `rgw`, `mds`, `all`을 빌드합니다. 기존 Quay RPM 입력과 같은 역할·공유 layer·Go API를 사용합니다. 호스트에는 Docker와 Python 3.9 이상만 필요하며 `dpkg`, APT, Ceph native 라이브러리는 Linux 컨테이너 안에서 실행합니다.

## 사용

```sh
python3 image/slim/build.py \
  --deb-packages /path/to/company-build/*.deb \
  --base-image ubuntu:24.04 \
  --platform linux/arm64 \
  --repository ceph-company-test \
  --tag patched-build-001 \
  --integration
```

출력 tag는 `ceph-company-test:patched-build-001-control`, `-osd`, `-rgw`, `-mds`, `-all`입니다. `--tag`를 생략하면 실제 `ceph --version`의 버전을 사용합니다. 같은 버전 번호로 여러 패치를 빌드할 때는 빌드 번호나 커밋을 tag에 넣습니다. Manifest에는 tag와 별도로 실제 Ceph release/commit과 입력 `.deb` SHA256을 기록합니다.

파일 인자 대신 `--deb-directory DIR`로 디렉터리 바로 아래의 `*.deb`를 전달할 수 있습니다. 두 입력 옵션과 기존 `--source-image` 중 하나만 선택합니다. `.deb` 입력에는 `--base-image`가 필수입니다.

최종 이미지의 runtime 환경은 반복 가능한 `--runtime-env NAME=VALUE`로 명시합니다. 생략하면 기존 기본값을 유지합니다. 값은 생성 Dockerfile의 공통 `ENV`에 들어가므로 다섯 역할에 모두 적용되며, source 패키지 설치·호스트 환경에는 적용하지 않습니다.

```sh
python3 image/slim/build.py \
  --deb-directory /path/to/packages --base-image ubuntu:24.04 \
  --platform linux/arm64 --repository ceph-company-test \
  --tag patched-build-generic-fp \
  --runtime-env TCMALLOC_STACKTRACE_METHOD=generic_fp
```

`TCMALLOC_STACKTRACE_METHOD=generic_fp`는 tcmalloc의 stack trace 수집 방법을 명시적으로 선택하는 예입니다. 이 선택의 적용 여부와 runtime 결과는 image inspect·smoke·클러스터 테스트로 각각 확인합니다. CPU 지원 여부나 패키지 ABI 호환성 판정은 별도입니다. 자동으로 이 값을 넣거나 다른 기본 환경을 바꾸지 않습니다.

이름은 `[A-Za-z_][A-Za-z0-9_]*`를 따르며 중복 이름·제어문자는 거부합니다. `NAME=`으로 빈 값도 전달할 수 있습니다. `$`, 따옴표, 역슬래시는 값 그대로 보존합니다. shell의 사전 치환을 피하려면 `--runtime-env 'NAME=$literal'`처럼 인자를 작은따옴표로 감쌉니다. 빌드 후 각 이미지의 실제 `Config.Env`가 요청값과 정확히 일치하지 않으면 실패합니다. 요청한 설정·역할별 확인값·검증 상태는 `build-report.json`에 기록합니다.

```sh
make slim-images-deb \
  CEPH_DEB_DIRECTORY=/path/to/packages \
  CEPH_DEB_BASE_IMAGE=ubuntu:24.04 \
  CEPH_DEB_TAG=company-patch-001

make slim-images-deb-verify \
  CEPH_DEB_DIRECTORY=/path/to/packages \
  CEPH_DEB_BASE_IMAGE=ubuntu:24.04 \
  CEPH_DEB_TAG=company-patch-001
```

첫 target은 다섯 이미지와 smoke를, 두 번째는 혼합 역할과 `all`의 단일 클러스터 통합 테스트까지 실행합니다. 복제·백업 검증에는 CLI에 `--multicluster`를 추가합니다. 공통 옵션은 [기존 자동화 문서](SLIM_IMAGE_AUTOMATION.md)를 따릅니다.

## 입력 조건

기반 이미지는 패키지를 빌드한 Debian/Ubuntu release와 architecture에 맞춰 선택합니다. Ubuntu 24.04용 패키지에는 `ubuntu:24.04`를, Debian 회사 패키지에는 그 빌드 환경에 맞는 기반 이미지를 지정합니다. `.deb` 메타데이터만으로 배포판 ABI 호환성을 완전히 판별하지는 못하며, 설치·smoke·통합 테스트로 확인합니다.

다섯 역할을 모두 만들기 때문에 다음 runtime root를 로컬 패키지로 전달해야 합니다.

| 역할 | 필수 로컬 root package |
| --- | --- |
| control/client | `ceph-mon`, `ceph-mgr`, `ceph-common`, `python3-rados`, `python3-rbd`, `python3-cephfs`, `rbd-mirror`, `cephfs-mirror` |
| osd | `ceph-osd` |
| rgw | `radosgw` |
| mds | `ceph-mds` |

이 root들이 의존하는 **Ceph source에서 빌드된 라이브러리·Python·MGR core 패키지도 같은 로컬 빌드 묶음에 포함**합니다. Ubuntu 24.04에서 실험한 전체 목록은 아래에 있습니다. 일반 `libc`, Boost, Python 등은 기반 이미지의 APT 저장소에서 설치할 수 있으며, 추가 `.deb`를 함께 제공할 수도 있습니다.

설치된 `Source: ceph` 패키지가 로컬 입력에 없으면 실패합니다. 추출 시 선택된 Ceph source version이 같은지도 검사합니다. architecture는 컨테이너의 `dpkg --print-architecture` 또는 `all`만 허용하며, 같은 package/architecture를 두 번 제공하면 버전과 관계없이 실패합니다. 파일명 대신 `.deb` control 메타데이터를 사용합니다.

추가 개발·debug 패키지를 전달할 수 있지만 설치 자체의 의존성은 만족해야 합니다. 최종 역할은 runtime dependency closure에 필요한 파일을 선택합니다. Ceph package 이름을 변경한 vendor packaging과 일부 역할만 빌드하는 입력은 현재 지원하지 않습니다.

## 설치·조립·기록

1. 기반 이미지 digest/ID와 platform, 입력 파일 SHA256을 기록합니다.
2. 임시 source 이미지에서 `dpkg-deb`로 패키지명·버전·source·architecture와 복사된 파일 SHA256을 확인합니다.
3. 정확한 로컬 파일 경로를 APT에 전달해 `--reinstall --allow-downgrades --no-install-recommends`로 설치합니다. 기반 이미지에 이미 설치된 패키지보다 입력 버전이 낮아도 명시한 로컬 파일을 사용할 수 있으며, 같은 버전의 다른 artifact를 APT가 downgrade로 판정하는 경우도 처리합니다. 서비스 기동을 억제하고 기존 `policy-rc.d`를 복원합니다.
4. 모든 입력의 설치 상태·버전·architecture·source와 파일 SHA256을 다시 검사합니다. 같은 버전의 upstream 파일이 이미 설치돼 있어도 입력 파일을 재설치합니다.
5. 설치된 dpkg database의 `Depends`/`Pre-Depends`, 대안·가상 package·Multi-Arch를 따라 파일을 추출합니다. Debian copyright/common license, Python client/MGR module, multiarch 라이브러리·OSD plugin과 usrmerge symlink를 보존합니다.
6. 파일 membership에 따라 공유 tar layer를 만들고 다섯 이미지를 빌드합니다. 동일 payload DiffID 재사용과 선택한 smoke/Go 테스트를 검사한 뒤 임시 source tag를 정리합니다.

입력 설치와 배포판 의존성 확보에는 네트워크가 필요합니다. `--skip-pull`은 기반 이미지 pull만 생략합니다. 이후 추출·역할 build·smoke는 네트워크 없이 실행합니다. APT snapshot을 고정하지 않으면 일반 의존성 버전은 실행 시점에 따라 달라질 수 있으며, 실제 선택한 버전은 역할 manifest에 남습니다. Registry push는 하지 않습니다.

기본 결과 디렉터리는 `artifacts/slim-UTC-UUID/`이며 `--output-dir`로 빈 디렉터리를 지정할 수 있습니다.

| 기록 | 내용 |
| --- | --- |
| `base-image.json`, `deb-input.json` | 기반 이미지 inspect, 원본 파일명·SHA256 |
| `source-packages.json` | 검증된 package/version/architecture/source/SHA256 |
| `Dockerfile.source.generated`, `deb-install.log` | 임시 설치 이미지 recipe와 APT·검증 로그 |
| `source-image.json`, `plan.json` | 설치 이미지 inspect, 역할 package·license·파일 그룹 |
| `build-report.json` | provenance, 실제 버전, 이미지 ID/Size, 공유 layer·검증 상태, 명시한 runtime 환경과 역할별 inspect 확인값 |
| `build-ROLE.log`, `smoke-ROLE.log`, `integration-*.log` | 빌드와 선택한 테스트 결과 |

각 최종 이미지의 `/usr/share/ceph-testcontainers/image-manifest.json`에도 입력 provenance와 runtime package 버전이 들어갑니다. `.deb` 파일은 최종 역할 이미지에 넣지 않습니다. 임시 image tag 정리 후 Docker build cache는 남을 수 있습니다. [Metadata 보존 한계](SLIM_IMAGE_AUTOMATION.md#기록과-검증-경계)는 기존 방식과 동일합니다.

## Ubuntu 공개 패키지 재현

2026-10-02 확인한 Ubuntu 24.04 ARM64 `noble-updates` 후보는 Ceph `19.2.3-0ubuntu0.24.04.3`입니다. [공식 main index](https://ports.ubuntu.com/ubuntu-ports/dists/noble-updates/main/binary-arm64/Packages.xz), [universe index](https://ports.ubuntu.com/ubuntu-ports/dists/noble-updates/universe/binary-arm64/Packages.xz). Mirror는 `universe`에 있습니다.

```sh
docker run -i --name ceph-public-debs --platform linux/arm64 ubuntu:24.04 /bin/sh -s <<'SH'
set -eu
apt-get update
mkdir /out
cd /out
version=$(apt-cache policy ceph-common | sed -n 's/^  Candidate: //p')
for package in \
  ceph-base ceph-common ceph-mon ceph-mgr ceph-mgr-modules-core \
  ceph-osd ceph-mds radosgw rbd-mirror cephfs-mirror \
  libcephfs2 librados2 libradosstriper1 librbd1 librgw2 libsqlite3-mod-ceph \
  python3-ceph-argparse python3-ceph-common python3-cephfs python3-rados python3-rbd
do
  apt-get download "$package=$version"
done
SH
mkdir -p artifacts/ubuntu-noble-debs
docker cp ceph-public-debs:/out/. artifacts/ubuntu-noble-debs/
docker rm ceph-public-debs

python3 image/slim/build.py \
  --deb-directory artifacts/ubuntu-noble-debs \
  --base-image ubuntu:24.04 \
  --repository ceph-testcontainers-ubuntu \
  --tag noble-19.2.3 \
  --platform linux/arm64 \
  --integration
```

회사 패치 자체의 적용 여부는 실제 회사 `.deb`로 같은 절차를 실행해서 확인합니다.

## 실험 결과

Ubuntu 24.04 ARM64의 공식 Ceph `.deb` 21개를 로컬 파일로 전달했습니다. 실제 바이너리는 `ceph version 19.2.3 (c92aebb279828e9c3c1f5d24613efca272649e62) squid (stable)`이며, 다섯 역할의 executable/Python/plugin/license/metadata smoke가 모두 통과했습니다.

| 이미지 | Docker image `Size` |
| --- | ---: |
| `ceph-testcontainers-ubuntu:noble-19.2.3-control` | 618,727,656 bytes |
| `ceph-testcontainers-ubuntu:noble-19.2.3-osd` | 616,360,872 bytes |
| `ceph-testcontainers-ubuntu:noble-19.2.3-rgw` | 534,001,725 bytes |
| `ceph-testcontainers-ubuntu:noble-19.2.3-mds` | 526,626,750 bytes |
| `ceph-testcontainers-ubuntu:noble-19.2.3-all` | 824,310,895 bytes |

공유 payload 그룹은 6개, 역할별 manifest까지 포함한 고유 DiffID는 11개입니다. 이 값은 registry 전송량이나 물리 디스크 사용량이 아닙니다. 기존 Quay 측정은 Ceph 20.2.4이므로 패키징 방식만의 용량 차이로 해석하지 않습니다.

혼합 역할의 단일 클러스터 suite는 5개 최상위 테스트가 266.789초에 통과했습니다. RADOS lifecycle/OSD 추가·삭제·장애 후 데이터 유지, bootstrap 실패 cleanup, RGW/S3, RBD snapshot/clone, CephFS 파일 I/O를 포함합니다. `all` 단일 이미지 방식도 같은 5개 테스트를 280.866초에 통과했습니다. 두 방식 모두 `CGO_ENABLED=0`으로 실행했습니다. 이미지 조립/설치 검증의 순수 Python 테스트 40개, Go 단위 테스트·vet, 기존 Quay 기반 다섯 이미지의 공통 smoke 회귀 검증도 통과했습니다.

같은 버전 번호의 다른 바이너리를 적용하는 경로도 별도로 확인했습니다. Ubuntu `hello 2.10-3build1`을 먼저 설치한 뒤 Package/Version/Architecture/Source를 유지하고 payload 파일을 추가한 `.deb`로 재설치했습니다. 추가 파일의 존재·package 소유권, 동일한 설치 메타데이터, 로컬 archive SHA256과 provenance 일치를 확인했습니다. Ubuntu minimal 이미지의 doc 제외 규칙 때문에 이 fixture에만 해당 marker 경로를 include했습니다. APT가 이를 downgrade로 판정하는 경우를 재현했으며, `--allow-downgrades` 적용 후 통과했습니다. 결과는 `artifacts/deb-same-version-fixture/summary.json`에 있습니다. Ceph 자체를 수정·재컴파일한 실험은 아닙니다.

로그와 manifest는 로컬 `artifacts/slim-deb-noble-19.2.3-run3/`에 있습니다. `artifacts/`는 git에서 제외하므로 새 checkout에서는 앞의 절차로 재생성합니다. 회사 패키지와 AMD64 실행은 아직 검증하지 않았습니다. Ubuntu 패키지의 다중 클러스터 suite는 이번 실험에 포함하지 않았으며, 필요하면 `--multicluster`로 실행합니다.


APT local file 설치는 [Debian Reference](https://www.debian.org/doc/manuals/debian-reference/ch02.en.html)를, 의존성 해석은 [Debian Policy](https://www.debian.org/doc/debian-policy/ch-relationships.html)를 따릅니다. 같은 버전의 서로 다른 artifact를 구별하기 위해 version pin에 더해 SHA256과 정확한 로컬 재설치를 기록합니다. [APT preferences](https://manpages.debian.org/bookworm/apt/apt_preferences.5.en.html).
