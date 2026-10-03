# Native RGW 패치 빌드

`native_build.py`는 공식 source와 runtime `.deb`를 검증하고 실제 Ceph CMake 빌드·gtest를 실행합니다. `debian_runtime.py`는 같은 빌드의 RGW 실행 파일과 private 라이브러리를 Debian 패키지로 묶습니다. 결과는 기존 `image/slim/build.py --deb-directory`로 전달합니다. Go fixture와 slim 조립기는 Ceph를 직접 컴파일하지 않습니다.

현재 recipe는 **Ceph 20.2.4 / Ubuntu Noble 24.04 / Linux ARM64**만 지원합니다. 다른 버전·배포판·아키텍처를 지원한다고 가정하지 않습니다. 명령은 repository root에서 실행합니다.

## 입력 준비

Python 3.12 이상, `gpg`, `gpgv`가 필요합니다. 호스트에서 C++나 Docker를 실행하지 않고 준비할 수 있습니다.

```sh
python3 image/native/native_build.py --plan \
  --recipe image/patches/ceph-20.2.4/recipe.noble-arm64.json

python3 image/native/native_build.py --prepare-only \
  --recipe image/patches/ceph-20.2.4/recipe.noble-arm64.json \
  --output artifacts/native-inputs-20.2.4-arm64
```

Recipe는 ordered patch/source/input manifest SHA, CMake 옵션, 네 target, gtest 최소 9개, 패키지 버전·아키텍처를 고정합니다. 함께 보관한 release key·서명된 `InRelease` → `Sources.gz`/ARM64 `Packages` hash → source archive·22개 `.deb` hash를 확인합니다. 키는 공식 HTTPS에서 받은 뒤 고정했으며 별도 경로의 신원 검증을 주장하지 않습니다.

Source archive는 332,980,650 bytes, runtime `.deb`는 합계 105,219,270 bytes입니다. `src/.git_version`과 지정 원본 파일 SHA도 검사합니다. signed archive와 버전·지정 파일의 일치를 확인하며 전체 Git tree와 archive의 바이트 동일성을 주장하지 않습니다.

이미 받은 입력은 `--source-archive PATH`, `--deb-directory DIR`로 복사 없이 채택할 수 있습니다. `--offline`은 **Ceph source/runtime archive 다운로드**를 금지합니다. APT 설치나 CMake의 bundled Boost 다운로드까지 네트워크를 막는 옵션은 아닙니다. 준비 report의 native 상태는 `NOT RUN`입니다.

## Native 빌드와 패키징

Docker는 환경 배치 수단입니다. 도구 자체는 Docker나 Git을 호출하지 않습니다. 아래 예시는 helper/recipe와 준비 입력을 읽기 전용으로 연결하고, source/build를 named volume에 둡니다.

```sh
docker build --platform linux/arm64 \
  -t ceph-tc-native-build:20.2.4 image/native

docker volume create ceph-tc-native-rgw-20.2.4

docker run -d --name ceph-tc-native-rgw-20.2.4 \
  --mount type=volume,src=ceph-tc-native-rgw-20.2.4,dst=/native \
  --mount "type=bind,src=$PWD/image,dst=/inputs/image,readonly" \
  --mount "type=bind,src=$PWD/artifacts/native-inputs-20.2.4-arm64,dst=/prepared,readonly" \
  ceph-tc-native-build:20.2.4

docker exec ceph-tc-native-rgw-20.2.4 \
  python3 /inputs/image/native/native_build.py --build --offline \
  --recipe /inputs/image/patches/ceph-20.2.4/recipe.noble-arm64.json \
  --source-archive /prepared/archives/ceph_20.2.4-1noble.tar.gz \
  --deb-directory /prepared/official-debs --output /native/run

docker exec ceph-tc-native-rgw-20.2.4 \
  python3 /inputs/image/native/debian_runtime.py \
  --recipe /inputs/image/patches/ceph-20.2.4/recipe.noble-arm64.json \
  --run-dir /native/run --deb-directory /prepared/official-debs \
  --output /native/packages

mkdir -p artifacts/native-packages-20.2.4-arm64
docker cp ceph-tc-native-rgw-20.2.4:/native/packages/. \
  artifacts/native-packages-20.2.4-arm64

python3 image/slim/build.py \
  --deb-directory artifacts/native-packages-20.2.4-arm64/debs \
  --base-image ubuntu:24.04 --platform linux/arm64 \
  --repository ceph-testcontainers-native --tag 20.2.4-rgw-patched \
  --output-dir artifacts/slim-native-20.2.4-arm64
```

기존 cache를 `--source-archive`로 채택했다면 `/prepared/archives/...` 대신 해당 read-only 경로를 지정합니다. 빌드 output은 fresh directory여야 합니다. 로그·source·build·ELF·XML·`.deb`는 ignored artifacts나 외부 volume/cache에만 보관합니다.

실제 target은 `unittest_rgw_bucket_sync_pipe_rules`, `radosgw`, `radosgw-admin`, `rgw`입니다. shared RGW가 요구하는 common/librados/CLS dependency도 함께 빌드됩니다. MON·OSD·MDS를 다시 빌드하지 않습니다. `Release -O1 -DNDEBUG -g0`, GNU bfd, IPO/LTO off, Boost 1.87 `BOOST_J=1`, build parallelism 1을 사용합니다. Beast/OpenSSL·RADOS·native S3/multisite는 유지하며 RBD/CephFS/MGR/LDAP/XFS 및 선택 backend는 이 RGW 빌드에서 끕니다. 옵션 전체는 recipe와 `--plan`에 있습니다.

Dockerfile의 기본 bootstrap base digest는 recipe와 일치합니다. CLI는 실제 Linux 배포판·버전·아키텍처를 확인하며 Docker image digest 자체를 판별하지 않습니다. APT dependency snapshot은 고정하지 않습니다. 설치 package inventory·compiler version은 기록합니다. 입력과 절차를 재현하는 workflow이며 산출물의 바이트 동일성을 보장하지 않습니다. 4CPU/4GB에서 사용하는 j1/O1 설정도 임의 환경에서 성공을 보장하지 않습니다.

패키징은 실제 XML 9개 이상·실패/error/disabled 0과 완성된 native stage가 필요합니다. 모든 ELF의 RPATH를 먼저 옮기고 `ldd -r`로 build-tree 경로나 외부 Ceph 라이브러리 사용을 거부합니다. 같은 빌드의 `libceph-common`/`librados`/`librgw` 등을 `ceph-tc-native-rgw`의 private 경로에 둡니다. 원본 `radosgw`와 `ceph-common`에서는 해당 RGW 실행 파일만 교체하고, 공식 `Version: 20.2.4-1noble`·Source·기존 exact dependency를 보존합니다. 나머지 20개 `.deb`는 그대로 복사합니다. 새 private 패키지는 `Source: ceph (20.2.4-1noble)`와 별도 patch-derived version을 가집니다. 다른 daemon의 공식 라이브러리는 유지됩니다.

`--version`은 원래 Ceph release commit을 계속 출력합니다. 커스텀 빌드는 ordered patch SHA·native report·수정 `.deb` SHA로 구분합니다. slim이 일반 문서 파일을 제거하므로 외부 `package-report.json`과 slim provenance를 함께 보관합니다.

## 재개와 검증 경계

동일 snapshot 재개는 `--build` 대신 `--resume-build`로 실행합니다. recipe/input/source/patch manifest, 지정 원본·패치 결과 파일, `src/.git_version`, 설치 inventory, compiler와 선언 CMake cache를 다시 확인합니다. 이전 report·실패·XML은 attempt history에 보존하고 네 target 빌드와 native test를 실제로 다시 실행합니다. 명시적인 초기 lld→bfd 전환만 추가 허용하며 다른 설정 변경은 거부합니다. 검사 범위는 지정 source 파일과 선언한 CMake 설정이며 전체 tree·모든 내부 cache 변수는 아닙니다. fresh prepare/build로 이전 native 증거를 덮어쓰지 않습니다.

이 wrapper의 recipe SHA/XML SHA/artifact manifest SHA는 별도 report 계약입니다. 임시 prototype의 기존 report를 수정해서 성공 상태를 채택하지 않습니다. 기존 prototype에서 확인한 native build·9개 gtest·ELF stage 성공과, repository wrapper의 호스트 경계 테스트 성공을 구분해야 합니다. 패키지 설치·slim smoke·실제 bridge/host 클라이언트 복제 회귀는 각각 추가 실행 증거가 필요합니다. 특히 source GetObject 거부/미복제/권한 grant 후 복제와 numeric priority/OR/fallback payload 회귀를 완화하지 않습니다.

호스트 경계 테스트:

```sh
python3 -m unittest discover -s image/native/tests -v
```

공식 source: [RGW CMake](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/rgw/CMakeLists.txt), [Boost build](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/cmake/modules/BuildBoost.cmake), [native gtest target 정의](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/rgw/CMakeLists.txt). 패치와 upstream 범위는 [20.2.4 패치 설명](../patches/ceph-20.2.4/README.md)을 참조합니다.
