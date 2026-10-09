# Go 모듈과 이미지의 실행 계약

이 모듈은 [이미지 프로젝트의 요구사항](../../ceph-testcontainers-images/docs/IMAGE_REQUIREMENTS.md)을 입력으로 사용합니다. 이미지 요구사항·검사·공식 역할 추출·배포판 패키지 이미지 생성은 이미지 프로젝트의 책임이며, Go 모듈은 준비된 이미지에서 클러스터를 구성합니다. 이미지 정책을 Go 코드의 편의에 맞춰 확대하지 않습니다.

기본 이미지는 `ceph.DefaultImage`의 원본 Quay Ceph 20.2.4입니다. FSID, 키, 설정, daemon ID와 endpoint, bootstrap 명령과 entrypoint를 모듈이 실행 시점에 제공하므로 이미지에 cluster setup을 넣을 필요가 없습니다. 배포판·패키지 이름·빌드 방법·label·builder manifest를 검사하거나 전제하지 않습니다.

## 역할 선택과 실행 위치

| 이미지 역할 | Go API의 선택 | 실행 위치와 책임 |
|---|---|---|
| `control` | `ceph.Run(ctx, image, ...)` | 별도 MON/MGR, 필요 시 CLI container, Python client/probe와 관리 CLI; librbd 암호화·libcryptsetup·`cryptsetup`, `rados --striper`·libradosstriper |
| `osd` | `ceph.WithOSDImage(image)` | OSD마다 sparse BlueStore 파일과 daemon; `hello`·`lock`을 포함한 object class와 runtime dependencies; ID·키는 control에서 준비 |
| `rgw` | `ceph.WithRGWImage(image)` 또는 `multicluster.RunRGWMultisite/RunRGWTopology`의 image | gateway, `radosgw-admin`, listener 소유권 확인용 `readlink`; HTTP/TLS readiness는 control에서 실행 |
| `mds` | `ceph.WithMDSImage(image)` | active/standby/replay MDS; filesystem 구성과 Python I/O는 control에서 실행 |
| `all` | `Run`과 역할 override에 같은 image 사용 | 네 역할의 합집합; 하나의 컨테이너에 모든 daemon을 실행한다는 의미는 아님 |

override를 생략하면 `Run`에 전달한 이미지로 해당 역할을 실행합니다. 따라서 `control` 전용 이미지를 사용할 때는 최소한 OSD 이미지를 지정하고, RGW/CephFS를 시작할 때는 각각 RGW/MDS 이미지도 지정합니다. 조합하는 이미지는 같은 Ceph release와 지원 platform이어야 합니다.

Mirror 도구와 daemon은 항상 `control` 계약에 포함됩니다. 이전 `base`/`multicluster` 단계 구분은 사용하지 않습니다. `RunRBDMirror`와 `RunCephFSMirror`에는 `control` 또는 `all` 이미지를 전달합니다. 통합 테스트는 source 클러스터의 `ControlImage()`를 전달하며 mirror 전용 이미지 환경 변수를 두지 않습니다. RBD는 전달한 이미지로 양쪽 setup client와 destination의 mirror daemon을 별도 컨테이너로 실행합니다. CephFS는 각 클러스터의 `Ceph` CLI로 설정하고 전달한 이미지로 source의 mirror daemon을 별도 컨테이너로 실행합니다.

RGW multisite/topology의 image 인자는 gateway 역할만 선택합니다. 설정 client는 기본적으로 **각 zone의 클러스터가 가진 `ControlImage()`**를 사용합니다. `RGWMultisiteConfig.ControlImage`나 `RGWTopologyConfig.ControlImage`를 지정하면 모든 설정 client가 그 공용 control/all 이미지를 사용합니다. 이후 `AddZone`/`AddZonegroup`에도 같은 규칙을 적용합니다. RGW 이미지에 Python이나 `ceph` CLI를 요구하지 않습니다.

공통 writable 경로는 `/var/run/ceph`를 포함한 이미지 계약을 따릅니다. CephFS mirror의 admin socket도 이 경로에 생성·조회합니다. 이미지에서 `/run/ceph`를 별도로 제공할 필요는 없습니다. Native 명령의 시간 제한은 control의 Python subprocess로 적용하며 외부 `timeout` executable을 요구하지 않습니다.

## 이미지 검사와 Go 검증

필수 Go CI는 공개 `official`의 네 역할 이미지로 Linux AMD64에서 Ceph
시나리오를 실행합니다. 자동으로 실행하던 계열 3개 × layout 2개 ×
architecture 2개의 Go 호환성 matrix는 제거합니다. 이미지 계열·architecture와
`all`/역할 조합의 quick/full 검사·생성·배포 검증은
[이미지 프로젝트 CI](../../ceph-testcontainers-images/.github/workflows/test.yml)가
담당합니다. Go CI는 제공된 역할 이미지에서 모듈의 클러스터 구성·변경과
native 데이터·상태 assertion, 실제 이미지 identity와 자체 cleanup을 검증합니다.

이미지 검사기는 독립 Docker harness를 사용합니다. 그 multi-architecture
PASS는 각 platform에서의 Go API·Testcontainers 연결까지 같은 방식으로
검증한 결과가 아닙니다. 자동 Go matrix를 제거한 뒤 ARM64·다른 계열에 대한
Go 연결 검증이 필요하면 아래 수동 target을 실행하고 그 source·입력·결과를
별도로 기록합니다.

[이미지 검사기](../../ceph-testcontainers-images/image/check.py)는 이미 준비된 로컬 이미지를 대상으로 동작합니다. `quick`은 역할별 실행 파일·라이브러리·class의 존재와 로딩을 검사하고, `full`은 quick 이후 자체 Docker harness로 cluster lifecycle, RBD, CephFS, RGW-S3, RBD encryption, RADOS object class, RADOS striper, RBD backup, RBD snapshot mirror, CephFS snapshot mirror, RGW multisite의 11개 시나리오를 실행합니다. 검사기는 Go module이나 go-ceph에 의존하지 않습니다.

`full` 통과는 그 검사기의 11개 시나리오에 대한 증거입니다. 파일 존재나 `--version` 성공만으로 기능 호환성을 판단하지 않습니다. Go 모듈의 모든 topology/fixture/SDK test 통과로 확대하지 않습니다. 이 프로젝트에서는 별도로 준비된 이미지를 Go API로 검증합니다.

```sh
# all 이미지 하나
CEPH_TEST_IMAGE=my-company/ceph:dev make image-compatibility

# 네 역할 이미지
CEPH_TEST_IMAGE=my-company/ceph-control:dev \
CEPH_TEST_OSD_IMAGE=my-company/ceph-osd:dev \
CEPH_TEST_RGW_IMAGE=my-company/ceph-rgw:dev \
CEPH_TEST_MDS_IMAGE=my-company/ceph-mds:dev \
make image-compatibility
```

이 target은 아래 수동 matrix에 명시한 **대표 9개 Go test**를 순차 실행하는 선택 경로입니다. `CGO_ENABLED=0`이며 이미지 빌드를 수행하지 않습니다. 테스트는 testcontainers의 일반 이미지 선택/획득 동작을 사용합니다. 더 넓은 토폴로지에는 `make topology`, `make multicluster`, `make topology-extensions`를 사용합니다. `scenario-*`는 기본 `SCENARIO_IMAGE_LAYOUT=all`에서 control/OSD/RGW/MDS override 네 개를 해제하고 원본 Quay를 사용합니다. 명시적인 `roles` layout은 준비한 역할 이미지 네 개를 보존하며 주요 CI는 이 경로를 사용합니다.

필수 CI의 이름 목록과 검증 상태는 [CI_FIXTURES.md](CI_FIXTURES.md), 구성별 증거는 [CLUSTER_SCENARIOS.md](CLUSTER_SCENARIOS.md)를 따릅니다. 이미지 프로젝트의 검사 결과와 Go 프로젝트의 실행 결과는 각각 기록합니다.

## 공식·Debian·Ubuntu 이미지 matrix

이 조합 목록은 수동 Go 검증의 입력 선택입니다. 자동 Go CI의 matrix로
실행하지 않습니다. `make image-matrix`로 `official`·`debian`·`ubuntu`,
`all`/`roles`, Linux AMD64/ARM64 중 한 조합을 선택해 같은 대표 9개를
실행할 수 있습니다. 모든 조합을 수동으로 반복하면 12개 조합·108회 실행이지만
서로 다른 테스트 이름은 9개입니다. 지정한 platform은 Docker 엔진과 실제
이미지의 native architecture가 일치해야 하며 에뮬레이션 결과로 대신하지 않습니다.

| 계열 | `all` 입력 | `roles` 입력 |
|---|---|---|
| `official` | digest로 고정한 `ceph.DefaultImage`의 원본 Quay 이미지 | GHCR `official-20.2.4-{control,osd,rgw,mds}` |
| `debian` | GHCR `debian-20.2.4-all` | GHCR `debian-20.2.4-{control,osd,rgw,mds}` |
| `ubuntu` | GHCR `ubuntu-20.2.4-all` | GHCR `ubuntu-20.2.4-{control,osd,rgw,mds}` |

GHCR repository는 `ghcr.io/jsyoo5b/ceph-testcontainers-images`입니다. `roles`에서는 `CEPH_TEST_IMAGE`에 control, 각 역할 override에 OSD/RGW/MDS를 지정하고 mirror도 같은 control을 사용합니다. `all`에서는 동일 이미지를 모든 역할에 사용합니다. Architecture를 섞거나 에뮬레이션 실행 결과를 native platform의 성공으로 표시하지 않습니다. 이 matrix는 준비·배포된 이미지를 소비하며 이미지 생성이나 registry publish를 수행하지 않습니다. Registry tag 이름만으로 검증 성공을 주장하지 않고 실제 선택한 image ID·digest·platform과 테스트 결과를 연결합니다.

로컬에서는 `make image-matrix`가 한 조합을 선택해 같은 9개 테스트를 실행합니다. 기본 계열/layout은 `official`/`all`, release는 20.2.4이며 platform을 생략하면 현재 Docker 엔진의 native architecture를 사용합니다. 다음과 같이 선택할 수 있습니다.

```sh
make image-matrix
make image-matrix IMAGE_VARIANT=ubuntu IMAGE_LAYOUT=all

# This requires a native Linux ARM64 Docker engine.
make image-matrix IMAGE_VARIANT=debian IMAGE_LAYOUT=roles \
  IMAGE_PLATFORM=linux/arm64
```

각 실행은 `TestClusterLifecycle`, `TestManagerLifecycle`, `TestRBDLifecycle`, `TestCephFSFilesystem`, `TestRGWS3`, `TestMultiClusterRBDBackup`, `TestMultiClusterRBDSnapshotMirror`, `TestMultiClusterCephFSSnapshotMirrorAndBackup`, `TestMultiClusterRGWMultisite`를 선택합니다. Image checker의 full 11개 시나리오, 수동 Go 호환성 검사의 9개 대표 테스트, 필수 Go 시나리오와 선택적 go-ceph 소비자 검증은 서로 다른 결과입니다. 한 계열·architecture·layout의 PASS를 다른 조합이나 전체 fixture 지원으로 확대하지 않습니다. [CI 구성과 기존 matrix 기록](CI_FIXTURES.md#이미지-호환성-matrix)을 구분하여 확인합니다. 수동 실행과 CI의 역할 이미지 준비는 같은 `run_image_matrix.py`를 사용하며, 준비 기능을 유지하기 위해 자동 12개 matrix를 실행할 필요는 없습니다.

## 주요 시나리오의 역할 이미지 선택

필수 Go CI의 Ceph 시나리오는 Linux AMD64 runner에서 먼저 자체 Docker
resource baseline을 기록하고 공개 `official-20.2.4-{control,osd,rgw,mds}`를 준비합니다. 같은 Ceph
upstream 빌드의 역할 조합으로 현재 fixed runtime 계약을 소비합니다.
이미지를 빌드하거나 계약에 도구를 추가하지 않습니다. Python 준비 단계는
native platform·로컬 immutable image ID·registry digest를 기록하고,
확인된 cache miss에 한해 이미지를 pull합니다. 다운로드 시간과 native
테스트 시간을 구분합니다.

준비 결과의 `scope=image_preparation`, `preparation_passed=true`는
이미지 선택만 확인합니다. `passed=false`, `runtime_result=not_run`을
유지하며 Go/native 테스트의 성공으로 표시하지 않습니다. 네 역할을 모두
검증한 뒤 `CEPH_TEST_*` 네 값만 전달하고, CI는
`SCENARIO_IMAGE_LAYOUT=roles`로 기존 필수 selector를 실행합니다.
필수 시나리오의 parent/child assertion과 cleanup 판정은 유지하며, 자동
12개 호환성 matrix의 실행 범위와는 구분합니다.

로컬 `make scenario-*`는 기본 `SCENARIO_IMAGE_LAYOUT=all`로 기존
원본 Quay를 선택합니다. 준비된 네 역할을 지정하고
`SCENARIO_IMAGE_LAYOUT=roles`를 명시하면 같은 target을 해당 조합으로
실행합니다. 역할 값이 하나라도 빠지면 Go 실행 전에 오류로 종료합니다.
다음 준비 명령 자체는 테스트를 실행하지 않습니다.

```sh
python3 .github/scripts/run_image_matrix.py --variant official --layout roles \
  --prepare-only --output-dir artifacts/role-preparation
```

`summary.json`의 `prepared_environment`에 기록된 네 ID를 환경에 지정한
뒤 `make scenario-topology SCENARIO_IMAGE_LAYOUT=roles`처럼 실행합니다.
`--github-env`는 CI가 지정한 환경 파일에 네 값을 기록하는 선택 옵션이며
`--prepare-only`와 함께만 사용할 수 있습니다.

2026-10-08 로컬 Linux ARM64에서 동일한 MON bridge/host 교체와 RGW
bootstrap/owned S3·OSD 2→3→2 시나리오를 `all → roles → roles → all`
순서로 실행했습니다. 네 실행 모두 PASS했고 자체 cleanup의 새
container/network는 0개였습니다. Native 테스트 시간은 다음과 같습니다.

| 실행 | 원본 Quay all | official roles |
| --- | ---: | ---: |
| 첫 비교 | 356.941초 | 359.021초 |
| 역순 비교 | 373.132초 | 356.386초 |
| 평균 | 365.037초 | 357.704초 |

이 focused 관측에서는 roles가 평균 7.333초(2.0%) 짧아 주요 CI를
전환했습니다. Native 입력 178개는 동일했고 각 실행 중 전체 source도
변경되지 않았습니다. 첫 role 이미지 다운로드·준비 40.751초는 위 테스트
시간에 포함하지 않았습니다. 두 번의 로컬 관측은 모든 시나리오나 다른
runner에서의 시간 단축을 보장하지 않습니다. 원문·프로세스 종료·이미지
identity·source·cleanup을 대조한
[비교 증거](../artifacts/roles-major-comparison-20261008/focused-comparison-verification.json)를
보관하며 전체 필수 CI의 결과는 별도로 확인합니다.

## 추가 소비자 조건

Ceph server 역할 이미지에는 compiler·개발 헤더·go-ceph·kernel mount 도구를 요구하지 않습니다. 애플리케이션 컨테이너는 사용하려는 SDK/runtime을 갖추고 `cluster.WithClient()`로 네트워크와 설정·키를 전달받습니다. 이 옵션은 패키지를 설치하지 않습니다.

Linux go-ceph의 선택 실행 `make goceph-linux`/`make scenario-goceph-linux`에는 호출자가 다음 두 이미지를 로컬 Docker 엔진에 준비합니다.

- `CEPH_TEST_GOCEPH_CLIENT_IMAGE`: Linux native 라이브러리와 실행 가능한 `/usr/local/bin/go-ceph-probe`. Probe 소스와 별도 module은 [goceph/probe](../internal/integration/goceph/probe)에 있으며 현재 go-ceph v0.41.0/tentacle 기준입니다. Idle `sleep infinity`, 설정·키 복사와 probe 실행을 지원해야 합니다.
- `CEPH_TEST_GOCEPH_RUNNER_IMAGE`: 검증할 Go checkout에서 `integration,goceph` tag로 컴파일한 Linux integration test binary를 entrypoint로 실행합니다. Docker socket에 접근하며 host-network native 검증용 probe와 native 라이브러리도 같은 runner에 있어야 합니다.

[run.py](../internal/integration/goceph/run.py)는 두 이미지의 로컬 Linux ID를 고정한 뒤 사용하며 build/pull을 하지 않습니다. Runner에 담긴 소스가 실제 검증 대상인지 확인하는 책임은 호출자에게 있습니다. 이미지 프로젝트가 이 소비자 이미지를 만들거나 SDK 검사를 실행한다는 전제는 없습니다.

RBD encryption의 librbd·libcryptsetup·`cryptsetup` 실행 파일과 RADOS striper client는 control/all의 필수 실행 계약입니다. RBD native consumer는 기본적으로 선택한 control/all을 사용하며 별도 consumer override는 일반 client recipe의 선택 경로입니다. OSD의 `hello`·`lock` class와 그 runtime dependencies는 osd/all 계약입니다. Striper는 libradosstriper와 `lock`을 사용하므로 별도 `cls_striper`를 요구하지 않습니다. 이 구성요소의 위치·기능 계약은 [RBD recipe](RBD_CLIENT_FIXTURES.md)와 [RADOS recipe](CLIENT_FIXTURE_COVERAGE.md)에 기록합니다.

Vault 같은 외부 KMS/backend 서비스와 애플리케이션별 SDK/probe는 해당 fixture의 별도 조건입니다. Ceph 역할 이미지에 Vault 서버나 go-ceph 프로그램·개발 헤더를 요구하지 않습니다. RGW SSE-KMS 시나리오는 `testcontainers-go/modules/vault`로 별도 Vault 컨테이너를 구성하며 `CEPH_TEST_VAULT_IMAGE`로 사내 registry 등 사용할 이미지를 선택할 수 있습니다. Linux go-ceph client/runner와 Vault는 Ceph 역할 이미지 선택과 별도로 준비하는 소비자·외부 서비스 조건입니다.

## 정책 반영 기준

2026-10-05 작업 시작 시 이미지 프로젝트 HEAD `062f80e`와 작업 중 정책을 읽었습니다. 고정한 `docs/IMAGE_REQUIREMENTS.md`의 SHA-256은 `4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`입니다. 입력 파일의 내용·mode·Git 상태는 로컬 `artifacts/image-policy-alignment-20261005/policy-snapshot.json`에 보관합니다. 이 작업은 이미지 프로젝트 파일을 수정하지 않습니다.

2026-10-08 목표 재개 시 이미지 프로젝트 HEAD `2b920d8c462bf4714f08e3061762e39bab452c93`의 정책을 새 입력으로 확인했습니다. `docs/IMAGE_REQUIREMENTS.md`의 SHA-256은 `0e7d125e4a9f1b05c2f61b71d990c9aa199ecff1d469408e6043808424ea9392`입니다. 위 2026-10-05 기록과 그 실행 결과는 당시 정책의 증거로 보존합니다. 새 정책은 control/all의 cryptsetup·암호화·striper와 osd/all의 hello·lock을 명시하며 이미지 checker의 full은 11개 시나리오입니다. 이미지 프로젝트는 읽기 전용으로 참조합니다.

같은 source의 [이미지 배포 run 37735853373](https://github.com/JSYoo5B/ceph-testcontainers-images/actions/runs/37735853373)은 원본 Quay 검증, 세 계열의 AMD64/ARM64 빌드·검사·배포와 배포 digest 재검사까지 모두 성공했습니다. 아래 Go 검증은 그 run의 `promotion.json`에 기록된 ARM64 control/OSD digest를 직접 pull하고, native platform·RepoDigest를 확인한 로컬 immutable ID로 실행했습니다.

| 계열 | Go native test 시간 | RBD bridge/host 단계 | RADOS bridge/host | cleanup |
| --- | ---: | ---: | --- | --- |
| official | 280.807초 | 16/16 PASS | PASS | PASS |
| debian | 288.385초 | 16/16 PASS | PASS | PASS |
| ubuntu | 276.855초 | 16/16 PASS | PASS | PASS |

각 실행은 `TestRBDClientFeatures`와 `TestRADOSClientFixtures` 두 parent를 선택했습니다. 별도 RBD client override 없이 control의 Python native clients를 사용했고, LUKS1/LUKS2 재키잉 후 새 키로 원래 bytes를 읽고 이전 키를 거부했습니다. RADOS는 hello class, 두 client의 watch/notify, snapshot bytes, 세 striper shard·SHA256과 object 정리를 확인했습니다. Go host는 `CGO_ENABLED=0`이고 이미지 빌드는 수행하지 않았습니다.

[실행 결과](../artifacts/image-contract-resume-20261008/native-summary.json), [이미지 계약·배포 입력](../artifacts/image-contract-resume-20261008/policy-snapshot.json)과 [실행 source](../artifacts/image-contract-resume-20261008/runtime-source.json)를 보관합니다. 실행 중 native 입력 269개와 SHA256 `0882f68eefc80e7600a92c11fc94f28259a4fa303da9dceadba158db1e56322d`는 동일했고, 각 실행 후 새 container/network는 0개였습니다. 이 focused ARM64 검증은 당시의 12개 Go matrix나 필수 상세 119개 전체 CI와 별도 증거입니다.

## 로컬 원본 실행 증거

2026-10-05 Docker Desktop Linux ARM64에서 새 Go 소스를 `CGO_ENABLED=0`으로 컴파일한 binary를 준비된 원본 Quay 컨테이너에서 실행했습니다. 대표 9개가 모두 PASS했으며 runner·cleanup을 포함한 실행은 1020.117초였습니다. CephFS mirror socket, required rbd_support 명령을 통한 MGR 승격 후 준비 상태, ControlImage override를 생략한 RGW multisite를 실제 데이터와 함께 확인했습니다. 이미지 빌드·pull은 0회입니다.

초기 `runtime-summary.json`의 테스트 exit code는 0이지만 정상 재접속 유예 중인 Ryuk 1개를 즉시 검사하여 종합 `passed`를 false로 기록했습니다. 원본 기록을 보존하고 유예 후 `post-runtime-audit.json`에서 owned container 0개·추가 network 0개와 실행 중 Go source 변경 0개를 확인했습니다. 증거는 로컬 `artifacts/image-policy-alignment-20261005/`입니다. 이 결과는 `official/all/linux-arm64` 한 조합의 증거이며 Debian·Ubuntu나 12개 CI 전체 완료로 확대하지 않습니다.

추가 `TestMonitorManagerTopology`도 bridge/host 모두 PASS했습니다. Parent test는 394.09초, harness의 Go 실행·cleanup은 394.264초였고 정상 Ryuk 유예 뒤 owned container·추가 network가 모두 0개였습니다. Quorum 상실 시 fresh session의 bounded 거부, 복구·MON 교체·MGR 승격 뒤 retained/fresh RADOS 데이터와 volumes 상태를 확인했습니다. 외부 `timeout`을 제거한 native watchdog 경로의 실제 증거이며 로그는 같은 artifact의 `monitor-watchdog/`입니다.

## 이전 Native CI matrix 실행 증거

아래 결과는 자동 12개 Go matrix를 운영하던 각 source의 실제 실행 기록입니다.
새 CI 정책에서도 원문·이미지 identity·cleanup 증거를 보존하며, 현재 필수 CI의
실행 범위나 새 source의 성공으로 표시하지 않습니다.

### be58018 이전 실행

2026-10-05 source `be58018`의 [CI run 37226924156](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37226924156)에서 아래 12개 조합이 모두 PASS했습니다. 각 조합의 artifact를 내려받아 9개 top-level test가 각각 한 번 완료하고 모든 child의 skip·fail이 없으며 package 결과가 성공인지 확인했습니다. 총 108회 실행이지만 서로 다른 이름은 9개입니다.

| 계열 | all / AMD64 | all / ARM64 | roles / AMD64 | roles / ARM64 |
|---|---|---|---|---|
| 원본 Quay all · 공식 추출 roles | 9/9 PASS | 9/9 PASS | 9/9 PASS | 9/9 PASS |
| Debian | 9/9 PASS | 9/9 PASS | 9/9 PASS | 9/9 PASS |
| Ubuntu | 9/9 PASS | 9/9 PASS | 9/9 PASS | 9/9 PASS |

모든 artifact의 source SHA-256은 `f27b8d232257cb9a9631fa9c538339bcf010f3684b98b9d0022fd0f613e984eb`로 동일했습니다. 선택한 image ID·registry digest·native engine/image platform, Go version, 실제 make 명령과 실행 시간을 함께 보관합니다. 각 구성은 4 CPU·약 16 GiB Linux runner와 Go 1.25.14, Docker 28.0.4를 사용했고 make 실행은 1016.040–1139.225초였습니다. 각 source/bootstrap hash와 실제 이미지 ID에 대한 한 번의 결과이며 다른 tag 내용·release 또는 전체 fixture·go-ceph 지원으로 확대하지 않습니다.

CI artifact 이름은 `image-<variant>-<layout>-<architecture>`입니다. 로컬 원본은 `artifacts/image-policy-alignment-20261005/ci-ready/`, 12개 완료 기록과 source/platform 일치를 검증한 집계는 `ci-matrix-verified.json`에 보관합니다. 이미지 빌드는 모든 조합에서 0회였습니다.

### d9115f4 matrix 검증

2026-10-05 source `d9115f4`의 [CI run 37240162309](https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/37240162309)에서도 같은 12개 native 조합 각각 대표 9개가 한 번씩 RUN/PASS했고 child FAIL/SKIP은 0개, package 결과는 모두 `ok`였습니다. 네 개 component 환경 변수로 control/OSD/RGW/MDS를 선택하고 mirror 연결에는 source 클러스터의 `ControlImage()`를 전달했습니다. Mirror 전용 role 선택을 제거한 control mirror 경로를 모든 12개 조합에서 검증한 결과입니다.

Source manifest SHA-256은 12개 모두 `1486b355cef20286a528ef7ea1e25a6b2fef1954a8eb64c9a18b14953e52baf6`입니다. 각 artifact에 실제 image ID·registry digest·native engine/image platform을 기록했으며 Go 1.25.14, Docker 28.0.4를 사용했습니다. `make image-compatibility` 실행은 1036.757–1086.040초, 이미지 빌드는 모두 0회였습니다.

각 조합의 `runtime-cleanup-image-<variant>-<layout>-<architecture>` artifact도 전후 동일 engine·source를 확인했고 새 container/network가 각각 0개여서 12개 모두 PASS했습니다. 중간 snapshot `artifacts/scenario-fixture-completion-20261005/cleanup-ci-snapshot-20261004T225233-317c6601/audit.json`의 상세 job 진행 상태는 당시 관측 그대로 보존합니다. 이후 같은 run의 terminal SUCCESS와 상세 101개·필수 cleanup 22개 성공은 최종 snapshot `cleanup-ci-snapshot-20261004T235822-469b79e6/audit.json`에 별도 기록했으며 [전체 CI 완료 증거](CI_FIXTURES.md#d9115f4-전체-ci-완료)를 따릅니다. Matrix의 대표 9개 반복은 상세 101개 검증과 구분합니다.
