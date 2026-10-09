# 테스트 선택과 build tag

태그 없이 `go test ./...`를 실행하면 Docker가 필요 없는 단위 테스트를
실행한다. 통합 테스트를 선택하려면 `all`이나 기존 capability tag를 명시한다.
`all`은 일반적으로 지원하는 전체 runtime suite를 한 번에 선택하는 로컬
진입점이다. 테스트마다 `testing.Short()`나 category별 `t.Skip`을 붙이지 않고,
선택하지 않은 Test 함수는 컴파일 대상에서 제외한다.

```sh
# 호스트 단위 테스트
make test

# 단위·race·vet 및 통합 테스트의 컴파일 확인; Docker 실행 없음
make check

# 일반 전체 suite: 단위 테스트와 Docker runtime 시나리오
make test-all
```

`test-all`은 `CGO_ENABLED=0 go test -mod=readonly -tags=all -count=1 -v
-timeout=180m ./...`를 실행한다. Docker와 실행할 Ceph 이미지가 필요하며,
`CEPH_TEST_IMAGE`, `CEPH_TEST_OSD_IMAGE`, `CEPH_TEST_RGW_IMAGE`,
`CEPH_TEST_MDS_IMAGE`로 준비된 이미지를 선택할 수 있다. 이미지 생성이나
패키지 설치는 수행하지 않는다. `all` build tag와 Ceph의 `all` 역할 이미지는
서로 다른 개념이다.

전체 suite는 오래 걸릴 수 있다. `ALL_TEST_TIMEOUT=6h make test-all`처럼
process 예산을 조정하거나 아래 runner로 필요한 batch만 실행할 수 있다.

## PR에 표시되는 workflow 종류

자동 검증은 다음 workflow로 나눈다. PR의 Checks와 Actions 목록에서 실패한
검사 유형을 먼저 확인할 수 있다. 각 workflow는 main의 `push`·`pull_request`·
수동 실행으로 시작하며 다른 유형의 완료를 기다리지 않는다. PR 브랜치의
`push`에서는 자동 실행하지 않아 같은 변경의 push/PR suite 중복을 막는다.
PR 업데이트는 이전 CI를 취소하고 최신 커밋으로 검사한다. 각 자동 workflow는
workflow 이름·event로 concurrency group을 나누고, PR은 PR 번호, push는
ref를 사용한다. 같은 PR·같은 유형의 이전 실행과 같은 main ref·같은 유형의
이전 push 실행을 취소해 최신 커밋을 검사한다. 수동 실행은 run ID를 사용해
독립적으로 진행하며, reusable template에는 중복 concurrency 설정을 넣지 않는다.

Stacked PR을 사용할 때는 바로 앞 PR의 head 브랜치를 새 PR의 base로 지정한다. PR 번호가
다르면 concurrency group도 달라 부모 PR의 검증이 유지된다. 브랜치 이름이
달라서가 아니라 PR 번호가 취소 범위를 나누기 때문이다. 새 PR을 열 때 부모의
CI를 취소하지 않고, 같은 PR을 갱신할 때만 이전 실행을 취소한다.
[GitHub concurrency 규칙](https://docs.github.com/en/actions/using-jobs/using-concurrency)을 따른다.

부모 PR merge 후 자식의 base를 바꾸거나 부모 브랜치가 갱신됐을 때는 자식
head를 rebase하고 push해 새 `synchronize` 검증을 시작한다. 현재 기본
`pull_request` 활동은 `opened`, `synchronize`, `reopened`이므로 base 변경만으로
새 검증이 실행된다고 가정하지 않는다. 제목·본문 수정도 받는 `edited`를
추가하면 CI 취소 범위가 늘어나므로 사용하지 않는다.
[GitHub PR 이벤트](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request)를 참고한다.

| Workflow 이름 | 파일 | PR 검사 범위 |
| --- | --- | --- |
| `Code checks` | [code.yml](../.github/workflows/code.yml) | 독립 `unit`, `race`, `static`, `tag-coverage` job |
| `Docker checks` | [docker.yml](../.github/workflows/docker.yml) | Ceph 없이 Docker bridge SDK 환경 검증 |
| `Ceph short` | [ceph-short.yml](../.github/workflows/ceph-short.yml) | 기본 기동·서비스·관련 fixture |
| `Ceph topology` | [ceph-topology.yml](../.github/workflows/ceph-topology.yml) | 클러스터 구성·노드 lifecycle·진단 |
| `Ceph multicluster` | [ceph-multicluster.yml](../.github/workflows/ceph-multicluster.yml) | 독립 cluster와 zone·peer 연결 |
| `Ceph recovery` | [ceph-recovery.yml](../.github/workflows/ceph-recovery.yml) | 장애·단절·제거 후 복구 관측 |

`Code checks`의 `tag-coverage`가 전체 source/compiled inventory와 Docker 없는
helper 검사를 확인한다. `static`은 vet·tag 컴파일과 CI harness 검사를 수행한다.
Runtime workflow의 planner는 전체 source tag에서 자기 category의 batch만 선택하므로
각 workflow가 전체 컴파일 검사를 반복하거나 다른 category를 실행하지 않는다.

Runtime 실행은 [tagged-runtime.yml](../.github/workflows/tagged-runtime.yml)의
`workflow_call`을 공유한다. 이 파일은 별도 `push`·PR 이벤트를 받지 않으며
실제 compile·Docker 준비·native·cleanup은 각 batch의 같은 runner에서 진행한다.
같은 batch를 다시 실행하는 이미지 matrix도 추가하지 않는다.

[Native regressions](../.github/workflows/native-regressions.yml)는 별도 수동
workflow다. `rgw_image` 입력으로 준비된 RGW 이미지 하나만 바꿀 수 있으며
기본 PR 검증에 포함하지 않는다. Linux go-ceph 소비자 검사는 기존 수동
harness와 이미지 입력 계약을 유지한다.

표시 이름의 각 단계는 역할을 나눈다. Workflow는 검사 유형, reusable 호출은
`Scenarios`, 실제 job은 `Plan` 또는 batch 이름을 사용한다. 같은 유형을 호출과
job 이름에 다시 붙이지 않는다. 예를 들어 `Ceph multicluster / Scenarios /
mirror_initial_daemons`에서 시나리오를 바로 찾을 수 있다. 정확한 표시는 GitHub의
화면에 따라 workflow 이름과 check 이름을 함께 읽는다.

예를 들어 `Code checks`의 `Race detector` 실패는 race 검사의 결과이고,
`Ceph recovery`에서 `Native` 실패는 해당 복구 시나리오의 assertion 또는
완료 gate 실패다. `Environment`나 `Cleanup` 실패도 별도로 표시한다.
유형과 실제 실패 단계는 탐색 기준이며, Ceph timeout이 무조건 실행환경
결함이라는 의미는 아니다. 아래의 로그와 receipt로 원인을 확인한다.

## CI category와 batch

| Category | Build tag | 검사 범위 |
| --- | --- | --- |
| `code` | `ci_code` | Docker 없는 signer·fixture helper 검사 |
| `environment` | `ci_environment` | Ceph 없는 Docker bridge SDK 검증 |
| `short` | `ci_short` | 기본 기동·서비스와 관련 fixture 묶음 |
| `topology` | `ci_topology` | 클러스터 구성·노드 lifecycle·진단 |
| `multicluster` | `ci_multicluster` | 독립 cluster와 zone·peer 연결 |
| `recovery` | `ci_recovery` | 장애·단절·제거 후 복구와 완료 관측 |

`short`는 suite의 선택 유형이며 Go의 `-short` 옵션이나 정해진 실행시간
상한을 뜻하지 않는다. 각 batch의 process/job 시간 예산은 소스에 기록한다.
긴 독립 fixture는 같은 category 안의 별도 batch로 실행한다. 기존 native
phase·negative window·데이터·완료 조건은 유지한다.

소스에서 현재 목록을 생성하고, 실제 컴파일 결과와 비교할 수 있다.
이 두 명령은 Docker를 실행하지 않는다.

```sh
python3 .github/scripts/tag_scenarios.py plan \
  --output artifacts/tag-plan.json
python3 .github/scripts/tag_scenarios.py plan --verify \
  --output artifacts/tag-plan-verified.json
```

생성된 목록의 `category`, `batch`, `package`로 하나를 실행한다.
`--directory`는 결과를 새로 보존할 경로이며 기존 디렉터리를 덮어쓰지 않는다.

```sh
python3 .github/scripts/tag_scenarios.py run \
  --category short --batch default --package ./internal/integration \
  --directory artifacts/local-short-default
```

Runner는 실제 파일의 build expression과 Test 함수 목록에서 tag 조합을
만들고 `go test -list` 결과를 검증한다. 실행 명령에는 테스트 이름의 `-run`,
`-short`, category 환경 변수나 이름 필터를 넣지 않는다. 별도의 untagged
단위 테스트가 같은 package에 있으면 함께 실행할 수 있다.

Tag runner의 AST 수집·compiled inventory·compile·실행은 모두
`CGO_ENABLED=0`, `GOWORK=off`, `GOFLAGS=-mod=readonly`로 진행한다.
환경 변수나 persisted `GOENV`의 overlay·추가 tag·이름 필터와 외부 workspace
replacement가 기록한 소스와 다른 코드를 실행하지 않도록 이 세 값을 고정한다.
Cache·proxy·Docker 연결과 준비된 role 이미지 설정은 상속하며, compile/native
receipt에는 고정한 세 값만 기록한다. 이는 tag runner의 실행 계약이며 직접
`go test`나 수동 Make target의 환경을 바꾸지 않는다. 실행 도중의 소스 변경이나
Go toolchain 자체의 무결성을 검증하는 계약은 아니다.

`all`은 build expression의 우선 선택이므로 `all,ci,ci_short`를 category
필터로 사용할 수 없다. Category/batch 선택에는 위의 planner/runner를
사용한다. 기존 `make scenario-*`와 이름 기반 `-run` 명령은 개별 장애를
조사하는 수동 경로로 유지한다.

## 테스트 추가

일반 wrapper 파일은 다음 형태를 사용한다.

```go
//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))
//ci: timeout=20m job-timeout=35

package integration_test

import "testing"

func TestAnotherFixture(t *testing.T) {
    // 기존 fixture API로 준비하고 실제 결과를 확인한다.
}
```

같은 파일에 Test 함수를 추가하면 기존 category/batch에 자동으로 포함된다.
새 파일은 적절한 category와 `ci_batch_<이름>`, 시간 예산을 지정한다. 기존
batch에 합칠 때는 그 batch의 시간 예산을 공유한다. 새 batch도 source
metadata에서 발견하므로 workflow의 matrix나 테스트 이름 목록을 수정하지
않는다. `//ci:`는 package 선언 앞에 파일당 한 번만 기록한다.

공용 helper·타입·상수는 `all || <기존 capability 조건>`의 별도 helper 파일에
둔다. 서로 다른 category가 같은 helper를 사용할 수 있으며 Test 함수의
컴파일 선택과 구분한다. Pure helper 검사는 `ci_code`로 분류한다.

CephFS process quiescence/recovery와 RBD receiver 검사는 원래 parent와
`bridge|host`, `peer|directory` ancestry를 유지한 CI leaf wrapper로 분리한다.
각 leaf가 원래 helper 전체를 호출하고 기존 strict completion checker를
실행한다. Category aggregate와 `all`의 로컬 전체 parent도 유지한다.
미분류·누락·중복 category/batch와 실제 컴파일 목록의 불일치는 planner의
검증에서 실패한다.

## 명시적으로 선택하는 native/SDK 검사

`all`만으로는 알려진 upstream native regression이나 Linux go-ceph SDK
소비자 검사를 선택하지 않는다. 추가 build tag는 각각 `native_regression`,
`goceph`이며 선택한 검사는 skip 없이 실행한다.

```sh
# 알려진 native regression만 source-owned optional batch로 조사
python3 .github/scripts/tag_scenarios.py optional \
  --batch native_shuffle --package ./internal/integration \
  --directory artifacts/local-native-shuffle
python3 .github/scripts/tag_scenarios.py optional \
  --batch native_rgw_translation --package ./internal/integration \
  --directory artifacts/local-native-rgw

# SDK/native opt-in까지 컴파일 확인; Docker 실행 없음
make tag-compile
```

RGW의 일반 `all`/CI dispatch는 `tag_owner_class`와
`tenant_system_user_isolation`을 실행한다. Optional CI dispatch는
`priority_tags_owner_class`와 `ordinary_user_denial_grant`를 실행한다.
기존 capability tag의 수동 전체 parent와 `all,native_regression`의 로컬
전체 parent는 네 child를 모두 실행한다. 원래 parent 이름과 child의
assertion은 유지하며 알려진 실패를 지원 범위의 PASS로 표시하지 않는다.

Linux go-ceph는 준비된 client/runner 이미지가 필요한 별도 소비자 계약이다.
`make goceph-linux`와 [실행 조건](IMAGE_COMPATIBILITY.md)을 따른다. 해당 SDK의
native linking은 Linux 안에서 수행하며 Go 모듈 자체에 go-ceph/cgo 의존성을
추가하지 않는다.

## 실패 단계와 증거

Runtime CI는 각 실제 runner에서 다음 단계를 분리하고, 각 step의 실제 outcome을
job summary와 실패 annotation에 기록한다. Workflow 이름은 검사 유형을,
runtime job 이름은 source-owned batch를 표시한다. Summary와 annotation에는
해당 유형과 profile 식별자를 함께 기록한다.

1. **Compile**: 소스에서 tag 소유 관계를 확인하고 선택한 suite를 컴파일한다.
2. **Environment**: Docker engine과 owned resource baseline을 기록하고,
   Ceph가 필요한 batch는 배포된 roles 이미지를 해석·준비한다.
3. **Native**: Test assertion, parent/package 완료와 필요한 strict checker를 확인한다.
4. **Cleanup**: baseline이 성공한 경우 native 실패 뒤에도 같은 engine의
   owned container·network·named volume 부재를 확인한다.

Compile 실패와 이미지/Docker 준비 실패를 native assertion 실패와 별도로
찾을 수 있다. Native의 assertion/completion 실패도 기록하지만 category나
실패 단계만으로 코드 결함과 환경 결함의 근본 원인을 확정하지 않는다.
`compile.log`, `compile-report.json`, `native.log`, `report.json`과 별도 이미지·
cleanup receipt를 함께 확인한다. 분류와 자동 선택을 바꾼 것만으로 CI 시간
단축이나 새 runtime 성공을 주장하지 않으며 [기존 실행 기록](CI_FIXTURES.md)은
각 source의 역사적 증거로 유지한다.

실제 컴파일된 Test 이름과 소스 목록의 비교는 `plan --verify`와 native 실행
직전의 admission에서 수행한다. Compile 단계는 `go test -c`로 확인하며
테스트를 실행하지 않는다.

과거 CephFS 완료 검사 로그는 `.github/scripts/fixtures/`의 원본 소스
snapshot과 당시 provenance 해시에 연결한다. 현재 wrapper/helper로 나눈
assertion의 보존은 별도로 비교하며, 과거 로그를 현재 소스의 새 실행 결과로
표시하지 않는다.
