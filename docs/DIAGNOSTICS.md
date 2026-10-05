# 클러스터 진단 snapshot

`cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{})`는 클러스터 상태와 daemon 로그를 JSON으로 저장할 수 있는 `*ceph.DiagnosticsReport`를 반환합니다. 부트스트랩 중 일부만 생성된 클러스터, 정지한 daemon이나 종료된 클러스터도 진단 대상으로 사용할 수 있습니다. 수집은 조회만 수행하며 daemon을 시작·정지하거나 cluster 상태·리소스 소유권을 변경하지 않습니다.

이 API는 명시적으로 호출합니다. `Run`이 실패했을 때 자동으로 수집하거나 파일을 저장하지 않습니다. 반환된 `cluster`가 있다면 cleanup 전에 수집하고, 이미 취소된 테스트 context 대신 별도의 짧은 background context를 사용합니다. 종료 후에도 요청할 수 있지만 삭제된 container/network의 조회는 해당 artifact의 오류로 남습니다.

## JSON 저장

이 예시는 호출자가 준비한 `io.Writer`에 snapshot을 저장합니다. Writer를 닫거나 출력 파일을 선택하는 책임은 호출자에게 있습니다. 수집 오류가 있어도 반환된 report를 먼저 저장하므로 일부 daemon의 실패 때문에 다른 artifact를 잃지 않습니다.

```go
func writeCephDiagnostics(cluster *ceph.Container, output io.Writer) error {
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()

    report, collectErr := cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{})
    if report == nil {
        return collectErr
    }
    data, encodeErr := json.MarshalIndent(report, "", "  ")
    if encodeErr != nil {
        return errors.Join(collectErr, encodeErr)
    }
    _, writeErr := output.Write(append(data, '\n'))
    return errors.Join(collectErr, writeErr)
}
```

위 함수는 `context`, `encoding/json`, `errors`, `io`, `time` 및 `github.com/jsyoo5b/ceph-testcontainers-go/ceph`를 사용합니다. Report의 `SchemaVersion`은 1이며 `StartedAt`, `FinishedAt`, `HostNetwork`, `Closed`, `Complete`와 `Artifacts`를 포함합니다. `Closed`는 종료 상태를 나타내며 수집 호출이 클러스터를 종료했다는 의미가 아닙니다.

각 `DiagnosticArtifact`는 `Kind`, `Name`, `Role`, `ContainerID`, `Data`, `Truncated`, `Error`를 갖습니다. `Data`는 문자열이며 잘리지 않은 inspect 결과는 JSON text입니다. JSON field 이름은 `schema_version`, `started_at`, `container_id`처럼 snake case를 사용합니다. `Error`는 해당 조회의 실패를 나타내고 다른 artifact는 계속 보존합니다. 일부 조회가 실패하거나 deadline에 도달하면 report와 aggregate error를 함께 반환합니다. 출력이 한도를 넘으면 `Truncated: true`, report의 `Complete: false`로 표시하며 잘림만으로 aggregate error를 만들지는 않습니다. 조회들은 하나의 원자적 시점에 실행되는 것이 아니므로 수집 중 외부에서 구성 변경이 있었다면 관측 시점도 서로 다를 수 있습니다.

## 수집 범위와 한도

| Kind | 조회 범위 |
|---|---|
| `inventory` | 수집 목록 구성·Docker client·container 수 제한 등의 오류 |
| `container-inspect` | 대상 container의 선택된 실행 metadata |
| `container-logs` | 대상 container의 최근 로그 |
| `network-inspect` | 클러스터 네트워크의 선택된 metadata |
| `ceph` | 고정된 Ceph 상태 명령의 결과 |

Ceph query 이름은 `version`, `status`, `health`, `pgs`, `quorum`, `monmap`, `mgrmap`, `osdmap`, `fsmap`입니다. 임의 `Exec` 명령을 추가하는 기능은 제공하지 않습니다. Container의 `Env`, `Cmd`, `Mounts`, `Labels`나 Ceph 설정 파일·keyring을 수집하지 않으며 auth 조회도 실행하지 않습니다. 공개 모듈의 다른 제어 API와 마찬가지로 go-ceph/cgo에 의존하지 않습니다.

| `DiagnosticsConfig` | 기본값 | 의미 |
|---|---|---|
| `Timeout` | 1분 | 수집 전체의 제한 시간; 호출자 context의 더 짧은 deadline도 적용 |
| `OperationTimeout` | 10초 | 개별 조회의 제한 시간 |
| `MaxOutputBytes` | 64 KiB | 각 artifact의 `Data`, `Error` 각각에 적용하는 최대 크기 |
| `LogTail` | 1000줄 | container 로그의 tail 범위 |
| `Concurrency` | 4 | 동시에 실행하는 수집 worker 수 |
| `MaxContainers` | 128 | 중복 ID를 제외한 container 수의 상한; 초과 대상은 누락 오류로 기록 |
| `AdditionalContainers` | 없음 | 호출자가 소유한 mirror/client 등 추가 container |
| `RedactValues` | 없음 | 추가로 마스킹할 문자열 |

0 값은 기본값을 선택합니다. 전체 timeout은 최대 5분, 개별 timeout은 최대 1분, 출력은 최대 1 MiB, log tail은 최대 10000줄, worker는 최대 8개, container는 최대 128개입니다. 추가 container는 최대 64개, redaction 문자열은 최대 128개이며 각 문자열은 최대 4096 bytes입니다. 잘못된 한도는 수집 전에 오류로 거부하고 report를 반환하지 않습니다.

출력 제한은 artifact마다 적용합니다. 전체 JSON 파일이 64 KiB 이하라는 보장은 아니며 metadata와 여러 artifact의 합계가 포함됩니다. `Complete`와 각 artifact의 `Error`·`Truncated`를 함께 확인해야 snapshot의 빠진 범위를 판단할 수 있습니다.

## Mirror와 client 추가

클러스터가 직접 소유하지 않는 mirror와 애플리케이션 client는 호출자가 명시적으로 추가합니다. 추가 대상의 로그·상태를 조회할 수 있지만 cleanup 소유권을 넘기거나 cluster에 편입하지 않습니다.

```go
report, err := cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{
    Timeout:          30 * time.Second,
    OperationTimeout: 5 * time.Second,
    MaxOutputBytes:   16 << 10,
    AdditionalContainers: []ceph.DiagnosticsContainer{
        {Role: "rbd-mirror", Name: "receiver", Container: mirrorDaemon},
        {Role: "client", Name: "application", Container: applicationClient},
    },
    RedactValues: []string{applicationToken},
})
// err가 있어도 report가 반환되면 JSON으로 보존합니다.
```

클러스터가 알고 있는 admin/RGW 비밀 값과 추가 `RedactValues`, 이름이 붙은 secret pattern은 artifact data와 오류에서 마스킹합니다. 임의 애플리케이션 로그의 알려지지 않은 opaque secret까지 자동으로 식별하지는 않습니다. 그런 값은 `RedactValues`로 지정하고 JSON bundle을 공유하기 전에 내용을 확인합니다.

선택 검증 target은 `make scenario-diagnostics`이며 `integration,diagnostics` 태그의 `TestClusterDiagnostics`, `TestPartialClusterDiagnostics`를 실행합니다. 기존 [상세 시나리오 CI](CI_FIXTURES.md#d9115f4-전체-ci-완료)의 101개 완료 기록과 별도 범위입니다. 수집에 성공했다는 사실만으로 클러스터의 health나 client I/O가 정상이라고 판정하지 않습니다.

2026-10-05 로컬 검증은 digest로 고정한 원본 Quay `v20.2.4`와 Docker Desktop의 Linux ARM64 engine에서 host networking을 켠 상태로 실행했습니다. 상위 테스트 2개와 하위 테스트 10개가 모두 PASS했으며 package 실행 시간은 194.933초입니다. Bridge와 host 각각에서 healthy, 정지한 RGW, 삭제된 추가 client, 만료된 deadline을 확인했습니다. Healthy 수집은 `MaxOutputBytes: 256 << 10`으로 daemon/client container 7개와 Ceph query 9개, bridge의 public/backend network 및 host의 공유 network metadata를 보존했습니다. 초기 MON이 exit 17로 종료된 실제 bootstrap 실패에서도 inspect·로그·network와 CLI 오류를 report에 남기고 대체 container를 만들거나 MON을 시작하지 않았습니다.

수집 전후 Docker/native identity, OSD membership, 설정과 auth capability 및 RADOS/CephFS의 각 32 KiB 데이터를 비교해 변동이 없음을 확인했습니다. 삭제된 추가 client의 조회 오류는 정상 대상의 artifact를 잃게 하지 않았고 cleanup 검사에서 새 container와 network는 각각 0개였습니다. 로컬 원문은 [실행 로그](../artifacts/diagnostics-20261005/native-final.log), cleanup [before](../artifacts/diagnostics-20261005/cleanup-final/before.json)·[after](../artifacts/diagnostics-20261005/cleanup-final/after.json)에 남겼습니다. 현재 진단 runtime 검증 범위는 이 Quay/Linux ARM64 조합입니다.

`make check`의 전체 unit·race·vet·태그 컴파일도 PASS했습니다. 설치된 Python으로 큰 stdout/stderr를 출력 상한 이후에도 끝까지 읽는 회귀와 deadline에 조회 자식 process group이 종료되는 회귀를 실행했습니다. 전용 control container 선택, 비밀 값 마스킹, UTF-8 출력 한도, typed-nil container와 부분 오류 보존도 unit에서 확인했습니다. Runtime 검증 시작·종료 시 API·통합 테스트·Makefile의 SHA-256이 일치했으며 [source 기록](../artifacts/diagnostics-20261005/source-final.json)과 [검사 로그](../artifacts/diagnostics-20261005/make-check-final.log)를 보존했습니다. 이 artifact 파일들은 로컬 검증 자료이며 Git에 포함하지 않습니다.
