# Scrub 불일치 fixture

`Container.InjectObjectDataError`는 replicated pool의 object 하나에 대해 지정한
OSD의 replica를 읽을 수 없는 상태로 만든다. `Container.DeepScrubPG`는 deep
scrub을 요청하고 완료를 기다리며, `Container.RepairPG`는 repair를 요청하고
불일치가 사라질 때까지 기다린다. `Container.PGInconsistencies`는 마지막 deep
scrub이 찾은 불일치 object를 읽는 Check다. 모니터링 도구의 `OSD_SCRUB_ERRORS`·
`PG_DAMAGED` 처리, 불일치 PG에서의 client 동작, repair 자동화를 테스트하는 데
쓴다. 디스크를 직접 손상시키지 않으며 OSD를 멈출 필요도 없다.

```go
if err := cluster.InjectObjectDataError(ctx, "app", "victim", replicaOSD); err != nil {
    return err
}
if err := cluster.DeepScrubPG(ctx, pgid); err != nil {
    return err
}
objects, err := cluster.PGInconsistencies(ctx, pgid)
// objects[0].UnionShardErrors == []string{"read_error"}
// HealthDetails에는 OSD_SCRUB_ERRORS와 PG_DAMAGED가 보고된다.
if err := cluster.RepairPG(ctx, pgid); err != nil {
    return err
}
```

## 주입

Native `injectdataerr` debug 명령을 사용한다. 이 명령은 BlueStore의
`bluestore_debug_inject_read_err`가 켜져 있어야 효과가 있다. 메서드는 대상 OSD
daemon에만 runtime 설정으로 이 값을 켠다. Central config에 저장하지 않으므로
OSD가 재시작하면 꺼진다. 이 설정은 명시적으로 주입한 object에만 영향을 준다.

대상 OSD는 이 cluster가 소유한 OSD여야 하고, `ceph osd map`이 보고한 object의
현재 acting set에 있어야 한다. 그렇지 않으면 명령을 보내지 않고 거부한다. EC
pool은 shard 지정이 필요하므로 지원하지 않는다. Object 이름은 CLI 인자로
전달하므로 비어 있거나, NUL을 포함하거나, `-`로 시작하는 이름을 거부한다.

주입은 OSD 메모리에 남는다. Repair가 replica를 다시 쓰거나 OSD가 재시작하면
사라진다. Ceph 20.2.4에서 확인한 동작은 다음과 같다.

- Replica에 주입하면 primary가 서비스하는 client read는 영향을 받지 않는다.
  Deep scrub이 해당 shard의 `read_error`를 보고하고 PG는
  `active+clean+inconsistent`가 된다.
- Repair는 primary의 사본으로 replica를 다시 쓴다. 이후 deep scrub은 불일치를
  보고하지 않는다.
- Primary에 주입한 object를 client가 읽어도 읽기는 성공하고, 이후 deep scrub도
  불일치를 보고하지 않는다. 이 검증은 결과만 확인했으며 primary가 어떤 경로로
  복구했는지는 관측하지 않았다. Primary 손상 상태를 scrub 결과로 남기는 용도로는
  replica 주입을 사용한다.

## Scrub 대기

`DeepScrubPG`와 `RepairPG`는 요청 전 PG의 `last_deep_scrub_stamp`를 기록하고,
더 새로운 stamp가 보고되고 PG 상태에 `scrubbing`·`repair`가 없을 때 반환한다.
`RepairPG`는 이때도 PG가 `inconsistent`이면 오류를 반환한다. 권위 있는 사본이
없는 object는 repair로 고칠 수 없기 때문이다. `noscrub`·`nodeep-scrub` flag나
primary 부재로 scrub이 시작되지 않으면 cluster startup timeout과 caller context
중 짧은 쪽에서 실패한다.

`PGInconsistencies`는 새 scrub을 시작하지 않는다. 마지막 deep scrub 결과를
`rados list-inconsistent-obj`로 읽는다. 빈 결과는 그 scrub이 불일치를 찾지
못했다는 뜻이다. Shard 번호는 replicated pool에서 -1이고, `Primary`는 native
보고의 primary 표시를 그대로 보존한다.

## 실행 검증

`TestScrubInconsistency`는 OSD 두 개와 PG 하나, replica 2 pool을 사용해 모든
object가 같은 primary와 replica를 갖게 한다. Bridge와 host network 각각 다음을
확인한다.

| 단계 | 조작 | 확인 |
| --- | --- | --- |
| guard | 소유하지 않은 OSD에 주입 | 거부 |
| injected | replica에 `victim` 주입 | client의 object 3개 read 성공 |
| scrubbed | `DeepScrubPG` | `active+clean+inconsistent`, `victim` 하나의 `read_error`가 replica shard에만 있음, primary 표시, `OSD_SCRUB_ERRORS`·`PG_DAMAGED`, client read 성공 |
| repaired | `RepairPG`, 다시 `DeepScrubPG` | 두 health code 해제, 불일치 없음, client read 성공 |
| primary | primary에 `primary-victim` 주입 후 client read, `DeepScrubPG` | read 성공, 불일치 없음 |

CI에서는 `Ceph recovery`의 `scrub_inconsistency` batch로 실행한다.

```sh
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=30m -run '^TestScrubInconsistency$' ./internal/integration
```

2026-10-09 macOS ARM64 Docker Desktop(Linux ARM64 VM, 메모리 4 GiB)에서 기본
digest 고정 Quay Ceph 20.2.4 이미지로 실행한 결과는 bridge/host 모두 PASS, 전체
111.34초였다. 두 network 모두 replica OSD 0의 shard에만 `read_error`가 보고됐고
primary OSD 1의 shard는 오류가 없었다. Health는 `HEALTH_ERR`에서 repair 뒤
`HEALTH_OK`로 돌아왔다. 공식 역할 이미지 조합과 CI 실행은 이 기록에 포함하지
않는다.
