# 실행 중 PG 재배치 fixture

`Container.SetPoolPGCount`는 기존 pool의 PG 수를 바꾸고,
`Container.SetPoolPlacement`는 replicated pool을 다른 CRUSH root·failure
domain·device class로 옮긴다. `Container.WaitForPoolPGCount`는 native pool이
요청한 PG 수에 도달했는지 확인하는 Check다. Client I/O가 PG split·merge,
remap, backfill 대기 상태와 OSDMap epoch 변경을 견디는지 테스트하는 데
사용한다. 운영용 rebalancing 도구가 아니며 Ceph native Go 라이브러리나 추가
이미지 도구는 필요 없다.

```go
if err := cluster.SetPoolPGCount(ctx, "app", 16); err != nil {
    return err
}
if err := cluster.WaitForPoolPGCount(ctx, "app", 16); err != nil {
    return err
}
if err := cluster.WaitForClean(ctx); err != nil {
    return err
}

// 데이터 이동을 붙잡으려면 backfill과 recovery를 모두 멈춘다.
for _, flag := range []string{"nobackfill", "norecover"} {
    hold, err := cluster.TemporaryOSDFlag(ctx, flag, true)
    if hold != nil {
        defer hold.Restore(cleanupCtx)
    }
    if err != nil {
        return err
    }
}
if err := cluster.SetPoolPlacement(ctx, "app", ceph.PoolPlacement{DeviceClass: "slow"}); err != nil {
    return err
}
// PoolPGs로 새 배치와 recovery 대기 상태를 확인하고 client I/O를 검사한다.
// 이후 flag를 복원하고, 기대한 배치의 active+clean을 PoolPGs로 기다린다.
```

## PG 수 변경

`SetPoolPGCount`는 native `pg_num`과 `pgp_num` target을 차례로 기록한다.
Ceph Tentacle에서 이 명령은 target만 바꾸고, 실제 split과 merge는 active MGR이
`target_max_misplaced_ratio`에 맞춰 단계적으로 진행한다. Merge는 관련 PG가
clean일 때만 진행한다. 그래서 성공 반환은 target 기록만 뜻하며 PG 수 변경이나
데이터 이동 완료를 뜻하지 않는다. MGR이 없으면 target은 대기 상태로 남는다.

입력은 1 이상 65536 이하다. 상한은 native 기본 `mon_max_pool_pg_num`이며 이
fixture가 해당 설정을 암묵적으로 바꾸지 않는다. OSD당 PG 상한 같은 native
거부는 오류로 그대로 반환한다. Autoscale mode가 `on`인 pool은 autoscaler가
요청값을 덮어쓰므로 거부한다. `CreatePool`은 autoscale을 끈 pool을 만든다.

각 명령 직전에 native pool ID를 다시 확인한다. 같은 이름의 pool이 다시
만들어졌으면 변경하지 않는다. 두 명령 중 첫 명령만 성공할 수 있으며 자동
rollback은 없다. 오류 뒤에는 `PoolStatus`로 현재 target을 확인한다.

`PoolState`는 현재 `PGNum`과 함께 `PGNumTarget`, `PGNumPending`,
`PGPlacementNum`, `PGPlacementNumTarget`을 보고한다. Native map에 필드가 없으면
0이다. `PoolPGs`의 `PoolBefore`·`PoolAfter`도 같은 값을 보존한다.

`WaitForPoolPGCount`는 위 네 값과 `PGNum`이 모두 요청값과 같고, MGR이 보고한
해당 pool의 PG 목록도 요청한 개수일 때 반환한다. Merge 중에는 `pg_num`이 target과
같아도 `pg_num_pending`이 남아 있으므로 이 값을 함께 확인한다. OSDMap이 먼저 목표에
도달하고 PG 보고가 늦게 따라오는 동안에는 `WaitForClean`이 이전 PG 목록만 보고
성공할 수 있다. 보고 목록까지 기다리는 이유다. 처음 관측한 pool ID를 기억하고, 대기 중 같은 이름이 다른 pool로
바뀌면 실패한다. PG 수 도달은 clean·backfill 완료·데이터 가시성과 별개다.
이동 완료는 `WaitForClean`, PG 배치는 `PoolPGs`로 확인한다. 대기 시간은
cluster startup timeout과 caller context 중 짧은 쪽이다.

## Placement 변경

`SetPoolPlacement`는 `CreatePool`이 만드는 단순 take/choose/emit rule만
다룬다. `PoolPlacement`의 빈 값은 `PoolConfig`와 같은 기본값(failure domain
`osd`, root `default`, device class 없음)을 사용한다. Size와 min_size는 바꾸지
않으며, native CRUSH map에서 새 placement에 해당하는 소유 domain 수가 pool
size 이상이어야 한다.

Rule은 다음 순서로 고른다.

1. 현재 rule이 요청 placement와 같으면 아무 명령도 보내지 않는다.
2. `CreatePool`이 만든 `tc-<pool>-replicated`가 요청과 같으면 재사용한다.
3. `tc-<pool>-placement-<hash>`가 있고 native step이 요청과 같으면 재사용한다.
   같은 이름이 다른 step을 가지면 거부한다.
4. 없으면 `tc-<pool>-placement-<hash>`를 만든다.

Hash 이름은 8자리 hex로 끝나므로 `CreatePool`의 `-replicated`·`-ec` rule
이름과 겹치지 않는다. 만든 rule은 삭제하지 않으므로 원래 placement로 돌아갈
때는 기존 rule을 다시 사용한다. Rule은 cluster 종료와 함께 사라진다.

성공 반환은 pool이 새 rule을 참조한다는 뜻이다. 데이터 이동은 비동기로
진행한다. 새 OSDMap이 PG 보고에 반영되기 전에는 모든 PG가 이전 배치의
`active+clean`으로 보이므로, 변경 직후의 `WaitForClean`은 이동 전에 성공할 수
있다. 완료를 확인할 때는 `PoolPGs`로 기대한 up/acting과 `active+clean`을 함께
기다린다.

중간 상태를 붙잡을 때는 `TemporaryOSDFlag`로 `nobackfill`과 `norecover`를 함께
켠다. PG log가 전체 이력을 덮을 만큼 데이터가 적으면 Ceph는 backfill 대신
log 기반 recovery로 이동하므로 `nobackfill`만으로는 멈추지 않는다. Ceph 20.2.4에서
확인한 동작은 다음과 같다. 새 OSD가 곧바로 up과 acting이 되고 PG는 대부분
`active+recovery_wait+degraded`로 남는다. `norecover` 중에도 일부 PG가 잠시
`active+recovering`으로 보고될 수 있다. 기존 OSD에만 있는 object를 읽으면 해당
object만 on-demand recovery로 가져와 읽기가 성공하며, 새 쓰기도 성공한다. 두
flag를 복원하면 나머지 object가 이동해 clean이 된다. Backfill이 필요한 큰 PG는
up과 acting이 다른 `backfill_wait` 상태가 될 수 있으므로 특정 up/acting 조합을
일반 계약으로 가정하지 않는다.

EC pool은 rule이 EC profile에 묶여 있어 거부한다. Rule 생성과 pool 변경 직전에
native pool ID를 확인한다. Rule 생성 뒤 pool 변경이 실패하면 새 rule만 남는다.
외부 pool·CRUSH 변경과 동시에 호출하면 안 된다.

## 실행 검증

`TestPoolPGRelocation`은 device class `fast`와 `slow`의 512 MiB OSD 두 개와
replica 1 pool을 사용한다. PG마다 정확히 한 OSD에 있으므로 모든 placement
변경이 실제 이동으로 나타난다. Bridge와 host network 각각 다음 단계를 확인한다.

| 단계 | 조작 | 확인 |
| --- | --- | --- |
| seed | 8 PG pool 생성 | 모든 PG up/acting이 fast OSD, 24개 object 기록·검증 |
| split | `SetPoolPGCount(16)` | native target 기록, `WaitForPoolPGCount`, clean 뒤 16 PG 모두 fast OSD |
| held | `nobackfill`·`norecover` 후 `SetPoolPlacement(slow)` | 같은 pool ID의 새 rule, 16 PG 모두 up/acting이 slow, recovery 대기 PG 존재, 기존 object read와 새 write |
| relocated | 두 flag 복원 | 16 PG 모두 slow OSD의 `active+clean` |
| merge | `SetPoolPGCount(8)` | pending까지 8에 도달, clean 뒤 8 PG 모두 slow OSD |
| returned | `SetPoolPlacement(fast)` 두 번 | 원래 `CreatePool` rule ID 재사용, 반복 호출 no-op, 8 PG 모두 fast OSD의 `active+clean` |

각 단계는 새 object 24개를 쓰고 이전 단계의 object를 모두 다시 읽어 SHA-256
기반 payload를 비교한다. 마지막에 pool ID·size·min_size·PG 수·quota가 처음과
같은지 확인한다. CI에서는 `Ceph short`의 `pool_relocation` batch로 실행한다.

```sh
CGO_ENABLED=0 go test -tags=integration -count=1 -v -timeout=40m -run '^TestPoolPGRelocation$' ./internal/integration
```

2026-10-09 macOS ARM64 Docker Desktop(Linux ARM64 VM, 메모리 4 GiB)에서 기본
digest 고정 Quay Ceph 20.2.4 이미지로 실행한 결과는 bridge/host 각 4개 단계
subtest 모두 PASS, 전체 304.19초였다. Held 단계에서는 두 network 모두 16 PG 중
15개가 `active+recovery_wait+degraded`, 1개가 `active+recovering`이었다. 마지막
단계에서 object 144개를 모두 다시 확인했다. 공식 역할 이미지 조합과 CI 실행은 이
기록에 포함하지 않는다.
