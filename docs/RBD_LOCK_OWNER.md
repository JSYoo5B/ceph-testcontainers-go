# RBD image client와 exclusive lock 관측

`rbd.ImageClients`는 image header를 watch하는 client와 image의 lock을 읽는다.
`ceph.Container.PauseContainer`, `TemporaryBlocklist`와 함께 쓰면 응답하지 않는 RBD
client가 쥔 exclusive lock이 다른 client에게 넘어가는 경로를 재현할 수 있다.

- 멈춘 owner의 watch가 만료될 때까지 다른 client의 쓰기가 기다리는 상황
- lock을 넘겨받은 client가 이전 owner를 blocklist하는 동작
- 멈춘 owner를 직접 blocklist해서 기다림 없이 lock을 넘기는 fencing
- 되살아난 이전 owner가 더는 쓰지 못하는 동작

```go
status, err := rbd.ImageClients(ctx, cluster, "pool", "", "disk")
if err != nil {
    return err
}
owner := status.ExclusiveOwner() // 아무도 lock을 쥐지 않았으면 nil
pause, err := cluster.PauseContainer(ctx, client)
// ...
fence, err := cluster.TemporaryBlocklist(ctx, owner.Address, 10*time.Minute)
// 다른 client가 lock을 넘겨받는다. 이전 owner를 Resume하고 그 client가
// errno 108을 받은 뒤에 fence.Restore를 호출한다.
```

## 계약

`ImageClients`는 `rbd status`와 `rbd lock ls`를 차례로 읽는다. fixture 밖의 client도
포함하며, 읽기만 하고 소유권을 주지 않는다. namespace가 빈 문자열이면 기본
namespace다. pool, namespace, image 이름은 문자, 숫자, `_`, `.`, `-`만 받는다.

두 목록은 따로 읽으므로 lock이 넘어가는 순간에는 새 owner의 lock은 보이는데 watch는
아직 없거나, 그 반대일 수 있다. 안정된 결과가 필요하면 polling한다.

`Watcher.Address`와 `Lock.Address`는 `IP:port/nonce` 형식이다. `BlocklistEntries`와
같은 표기이고 `TemporaryBlocklist`에 그대로 넘길 수 있다. `Lock.Managed`는 librbd가
exclusive-lock 기능으로 잡은 lock(`auto <watch cookie>`)이고, 나머지는 `rbd lock add`로
만든 advisory lock이다. `ExclusiveOwner`는 managed lock을 반환한다.

## Ceph에서 확인한 동작

Ceph 20.2.4에서 owner client 컨테이너를 멈추고 다른 client가 같은 image에 썼다.

| 경로 | 관측 |
| --- | --- |
| 멈추기 전 | managed lock 하나, locker가 owner의 `client.<ID>`, owner 주소의 watcher 하나 |
| watch 만료 | 다른 client의 쓰기가 28초 정도 기다린 뒤 성공함. `osd_client_watch_timeout`(기본 30초)이 지나 멈춘 owner의 watch가 사라지자 lock을 깨고 넘겨받음 |
| 넘겨받은 뒤 | 새 client가 owner가 되고, 이전 owner의 watch는 없고, 이전 owner 주소가 blocklist에 있음 |
| fencing | 멈춘 owner를 `TemporaryBlocklist`로 막자 watch 만료를 기다리지 않고 다른 client가 0.7초 만에 lock을 넘겨받음 |
| Resume 뒤 이전 owner | 쓰기가 errno 108(ESHUTDOWN)로 실패함 |

fencing 항목은 이전 owner가 blocklist를 확인할 때까지 유지해야 한다. exclusive lock은
협조형이라서, 막힘이 풀린 이전 owner가 쓰기를 시도하면 현재 owner에게 lock을 요청하고
현재 owner는 이를 넘겨준다. 실제로 이전 owner를 Resume하기 전에 `Restore`했을 때,
bridge network에서 이전 owner의 쓰기가 성공했다. 그래서 테스트는 이전 owner가
errno 108을 받은 뒤에 `Restore`한다. lock을 깨면서 librbd가 넣는 blocklist 항목은
fixture 소유가 아니므로 Ceph가 정한 만료 시간(기본 1시간)까지 남는다.

## 실행 검증

`TestRBDPausedLockOwner`는 bridge와 host network 각각에서 watch 만료 경로와 fencing
경로를 이어서 확인한다. 마지막 owner는 그대로 쓸 수 있는지도 확인한다. CI에서는
short 범주의 `rbd_fixtures_locks` batch로 Tentacle과 Squid에서 실행한다.
