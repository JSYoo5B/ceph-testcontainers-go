# CephFS 디렉터리 제거와 release 관찰

`BeginDirectoryRemoval`은 fixture가 등록한 한 경로의 제거 의도와 원래 살아
있던 daemon 집합을 보관한다. `Status`와 `WaitReleased`는 해당 경로가 그
원래 replayer 모두에서 sync cycle 등록을 해제했는지 확인한다. 제거한 경로의 기존 snapshot과 데이터는 유지하며 peer와 daemon도 그대로 둔다.

## 사용법

```go
receipt, err := mirror.BeginDirectoryRemoval(ctx, "/data")
if err != nil {
    if receipt == nil {
        return err // 사전 검증 실패: 제거 의도가 등록되지 않았다.
    }
    // 응답 유실 등으로 요청 결과가 불확실하다.
    // 새 context로 같은 경로의 BeginDirectoryRemoval을 재시도한다.
    receipt, err = mirror.BeginDirectoryRemoval(retryCtx, "/data")
    if err != nil {
        return err // receipt를 보관하고 이후 다시 관찰/재시도할 수 있다.
    }
}

status, err := receipt.Status(observeCtx)
if err != nil {
    return err // status에는 그때까지 확인한 daemon별 결과가 남는다.
}
if !status.Released {
    status, err = receipt.WaitReleased(waitCtx)
    if err != nil {
        return err
    }
}
```

`BeginDirectoryRemoval`과 `WaitReleased`는 최대 2분, 한 번의 `Status`는
최대 30초로 제한된다. 더 짧은 호출자 deadline과 취소가 우선한다.
`WaitReleased`는 관찰 사이에 fixture/member lock을 놓고 다시 확인한다.
취소나 deadline에서는 마지막 부분 결과와 호출자 원인, 마지막 관찰 오류를
보존하며 `Released`는 `false`다.

## 보관된 의도와 재시도

native remove 전에 receipt를 fixture에 저장한다. 따라서 remove가 적용된
뒤 응답만 유실되어도 non-nil receipt가 남는다. 같은 경로의 Begin 재시도는
동일한 receipt를 돌려준다. native 정책에서 경로가 이미 사라졌다면 remove를
다시 보내지 않는다. 성공 응답을 받은 뒤 MGR의 경로 목록이 아직 남아 있어도
정책 수렴을 기다린다.

응답이 불확실하고 경로 정책이 여전히 존재할 때만, 원래 mapped owner와
원래 cohort를 다시 검증한 뒤 같은 경로에 remove를 재시도할 수 있다.
mapping 전환 중에는 보내지 않고 기다리며, owner가 바뀌면 거부한다.
불확실한 전송 결과에 대한 CLI 호출 횟수의 exactly-once 보장은 없다.
pending receipt가 있는 같은 경로의 `RemoveDirectory`도 이 재시도 경로를
사용한다. `Status`와 `WaitReleased`는 native 제거 요청을 보내지 않는다.

## 엄격한 원래 세션 계약

첫 Begin은 fixture의 private 소유 경로와 등록 generation, 양쪽 원래 cluster
FSID/filesystem ID/metadata pool, 원래 peer UUID와 목적지 tuple을 확인한다.
경로는 안정된 mapped owner를 가져야 하며 그 owner의 peer stats에 실제로
존재해야 한다. 모든 소유 daemon이 정상 실행 중이어야 하고 각 daemon의
ContainerID, StartedAt, filesystem session, watcher 주소/instance ID, 정확한
peer status 명령을 확보해야 한다. 0개, 정지한 daemon 또는 불완전한 cohort는
native 제거 전에 거부한다.

`Released=true`는 다음 관찰이 모두 성공했을 때만 반환한다.

- 경로 정책이 관찰 전후 모두 없다(`PolicyRemoved=true`).
- 원래 peer가 MON/MGR 정책과 각 정상 filesystem session에 계속 존재한다.
- 원래 watcher 집합과 모든 daemon의 container/process/session이 일치한다.
- 각 원래 peer의 전체 stats 객체가 유효하며 선택 경로가 모두 없다.
- 각 원래 peer status 명령이 stats 관찰 전후 계속 등록되어 있다.

daemon별 `State`, `Problem`, `ContainerID`, `InstanceID`는 부분 결과로 남는다.
query 실패는 release 증거가 아니며 새 context로 재시도할 수 있다.
schema나 원래 identity가 맞지 않는 관찰은 guard 오류로 끝난다.
정지·재시작·교체·퇴역한 cohort를 새로운 원래 cohort로 받아들이지 않는다.
이 API와 관찰 중 외부 native 정책, filesystem, container 변경을 경합시키면
안 된다. generation은 fixture 내부 등록 이력이며 외부 path/peer ABA를
식별하는 보장은 없다.

## pending 작업과 재등록

release가 완료되기 전에는 `AddDirectory`, `AddDaemon`, `RebalanceDirectories`,
peer 제거/Begin, `RebootstrapPeer`, 다른 경로의 Begin/Remove를 거부한다.
`RemoveDaemon`과 `Terminate`로 cleanup할 수 있지만, 그러면 원래 살아 있는
세션의 release 증명을 더 이상 얻지 못할 수 있다. receipt 자체는 cluster나
container를 소유하거나 cleanup하지 않는다.

완료 뒤 같은 경로의 `AddDirectory` 시도는 native add 전에 이전 receipt를
무효화한다. add가 적용되고 응답이 유실되어도 이전 release 증거를 새 등록에
재사용할 수 없다. constructor, public Add, rebalance의 성공한 add 응답만
등록 generation을 증가시킨다. 불확실한 add는 새 소유권이나 generation을
확정하지 않는다. 성공한 재등록의 다음 Begin은 새 receipt를 만든다.

별도 receipt 없이 사용하는 기존 `RemoveDirectory`는 정책 제거 요청의
계약을 유지한다. daemon이 없거나 정지한 구성에서도 사용할 수 있으며,
성공 응답만으로 release를 증명하지 않는다.

## release 증거의 범위

고정된 [Ceph v20.2.4 `remove_directory`](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L333-L349)는
scheduling 목록에서 경로를 제거한다. 등록된 cycle이 있으면 cancel 표시와
stats를 남기고, 없으면 stats를 제거한다.
[worker의 unregister 호출](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L2087-L2101) 이후
[등록 해제와 stats 삭제](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L395-L405)가
진행되며, [peer status](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L2110-L2147)는
같은 lock 아래 stats를 읽는다.
[FSMirror의 경로 release](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/FSMirror.cc#L393-L406)는
이 제거를 각 peer replayer에 전달한다. 이 순서가 정상 원래 세션에서 경로
stats 부재를 cycle unregister 증거로 해석하는 근거다.

`Released`는 remote flock unlock 성공, worker thread 종료/join, process
quiescence, snapshot checkpoint 완료, 데이터·metadata 불변성을 증명하지
않는다. 특히 [remote unlock 실패](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L462-L477)는
오류를 기록하고 반환하므로 뒤의 등록/stats 삭제만으로 unlock 성공을 알 수
없다. checkpoint와 실제 파일 내용은 별도의 snapshot 관찰과 I/O로 검증해야
한다. 원래 process의 종료·watcher retirement는 선택적 raw observer를 제공한
receipt의 [ProcessQuiescence](CEPHFS_PROCESS_QUIESCENCE.md)로 별도 관측한다.
그 결과는 Released나 pending gate를 변경하지 않는다.

## 검증

14개 unit parent는 tracked `idle`/`failed`/`syncing` entry, 응답 유실·stale 정책·전환 중 mapping, 전체 stats schema, 원래 identity/cohort 변경, 잠금 deadline, poll 사이 잠금 반환, 부분 report/cause, overlap, 성공·불확실한 re-add와 generation을 검증합니다. 실제 native 조회 뒤 취소하는 causal 테스트와 같은 fixture의 새 context 재시도도 포함합니다. `make check`의 unit·race·vet·전체 tag compile과 독립 코드 검토가 통과했습니다.

`TestMultiClusterCephFSDirectoryRemovalRelease`는 고정 원본 Quay Ceph 20.2.4 digest `6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`, Linux ARM64에서 bridge 259.47초·host 253.83초 PASS입니다. 두 독립 cluster에 2개 mirror daemon·4개 directory를 구성하고 실제 native remove 성공 응답을 한 번 유실시켰습니다. Public Begin은 nonnil receipt와 원래 오류를 반환했고 같은 경로의 새 context 재시도 및 legacy retry에서 동일 receipt·native 삭제 1회를 유지했습니다. Pending rebalance와 peer 제거는 추가 native 호출 없이 거부됐습니다.

Public `WaitReleased` 결과를 별도 Docker Inspect·native status/help·RADOS watcher 조회와 대조해 두 원래 container ID·StartedAt·session·watcher 및 peer tuple을 유지하며 모든 원래 peer stats에서 선택 경로가 없음을 확인했습니다. 제거한 경로의 새 snapshot은 10초 동안 5회씩 미도착했고, 다른 세 경로는 각각 35,072 bytes와 SHA256을 전달했습니다. 같은 경로를 다시 등록하자 이전 receipt는 guard로 무효화되고 backlog 및 새 snapshot이 실제 bytes와 함께 도착했습니다. 기존 snapshot도 보존됐습니다.

각 mode의 source client에서 독립적으로 읽은 snapshot ID/name은 12개, 양쪽 총 24개입니다. 재등록 전후 같은 pending checkpoint를 다시 확인하므로 observer 성공 기록은 15회씩 총 30회이며 이를 30개의 독립 checkpoint나 native I/O로 합산하지 않습니다. Native test는 제거 직전 active sync cycle을 의도적으로 오래 유지하지 않습니다. In-flight 취소의 코드 순서는 fixed-tag source와 unit의 tracked entry로 확인하며 native timing proof로 표시하지 않습니다.

`artifacts/cephfs-directory-removal-20261007/`의 `check.log`, `native-runtime.log`, `verification.json`, `independent-review.md`에 증거를 보존합니다. Package terminal은 PASS 513.762초이고 동일 engine의 신규 container/network/Ryuk는 0개입니다. 관련 source·Makefile 222개 SHA256이 실행 전후 동일하며 고정 이미지 정책 SHA256은 `4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`입니다. `scenario-multicluster-topology`에 새 parent를 추가한 해당 source의 필수 selector는 107개로 확인했습니다. 이 focused 실행을 전체 CI 또는 다른 이미지 계열의 새 전체 PASS로 표시하지 않습니다.

현재 필수 실행 경로는 `make scenario-cephfs-removal`입니다. 원래 daemon을 모두 명시적으로 제거한 뒤 중단된 제거를 별도 승인하는 방법은 [복구 승인 계약](CEPHFS_PROCESS_ACKNOWLEDGMENT.md)을 따릅니다. 기존 `Drained`·`Released` 및 위 실행 증거의 source 범위는 유지합니다.
