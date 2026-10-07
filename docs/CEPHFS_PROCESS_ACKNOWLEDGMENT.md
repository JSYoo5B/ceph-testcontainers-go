# 중단된 CephFS 제거의 명시적 승인

Peer/directory receipt의 `AcknowledgeProcessQuiescence(ctx)`는 원래 daemon들을 명시적으로 제거한 뒤 중단된 제거를 별도 승인 상태로 승인합니다. 새 native 제거 명령이나 자동 cleanup을 실행하지 않습니다. `Drained`·`Released`는 원래 live 세션에서 정상 teardown/cycle 해제를 확인한 결과로 유지합니다.

## 사용 순서

제거 전에 `OriginalProcessClientFactory`를 설정하고 strict Begin으로 receipt를 확보해야 합니다. 준비 방법과 endpoint의 신뢰 조건은 [원래 process 관측 계약](CEPHFS_PROCESS_QUIESCENCE.md)을 따릅니다.

```go
receipt, err := mirror.BeginPeerRemoval(ctx, peerID)
if receipt == nil {
    return err
}
// 오류와 함께 받은 receipt도 보존한다. 원래 cohort가 살아 있으면
// fresh Begin 재시도로 원래 native 정책을 다시 확인할 수 있다.
if err != nil {
    receipt, err = mirror.BeginPeerRemoval(retryCtx, peerID)
    if err != nil {
        return err
    }
}
for _, daemon := range mirror.Daemons() {
    if err := mirror.RemoveDaemon(ctx, daemon.DaemonName); err != nil {
        return err
    }
}
accepted, err := receipt.AcknowledgeProcessQuiescence(retryCtx)
if err != nil {
    // Fresh Observation과 오류를 읽고 같은 receipt를 보존한다.
    return err
}
if !accepted.Acknowledged {
    return errors.New("original removal was not acknowledged")
}
// 기존 RebootstrapPeer·AddDaemon으로 새 peer/run을 구성한다.
```

Directory receipt도 같은 승인 메서드를 제공합니다. 승인 뒤 `AddDirectory`는 명시적인 새 path generation을 생성하고 `AddDaemon`으로 새 daemon을 구성할 수 있습니다. Source/destination filesystem과 기존 snapshot 데이터는 별도로 유지합니다.

## 승인 조건

같은 fixture 잠금을 유지하며 최대 30초의 새 관측을 수행하고, 다음 조건을 모두 확인한 뒤 local 상태를 기록합니다.

- 원래 receipt의 cluster FSID·filesystem·metadata pool·peer/path generation이 유지된다.
- 원래 peer는 MON/MGR 양쪽에서 없거나, directory receipt의 원래 peer tuple은 유지하면서 exact path 정책이 없다.
- 현재 소유 daemon inventory가 비어 있고 모든 원래 daemon의 명시적 제거가 기록됐다.
- 각 원래 retained engine/full CID를 새로 조회해 `container-removed`를 확인하고 전체 native watcher set에서 원래 GID가 없다.
- 마지막 policy·owner·context 확인이 성공한다.

정상 Stop이나 나중에 시작된 run은 원래 task 종료의 관측이지만 승인에는 충분하지 않습니다. `ProcessQuiescence`의 read-only 성공만으로 gate를 해제하지 않습니다. 직접 `daemon.Terminate`만 호출한 경우에는 `RemoveDaemon`으로 fixture inventory도 조정해야 합니다. Capability가 없던 receipt에는 새 endpoint를 붙여 증거를 만들어내지 않습니다.

원래 cohort를 보존한 nonnil receipt라면 삭제 응답을 잃은 상태에서도 승인 조건을 새로 확인할 수 있습니다. 예시의 Begin 재시도가 승인에 필수인 것은 아닙니다.

승인은 원래 삭제 응답을 잃어 남은 local peer ID 또는 선택 directory의 desired 상태를 조정하고, 별도 승인 상태로 overlap gate를 엽니다. Native graceful 완료 상태와 CLI ACK 상태는 바꾸지 않습니다. 새 peer/bootstrap/path 추가는 기존 API의 정책·응답 유실·generation 규칙을 따릅니다.

## 재시도와 새 generation

반복 승인은 매번 전체 증거를 새로 읽습니다. 결과의 `Acknowledged`는 이번 호출이 승인됐는지 나타내며, `Observation`은 이번 관측입니다. 반복 관측이 실패해도 과거의 승인 결정은 취소하지 않습니다. 이미 정상 `Drained`·`Released`로 완료한 receipt에는 대체 승인을 적용하지 않습니다.

같은 generation의 `BeginPeerRemoval`/`BeginDirectoryRemoval` 및 기존 Remove 재시도는 원래 native identity·정책을 다시 확인하고 같은 receipt/no-op를 유지합니다. Native 삭제나 container cleanup을 반복하지 않습니다. Rebootstrap으로 새 peer를 채택하거나 같은 path를 re-add하면 이전 receipt의 authority는 guard로 무효화됩니다. 불확실한 re-add도 native 호출 전에 이전 receipt를 supersede하고, 새 소유권·generation은 성공 응답 뒤에만 기록합니다.

이 기능은 정상 thread join·remote unlock 성공·전체 fixture 침묵이나 snapshot 완료를 주장하지 않습니다. Engine incarnation, checkpoint restore, 경합하는 외부 policy/container 변경은 기존 observer 계약 밖입니다. 복구 뒤의 실제 destination 데이터는 새 source checkpoint와 client I/O로 별도 검증합니다.

## 검증

13개 새 unit parent는 원래 inventory와 removed-CID 승인 조건, 별도 terminal 상태·fresh 반복 증거·실패 뒤 과거 결정 보존, 정책·engine·identity·generation 변화, 원래 graceful 완료 거부, pending overlap과 held owner/member/raw gate의 deadline을 검사합니다. 원래 observer와 함께 수행한 focused race 검사도 통과했습니다.

실제 적용 소스의 전체 `make check`(unit·race·vet·전체 tag compile), 독립 코드 검토와 CI selector 109개의 실제 Go-list 확인이 통과했습니다. 새 필수 `make scenario-cephfs-removal`의 정확한 build tag에서도 네 parent가 컴파일·선택됐습니다.

원본 고정 Quay Ceph 20.2.4 digest `6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`, Linux ARM64에서 `TestMultiClusterCephFSOriginalProcessQuiescenceRecovery`의 독립 cluster pair 네 개를 순서대로 실행했습니다.

| Network | Receipt | 결과 |
| --- | --- | --- |
| bridge | peer | PASS 340.01초 |
| bridge | directory | PASS 366.07초 |
| host | peer | PASS 338.88초 |
| host | directory | PASS 364.62초 |

각 케이스는 실제 native 제거 성공의 출력을 끝까지 소비한 뒤 응답을 한 번 유실시킵니다. 같은 nonnil receipt를 보존하고 Begin 재시도 없이 정상 Stop→Start→RemoveDaemon을 진행합니다. Stop/later-run 승인 거부와 replacement 추가 뒤 재승인 거부는 호출당 native CLI 0회입니다. Empty inventory에서 최초·반복 승인은 각각 원래 CID/GID·engine과 native 정책·watcher를 새로 확인합니다. 같은 generation의 Begin 및 기존 Remove 재시도 두 번에서도 원래 제거 명령은 총 1회로 유지됐습니다. Drained·Released는 false로 남았습니다.

그 뒤 케이스당 새 CID/GID의 daemon을 추가했습니다. Peer receipt는 새 peer UUID로 rebootstrap하고 directory receipt는 원래 peer UUID에서 같은 경로를 re-add합니다. 이전 receipt는 새 generation을 채택하지 않고 추가 CLI 없이 거부됐습니다. Stateful factory/customizer는 원래·교체 daemon마다 각각 한 번 호출됐습니다.

Source/destination의 positive byte/hash 기록 56개는 기존 frozen snapshot과 backlog·새 checkpoint의 실제 원문입니다. Public checkpoint 관측은 16회이며, 케이스당 초기 snapshot을 두 번 읽으므로 독립 source ID/name 기록은 12개입니다. Snapshot ID는 각 source 범위입니다. 원래 task/watcher 종료 뒤 만든 backlog의 미도착을 Stop/later-run/removed의 12개 구간에서 각각 10초 이상 반복 조회했고, 복구 뒤 backlog와 새 snapshot이 destination에 도착했습니다. 기존 frozen snapshot 원문도 유지됐습니다. 실제 raw Docker oracle와 native 정책·watcher·filesystem/metadata pool 조회는 고정된 테스트 소스의 assertion으로 대조합니다.

`artifacts/cephfs-process-quiescence-acknowledgment-20261007/`에 원본 로그·독립 검토·manifest·verifier contract·검증 결과를 보존합니다. Package terminal은 PASS 1409.874초이며 source·Makefile·CI workflow 231개 SHA256이 전후 동일합니다. 동일 engine의 신규 container/network/Ryuk는 0개이고 고정 이미지 정책 SHA256 `4682819b3174a632b9da32eb955f9c52593a24112f780f64280380f0e6ccee62`를 유지했습니다. 엄격한 원본 검증은 7개 named RUN/PASS와 56 byte·12 absence·4 lost-reply·20 approval·8 no-replay·4 recovery 기록을 확인했습니다. 이번 focused 실행은 새 전체 CI나 공식·Debian·Ubuntu matrix 전체 PASS를 뜻하지 않습니다.
