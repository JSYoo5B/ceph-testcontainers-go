# CephFS mirror peer 제거와 replayer 종료 관측

`RemovePeer(ctx, id)`는 소유 peer의 제거 정책을 요청하는 기존 API입니다. `BeginPeerRemoval(ctx, id)`는 제거 전에 현재 소유한 live daemon cohort를 확인하고, 제거 요청과 원래 identity를 보존하는 `cephfs.MirrorPeerRemoval` handle을 반환합니다. `Status(ctx)`와 `WaitDrained(ctx)`로 그 cohort의 종료 처리를 확인할 수 있습니다.

```go
receipt, err := mirror.BeginPeerRemoval(ctx, peerID)
if err != nil {
    // receipt != nil이면 native 요청 결과가 불확실해도 intent는 유지됩니다.
    // 같은 UUID로 새 context에서 BeginPeerRemoval을 다시 호출해 조정합니다.
    return err
}
status, err := receipt.WaitDrained(ctx)
if err != nil {
    return err // 같은 receipt로 새 context에서 관측을 재시도할 수 있습니다.
}
if !status.Drained {
    return fmt.Errorf("peer %s is not drained", status.PeerID)
}
newPeer, err := mirror.RebootstrapPeer(ctx)
```

## 완료 기준

제거 전 source/destination의 원래 cluster FSID, filesystem ID/name·metadata pool ID, 정확한 peer UUID와 destination tuple을 확인합니다. 각 소유 daemon이 정상 실행 중이며 동일한 filesystem watcher·`rados_inst`와 정확한 peer admin command를 제공해야 합니다. Daemon이 0개이거나 중지됐거나 준비되지 않은 경우 native 변경 전에 거부합니다. 이 구성에서는 기존 `RemovePeer`의 정책 요청 계약을 사용할 수 있습니다.

제거 후 아래 관측을 모두 만족해야 `Drained=true`입니다.

1. Source의 MON FSMap과 MGR `peer_list` 양쪽에서 원래 UUID가 없습니다.
2. 모든 원래 daemon의 container ID·process `StartedAt`·filesystem session·watcher identity가 같습니다.
3. Filesystem admin command가 계속 제공되는 정상 catalog에서 정확한 원래 UUID의 peer command가 제거됐습니다.
4. 관측 전후 원래 cluster/filesystem/peer identity와 watcher cohort가 유지됩니다.

Ceph 20.2.4에서 peer map의 삭제는 replayer shutdown보다 먼저 발생합니다. 종료 코드는 worker thread를 join한 뒤 원격 mount를 해제하며, replayer destructor가 peer admin command를 unregister합니다. 이 순서를 바탕으로 command 제거를 같은 live session의 종료 witness로 사용합니다. [FSMirror 구현](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/FSMirror.cc#L439-L455), [PeerReplayer 종료와 command 정리](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/cephfs_mirror/PeerReplayer.cc#L219-L321).

`Drained`는 이 native worker 종료 관측입니다. Snapshot 복제 완료, destination metadata 불변, 원격 lock 해제, config-key/credential cleanup 성공을 보장하지 않습니다. 복제된 snapshot과 원문은 기존 [directory checkpoint API](MIRROR_OBSERVABILITY.md) 및 destination client로 별도 확인합니다.

## 부분 실패와 lifetime

Intent는 native 제거 요청 전에 등록합니다. 응답 유실 시 nonnil handle과 오류를 반환하고 소유권을 유지합니다. 같은 UUID로 `BeginPeerRemoval`을 재시도하면 먼저 정책을 읽고, 이미 삭제됐으면 native 삭제를 반복하지 않습니다. 성공 응답 이후 stale native view가 남아도 삭제를 다시 요청하지 않습니다. MON/MGR 정책 수렴과 원래 daemon의 종료를 `Status`/`WaitDrained`로 기다립니다.

`BeginPeerRemoval`은 최대 2분, `Status`는 최대 30초, `WaitDrained`는 최대 2분이며 caller의 더 짧은 deadline/cancel이 잠금 대기와 native 조회에도 적용됩니다. Wait는 관측 사이에 fixture 잠금을 해제합니다. Query 오류와 deadline은 마지막 report 및 원래 cause를 유지합니다. Schema·identity·generation 변경은 guard 오류로 종료하며 intent를 버리지 않습니다. 공통 MON bootstrap attestation이 context 이외 오류를 반환하면 보수적으로 그 관측을 중단합니다. Quorum 가용성 회복 뒤 같은 handle을 새 context로 다시 관측할 수 있습니다.

진행 중인 explicit receipt가 있으면 새 peer bootstrap, daemon 증설, directory 추가/제거·재분배를 막아 원래 cohort와 정책을 유지합니다. Daemon 제거와 전체 fixture cleanup은 계속 가능합니다. 이 첫 구현은 같은 live session의 witness를 요구하므로 cohort를 중지·재시작·제거하면 완료를 확인하지 못할 수 있습니다. 새 process를 원래 worker의 종료 증거로 채택하지 않습니다. Directory cycle 해제는 별도 [directory receipt](CEPHFS_DIRECTORY_REMOVAL.md)로 확인합니다. 원래 process의 종료·watcher retirement는 선택적 raw observer와 receipt의 [ProcessQuiescence](CEPHFS_PROCESS_QUIESCENCE.md)로 별도 관측할 수 있습니다. 그 read-only 결과는 Drained나 pending gate를 변경하지 않습니다.

완료 뒤 새로운 peer bootstrap은 generation을 진행합니다. 이전 receipt는 원래 UUID의 당시 관측 기록이며 새 peer 상태를 대신하지 않습니다. 이후 상태 조회는 superseded guard로 반환합니다. 외부 native policy·직접 container lifecycle 변경을 receipt 작업과 동시에 실행하지 않아야 합니다.

## 검증

Production API/parser의 13개 unit parent는 peer map 삭제 이후에도 command가 남은 구간, 전체 cohort 종료, 불확실한 삭제 응답, stale MON/MGR view, 원래 UUID/FSID/FS/metadata/process/watchers/schema 변경, busy owner/daemon 잠금, wait 중 잠금 해제, cleanup 및 새로운 peer generation을 검증합니다. Unit·race·vet와 전체 tag compile이 통과했습니다. 취소 관측 테스트의 마지막 수정은 실제 상태를 먼저 읽고 취소하는 방식이며 별도 unit·race 로그를 보존합니다.

`TestMultiClusterCephFSPeerRemovalDrain`은 원본 Quay 20.2.4 Linux ARM64에서 실제 public API로 bridge 234.36초·host 234.64초 PASS입니다. 각 mode에서 2개 live daemon과 4개 directory의 초기 snapshot·원문을 확인하고, 실제 `peer_remove`의 성공 응답을 한 번 유실시킵니다. Nonnil receipt와 원래 오류를 확인하고 같은 UUID의 새 context 재시도로 같은 handle을 받으며 native 삭제는 1회로 유지됩니다. Pending 상태의 rebootstrap은 native 호출 전에 거부됩니다.

`WaitDrained` 뒤 독립 Docker Inspect·admin status/help·RADOS watcher 조회로 원래 container ID·StartedAt·session·watcher와 정확한 old command 부재를 대조합니다. 기존 destination snapshot과 bytes를 유지하고, peer가 없는 동안 만든 snapshot의 부재를 확인한 뒤 새 peer로 전달합니다. 이후 새 snapshot까지 source ID/name 12개씩, 양쪽 총 24개 checkpoint를 검증합니다. 이전 receipt가 새 UUID를 채택하지 않는지도 확인합니다. Peer map 삭제와 command unregister 사이의 짧은 구간은 unit과 upstream 코드 순서로 검증하며, native 실행에서 in-flight teardown 구간을 의도적으로 늘려 관측했다는 뜻은 아닙니다.

증거는 `artifacts/peer-drain-host-ports-20261007/`의 `check.log`, `changed-test-unit.log`, `changed-test-race.log`, `native-runtime.log`, `verification.json`입니다. 같은 suite의 RGW TLS·gateway topology bridge/host 회귀도 통과했고 package terminal은 PASS 710.650초입니다. 같은 engine의 신규 container/network/Ryuk는 0개이며 관련 source·Makefile 217개 SHA256이 실행 전후 동일합니다. `upstream-ordering.json`과 두 fixed-tag source copy는 사용한 코드 순서의 출처·해시를 보존합니다. 필수 selector 106개의 목록은 확인했지만 전체 CI·다른 이미지 계열의 새 전체 PASS로 합산하지 않습니다.

현재 필수 실행 경로는 `make scenario-cephfs-removal`입니다. 원래 daemon을 모두 명시적으로 제거한 뒤 중단된 제거를 별도 승인하는 방법은 [복구 승인 계약](CEPHFS_PROCESS_ACKNOWLEDGMENT.md)을 따릅니다. 기존 `Drained`·`Released` 및 위 실행 증거의 source 범위는 유지합니다.
