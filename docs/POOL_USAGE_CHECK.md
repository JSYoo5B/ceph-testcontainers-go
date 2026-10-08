# Pool 사용량 Check

`cluster.PoolUsage(ctx, "test-data")`는 control container의 `ceph df detail`
JSON에서 한 pool의 사용량을 읽는다. Go 쪽 Ceph SDK나 cgo가 필요하지 않다.
결과는 원본 bootstrap FSID와 조회 전후의 native pool ID/name이 일치해야
성공한다. 통계 미보고, 필수 field 누락, ambiguous identity, command 실패나
호출자 취소는 zero snapshot과 error를 반환한다. error와 함께 온 값을
성공한 assertion 입력으로 쓰지 않는다.

필수 JSON field 이름은 native spelling과 정확히 일치해야 한다. 중복 field와
대소문자 또는 Unicode case folding으로 같은 필드로 해석되는 별칭을 거부한다.
결과에 영향을 주지 않는 알 수 없는 추가 field는 허용한다.

```go
usage, err := cluster.PoolUsage(ctx, "test-data")
if err != nil {
    t.Fatal(err)
}
if usage.Objects < 1 {
    t.Fatal("expected at least one reported object")
}
t.Logf("pool=%d logical=%d allocated=%d used=%.2f%%",
    usage.ID, usage.StoredBytes, usage.AllocatedBytes, usage.UsedRatio*100)
```

모든 size field 단위는 bytes이고 `UsedRatio`는 0..1 비율이다.
`StoredBytes`는 논리 DATA+OMAP 추정량, `AllocatedBytes`는 replica/EC·allocation을
포함한 OSD 할당량이며 BlueStore database는 포함하지 않는다.
`MaxAvailableBytes`는 논리 쓰기 가능량의 추정치다.

MON이 받는 PG 통계는 비동기다. 위 assertion은 보고된 통계를 확인하며,
객체 쓰기 직후에 통계가 갱신되었음을 보장하지 않는다. 갱신을 기다리는
테스트는 deadline이 있는 context로 직접 polling한다. 통계 row가 없는
pool을 사용량 0으로 해석하지 않는다. compression·OMAP·allocation granularity
등으로 인해 allocated bytes가 stored bytes×replica와 일치한다고 가정하지
않는다. 외부 pool/config writer는 이 조회와 동시에 실행하지 않는다.

근거: [Ceph Tentacle usage 설명](https://docs.ceph.com/en/tentacle/rados/operations/monitoring/#checking-a-cluster-s-usage-stats),
[v20.2.4 native formatter](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/PGMap.cc#L714-L950).

원본 v20.2.4 이미지의 Linux ARM64에서 기존 `TestRBDLifecycle`과
`TestHostNetworkRBDLifecycle`이 109.955초에 모두 통과했다. 각 경로에서
8MiB nonzero import 뒤와 OSD 2→3→2 뒤에 원래 FSID/pool ID,
최소 8MiB logical bytes·8개 객체와 양수 allocated bytes를 확인했다.
기존 전체 bytes·snapshot·clone·flatten·삭제 검사도 유지했다.
실행 중 저장소 입력 320개의 manifest가 동일했고 자체 cleanup은 새
container/network 0개였다. 이 focused 결과는 다른 이미지·플랫폼이나
전체 CI 성공을 의미하지 않는다.
