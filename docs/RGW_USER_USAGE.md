# RGW 사용자 저장량 Check

`gateway.UserUsage(ctx, user)`는 이 gateway를 통해 생성한 `*rgw.User`의 현재 native 저장량을 읽습니다. `UserInfo`의 identity/quota policy와 별개입니다.

```go
usage, err := gateway.UserUsage(ctx, user)
if err != nil {
    return err
}
// usage.Scope / OwnerID로 집계 범위를 먼저 확인한다.
// usage.SizeBytes, SizeActualBytes, NumObjects로 caller의 assertion을 작성한다.
```

일반 사용자와 tenant 사용자는 `Scope == "user"`, `OwnerID == UserID`입니다. account root는 `Scope == "account"`, `OwnerID == account.ID()`이며 **계정 전체의 합계**입니다. root 사용자 한 명이 쓴 데이터만의 합계로 해석하면 안 됩니다. native `user stats`는 user의 account ID가 있으면 그 account를 통계 owner로 선택합니다. [Ceph v20.2.4 user stats](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/radosgw-admin/radosgw-admin.cc#L9563-L9649)

`SizeBytes`는 native `size`, `SizeActualBytes`는 반올림 accounting 값인 `size_actual`, `NumObjects`는 `num_objects`입니다. replica를 포함한 실제 디스크 할당량이나 요금은 아닙니다. `LastStatsSync`와 `LastStatsUpdate`는 native timestamp 문자열을 보존하며, zero timestamp는 `0.000000`일 수 있습니다. [storage counter 출력](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_common.cc#L3164-L3177), [timestamp 출력](https://github.com/ceph/ceph/blob/v20.2.4/src/common/ceph_json.cc#L577-L580), [utime 형식](https://github.com/ceph/ceph/blob/v20.2.4/src/include/utime.h#L247-L275)

조회는 기존 owner/gateway gate와 caller context에 묶입니다. 조회 전후에 user credentials, native type, tenant, account lifetime, runtime scope를 다시 확인하며, identity가 바뀌거나 마지막 context 검사가 실패하면 counters를 공개하지 않습니다. missing/null/duplicate 필드와 uint64 범위를 벗어난 값, malformed timestamp를 거부합니다. 오류에 credentials 또는 native output을 포함하지 않습니다.

native 통계는 비동기적으로 갱신됩니다. 이 Check는 `--sync-stats`, reset, flush 또는 quota cache 변경을 실행하지 않습니다. 테스트 준비에서 명시적으로 sync를 수행할 수 있지만 그것은 별도의 fixture operation입니다. 통계는 object bytes 전달, quota enforcement, 날짜 범위별 request/bandwidth 사용량을 증명하지 않습니다. [Ceph quota statistics](https://docs.ceph.com/en/tentacle/radosgw/admin/#update-quota-stats)

이번 API는 `UserUsage` 하나입니다. 별도의 account/bucket Check와 polling API는 포함하지 않습니다. 후보의 unit/race/vet/tag compile은 native 실행 증거를 대신하지 않습니다.

원본 v20.2.4 이미지의 Linux ARM64에서 기존 `TestRGWS3`,
`TestRGWTenantsAndAccounts`, `TestHostNetworkRGWTenantsAndAccounts`가
212.020초에 모두 통과했습니다. 일반 사용자는 실제 S3 쓰기·OSD 교체·삭제에
따라 `0→4개/393216 bytes→5개/491520 bytes→0`으로 조회했습니다.
Account의 bridge/host 두 경로에서는 한 root가 쓴 1개 객체·47 bytes를
아무 객체도 쓰지 않은 다른 root에서도 같은 account 합계로 확인했습니다.
테스트 준비의 명시적 native sync와 이 API의 읽기 전용 조회는 별도입니다.
기존 bytes·권한·quota·정리 검사를 유지했고, 저장소 입력 323개가 실행 중
동일했으며 자체 cleanup은 새 container/network 0개였습니다.
이 결과를 다른 이미지·플랫폼이나 전체 CI의 새 성공으로 합산하지 않습니다.
