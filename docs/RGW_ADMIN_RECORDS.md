# Usage log와 bucket rate-limit fixture

Usage logging은 기본적으로 꺼져 있으므로 consumer가 usage CRUD를 구현해도 server 준비가 필요합니다. Bucket rate-limit은 quota와 다른 server 조건입니다. 기존 `Run` → `TemporaryConfig` → `StartRGWWithConfig` → `CreateUser` → `UserInfo`/`Admin`을 조합하며, protocol CRUD wrapper나 go-ceph 의존성을 추가하지 않습니다. 실행 가능한 recipe는 [TestRGWAdminRecordsAndRateLimit](../internal/integration/rgw_admin_records_integration_test.go)이며 bridge/host를 각각 실행합니다.

Ceph 20.2.4 slim control/OSD/RGW image로 실제 Docker 검증을 통과했습니다. Bridge 51.70초·host 51.41초, 전체 103.11초 동안 실제 producer usage bytes/counters, read/write capability 거부, scoped UID trim과 다른 principal 보존, bucket별 `503 SlowDown`, 원래 policy 복원과 bytes 회복 및 owned cleanup을 확인했습니다. [Native 실행 기록](../artifacts/rgw-sync-admin-protocol-rbd-final.log), [G11 완료 기준](CLIENT_FIXTURE_COVERAGE.md). 이 결과는 아래 single-gateway 범위의 증거입니다.

## 시작 전 usage logging 준비

현재 RGW daemon identity가 `client.admin`이므로 gateway를 시작하기 전에 다음 exact section 설정을 적용합니다.

```go
settings := []ceph.ConfigSetting{
    {Section: "client.admin", Name: "rgw_enable_usage_log", Value: "true"},
    {Section: "client.admin", Name: "rgw_usage_log_tick_interval", Value: "1"},
    {Section: "client.admin", Name: "rgw_usage_log_flush_threshold", Value: "1"},
}
for _, setting := range settings {
    change, err := cluster.TemporaryConfig(ctx, setting)
    if change != nil {
        t.Cleanup(func() {
            cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
            defer cancel()
            if err := change.Restore(cleanup); err != nil { t.Error(err) }
        })
    }
    if err != nil { t.Fatal(err) }
}
gateway, err := cluster.StartRGWWithConfig(ctx, ceph.RGWConfig{SkipUserCreation: true})
if err != nil { t.Fatal(err) }
```

`TemporaryConfig`의 stored-entry readback만으로 logging 적용을 판정하지 않습니다. Recipe는 별도의 ordinary user 두 명으로 S3 PUT/GET을 실행하고 native usage category `put_obj`·`get_obj`의 successful operation 및 받은/보낸 payload bytes가 실제로 나타나야 합니다. Flush interval과 threshold를 줄여도 persistence는 비동기이므로 90초 child context로 기다립니다. [Ceph usage 옵션](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/rgw.yaml.in), [AdminOps usage 계약](https://docs.ceph.com/en/tentacle/radosgw/adminops/#get-usage).

## AdminOps capability와 exact UID trim

Consumer의 HTTP AdminOps는 S3 SigV4 credential을 사용합니다. Public fixture는 key identity를 캡처한 fresh ordinary user를 만들며, global `admin`이나 multisite `system` flag를 켜지 않습니다.

```go
operator, err := gateway.CreateUser(ctx, ceph.RGWUserConfig{
    ID: "fresh-usage-operator", AdminCaps: "usage=read;ratelimit=read",
})
```

| HTTP 동작 | Native capability | Recipe의 증거 |
| --- | --- | --- |
| `GET /admin/usage?uid=OWNED&show-entries=true&show-summary=true&format=json` | `usage=read` | actual owner categories/bytes; caps 없는 별도 유효 user는 403 `AccessDenied` |
| `DELETE /admin/usage?uid=OWNED&remove-all=false&format=json` | `usage=write` | read-only operator의 403 이후 write bit 추가; exact UID entries/summary가 사라지고 다른 principal counters 유지 |
| `GET /admin/ratelimit?bucket=OWNED&ratelimit-scope=bucket&format=json` | `ratelimit=read` | exact current native policy; caps 없는 user는 403 |
| 같은 ratelimit endpoint에 `POST` | `ratelimit=write` | read-only operator와 caps 없는 user의 403; write bit 추가 후 native 정책 및 실제 admission 변경 |

Caps는 operation 유형 권한이며 UID/bucket별 ACL이 아닙니다. Resource 범위는 consumer가 query와 native identity 확인으로 제한합니다. Recipe는 `UserInfo`로 여전히 owned key인지, global admin/system flag가 없는지, 정확히 두 capability type의 bit가 기대값인지 확인한 뒤 `Admin("caps", "add", "--uid", operator.ID(), "--caps", "usage=write;ratelimit=write")`를 호출합니다. Read|write 조합의 native formatter는 해당 capability의 `perm`을 `*`로 표시합니다. 이것이 global user admin을 의미하지는 않습니다. [Native capability formatter](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_common.cc).

Usage trim에는 항상 fresh producer의 UID를 명시합니다. `remove-all=true`, UID 없는 trim, cluster-wide usage clear를 사용하지 않습니다. 다른 producer는 S3 요청을 멈추고 세 native flush tick 동안 counters를 안정화합니다. 별도 operator의 usage 조회는 producer counters를 바꾸지 않습니다. Trim 이후 owned UID가 세 tick 동안 비어 있어야 하고 다른 producer의 entry/summary counters는 동일해야 합니다. [v20.2.4 exact native usage read/write check와 trim guard](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_rest_usage.cc).

## 실제 bucket rate-limit 집행과 복구

Rate-limit recipe는 fresh ordinary owner가 만든 두 bucket의 native ID·owner를 캡처하고 quota가 꺼져 있는지 확인합니다. AdminOps GET 응답의 `bucket_ratelimit`을 읽고 이미 enabled인 policy는 adopt하지 않습니다. Target bucket에만 다음 policy를 POST합니다.

```text
enabled=true
max-read-ops=1
max-write-ops=0
max-read-bytes=0
max-write-bytes=0
```

Ceph 20.2.4에서는 ops/bytes가 분당 token bucket limit이고 0은 해당 dimension을 제한하지 않습니다. 이 recipe는 `max-list-ops`·`max-delete-ops` 등 Ceph 21 필드를 보내지 않습니다. [Tentacle AdminOps rate-limit](https://docs.ceph.com/en/tentacle/radosgw/adminops/#rate-limit-operations), [v20.2.4 native field/capability contract](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_rest_ratelimit.cc).

Native 정책 조회만으로 완료하지 않습니다. Owned object를 반복 GET해 정확한 HTTP **503 `SlowDown`**을 관찰해야 하며, timeout·접속 실패·다른 HTTP 오류는 증거로 받지 않습니다. 같은 principal·gateway의 다른 bucket GET은 계속 exact bytes를 반환해야 합니다. Bucket ID와 현재 policy가 캡처한 값에서 바뀌지 않았는지 확인한 뒤 **enabled와 네 limit field 모두** 원래 값으로 POST 복원합니다. Native readback과 연속 세 GET의 exact bytes로 복구를 확인합니다. [Native request 집행](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_process.cc), [503 SlowDown mapping](https://github.com/ceph/ceph/blob/v20.2.4/src/rgw/rgw_common.cc).

Usage/ratelimit write bits도 owned operator의 최초 read-only 상태로 돌립니다. 성공 후 owned S3 object/bucket과 users를 제거하고 설정 overrides를 `Restore`합니다. Cleanup 요청으로 추가되는 usage와 실패한 partial fixture는 disposable cluster의 종료로 소멸합니다. Existing/shared bucket을 raw AdminOps로 수정하는 adoption 경로를 제공하는 recipe는 아닙니다.

## 실행

```sh
CEPH_TEST_IMAGE=ceph-testcontainers:official-20.2.4-control \
CEPH_TEST_OSD_IMAGE=ceph-testcontainers:official-20.2.4-osd \
CEPH_TEST_RGW_IMAGE=ceph-testcontainers:official-20.2.4-rgw \
CGO_ENABLED=0 go test -mod=readonly -count=1 \
  -tags=integration,features ./internal/integration \
  -run '^TestRGWAdminRecordsAndRateLimit$' -timeout 25m -v
```

추가 backend image나 host SDK는 필요하지 않습니다. Docker Desktop의 host networking이 활성화되어 있어야 host subcase가 같은 방식으로 실행됩니다. Production usage retention/performance, 여러 gateway 사이의 rate-limit aggregation, account/global/anonymous limit 및 모든 dimension 조합은 이 single-gateway bucket fixture의 검증 범위 밖입니다.
