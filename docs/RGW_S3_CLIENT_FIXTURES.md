# RGW S3 client fixture recipes

S3 object·bucket API는 실제 application client가 호출합니다. testcontainers는 `Run` → `StartRGWWithConfig` → fresh `CreateUser` → `Credentials`와 `S3Endpoint`로 서버와 principal을 제공합니다. 아래 테스트는 이 공개 API와 `Admin`/`TemporaryConfig`를 조합한 실행 가능한 recipe입니다. 공개 library에 S3 CRUD wrapper나 SDK 의존성을 추가하지 않습니다.

## 개별 bucket quota와 reshard

[TestRGWBucketMaintenance](../internal/integration/rgw_bucket_maintenance_integration_test.go)는 fresh user가 만든 두 bucket의 native ID·owner를 확인합니다. 한 bucket에만 `quota set/enable --quota-scope bucket --bucket <name>`을 적용하고, 실제 추가 object가 `QuotaExceeded`로 거부되는지, 기존 bytes와 sibling bucket의 쓰기가 유지되는지 확인합니다. quota를 끄면 같은 bucket의 쓰기가 복구되어야 합니다.

gateway 시작 전에 `TemporaryConfig`로 `client.admin/rgw_dynamic_resharding=false`를 적용하여 background worker와의 경쟁을 제거합니다. `reshard add` 후 queue에 owned bucket만 있는지 확인하고 명시적 `reshard process`를 실행합니다. shard 수·완료 상태·빈 queue·bucket identity·quota·모든 S3 bytes가 유지돼야 합니다. native `reshard process`는 전체 queue를 처리하므로 이 recipe는 fresh cluster의 owned queue에만 적용합니다. 설정은 복원하며 data/user도 명시적으로 정리합니다.

## S3 기능과 native server policy

[TestRGWS3ClientFeatures](../internal/integration/rgw_s3_client_features_integration_test.go)는 ordinary user 두 개를 사용합니다.

- versioning: 서로 다른 native version ID·과거 bytes·delete marker를 확인하고 각 version을 명시적으로 제거합니다.
- multipart: 5 MiB part와 마지막 part를 조립한 정확한 bytes, 다른 incomplete upload의 abort와 잔여 listing을 확인합니다.
- ACL·bucket policy: 두 번째 principal의 접근 거부 → read 허용 → revoke 후 거부와 policy가 허용하지 않은 write 거부를 확인합니다.
- lifecycle: native debug clock을 2초/day로 설정하고 background lifecycle을 끈 뒤 owned bucket만 `lc process --bucket`으로 처리합니다. selected prefix만 만료되고 outside bytes가 유지돼야 합니다. 실제 하루를 기다리는 production clock 검증은 아닙니다.
- object lock: version의 GOVERNANCE retention을 조회하고 삭제 거부, legal hold 중 bypass 거부, 명시적 hold 해제와 authorized governance bypass 후 삭제를 확인합니다.

서버 설정은 gateway를 시작하기 전에 적용하며 `ConfigOverride.Restore`로 복원합니다. S3 요청에는 실제 서명을 사용하고 status만으로 판단하지 않고 native ID·policy와 client payload를 확인합니다. 실패 시 cluster 정리로 owned resource를 회수합니다.

2026-10-03 현재 unit·전체 integration tag compile은 통과했으며 두 recipe의 native bridge/host Docker 실행은 대기 중입니다. 실행 결과는 [coverage matrix](CLIENT_FIXTURE_COVERAGE.md)에 별도로 기록합니다. TLS는 [native TLS fixture](RGW_TLS_FIXTURE.md), STS·Swift·실제 KMS backend는 coverage의 G09 조건으로 구분합니다. `make rgw-s3-fixtures`는 이 두 recipe와 TLS 검증을 순차 실행합니다.

[RGW quota](https://docs.ceph.com/en/tentacle/radosgw/admin/#quota-management), [dynamic resharding](https://docs.ceph.com/en/tentacle/radosgw/dynamicresharding/), [S3 bucket API](https://docs.ceph.com/en/tentacle/radosgw/s3/bucketops/).
