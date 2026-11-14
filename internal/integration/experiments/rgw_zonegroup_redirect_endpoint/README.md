# RGW redirect endpoint for a bucket of another zonegroup

Owner: RGW. Kind: upstream bug reproducer (`native_regression`).
Test: `TestMultiClusterRGWZonegroupRedirectEndpoint`, optional batch
`rgw_zonegroup_redirect_endpoint`. Pure tests of the period reader, of the
advertised endpoints, of the bucket location reader, of the classification
of a redirect and of the judgement (`TestParsePeriodReadsTheZonegroups`,
`TestParsePeriodRefusesWhatItCannotRead`, `TestAdvertisedSites`,
`TestParseLocationConstraint`, `TestClassifyRedirect` and
`TestJudgeRedirects`, batch `pure`).

## Use case and native contract

A realm with several zonegroups shares users and bucket metadata, and each
zonegroup keeps the data of its own buckets. A client that sends a request
for a bucket to a gateway of another zonegroup is answered with `301` and a
`Location`, and a client that follows redirects is expected to arrive at the
zonegroup that holds the bucket.

The trigger is a request for an existing bucket whose zonegroup is not the
receiving gateway's. The gateway builds the `Location` from one zonegroup's
advertised endpoint (the first of the zonegroup's `endpoints` in the
period) followed by the request's path and query. An affected build takes
the endpoint of its own zonegroup, so the `Location` names the gateway that
was just asked, and a client that follows it asks the same gateway again. The fix looks the bucket's zonegroup up in the period and takes
that zonegroup's endpoint
([tracker](https://tracker.ceph.com/issues/64983), in Tentacle since 20.2.5
and in Umbrella 21.2.0).

The realm has two zonegroups, each with one zone on its own cluster:

| Zonegroup | Zone | Bucket | Role |
| --- | --- | --- | --- |
| `us` (master) | `a` | `tc-redirect-us`, one object | asked for through `eu` |
| `eu` | `b` | `tc-redirect-eu`, one object | asked for through `us` |

Both directions are asked, so one run sees each zonegroup's endpoint as a
`Location` once it is right and once it is wrong: an observation that could
only ever show one endpoint would not tell the two builds apart.

Not every request is redirected. The source at v20.2.4 and v20.2.5
(`rgw_build_bucket_policies`, `src/rgw/rgw_op.cc`) excludes
`GET ?location`, a system user's request to the master zonegroup, and a
`PUT` or copy of an object whose copy source is in the receiving zonegroup.
The case sends two requests that are redirected, a `GET` of the object and
a listing with a query, as an ordinary user.

## Requirements

- Two clusters with one OSD each and a realm of two zonegroups with one
  zone each (`multisite.Run` with `WithTopology` and `Zonegroups`), one
  gateway per zone, plain HTTP. Both clusters and both gateways run the
  package's `image`, one aggregate image. Bridge networking.
- Requests are signed by the test's own SigV4 client with each gateway's
  generated user and are sent without following redirects. The period is
  read with `Fixture.ZoneAdmin` (`period get`).
- The advertised endpoints are names of the fixture's Docker network, which
  the test host does not resolve. The case compares the `Location` and
  never requests it.
- The test bounds itself to 20 minutes; the batch timeout is 30 minutes.

## Tier and triggers

Standalone reproducer: the test runs on its package-level `image`, Tentacle
20.2.4 by default, which reproduces the defect. Its given and when steps fail
the test; the then step does not. It prints one line,
`EXPERIMENT_RESULT case=rgw_zonegroup_redirect_endpoint result=<REPRODUCED|NOT_REPRODUCED>`,
for all four redirects, and passes either way. No automatic run includes it
(the default checks of a pull request, pushes to `main`, periodic samples),
and it never gates anything. A maintainer can run it on demand, also on a
pull request's head with the comment below.

- Manual: `gh workflow run "Experiments" -f case=rgw_zonegroup_redirect_endpoint`
  requires `REPRODUCED`; adding `-f image=quay.io/ceph/ceph:v20.2.5`
  requires `NOT_REPRODUCED` on Tentacle 20.2.5 afterwards.
  `gh workflow run "Native regressions" -f batch=rgw_zonegroup_redirect_endpoint`
  reports its pass or failure only.
- Comment: `/ci run rgw_zonegroup_redirect_endpoint` or
  `/ci run rgw_zonegroup_redirect_endpoint on umbrella`.

## Running it

On the default image, which must print `result=REPRODUCED`:

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -run '^TestMultiClusterRGWZonegroupRedirectEndpoint$' \
  ./internal/integration/experiments/rgw_zonegroup_redirect_endpoint
```

On a fixed image, injected into the package's `image` variable, which must
print `result=NOT_REPRODUCED` (Tentacle 20.2.5 here; Umbrella 21.2.0,
`quay.io/ceph/ceph:v21.2.0`, is fixed too):

```sh
CGO_ENABLED=0 go test -tags all,native_regression -count=1 -v -timeout=30m \
  -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rgw_zonegroup_redirect_endpoint_test.image=quay.io/ceph/ceph:v20.2.5" \
  -run '^TestMultiClusterRGWZonegroupRedirectEndpoint$' \
  ./internal/integration/experiments/rgw_zonegroup_redirect_endpoint
```

The role image variables (`CEPH_TEST_IMAGE`, `CEPH_TEST_RGW_IMAGE`, ...) do
not select the case's images. The log names the image
(`RGW_REDIRECT image=`), the Ceph version of both clusters
(`RGW_REDIRECT ceph=`), each zone's ID and gateway binary
(`RGW_REDIRECT zone=`), each zone's period (`RGW_REDIRECT period`), the two
advertised endpoints (`RGW_REDIRECT advertised`), the wait for the user in
each zonegroup (`RGW_REDIRECT user`), each bucket location answer
(`RGW_REDIRECT location`) and each redirect with its status, `Location` and
body (`RGW_REDIRECT answer`).

## Observations and controls

1. Condition, period: `period get` of both zones must name the same period
   at the same epoch, and in it each of the two zonegroups exactly once with
   exactly one endpoint and an API name; the two endpoints and the two API
   names must differ (`parsePeriod`, `advertisedSites`). A period without
   its ID, epoch or zonegroup list, or a zonegroup without its endpoint
   list, is refused and not read as empty. Otherwise the test fails with
   `condition not established`.
2. Condition, user: both gateways must accept the user's key (`GET /`
   answers 200), waited for at most 5 minutes. The user is created in the
   master zonegroup and reaches `eu` through metadata sync.
3. Condition, direct access: each bucket is created through its own
   zonegroup's gateway with that zonegroup's API name as its location
   constraint, and its object is written and read back, byte for byte,
   through the same gateway.
4. Condition, trigger: for each bucket both gateways must answer
   `GET ?location`, which is never redirected, with the owning zonegroup's
   API name, waited for at most 5 minutes. The receiving gateway then holds
   the bucket and knows that another zonegroup holds it.
5. Control: a `GET` in a bucket that does not exist must answer
   `404 NoSuchBucket` without a `Location` at both gateways, so a redirect
   is not what a gateway answers to any bucket it does not serve. Otherwise
   `control not established`.
6. When: each gateway is sent `GET /<bucket>/object` and
   `GET /<bucket>?list-type=2&prefix=obj` for the other zonegroup's bucket.
7. Oracle, per answer (`classifyRedirect`): it must be a `301` with exactly
   one `Location`. The whole `Location` is compared: it names the owner when
   it is the owning zonegroup's endpoint followed by the request's path and
   query, and the receiver when it is the receiving zonegroup's endpoint
   followed by them. Another status is `condition not established`; no
   `Location`, several, or one that is neither (another host, a longer
   port, another path or a dropped query) is `another state`.
8. Oracle, together (`judgeRedirects`): redirects received by both
   zonegroups are required. Every redirect naming its receiver is the
   symptom, every redirect naming its owner is its absence, and redirects
   that disagree are `another state`.

Rule basis:

| Form | Native | Source |
| --- | --- | --- |
| `301`, one `Location`, no body | 19.2.5, 20.2.4, 20.2.5, 21.2.0 | `abort_early`, `src/rgw/rgw_rest.cc` lines 721 to 731 at v20.2.4: `ERR_PERMANENT_REDIRECT` with a zonegroup endpoint dumps one redirect |
| `Location` is `<endpoint><path>[?<query>]` | 19.2.5, 20.2.4, 20.2.5, 21.2.0, with and without a query | `build_redirect_url`, `src/rgw/rgw_rest.cc` lines 662 to 679 at v20.2.4: the endpoint without a trailing slash, the request URI, and `?` with the request parameters when there are any |
| the endpoint is the receiving zonegroup's | 19.2.5 and 20.2.4, both directions | `rgw_build_bucket_policies`, `src/rgw/rgw_op.cc` lines 585 to 586 at v20.2.4: `get_zonegroup_endpoint` of the gateway's zonegroup |
| the endpoint is the owning zonegroup's | 20.2.5 and 21.2.0, both directions | the same function at v20.2.5, lines 594 to 603: the bucket's zonegroup is looked up in the period (`find_zonegroup_by_id`, `src/rgw/rgw_zone.cc`) |
| a zonegroup's endpoint is the first of its `endpoints` | every run: one endpoint per zonegroup | `get_zonegroup_endpoint`, `src/rgw/rgw_zone.cc` lines 1320 to 1331 at v20.2.4; the fallback to the master zone's endpoints is not reachable here and has no rule |
| `GET ?location` answers the owning zonegroup's API name at both gateways | 19.2.5, 20.2.4, 20.2.5, 21.2.0 | `RGWGetBucketLocation_ObjStore_S3::send_response`, `src/rgw/rgw_rest_s3.cc` lines 2230 to 2253, and the exclusion in `rgw_build_bucket_policies`, `src/rgw/rgw_op.cc` line 597, at v20.2.4 |
| `404 NoSuchBucket` without a `Location` for a bucket that does not exist | 19.2.5, 20.2.4, 20.2.5, 21.2.0 | `rgw_build_bucket_policies`, `src/rgw/rgw_op.cc` line 570 at v20.2.4 |

A `301` without a `Location`, which the source at v20.2.5 sends when the
period holds no endpoint for the bucket's zonegroup, was not observed and is
`another state`.

## Reading the result

- **REPRODUCED:** all four redirects name the endpoint of the zonegroup
  that received the request: `eu`'s endpoint for `tc-redirect-us`, `us`'s
  endpoint for `tc-redirect-eu`.
- **NOT_REPRODUCED:** all four redirects name the endpoint of the zonegroup
  that holds the bucket. Check that both gateways are of the build under
  test (`RGW_REDIRECT zone=`).
- **Test failure (no result):** `condition not established` (the period,
  the user, direct access or the bucket location does not hold, or a
  gateway answers the request with something other than a `301`),
  `control not established` (a bucket that does not exist is not answered
  with `404 NoSuchBucket`), `another state` (a redirect without one
  `Location`, a `Location` that is neither endpoint followed by the
  request, or redirects that disagree), `outcome not observed` (redirects
  of one zonegroup only), or the clusters or the realm do not start.

Native results (Linux amd64 host, Docker 29.9.1, aggregate images, one run
each):

| Image | Result | Time |
| --- | --- | --- |
| Tentacle 20.2.4, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-20.2.4-all` | `REPRODUCED`: `eu` answers `http://rgw-secondary-…:7480/tc-redirect-us/object`, its own endpoint, and `us` answers `http://rgw-primary-…:7480/tc-redirect-eu/object`, its own; the listings likewise | 129 s |
| Tentacle 20.2.5, `quay.io/ceph/ceph:v20.2.5` | `NOT_REPRODUCED`: `eu` answers `http://rgw-primary-…:7480/tc-redirect-us/object` and `us` answers `http://rgw-secondary-…:7480/tc-redirect-eu/object`; the listings likewise | 130 s |
| Squid 19.2.5, `ghcr.io/jsyoo5b/ceph-testcontainers-images:official-19.2.5-all` | `REPRODUCED`, the same four redirects as 20.2.4 | 138 s |
| Umbrella 21.2.0, `quay.io/ceph/ceph:v21.2.0` | `NOT_REPRODUCED`, the same four redirects as 20.2.5 | 107 s |

## Cleanup and limits

The case creates two buckets with one object each in clusters of its own;
the realm and both clusters are terminated by their cleanups, and nothing
outside them is changed.

The case reads the first redirect only and never follows it. It says
nothing about what a client that follows redirects does, about a request
that is not redirected (`GET ?location`, a system user's request to the
master zonegroup, a copy from the receiving zonegroup), about a zonegroup
with several endpoints or without one, about a zonegroup with several
zones, or about the endpoint of a zone's `redirect_zone`. Not run: arm64,
the host network profile, HTTPS and virtual-hosted bucket names.
