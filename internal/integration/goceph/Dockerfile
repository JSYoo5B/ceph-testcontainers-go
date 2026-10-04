# Linux-only integration fixture; the published library has no cgo dependency.
ARG GO_IMAGE=golang:1.27.1
ARG CEPH_SOURCE_IMAGE=quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
ARG CEPH_CLIENT_IMAGE=quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9

FROM ${GO_IMAGE} AS toolchain

FROM ${CEPH_SOURCE_IMAGE} AS build
COPY --from=toolchain /usr/local/go /usr/local/go
ENV PATH=/usr/local/go/bin:${PATH}
# The Quay runtime already supplies GCC and Ceph libraries, but omits the
# development headers and linker symlinks. Use public headers from its release.
ADD https://raw.githubusercontent.com/ceph/ceph/v20.2.4/src/include/rados/librados.h /usr/include/rados/librados.h
ADD https://raw.githubusercontent.com/ceph/ceph/v20.2.4/src/include/rados/rados_types.h /usr/include/rados/rados_types.h
ADD https://raw.githubusercontent.com/ceph/ceph/v20.2.4/src/include/rbd/librbd.h /usr/include/rbd/librbd.h
ADD https://raw.githubusercontent.com/ceph/ceph/v20.2.4/src/include/rbd/features.h /usr/include/rbd/features.h
ADD https://raw.githubusercontent.com/ceph/ceph/v20.2.4/src/include/cephfs/libcephfs.h /usr/include/cephfs/libcephfs.h
ADD https://raw.githubusercontent.com/ceph/ceph/v20.2.4/src/include/cephfs/ceph_ll_client.h /usr/include/cephfs/ceph_ll_client.h
RUN mkdir -p /tc-link /out && \
    ln -s /usr/lib64/librados.so.2 /tc-link/librados.so && \
    ln -s /usr/lib64/librbd.so.1 /tc-link/librbd.so && \
    ln -s /usr/lib64/libcephfs.so.2 /tc-link/libcephfs.so
WORKDIR /probe
COPY probe/go.mod probe/go.sum ./
RUN --mount=type=cache,target=/root/go/pkg/mod go mod download
COPY probe/*.go ./
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 CGO_LDFLAGS=-L/tc-link go build -mod=readonly -tags=tentacle -o /out/go-ceph-probe .

WORKDIR /module
COPY --from=module go.mod go.sum ./
RUN --mount=type=cache,target=/root/go/pkg/mod go mod download
COPY --from=module ceph ./ceph
COPY --from=module multicluster ./multicluster
COPY --from=module internal/dockerbridge ./internal/dockerbridge
COPY --from=module internal/integration ./internal/integration
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go test ./ceph ./multicluster && \
    CGO_ENABLED=0 go test -c -tags=integration,goceph -o /out/go-ceph-integration.test ./internal/integration

FROM ${CEPH_CLIENT_IMAGE} AS client
COPY --from=build /out/go-ceph-probe /usr/local/bin/go-ceph-probe
ENTRYPOINT ["sleep", "infinity"]

FROM client AS runner
COPY --from=build /out/go-ceph-integration.test /usr/local/bin/go-ceph-integration.test
ENTRYPOINT ["/usr/local/bin/go-ceph-integration.test"]
