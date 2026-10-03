# Dependency bootstrap only. No source download, Ceph build, or test execution.
ARG BASE_IMAGE=ubuntu@sha256:a853f94d226358a79c740cfc7bce0c289748f3fe3488d921d038ccd752c61b60
FROM ${BASE_IMAGE}
COPY install-build-deps.sh /usr/local/bin/ceph-tc-install-build-deps
RUN /usr/local/bin/ceph-tc-install-build-deps
ENV TZ=UTC LC_ALL=C.UTF-8 CMAKE_BUILD_PARALLEL_LEVEL=1 MAKEFLAGS=-j1
WORKDIR /native
CMD ["sleep", "infinity"]
