# Keep the exact Ceph release, glibc and Python ABI of the verified upstream
# image. Package selection happens offline against its installed RPM database.
ARG CEPH_SOURCE_IMAGE=quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
FROM ${CEPH_SOURCE_IMAGE} AS assemble
ARG CEPH_SOURCE_IMAGE
COPY assemble.py /tmp/assemble.py
RUN SOURCE_IMAGE="${CEPH_SOURCE_IMAGE}" python3 /tmp/assemble.py

# A fresh final stage actually removes unused upstream layers from this image.
FROM scratch
ARG CEPH_SOURCE_IMAGE
LABEL org.opencontainers.image.title="Ceph testcontainers slim runtime" \
      org.opencontainers.image.description="Selected upstream Ceph runtime packages for disposable integration tests" \
      org.opencontainers.image.base.name="${CEPH_SOURCE_IMAGE}"
COPY --from=assemble /runtime-rootfs/ /
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    LANG=C.UTF-8
CMD ["/bin/bash"]
