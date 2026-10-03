# A test consumer image. The public Go module remains independent of native SDKs.
# Use the same source release/architecture as the server role images. This full
# RPM source retains its package database and dnf, unlike the extracted slim role.
ARG CEPH_CLIENT_IMAGE=quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
FROM ${CEPH_CLIENT_IMAGE}
USER root
RUN dnf -y --setopt=install_weak_deps=False install cryptsetup && \
    dnf clean all && \
    cryptsetup --version && \
    python3 -c "import rados, rbd"
ENTRYPOINT ["sleep", "infinity"]
