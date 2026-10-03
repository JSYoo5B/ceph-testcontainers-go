#!/bin/sh
# Run only in a disposable Ubuntu Noble ARM64 native build environment.
# Explicit APT dependency recipe, not install-deps.sh's unauthenticated boost fallback.
set -eu
[ "$(uname -s)" = Linux ] || { echo 'Requires Linux' >&2; exit 1; }
[ "$(dpkg --print-architecture)" = arm64 ] || { echo 'Requires native arm64 dpkg' >&2; exit 1; }
. /etc/os-release
[ "$ID" = ubuntu ] && [ "$VERSION_ID" = 24.04 ] || { echo 'Requires Ubuntu Noble 24.04' >&2; exit 1; }
[ "$(id -u)" = 0 ] || { echo 'Requires root inside disposable build environment' >&2; exit 1; }
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  build-essential cmake ninja-build lld binutils pkg-config patch \
  ca-certificates gnupg gpgv curl git cpio bison flex gperf \
  python3 python3-dev python3-venv python3-setuptools python3-yaml \
  python3-jinja2 python3-markupsafe python3-natsort cython3 \
  libaio-dev libblkid-dev libcap-ng-dev libcap-dev libcunit1-dev \
  libcurl4-openssl-dev libevent-dev libexpat1-dev libfmt-dev \
  libicu-dev libkeyutils-dev liblmdb-dev liblua5.3-dev liblz4-dev \
  libncurses-dev libnl-genl-3-dev libnuma-dev liboath-dev \
  libre2-dev libsnappy-dev libsqlite3-dev libssl-dev libthrift-dev \
  libudev-dev libutf8proc-dev libxml2-dev libyaml-cpp-dev libzstd-dev \
  nlohmann-json3-dev uuid-dev zlib1g-dev patchelf dpkg-dev
# APT is not snapshot-pinned; exact installed versions are captured by native_build.py.
# Boost 1.87 is built from official Ceph CMake's URL_HASH with BOOST_J=1.
