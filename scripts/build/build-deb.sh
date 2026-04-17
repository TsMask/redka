#!/bin/bash

set -e

VERSION="2.0.0"
PKG_NAME="redka"

SCRIPTS_DIR="$(dirname "$(realpath "$0")")"
PROJECT_DIR="$(cd "${SCRIPTS_DIR}/.." && pwd)"

OS_VERSION_ID=$(. /etc/os-release && echo "${VERSION_ID}")
OS_ID=$(. /etc/os-release && echo "${ID}")
ARCH=$(uname -m)
case "${ARCH}" in
    x86_64) ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    armv7l) ARCH="armhf" ;;
esac

PKG_OUTPUT="${PKG_NAME}-${VERSION}-${OS_ID}${OS_VERSION_ID}-${ARCH}.deb"

## Build binary
cd "${PROJECT_DIR}" && make build

## Build DEB package
DEST_DIR="${SCRIPTS_DIR}/dist"
rm -rf "${DEST_DIR}"
mkdir -p "${DEST_DIR}"

mkdir -p "${DEST_DIR}/DEBIAN"
mkdir -p "${DEST_DIR}/usr/local/bin"
mkdir -p "${DEST_DIR}/usr/local/etc/redka"
mkdir -p "${DEST_DIR}/etc/systemd/system"

install -m 755 "${PROJECT_DIR}/redka" "${DEST_DIR}/usr/local/bin/redka"
install -m 644 "${SCRIPTS_DIR}/build/redka.yaml" "${DEST_DIR}/usr/local/etc/redka/redka.example.yaml"
install -m 644 "${SCRIPTS_DIR}/build/redka.service" "${DEST_DIR}/etc/systemd/system/redka.service"

cat > "${DEST_DIR}/DEBIAN/control" <<EOF
Package: ${PKG_NAME}
Version: ${VERSION}
Section: database
Priority: optional
Architecture: ${ARCH}
Maintainer: TsMask <https://github.com/TsMask/redka>
Description: Redka Database Service
 Redka is a Redis-compatible storage solution that uses a relational database as its backend storage.
EOF

cat > "${DEST_DIR}/DEBIAN/postinst" <<'POSTINST'
#!/bin/bash
set -e

case "$1" in
    configure)
        if [ ! -f /usr/local/etc/redka/redka.yaml ]; then
            cp /usr/local/etc/redka/redka.example.yaml /usr/local/etc/redka/redka.yaml
        fi
        
        if [ -d /run/systemd/system ]; then
            systemctl daemon-reload
            systemctl enable redka.service
        fi
        ;;
    abort-upgrade|abort-remove|abort-deconfigure)
        ;;
    *)
        ;;
esac

exit 0
POSTINST

cat > "${DEST_DIR}/DEBIAN/prerm" <<'PRERM'
#!/bin/bash
set -e

case "$1" in
    remove|deconfigure)
        if [ -d /run/systemd/system ]; then
            systemctl --no-reload disable redka.service
            systemctl stop redka.service
        fi
        ;;
    upgrade)
        ;;
    failed-upgrade)
        ;;
    *)
        ;;
esac

exit 0
PRERM

chmod 755 "${DEST_DIR}/DEBIAN/postinst"
chmod 755 "${DEST_DIR}/DEBIAN/prerm"

mkdir -p "${SCRIPTS_DIR}"
dpkg-deb --build "${DEST_DIR}" "${SCRIPTS_DIR}/${PKG_OUTPUT}"
rm -rf "${DEST_DIR}"

echo "${SCRIPTS_DIR}/${PKG_OUTPUT}"
