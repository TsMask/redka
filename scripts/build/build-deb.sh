#!/usr/bin/env bash

set -e
export PATH=$PATH:/usr/local/go/bin

VERSION="1.5.0"
PKG_NAME="redka"

while getopts "v:h" opt; do
    case $opt in
        v) VERSION="$OPTARG" ;;
        h) echo "Usage: $0 [-v VERSION]" && exit 0 ;;
        *) exit 1 ;;
    esac
done
shift $((OPTIND -1))

SCRIPTS_DIR="$(dirname "$(realpath "$0")")"
PROJECT_DIR="$(cd "${SCRIPTS_DIR}/../.." && pwd)"

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
mkdir -p "${DEST_DIR}/usr/share/doc/redka"

install -m 755 "${PROJECT_DIR}/build/redka" "${DEST_DIR}/usr/local/bin/redka"
install -m 644 "${SCRIPTS_DIR}/redka.yaml" "${DEST_DIR}/usr/local/etc/redka/redka.example.yaml"
install -m 644 "${SCRIPTS_DIR}/redka.service" "${DEST_DIR}/etc/systemd/system/redka.service"
install -m 644 "${PROJECT_DIR}/LICENSE" "${DEST_DIR}/usr/share/doc/redka/LICENSE"
install -m 644 "${PROJECT_DIR}/NOTICE" "${DEST_DIR}/usr/share/doc/redka/NOTICE"
install -m 644 "${PROJECT_DIR}/THIRD_PARTY_LICENSES.md" "${DEST_DIR}/usr/share/doc/redka/THIRD_PARTY_LICENSES.md"

cat > "${DEST_DIR}/usr/share/doc/redka/copyright" <<'COPYRIGHT'
Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/
Upstream-Name: Redka
Source: https://github.com/TsMask/redka

Files: *
Copyright: 2026 TsMask <340112800@qq.com>
License: MIT
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 .
 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.
 .
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.

Files: *
Copyright: 2024 Anton Zhiyanov <https://antonz.org>
License: BSD-3-clause
 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:
 .
 1. Redistributions of source code must retain the above copyright notice,
 this list of conditions and the following disclaimer.
 .
 2. Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.
 .
 3. Neither the name of the copyright holder nor the names of its
 contributors may be used to endorse or promote products derived from this
 software without specific prior written permission.
 .
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
COPYRIGHT

cat > "${DEST_DIR}/DEBIAN/control" <<EOF
Package: ${PKG_NAME}
Version: ${VERSION}
Section: database
Priority: optional
Architecture: ${ARCH}
Maintainer: TsMask <https://github.com/TsMask/redka>
Description: Database Service
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

dpkg-deb --build "${DEST_DIR}" "${PROJECT_DIR}/build/${PKG_OUTPUT}"
rm -rf "${DEST_DIR}"

echo "${PROJECT_DIR}/build/${PKG_OUTPUT}"
# sudo bash scripts/build/build-deb.sh -v 1.5.0
