#!/bin/bash

set -e

VERSION="2.0.0"
PKG_NAME="redka"

SCRIPTS_DIR="$(dirname "$(realpath "$0")")"
PROJECT_DIR="$(cd "${SCRIPTS_DIR}/.." && pwd)"

OS_VERSION_ID=$(. /etc/os-release && echo "${VERSION_ID}")
OS_ID=$(. /etc/os-release && echo "${ID}")
ARCH=$(uname -m)
PKG_ARCH="${ARCH}"
case "${PKG_ARCH}" in
    x86_64) PKG_ARCH="amd64" ;;
    aarch64) PKG_ARCH="arm64" ;;
    armv7l) PKG_ARCH="armhf" ;;
esac

PKG_VERSION="${PKG_NAME}-${VERSION}-${OS_ID}${OS_VERSION_ID}.${ARCH}.rpm"
PKG_OUTPUT="${PKG_NAME}-${VERSION}-${OS_ID}${OS_VERSION_ID}-${PKG_ARCH}.rpm"

## Build binary
cd "${PROJECT_DIR}" && make build

## Build RPM package
DEST_DIR="${SCRIPTS_DIR}/dist"
rm -rf "${DEST_DIR}"
mkdir -p "${DEST_DIR}"

mkdir -p "${DEST_DIR}/BUILD/usr/local/bin"
mkdir -p "${DEST_DIR}/BUILD/usr/local/etc/redka"
mkdir -p "${DEST_DIR}/BUILD/etc/systemd/system"

install -m 755 "${PROJECT_DIR}/redka" "${DEST_DIR}/BUILD/usr/local/bin/redka"
install -m 644 "${SCRIPTS_DIR}/redka.yaml" "${DEST_DIR}/BUILD/usr/local/etc/redka/redka.example.yaml"
install -m 644 "${SCRIPTS_DIR}/redka.service" "${DEST_DIR}/BUILD/etc/systemd/system/redka.service"

cat > "${DEST_DIR}/SPEC" <<EOF
Name:           ${PKG_NAME}
Version:        ${VERSION}
Release:        ${OS_ID}${OS_VERSION_ID}
Summary:        Redka Database Service
License:        BSD 3-Clause
URL:            https://github.com/TsMask/redka
BuildArch:      ${ARCH}
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd

%description
Redka is a Redis-compatible storage solution that uses a relational database as its backend storage.

%prep
# Nothing to prep

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/usr/local/bin
mkdir -p %{buildroot}/usr/local/etc/redka
mkdir -p %{buildroot}/etc/systemd/system

install -m 755 %{_builddir}/usr/local/bin/redka %{buildroot}/usr/local/bin/redka
install -m 644 %{_builddir}/usr/local/etc/redka/redka.example.yaml %{buildroot}/usr/local/etc/redka/redka.example.yaml
install -m 644 %{_builddir}/etc/systemd/system/redka.service %{buildroot}/etc/systemd/system/redka.service

%pre
# Nothing to pre

%post
if [ ! -f /usr/local/etc/redka/redka.yaml ]; then
    cp /usr/local/etc/redka/redka.example.yaml /usr/local/etc/redka/redka.yaml
fi

if [ -d /run/systemd/system ]; then
    /bin/systemctl daemon-reload > /dev/null 2>&1 || true
    /bin/systemctl enable redka.service > /dev/null 2>&1 || true
fi

%preun
if [ \$1 -eq 0 ]; then
    if [ -d /run/systemd/system ]; then
        /bin/systemctl --no-reload disable redka.service > /dev/null 2>&1 || true
        /bin/systemctl stop redka.service > /dev/null 2>&1 || true
    fi
fi

%postun
if [ \$1 -eq 0 ]; then
    if [ -d /run/systemd/system ]; then
        /bin/systemctl daemon-reload > /dev/null 2>&1 || true
    fi
fi

%files
%defattr(-,root,root,-)
/usr/local/bin/redka
/usr/local/etc/redka/redka.example.yaml
/etc/systemd/system/redka.service

%changelog
* $(date '+%a %b %d %Y') TsMask <https://github.com/TsMask/redka> - ${VERSION}-1
- Initial package release
EOF

cp "${SCRIPTS_DIR}/redka.yaml" "${DEST_DIR}/redka.example.yaml"
cp "${PROJECT_DIR}/redka" "${DEST_DIR}/redka"
cp "${SCRIPTS_DIR}/redka.service" "${DEST_DIR}/redka.service"

rpmbuild --define "_topdir ${DEST_DIR}" -bb "${DEST_DIR}/SPEC"

mkdir -p "${SCRIPTS_DIR}"
cp "${DEST_DIR}/RPMS/${ARCH}/${PKG_VERSION}" "${SCRIPTS_DIR}/${PKG_OUTPUT}"
rm -rf "${DEST_DIR}"

echo "${SCRIPTS_DIR}/${PKG_OUTPUT}"
