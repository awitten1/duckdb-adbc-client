#!/usr/bin/env bash
set -euo pipefail

work_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
version=26.9.12.8
[[ "$(uname -s)" == Linux ]] || { echo 'This installer requires Linux.' >&2; exit 1; }
case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) echo 'Unsupported architecture.' >&2; exit 1 ;;
esac

install_dir="$work_dir/.clickhouse"
archive="clickhouse-common-static-$version-$arch.tgz"
base_url=https://packages.clickhouse.com/tgz/stable
mkdir -p "$install_dir/downloads" "$install_dir/packages" "$install_dir/bin"
cd "$install_dir/downloads"
curl -fsSL --retry 3 -o "$archive.sha512" "$base_url/$archive.sha512"
if [[ ! -f "$archive" ]] || ! sha512sum -c --status "$archive.sha512"; then
    curl -fL --retry 3 -o "$archive" "$base_url/$archive"
fi
sha512sum -c "$archive.sha512"
tar -xzf "$archive" -C "$install_dir/packages"
ln -sfn "$install_dir/packages/clickhouse-common-static-$version/usr/bin/clickhouse" \
    "$install_dir/bin/clickhouse"
ln -sfn clickhouse "$install_dir/bin/clickhouse-client"
"$install_dir/bin/clickhouse" --version
