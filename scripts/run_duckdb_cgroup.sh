#!/usr/bin/env bash
# Run a command as the login user inside systemd's shared DuckDB resource slice.
set -euo pipefail

[[ $EUID -ne 0 ]] || { echo 'Run as your login user; this script uses sudo.' >&2; exit 1; }
[[ $# -gt 0 ]] || { echo "Usage: $0 COMMAND [ARG ...]" >&2; exit 1; }

# sudo can replace HOME/PATH; restore driver discovery after dropping privileges.
environment=("HOME=$HOME" "PATH=$PATH")
if [[ -v ADBC_DRIVER_PATH ]]; then
    environment+=("ADBC_DRIVER_PATH=$ADBC_DRIVER_PATH")
fi

exec sudo systemd-run --scope --quiet --collect --expand-environment=no \
    --slice=adbc-duckdb.slice \
    /usr/bin/setpriv --reuid="$(id -u)" --regid="$(id -g)" --init-groups \
    /usr/bin/env "${environment[@]}" "$@"
