#!/bin/sh
# Host-side launcher. Requires permission to create a systemd transient service.
set -eu
binary=$1
payload=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/run-delegated.sh
unit=sider-lua-delegated-$$.service
echo "TRANSIENT_SERVICE=$unit"
status=0
systemd-run --unit="$unit" --collect --wait --pipe --uid=nobody --gid=nogroup \
    --property='Delegate=cpu memory pids' /bin/sh "$payload" "$binary" || status=$?
if test -d "/sys/fs/cgroup/system.slice/$unit"; then
    echo 'FAIL transient subtree still exists' >&2
    exit 1
fi
echo 'PASS transient service subtree removed'
exit "$status"
