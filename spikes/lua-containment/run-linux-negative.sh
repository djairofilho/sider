#!/bin/sh
# Run as the same host administrator used to launch the delegated test service.
set -eu
binary=$1
test -x "$binary"
controller_fixture=
fake_fixture=
cleanup() {
    if test -n "$controller_fixture"; then
        rmdir -- "$controller_fixture" || true
    fi
    if test -n "$fake_fixture"; then
        for control in link cgroup.subtree_control cpu.stat memory.max memory.swap.max pids.max cgroup.procs cgroup.kill; do
            if test -e "$fake_fixture/$control" || test -L "$fake_fixture/$control"; then
                rm -- "$fake_fixture/$control"
            fi
        done
        rmdir -- "$fake_fixture" || true
    fi
}
trap cleanup EXIT
echo 'NEGATIVE permission: nobody has no delegation at hierarchy root'
status=0
output=$(runuser -u nobody -- "$binary" --cgroup-root /sys/fs/cgroup 2>&1) || status=$?
printf '%s\n' "$output"
test "$status" -eq 1
case "$output" in
  *'containment unavailable; source gate remained closed: Permission denied'*) ;;
  *) echo 'FAIL unexpected permission-path result' >&2; exit 1 ;;
esac
echo 'PASS permission denial exits 1 with GO withheld'
fixture=/sys/fs/cgroup/sider-lua-no-controller-$$
mkdir "$fixture"
controller_fixture=$fixture
echo 'NEGATIVE controller: real cgroup without enabled controllers'
status=0
output=$(runuser -u nobody -- "$binary" --cgroup-root "$fixture" 2>&1) || status=$?
printf '%s\n' "$output"
rmdir -- "$fixture"
controller_fixture=
test "$status" -eq 1
case "$output" in
  *'containment unavailable; no helper spawned: delegated controller unavailable: memory'*) ;;
  *) echo 'FAIL unexpected controller-path result' >&2; exit 1 ;;
esac
echo 'PASS missing controller exits 1 before helper spawn'
fixture=$(mktemp -d /tmp/sider-lua-fake-controls.XXXXXX)
fake_fixture=$fixture
chmod 755 "$fixture"
printf 'memory cpu pids\n' > "$fixture/cgroup.subtree_control"
printf 'usage_usec 0\n' > "$fixture/cpu.stat"
for control in memory.max memory.swap.max pids.max cgroup.procs cgroup.kill; do
    printf '0\n' > "$fixture/$control"
done
for candidate in "$fixture" "$fixture/link"; do
    if test "$candidate" = "$fixture/link"; then
        ln -s "$fixture" "$candidate"
        echo 'NEGATIVE symlink: resolves outside expected cgroup2 mount'
    else
        echo 'NEGATIVE ordinary directory: fake controller files'
    fi
    status=0
    output=$(runuser -u nobody -- "$binary" --cgroup-root "$candidate" 2>&1) || status=$?
    printf '%s\n' "$output"
    test "$status" -eq 1
    case "$output" in
      *'containment unavailable; no helper spawned: delegated root is outside real cgroup2 mount'*) ;;
      *) echo 'FAIL unexpected mount-path result' >&2; exit 1 ;;
    esac
    echo 'PASS fake or escaped mount rejected before helper spawn'
done
rm -- "$fixture/link" "$fixture/cgroup.subtree_control" "$fixture/cpu.stat" \
    "$fixture/memory.max" "$fixture/memory.swap.max" "$fixture/pids.max" \
    "$fixture/cgroup.procs" "$fixture/cgroup.kill"
rmdir -- "$fixture"
fake_fixture=
remaining=0
for link in /proc/[0-9]*/exe; do
    target=$(readlink "$link" 2>/dev/null || true)
    if test "$target" = "$binary"; then
        echo "FAIL remaining helper $link"
        remaining=1
    fi
done
test "$remaining" -eq 0
echo 'PASS no live helper executable remains'
