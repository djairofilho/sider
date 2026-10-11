#!/bin/sh
# Run only as the payload of a new systemd transient service with Delegate=yes.
# The system manager delegates this service's private subtree to its User=.
set -eu
binary=$1
case "$binary" in /*) ;; *) echo 'absolute binary path required' >&2; exit 1 ;; esac
echo 'DELEGATION PLATFORM'
date -u
id
uname -a
systemd --version | head -1
membership=$(sed -n 's/^0:://p' /proc/self/cgroup)
case "$membership" in
  /system.slice/sider-lua-delegated-*.service) ;;
  *) echo 'refusing a subtree outside the private transient service' >&2; exit 1 ;;
esac
root=/sys/fs/cgroup$membership
echo "DELEGATED_ROOT=$root"
cat "$root/cgroup.controllers"
test -w "$root/cgroup.procs"
test -w "$root/cgroup.subtree_control"
mkdir "$root/supervisor"
# Evacuate only this script's process to satisfy cgroup's no-internal-process rule.
printf '%s\n' "$$" > "$root/supervisor/cgroup.procs"
printf '+cpu +memory +pids\n' > "$root/cgroup.subtree_control"
echo 'ENABLED_CONTROLLERS'
cat "$root/cgroup.subtree_control"
"$binary" --cgroup-root "$root"
echo 'REMAINING_HELPER_GROUPS'
find "$root" -mindepth 1 -maxdepth 1 -type d -name 'sider-lua-spike-*' -print
echo 'DELEGATED_MATRIX_COMPLETE'
# systemd removes the private service subtree after this process exits.
