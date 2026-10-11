#!/usr/bin/env bash
# Read-only environment/cleanup inventory. No builds, fixtures or cgroup changes.
set -eu
cd "$(dirname "$0")"
{
  date -u
  uname -a
  cat /etc/os-release
  rustc --verbose --version
  cargo --version
  id
  cat /proc/self/cgroup
  stat -fc %T /sys/fs/cgroup
  cat /sys/fs/cgroup/cgroup.controllers
  cat /sys/fs/cgroup/cgroup.subtree_control
  if runuser -u nobody -- test -w /sys/fs/cgroup; then
    echo NOBODY_ROOT_WRITABLE
  else
    echo NOBODY_ROOT_NOT_WRITABLE
  fi
  echo REMAINING_SPIKE_CGROUPS
  find /sys/fs/cgroup -maxdepth 1 -name 'sider-lua-spike-*' -print
  echo REMAINING_HELPER_PROCESSES
  ps -eo pid,comm | awk '$2 == "sider-lua-conta" {print}'
} > evidence/linux-platform.txt
cat evidence/linux-platform.txt
