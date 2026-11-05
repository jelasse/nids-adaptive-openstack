#!/usr/bin/env bash
# Records the testbed's hardware and software versions for the paper (Section 3.1).
# Run as the stack user AFTER:  source /opt/stack/devstack/openrc admin admin
out="$HOME/testbed_versions_$(date +%F).txt"
{
  echo "== Date";              date
  echo "== CPU";               lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Core|Socket|Virtualization'
  echo "== Memory";            free -h
  echo "== Disk";              lsblk -d -o NAME,SIZE,ROTA,MODEL
  echo "== OS";                grep PRETTY_NAME /etc/os-release; uname -r
  echo "== KVM / libvirt";     virsh --version; qemu-system-x86_64 --version | head -1
  echo "== DevStack branch";   git -C /opt/stack/devstack rev-parse --abbrev-ref HEAD
                               git -C /opt/stack/devstack log -1 --format='%H %cd'
  echo "== Service versions"
  for p in keystone glance nova neutron placement; do
    printf '%s: ' "$p"; git -C "/opt/stack/$p" describe --tags 2>/dev/null || echo n/a
  done
  echo "== OpenStack CLI";     openstack --version 2>&1
  echo "== Services";          openstack service list
  echo "== Compute services";  openstack compute service list
  echo "== Network agents";    openstack network agent list
  echo "== Hypervisors";       openstack hypervisor list
} 2>&1 | tee "$out"
echo
echo "Saved to $out"
