#!/bin/bash

if [ ! -d ceph-fuse-osiris ]; then
  wget --no-check-certificate https://repo.osris.org/ceph-fuse-osiris.tar.gz
  tar -xvzf ceph-fuse-osiris.tar.gz
fi

source osiris-ceph.conf
EXIT_REQUIRED=false

if [ -z $MON_ADDRESS ]; then
  echo "Mon address not set."
  EXIT_REQUIRED=true
fi

if [ -z $OSIRIS_UID ]; then
  echo "UID not set."
  EXIT_REQUIRED=true
fi

if [ -z $OSIRIS_GID ]; then
  echo "GID not set."
  EXIT_REQUIRED=true
fi

if [ -z $GROUPS ]; then
  echo "Groups not set."
  EXIT_REQUIRED=true
fi

if [ -z $KEYRING ]; then
  echo "Keyring path not set."
  EXIT_REQUIRED=true
fi

if [ -z $ID ]; then
  echo "Client ID not set."
  EXIT_REQUIRED=true
fi

if [ -z $MOUNT ]; then
  echo "Mount location not set.s."
  EXIT_REQUIRED=true
fi

if [ ! -z @1 ]; then
  MOUNTPOINT=@1
fi

if [ -z $MOUNTPOINT ]; then
  echo "Mountpoint not set."
  EXIT_REQUIRED=true
fi

if [ $EXIT_REQUIRED = true ]; then
  echo "Exiting without running ceph-fuse-osiris."
  exit(1)
fi

# echo "ceph-fuse-osiris/ceph-fuse-osiris -m testmon.osris.org --uid=$OSIRIS_UID --gid=$OSIRIS_GID --groups=$OSIRIS_GROUPS -k $KEYRING --fuse-allow-other=false --id $ID --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r $MOUNT --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNTPOINT"echo "ceph-fuse-osiris/ceph-fuse-osiris -m testmon.osris.org --uid=$OSIRIS_UID --gid=$OSIRIS_GID --groups=$OSIRIS_GROUPS -k $KEYRING --fuse-allow-other=false --id $ID --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r $MOUNT --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNTPOINT"
ceph-fuse-osiris/ceph-fuse-osiris -m testmon.osris.org --uid=$OSIRIS_UID --gid=$OSIRIS_GID --groups=$OSIRIS_GROUPS -k $KEYRING --fuse-allow-other=false --id $ID --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r $MOUNT --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNTPOINT
