#!/bin/bash

if [ ! -d ceph-fuse-osiris ]; then
  wget --no-check-certificate https://repo.osris.org/ceph-fuse-osiris.tar.gz
  tar -xvzf ceph-fuse-osiris.tar.gz
fi

source osiris-ceph.conf

ceph-fuse-osiris/ceph-fuse-osiris -c $CEPH_CONF --uid=$OSIRIS_UID --gid=$OSIRIS_GID --groups=$GROUPS -k $KEYRING --fuse-allow-other=false --id $ID --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r $MOUNT --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNTPOINT
