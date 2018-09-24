#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"

SUBDIR="ceph-fuse-osiris"
TARNAME="ceph-fuse-osiris.tgz"
REPO="https://repo.osris.org"
CONFIG_FILE="osiris-ceph.conf"

if [ ! -d "${SCRIPT_DIR}/${SUBDIR}" ]; then
  wget "${REPO}/${TARNAME}"
  tar -xvzf "${TARNAME}"
fi

source $CONFIG_FILE

CONFIG=(MON_ADDRESS OSIRIS_UID OSIRIS_GID OSIRIS_GROUPS KEYRING ID FSROOT MOUNTPOINT)
CONFIG_MISSING=false

for V in ${CONFIG[@]}; do
  if [ -z "${!V}" ]; then
    echo "$V not set in $CONFIG_FILE"
    CONFIG_MISSING=true
  fi
done

if [ $CONFIG_MISSING = true ]; then
  echo "ERROR: Missing required configuration setting(s)"
  exit 1
fi

if [ ! -d "$MOUNTPOINT" ]; then
  mkdir "$MOUNTPOINT"
fi

${SCRIPT_DIR}/${SUBDIR}/ceph-fuse-osiris -m $MON_ADDRESS \
--uid=$OSIRIS_UID --gid=$OSIRIS_GID --groups=$OSIRIS_GROUPS \
-k $KEYRING --id $ID \
--fuse-allow-other=false  --client_try_dentry_invalidate=true \
--log_file=cephfs-osiris.log --admin_socket=cephfs-osiris.asok --conf=/dev/null \
--fuse_default_permissions=0 --client_acl_type=posix_acl \
--client_mountpoint=$FSROOT $MOUNTPOINT
