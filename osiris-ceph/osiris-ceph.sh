#!/bin/bash

SCRIPTDIR="$( cd "$( dirname "$0" )" && pwd )"

INSTALLDIR="ceph-fuse-osiris"
TARNAME="ceph-fuse-osiris.tgz"
REPO="https://repo.osris.org"
CONFIGFILE="osiris-ceph.conf"

if [ ! -d "${SCRIPTDIR}/${INSTALLDIR}" ]; then
  wget "${REPO}/${TARNAME}"
  tar -xvzf "${TARNAME}"
fi

source "${SCRIPTDIR}/${CONFIGFILE}"

CONFIG=(MON_ADDRESS OSIRIS_UID OSIRIS_GID OSIRIS_GROUPS KEYRING ID FSROOT MOUNTPOINT)
CONFIG_MISSING=false

for V in ${CONFIG[@]}; do
  if [ -z "${!V}" ]; then
    echo "$V not set"
    CONFIG_MISSING=true
  fi
done

if [ $CONFIG_MISSING = true ]; then
  echo "ERROR: Missing required configuration setting(s) - check ${SCRIPTDIR}/${CONFIGFILE}"
  exit 1
fi

if [ ! -d "$MOUNTPOINT" ]; then
  mkdir "$MOUNTPOINT"
fi

${SCRIPTDIR}/${INSTALLDIR}/ceph-fuse-osiris -m $MON_ADDRESS \
--uid=$OSIRIS_UID --gid=$OSIRIS_GID --groups=$OSIRIS_GROUPS \
-k $KEYRING --id $ID \
--fuse-allow-other=false  --client_try_dentry_invalidate=true --client_die_on_failed_dentry_invalidate=false \
--log_file=${SCRIPTDIR}/cephfs-osiris.log --admin_socket=${SCRIPTDIR}/cephfs-osiris.asok --conf=/dev/null \
--fuse_default_permissions=0 --client_acl_type=posix_acl --fuse_set_user_groups=0 \
--client_mountpoint=$FSROOT $MOUNTPOINT
