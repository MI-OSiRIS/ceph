# Please run from ceph directory

FULL_TREE=false
LOG_FILE="mount_log"
NUM_ARGS=$#

if [ ! -f ./$LOG_FILE ]
  then
    touch ./$LOG_FILE
fi

while getopts "k:t" opt; do
  case $opt in
    k) KEY_RING="$OPTARG" ;;
    t) FULL_TREE=true ;;
  esac
done

USERNAME=${@:$OPTIND:1}
COU=${@:$OPTIND+1:1}
MOUNT_PT=${@:$OPTIND+2:1}
REST=${@:OPTIND+3:1}

COU=$(echo "$COU" | tr '[:upper:]' '[:lower:]')

if [ $NUM_ARGS -lt 3 -o $NUM_ARGS -gt 6 -o -n "$REST" -o -z "$USERNAME" -o -z "$COU" -o -z "$MOUNT_PT" -o "$USERNAME" == "-k" -o "$USERNAME" == "-t" -o "$COU" == "-k" -o "$COU" == "-t" -o "$MOUNT_PT" == "-k" -o "$MOUNT_PT" == "-t" ]
  then
    echo; echo "Usage: ./mountfs.sh <username> <cou> <mountpoint> [OPTIONS]"
    echo; echo "username is your osiris username as defined in COmanage"
    echo "cou defines your virtual organization. unless -t is specified, you will mount the root of your COU data directory."
    echo "mountpoint defines where in your local directory you would like to mount at."
    echo; echo "OPTIONS:"
    echo "-k <keyring> specifies an alternative keyring from the one linked to your osiris username (e.g. client.osiris.username.keyring)"
    echo "-t specifies that you would like to mount the full COU tree, rather than just the data directory."; echo
    exit 1
fi

if [ -z $KEY_RING ]
  then
    KEY_RING="client.osiris.${USERNAME}.keyring"
fi

if [ ! -f ./$KEY_RING ]
  then
    echo "Key ring $KEY_RING not found. Please ensure the keyring is in your current working directory."
else
  echo "Key ring $KEY_RING found."
fi

if [ $FULL_TREE ]
  then
    echo "ceph-fuse -m mon.osris.org -k $KEYRING --fuse-allow-other=false --id $USERNAME --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r /${COU} --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNT_PT"
    ceph-fuse -m mon.osris.org -k $KEYRING --fuse-allow-other=false --id $USERNAME --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r /${COU} --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNT_PT
else
  echo "ceph-fuse -m mon.osris.org -k $KEYRING --fuse-allow-other=false --id $USERNAME --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r /${COU}/data --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNT_PT"
  ceph-fuse -m mon.osris.org -k $KEYRING --fuse-allow-other=false --id $USERNAME --client_try_dentry_invalidate=true --log_file=cephfs-osiris.log -r /${COU}/data --fuse_default_permissions=0 --client_acl_type=posix_acl --admin_socket=cephfs-osiris.asok $MOUNT_PT
fi
