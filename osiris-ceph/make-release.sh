#!/bin/bash

# this script will buiild a ceph-fuse-osiris and create distribution tarball ceph-fuse-osiris.tgz
# run script from top level of ceph git repository

# If updating to new ceph, use 'rm -rf build' before running this script

# dist will be created in this directory which will also be top level of tar
BUILD=/tmp
SUBDIR=ceph-fuse-osiris
BUILDTMP=${BUILD}/${SUBDIR}

if [ ! -f do_cmake.sh ]; then
    echo "Run script from top level of ceph git repo"
    exit 1
fi

if [ ! -d build ]; then
  ./do_cmake.sh -DWITH_RDMA=0  -DALLOCATOR=libc 
  # -DENABLE_SHARED=0 -DWITH_STATIC_LIBSTDCXX=1
  
fi

(cd build && make -j32 ceph-fuse)
mv build/bin/ceph-fuse build/bin/ceph-fuse-osiris

mkdir -p ${BUILDTMP}/lib

cp -r build/bin ${BUILDTMP}/
cp -r build/include ${BUILDTMP}/
cp build/lib/*.[^a] ${BUILDTMP}/lib/

# except for these two libs this build works as-is on Ubuntu Xenial
cp /lib64/libcrypto.so.10 /lib64/libssl.so.10 ${BUILDTMP}/lib

cp osiris-ceph/ceph-fuse-osiris ${BUILDTMP}/ceph-fuse-osiris

tar -cvzf ceph-fuse-osiris.tgz -C ${BUILD} ${SUBDIR}
rm -rf ${BUILDTMP}

