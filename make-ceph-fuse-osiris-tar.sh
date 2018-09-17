#!/bin/bash

# Run from inside ceph directory
# If updating to new ceph, use 'rm -rf build' before running this script

if [ ! -d build ]; then
  ./do_cmake.sh -DWITH_RDMA=0  -DALLOCATOR=libc
fi

(cd build && make -j32 ceph-fuse)
mv build/bin/ceph-fuse build/bin/ceph-fuse-osiris

mkdir -p ceph-fuse-osiris-temp/ceph-fuse-osiris/lib
cp -r build/bin ceph-fuse-osiris-temp/ceph-fuse-osiris/
cp -r build/include ceph-fuse-osiris-temp/ceph-fuse-osiris/
cp build/lib/*.[^a] ceph-fuse-osiris-temp/ceph-fuse-osiris/lib/
cp ceph-fuse-osiris ceph-fuse-osiris-temp/ceph-fuse-osiris/

cd ceph-fuse-osiris-temp
tar -cvzf ceph-fuse-osiris.tar.gz ceph-fuse-osiris
mv ceph-fuse-osiris.tar.gz ..
cd ..
rm -rf ceph-fuse-osiris-temp
