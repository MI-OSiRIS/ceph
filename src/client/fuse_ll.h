// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#define FUSE_USE_VERSION 30
#include <fuse.h>
#include <fuse_lowlevel.h>

struct fuse_req {
  struct fuse_session *se;
  uint64_t unique;
  int ctr;
  pthread_mutex_t lock;
  struct fuse_ctx ctx;
  struct fuse_chan *ch;
  gid_t* server_groups;
  size_t server_ngroups;
  int interrupted;
  unsigned int ioctl_64bit : 1;
  union {
    struct {
      uint64_t unique;
    } i;
    struct {
      fuse_interrupt_func_t func;
      void *data;
    } ni;
  } u;
  struct fuse_req *next;
  struct fuse_req *prev;
};

class CephFuse {
public:
  CephFuse(Client *c, int fd);
  ~CephFuse();
  int init(int argc, const char *argv[]);
  int start();
  int mount();
  int loop();
  void finalize();
  void set_perms(UserPerm& perms_);
  class Handle;
  std::string get_mount_point() const;
private:
  CephFuse::Handle *_handle;
  UserPerm perms;
};

class CephFuse::Handle {
public:
  Handle(Client *c, int fd);
  ~Handle();

  int init(int argc, const char *argv[]);
  int start();
  int loop();
  void finalize();

  uint64_t fino_snap(uint64_t fino);
  uint64_t make_fake_ino(inodeno_t ino, snapid_t snapid);
  Inode * iget(fuse_ino_t fino);
  void iput(Inode *in);
  void update_req_perms(fuse_req_t&);
  void set_perms(UserPerm& perms_);

  int fd_on_success;
  Client *client;

  struct fuse_chan *ch;
  struct fuse_session *se;
  char *mountpoint;

  Mutex stag_lock;
  int last_stag;

  ceph::unordered_map<uint64_t,int> snap_stag_map;
  ceph::unordered_map<int,uint64_t> stag_snap_map;

  UserPerm perms;

  pthread_key_t fuse_req_key = 0;
  void set_fuse_req(fuse_req_t);
  fuse_req_t get_fuse_req();

  struct fuse_args args;
};
