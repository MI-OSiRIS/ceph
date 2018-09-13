/* config.h file expanded by Cmake for build */

#ifndef CONFIG_H
#define CONFIG_H

/* fallocate(2) is supported */
#define CEPH_HAVE_FALLOCATE

/* Define to 1 if you have the `posix_fadvise' function. */
#define HAVE_POSIX_FADVISE 1

/* Define to 1 if you have the `posix_fallocate' function. */
#define HAVE_POSIX_FALLOCATE 1

/* Define to 1 if you have the `syncfs' function. */
#define HAVE_SYS_SYNCFS 1

/* sync_file_range(2) is supported */
#define HAVE_SYNC_FILE_RANGE

/* Define if you have mallinfo */
/* #undef HAVE_MALLINFO */

/* Define to 1 if you have the `pwritev' function. */
#define HAVE_PWRITEV 1

/* Define to 1 if you have the <sys/mount.h> header file. */
#define HAVE_SYS_MOUNT_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#define HAVE_SYS_VFS_H 1

/* Define to 1 if you have the <execinfo.h> header file. */
#define HAVE_EXECINFO_H 1

/* Define to 1 if the system has the type `__be16'. */
#define HAVE___BE16 1

/* Define to 1 if the system has the type `__be32'. */
#define HAVE___BE32 1

/* Define to 1 if the system has the type `__be64'. */
#define HAVE___BE64 1

/* Define to 1 if the system has the type `__le16'. */
#define HAVE___LE16 1

/* Define to 1 if the system has the type `__le32'. */
#define HAVE___LE32 1

/* Define to 1 if the system has the type `__le64'. */
#define HAVE___LE64 1

/* Define to 1 if the system has the type `__s16'. */
#define HAVE___S16 1

/* Define to 1 if the system has the type `__s32'. */
#define HAVE___S32 1

/* Define to 1 if the system has the type `__s64'. */
#define HAVE___S64 1

/* Define to 1 if the system has the type `__s8'. */
#define HAVE___S8 1

/* Define to 1 if the system has the type `__u16'. */
#define HAVE___U16 1

/* Define to 1 if the system has the type `__u32'. */
#define HAVE___U32 1

/* Define to 1 if the system has the type `__u64'. */
#define HAVE___U64 1

/* Define to 1 if the system has the type `__u8'. */
#define HAVE___U8 1

/* Define if you have res_nquery */
#define HAVE_RES_NQUERY

/* Defined if you have LZ4 */
#define HAVE_LZ4

/* Defined if you have BROTLI */
/* #undef HAVE_BROTLI */

/* Defined if you have libaio */
#define HAVE_LIBAIO

/* Defined if OpenLDAP enabled */
#define HAVE_OPENLDAP

/* Define if you have fuse */
#define HAVE_LIBFUSE

/* Define to 1 if you have libxfs */
#define HAVE_LIBXFS 1

/* SPDK conditional compilation */
#define HAVE_SPDK

/* DPDK conditional compilation */
/* #undef HAVE_DPDK */

/* PMEM conditional compilation */
/* #undef HAVE_PMEM */

/* Defined if LevelDB supports bloom filters */
#define HAVE_LEVELDB_FILTER_POLICY

/* Define if you have tcmalloc */
/* #undef HAVE_LIBTCMALLOC */

/* Define if you have jemalloc */
/* #undef HAVE_LIBJEMALLOC */

/* Define if have curl_multi_wait() */
#define HAVE_CURL_MULTI_WAIT 1

/* Define if using NSS. */
#define USE_NSS

/* Define if using OpenSSL. */
#define USE_OPENSSL

/* Accelio conditional compilation */
/* #undef HAVE_XIO */


/* AsyncMessenger RDMA conditional compilation */
/* #undef HAVE_RDMA */

/* ibverbs experimental conditional compilation */
/* #undef HAVE_IBV_EXP */

/* define if bluestore enabled */
#define WITH_BLUESTORE

/* define if cephfs enabled */
#define WITH_CEPHFS

/* define if rbd enabled */
#define WITH_RBD

/* define if kernel rbd enabled */
#define WITH_KRBD

/* define if key-value-store is enabled */
#define WITH_KVS

/* define if radosgw enabled */
#define WITH_RADOSGW

/* define if radosgw enabled */
/* #undef WITH_RADOSGW_FCGI_FRONTEND */

/* define if leveldb is enabled */
#define WITH_LEVELDB

/* define if radosgw's beast frontend enabled */
#define WITH_RADOSGW_BEAST_FRONTEND

/* define if radosgw has openssl support */
/* #undef WITH_CURL_OPENSSL */

/* define if HAVE_THREAD_SAFE_RES_QUERY */
/* #undef HAVE_THREAD_SAFE_RES_QUERY */

/* define if HAVE_REENTRANT_STRSIGNAL */
/* #undef HAVE_REENTRANT_STRSIGNAL */

/* Define if you want to use LTTng */
#define WITH_LTTNG

/* Define if you want to OSD function instrumentation */
/* #undef WITH_OSD_INSTRUMENT_FUNCTIONS */

/* Define if you want to use Babeltrace */
#define WITH_BABELTRACE

/* Define to 1 if you have the <babeltrace/babeltrace.h> header file. */
#define HAVE_BABELTRACE_BABELTRACE_H 1

/* Define to 1 if you have the <babeltrace/ctf/events.h> header file. */
#define HAVE_BABELTRACE_CTF_EVENTS_H 1

/* Define to 1 if you have the <babeltrace/ctf/iterator.h> header file. */
#define HAVE_BABELTRACE_CTF_ITERATOR_H 1

/* Define to 1 if you have the <arpa/nameser_compat.h> header file. */
#define HAVE_ARPA_NAMESER_COMPAT_H 1

/* FastCGI headers are in /usr/include/fastcgi */
/* #undef FASTCGI_INCLUDE_DIR */

/* splice(2) is supported */
#define CEPH_HAVE_SPLICE

/* Define if you want C_Gather debugging */
#define DEBUG_GATHER

/* Define to 1 if you have the `getgrouplist' function. */
#define HAVE_GETGROUPLIST 1

/* LTTng is disabled, so define this macro to be nothing. */
/* #undef tracepoint */

/* Define to 1 if you have fdatasync. */
#define HAVE_FDATASYNC 1

/* Defined if you have librocksdb enabled */
#define HAVE_LIBROCKSDB

/* Define to 1 if you have the <valgrind/helgrind.h> header file. */
#define HAVE_VALGRIND_HELGRIND_H 1

/* Define to 1 if you have the <sys/prctl.h> header file. */
#define HAVE_SYS_PRCTL_H 1

/* Define to 1 if you have the <linux/types.h> header file. */
#define HAVE_LINUX_TYPES_H 1

/* Define to 1 if you have the <linux/version.h> header file. */
#define HAVE_LINUX_VERSION_H 1

/* Define to 1 if you have sched.h. */
#define HAVE_SCHED 1

/* Support SSE (Streaming SIMD Extensions) instructions */
/* #undef HAVE_SSE */

/* Support SSE2 (Streaming SIMD Extensions 2) instructions */
/* #undef HAVE_SSE2 */

/* Define to 1 if you have the `pipe2' function. */
#define HAVE_PIPE2 1

/* Support NEON instructions */
/* #undef HAVE_NEON */

/* Define if you have pthread_spin_init */
#define HAVE_PTHREAD_SPINLOCK

/* name_to_handle_at exists */
#define HAVE_NAME_TO_HANDLE_AT

/* we have a recent yasm and are x86_64 */
#define HAVE_GOOD_YASM_ELF64 

/* yasm can also build the isa-l */
#define HAVE_BETTER_YASM_ELF64

/* Define to 1 if strerror_r returns char *. */
#define STRERROR_R_CHAR_P 1

/* Defined if you have libzfs enabled */
/* #undef HAVE_LIBZFS */

/* Define if the C complier supports __func__ */
#define HAVE_FUNC

/* Define if the C complier supports __PRETTY_FUNCTION__ */
#define HAVE_PRETTY_FUNC

/* F_SETPIPE_SZ is supported */
#define CEPH_HAVE_SETPIPE_SZ

/* Have eventfd extension. */
#define HAVE_EVENTFD

/* Define if enabling coverage. */
/* #undef ENABLE_COVERAGE */

/* Defined if you want pg ref debugging */
/* #undef PG_DEBUG_REFS */

/* Support ARMv8 CRC instructions */
/* #undef HAVE_ARMV8_CRC */

/* Support ARMv8 CRYPTO instructions */
/* #undef HAVE_ARMV8_CRYPTO */

/* Support ARMv8 CRC and CRYPTO intrinsics */
/* #undef HAVE_ARMV8_CRC_CRYPTO_INTRINSICS */

/* Define if you have struct stat.st_mtimespec.tv_nsec */
/* #undef HAVE_STAT_ST_MTIMESPEC_TV_NSEC */

/* Define if you have struct stat.st_mtim.tv_nsec */
#define HAVE_STAT_ST_MTIM_TV_NSEC

/* Define if compiler supports static_cast<> */
/* #undef HAVE_STATIC_CAST */

/* Version number of package */
#define VERSION "13.2.1"

/* Defined if pthread_setname_np() is available */
#define HAVE_PTHREAD_SETNAME_NP 1

/* Defined if pthread_rwlockattr_setkind_np() is available */
#define HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP

/* Defined if blkin enabled */
/* #undef WITH_BLKIN */

/* Defined if pthread_set_name_np() is available */
/* #undef HAVE_PTHREAD_SET_NAME_NP */

/* Defined if pthread_getname_np() is available */
#define HAVE_PTHREAD_GETNAME_NP 1

/* Support POWER8 instructions */
/* #undef HAVE_POWER8 */

/* Define if endian type is big endian */
/* #undef CEPH_BIG_ENDIAN */

/* Define if endian type is little endian */
#define CEPH_LITTLE_ENDIAN

#define PYTHON_EXECUTABLE "/usr/bin/python2.7"

/* Define to 1 if you have the `getprogname' function. */
/* #undef HAVE_GETPROGNAME */

/* Defined if getentropy() is available */
/* #undef HAVE_GETENTROPY */

/* Defined if boost::context is available */
#define HAVE_BOOST_CONTEXT

/* Defined if OpenSSL is available for the rgw beast frontend */
#define WITH_RADOSGW_BEAST_OPENSSL

/* Defined if std::map::merge() is supported */
#define HAVE_STDLIB_MAP_SPLICING

#endif /* CONFIG_H */
