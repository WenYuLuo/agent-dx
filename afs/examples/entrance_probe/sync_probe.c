#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>
#include <pthread.h>
static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static pthread_once_t once=PTHREAD_ONCE_INIT;
static atomic_ullong calls=0, nanos=0;
static int skip;
static void init(void){real_fsync=dlsym(RTLD_NEXT,"fsync");real_fdatasync=dlsym(RTLD_NEXT,"fdatasync");skip=getenv("CADX_SKIP_NODE_SYNC")!=NULL;}
static uint64_t ns(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (uint64_t)t.tv_sec*1000000000+t.tv_nsec;}
int fsync(int fd){pthread_once(&once,init);uint64_t t=ns();int r=skip?0:real_fsync(fd);atomic_fetch_add(&calls,1);atomic_fetch_add(&nanos,ns()-t);return r;}
int fdatasync(int fd){pthread_once(&once,init);uint64_t t=ns();int r=skip?0:real_fdatasync(fd);atomic_fetch_add(&calls,1);atomic_fetch_add(&nanos,ns()-t);return r;}
__attribute__((destructor)) static void report(void){fprintf(stderr,"CADX_SYNC_PROBE skip=%d calls=%llu cumulative_ns=%llu\n",skip,(unsigned long long)atomic_load(&calls),(unsigned long long)atomic_load(&nanos));}
