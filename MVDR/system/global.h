#pragma once

#include "stdint.h"
#include <cstddef>
#include <cstdlib>
#include <unistd.h>
#define NDEBUG
#include <cassert>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <math.h>
#include <mm_malloc.h>
#include <set>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/time.h>
#include <time.h>
#include <typeinfo>
#include <vector>

#if LATCH == LH_MCSLOCK
#include "mcs_spinlock.h"
#endif
#include "config.h"
#include "mem_alloc.h"
#include "pthread.h"
#include "stats.h"
#ifndef NOGRAPHITE
#include "carbon_user.h"
#endif
// #include "helper.h"

using namespace std;

class mem_alloc;
class Stats;
class Query_queue;
class IndexHash;

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;

typedef uint64_t ts_t;

/******************************************/
// Global Data Structure
/******************************************/
extern mem_alloc mem_allocator;
extern Stats stats;
extern Query_queue *query_queue;

extern bool volatile warmup_finish;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
extern carbon_barrier_t enable_barrier;
#endif

/******************************************/
// Global Parameter
/******************************************/
extern bool g_part_alloc;
extern bool g_mem_pad;
extern bool g_prt_lat_distr;
extern UInt32 g_part_cnt;
extern UInt32 g_virtual_part_cnt;
extern UInt32 g_thread_cnt;
extern ts_t g_abort_penalty;
extern bool g_central_man;
extern UInt32 g_ts_alloc;
extern bool g_key_order;
extern bool g_no_dl;
extern ts_t g_timeout;
extern ts_t g_dl_loop_detect;
extern bool g_ts_batch_alloc;
extern UInt32 g_ts_batch_num;

extern map<string, string> g_params;

// YCSB
extern UInt32 g_cc_alg;
extern ts_t g_query_intvl;
extern UInt32 g_part_per_txn;
extern double g_perc_multi_part;
extern double g_read_perc;
extern double g_write_perc;
extern double g_zipf_theta;
extern UInt64 g_synth_table_size;
extern UInt32 g_req_per_query;
extern UInt32 g_field_per_tuple;
extern UInt32 g_init_parallelism;
extern double g_last_retire;
extern double g_specified_ratio;
extern double g_flip_ratio;
extern double g_long_txn_ratio;
extern double g_long_txn_read_ratio;

// TPCC
extern UInt32 g_num_wh;
extern double g_perc_payment;
extern double g_perc_delivery;
extern double g_perc_orderstatus;
extern double g_perc_stocklevel;
extern double g_perc_neworder;
extern bool g_wh_update;
extern char *output_file;
extern UInt32 g_max_items;
extern UInt32 g_cust_per_dist;

enum RC { RCOK, Commit, Abort, WAIT, ERROR, FINISH };

/* Thread */
typedef uint64_t txnid_t;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t;  // row id
typedef uint64_t pgid_t; // page id

/* INDEX */
enum latch_t { LATCH_EX, LATCH_SH, LATCH_NONE };
enum idx_acc_t { INDEX_INSERT, INDEX_READ, INDEX_NONE };
typedef uint64_t idx_key_t;              // key id for index
typedef uint64_t (*func_ptr)(idx_key_t); // part_id func_ptr(index_key);

/* general concurrency control */
enum access_t { RD, WR, XP, SCAN, CM, IS };
/* LOCK */
enum lock_t { LOCK_EX, LOCK_SH, LOCK_NONE };
enum loc_t { RETIRED, OWNERS, WAITERS, LOC_NONE };
enum lock_status { LOCK_DROPPED, LOCK_WAITER, LOCK_OWNER, LOCK_RETIRED };
/* TIMESTAMP */
enum TsType { R_REQ, W_REQ, P_REQ, XP_REQ };
/* TXN STATUS */
// XXX(zhihan): bamboo requires the enumeration order to be unchanged
enum status_t : unsigned int { RUNNING, ABORTED, COMMITED, HOLDING };

/* COMMUTATIVE OPERATIONS */
enum com_t { COM_INC, COM_DEC, COM_NONE };

#define MSG(str, args...)                                                      \
  { printf("[%s : %d] " str, __FILE__, __LINE__, args); }                      \
//	printf(args); }

// principal index structure. The workload may decide to use a different
// index structure for specific purposes. (e.g. non-primary key access should
// use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX index_btree
#else // IDX_HASH
#define INDEX IndexHash
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX 18446744073709551615UL
#endif // UINT64_MAX
