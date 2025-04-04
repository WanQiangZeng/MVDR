#include "global.h"
#include "mem_alloc.h"
// #include "query.h"
#include "stats.h"

mem_alloc mem_allocator;
Stats stats;
Query_queue *query_queue;

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

ts_t g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_no_dl = NO_DL;
ts_t g_timeout = TIMEOUT;
ts_t g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
UInt32 g_cc_alg = CC_ALG;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_read_perc = READ_PERC;
double g_write_perc = WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
bool g_prt_lat_distr = PRT_LAT_DISTR;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_thread_cnt = THREAD_CNT;
UInt64 g_synth_table_size =
    SYNTH_TABLE_SIZE - (SYNTH_TABLE_SIZE % INIT_PARALLELISM);
UInt32 g_req_per_query = REQ_PER_QUERY;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;
double g_last_retire = BB_LAST_RETIRE;
double g_specified_ratio = SPECIFIED_RATIO;
double g_flip_ratio = FLIP_RATIO;
double g_long_txn_ratio = LONG_TXN_RATIO;
double g_long_txn_read_ratio = LONG_TXN_READ_RATIO;

UInt32 g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
double g_perc_delivery = PERC_DELIVERY;
double g_perc_orderstatus = PERC_ORDERSTATUS;
double g_perc_stocklevel = PERC_STOCKLEVEL;
double g_perc_neworder = 1 - (g_perc_payment + g_perc_delivery +
                              g_perc_orderstatus + g_perc_stocklevel);
bool g_wh_update = WH_UPDATE;
char *output_file = NULL;

map<string, string> g_params;

#if TPCC_SMALL
UInt32 g_max_items = 10000;
UInt32 g_cust_per_dist = 2000;
#else
UInt32 g_max_items = 100000;
UInt32 g_cust_per_dist = 3000;
#endif
