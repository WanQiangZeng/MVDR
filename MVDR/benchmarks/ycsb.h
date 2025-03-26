#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "global.h"
#include "helper.h"
#include "wl.h"

class ycsb_query;

class ycsb_wl : public workload {
public:
  RC init();
  RC init_table();
  // 通过目录初始化表的属性
  RC init_schema(string schema_file);
  int key_to_part(uint64_t key);
  INDEX *the_index;
  table_t *the_table; // g_synth_table_size行数

private:
  void init_table_parallel();
  void *init_table_slice();
  static void *threadInitTable(void *This) {
    ((ycsb_wl *)This)->init_table_slice();
    return NULL;
  }
  pthread_mutex_t insert_lock;
  static int next_tid;
};

// class ycsb_txn_man : public txn_man {
// public:
//   void init(thread_t *h_thd, workload *h_wl, uint64_t part_id);
//   RC exec_txn(base_query *query);

// private:
// #if CC_ALG != BAMBOO
//   uint64_t row_cnt;
// #endif
//   ycsb_wl *_wl;
// };

#endif
