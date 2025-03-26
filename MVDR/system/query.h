#pragma once

#include "global.h"
#include "my_list.h"
#include <mutex>
#include <set>

class workload;
class ycsb_query;
class tpcc_query;
class ycsb_request;

extern thread_local drand48_data per_thread_rand_buf;

class base_query {
public:
  virtual void init(uint64_t thd_id, workload *h_wl) = 0;
  uint64_t id; //[0,线程数*request_cnt-1]
  uint64_t waiting_time;
  uint64_t part_num;
  uint64_t *part_to_access;
  // 用于silo，存储request的id，可根据request找到row
  vector<uint64_t> read_set;
  vector<uint64_t> write_set;
  // 标识事务中止
  bool rerun = false;
  uint32_t num_abort = 0;  // 中止数
  uint64_t num_repair = 0; // 修复数

  // 测定事务延迟
  uint64_t starttime;
  uint64_t endtime;
  double timespan; // 事务执行时间
  uint64_t lock_wait_starttime = 0;
  uint64_t lock_wait_endtime = 0;
  double lock_wait_time = 0; // 1.锁的等待时间
  uint64_t commit_wait_starttime = 0;
  uint64_t commit_wait_endtime = 0;
  double commit_wait_time = 0; // 2.提交等待时间
  uint64_t net_times = 0;      // 3.i/o次数,也代表执行过的request次数
  double exec_time = 0; // 实际的执行时间，包括中止时间和有效时间
  double abort_time = 0;  // 4.中止时间
  double useful_time = 0; // 5.有效时间

  mutex query_mutex; // query锁

  bool in_aborted_set = false;
  bool in_commited_query = false;

  // 用于repair算法
  int64_t cid = -1;
  int64_t dep_cnt = 0;
  int64_t be_dep_cnt = 0;
  bool able_to_commit = false; // 当request全部完成后有资格提交
  bool commited = false; // 有资格提交且依赖事务为空时可以提交
  uint64_t next_rid = 0; // 下一个加入req队列的requets的id
  uint64_t prio = 0;     // 优先级
};

// 每个线程包含的事务链表
class Query_thd {
public:
  void init(workload *h_wl, int thread_id);
  base_query *get_next_query();
  uint64_t q_idx;
#if WORKLOAD == YCSB
  ycsb_query *queries;
  ycsb_request *long_txn;
  uint64_t *long_txn_part;
#else
  tpcc_query *queries; // 包含request_cnt个query事务
#endif
  char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
  drand48_data buffer;
  uint64_t request_cnt; // 每个query_thd包含的query数
};

class Query_queue {
public:
  void init(workload *h_wl);
  void init_per_thread(int thread_id);
  base_query *get_next_query(uint64_t thd_id);
  Query_thd **all_queries; // 包含线程数*request_cnt个query事务

private:
  static void *threadInitQuery(void *This);
  workload *_wl;
  static int _next_tid;
};
