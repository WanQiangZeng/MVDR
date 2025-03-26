#ifndef _YCSB_QUERY_H_
#define _YCSB_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;
class Query_thd;

class ycsb_request {
public:
  access_t rtype;
  uint64_t key;
  uint64_t query_id;
  uint64_t request_id; // query中的第几个request
  char value;
  // 用于范围查询
  UInt32 scan_len;
  // 用于silo验证与提交
  char *data_local;
  int64_t last_version = 0;
};

class ycsb_query : public base_query {
public:
  void init(uint64_t thd_id, workload *h_wl) { assert(false); };
  void init(uint64_t thd_id, workload *h_wl, Query_thd *query_thd);
  static void calculateDenom();
  uint64_t get_new_row(); // 返回row_id
  void gen_requests(uint64_t thd_id, workload *h_wl);

  uint64_t request_cnt;
  uint64_t
      local_req_per_query; // 每个query包含的req数，根据是否为长事务，初始化为REQ_PER_QUERY或MAX_ROW_PER_TXN
  bool is_long;            // 根据是否为长事务
  double local_read_perc;
  ycsb_request *requests;

private:
  // for Zipfian distribution
  static double zeta(uint64_t n, double theta);
  uint64_t zipf(uint64_t n, double theta);

  static uint64_t the_n;
  static double denom;
  double zeta_2_theta;
  Query_thd *_query_thd;
};

#endif
