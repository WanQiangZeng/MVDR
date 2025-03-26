#ifndef _TPCC_QUERY_H_
#define _TPCC_QUERY_H_

#include "global.h"
#include "helper.h"
#include "index_hash.h"
#include "query.h"
#include "tpcc.h"
#include "wl.h"
#include <variant>

class workload;

class tpcc_request {
public:
  access_t rtype;
  uint64_t key;
  uint64_t query_id;   // 事务id
  uint64_t request_id; // query中的第几个request
  INDEX *index;
  // 用于silo验证与提交
  char *data_local;
  int64_t last_version = 0;
  tpcc_request(access_t type, uint64_t key, uint64_t query_id,
               uint64_t request_id, INDEX *index)
      : rtype(type), key(key), query_id(query_id), request_id(request_id),
        index(index) {}
};

// new order事务的item_no
struct Item_no {
  uint64_t ol_i_id;
  uint64_t ol_supply_w_id;
  uint64_t ol_quantity;
};

class tpcc_query
    : public base_query { // 不同的事务类型生成不同的参数，不会全部生成，用错了就会段错误
public:
  void init(uint64_t thd_id, workload *h_wl);
  TPCCTxnType type;
  tpcc_request *requests;
  // 根据不同的tpccquery类型，包含的req数不同，分别为4，5+3*ol_cnt
  uint64_t request_cnt;
  tpcc_wl *m_wl;
  /**********************************************/
  // common txn input for both payment & new-order
  /**********************************************/
  uint64_t w_id;
  uint64_t d_id;
  uint64_t c_id;
  /**********************************************/
  // txn input for payment
  /**********************************************/
  uint64_t d_w_id;
  uint64_t c_w_id;
  uint64_t c_d_id;
  char c_last[LASTNAME_LEN];
  double h_amount;
  bool by_last_name;
  /**********************************************/
  // txn input for new-order
  /**********************************************/
  Item_no *items;
  uint64_t rbk;
  bool remote;
  uint64_t ol_cnt;
  uint64_t o_entry_d;
  // Input for delivery
  uint64_t o_carrier_id;
  uint64_t ol_delivery_d;
  /**********************************************/
  // txn input for 上下文依赖项
  /**********************************************/
  char w_name[11];
  char d_name[11];
  int64_t o_id;
  int64_t o_d_id;
  int64_t i_price;
  double w_tax;
  double d_tax;
  uint64_t c_discount;
  char *s_dist_01, *s_dist_02, *s_dist_03, *s_dist_04, *s_dist_05, *s_dist_06,
      *s_dist_07, *s_dist_08, *s_dist_09, *s_dist_10;
  uint64_t sum = 0;

private:
  void gen_payment(uint64_t thd_id);
  void gen_new_order(uint64_t thd_id);
  void gen_order_status(uint64_t thd_id);
  void gen_delivery(uint64_t thd_id);
  void gen_stock_level(uint64_t thd_id);
};

#endif
