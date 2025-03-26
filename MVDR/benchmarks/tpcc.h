#ifndef _TPCC_H_
#define _TPCC_H_

#include "wl.h"
#include <mutex>

class table_t;
class INDEX;
class tpcc_query;

class tpcc_wl : public workload {
public:
  RC init();
  RC init_table();
  RC init_schema(const char *schema_file);
  table_t *t_warehouse; // key[1,g_num_wh]
  table_t *t_district;  // key[1,DIST_PER_WARE*g_num_wh]
  table_t *t_customer;  // key[1,DIST_PER_WARE*g_num_wh*g_cust_per_dist]
  table_t *t_history;
  table_t *t_neworder;
  table_t *t_order; // key[1,DIST_PER_WARE*g_num_wh*g_cust_per_dist]
  table_t *t_orderline;
  table_t *t_item;  // key[1,g_max_items]
  table_t *t_stock; // key[1,g_max_items*g_num_wh]

  // // insert操作的表级锁
  // mutex t_order_mutex;
  // mutex t_orderline_mutex;

  INDEX *i_item;
  INDEX *i_warehouse;
  INDEX *i_district;
  INDEX *i_customer_id;
  INDEX *i_customer_last;
  INDEX *i_stock;
  INDEX *i_order;        // key = (w_id, d_id, o_id)
  INDEX *i_orderline;    // key = (w_id, d_id, o_id)
  INDEX *i_orderline_wd; // key = (w_id, d_id).

  bool **delivering;
  uint32_t next_tid;

private:
  uint64_t num_wh;
  void init_tab_item();
  void init_tab_wh(uint32_t wid);
  void init_tab_dist(uint64_t w_id);
  void init_tab_stock(uint64_t w_id);
  void init_tab_cust(uint64_t d_id, uint64_t w_id);
  void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
  void init_tab_order(uint64_t d_id, uint64_t w_id);

  void init_permutation(uint64_t *perm_c_id, uint64_t wid);

  static void *threadInitItem(void *This);
  static void *threadInitWh(void *This);
  static void *threadInitDist(void *This);
  static void *threadInitStock(void *This);
  static void *threadInitCust(void *This);
  static void *threadInitHist(void *This);
  static void *threadInitOrder(void *This);

  static void *threadInitWarehouse(void *This);
};

// class tpcc_txn_man : public txn_man {
// public:
//   void init(thread_t *h_thd, workload *h_wl, uint64_t part_id);
//   RC exec_txn(base_query *query);

// private:
//   tpcc_wl *_wl;
//   RC exec_payment(tpcc_query *m_query);
//   RC exec_new_order(tpcc_query *m_query);
//   RC exec_order_status(tpcc_query *query);
//   RC exec_delivery(tpcc_query *query);
//   RC exec_stock_level(tpcc_query *query);
//   bool has_local_row(row_t *location, access_t type, row_t *local,
//                      access_t local_type) {
//     if (location == local) {
//       if ((type == local_type) || (local_type == WR)) {
//         return true;
//       } else if (type == WR) {
//         return false;
//       }
//     }
//     return false;
//   };
// };

#endif
