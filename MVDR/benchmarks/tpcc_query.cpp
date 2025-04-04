#include "tpcc_query.h"
#include "mem_alloc.h"
#include "query.h"
#include "table.h"
#include "tpcc.h"
#include "tpcc_const.h"
#include "tpcc_helper.h"
#include "wl.h"

void tpcc_query::init(uint64_t thd_id, workload *h_wl) {
  // base_query init
  num_abort = 0;
  m_wl = static_cast<tpcc_wl *>(h_wl);
  // double y;
  // drand48_r(&per_thread_rand_buf, &y);
  // prio = y < HIGH_PRIO_RATIO ? ((SILO_PRIO_MAX_PRIO + 1) / 2) : 0;
  // max_prio = y < HIGH_PRIO_RATIO ? SILO_PRIO_MAX_PRIO : LOW_PRIO_BOUND;
  // tpcc query
  double x = (double)(rand() % 100) / 100.0;
  part_to_access =
      (uint64_t *)mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
  if (x < g_perc_payment) {
    gen_payment(thd_id);
  } else if (x < (g_perc_payment + g_perc_delivery))
    gen_delivery(thd_id);
  else if (x < (g_perc_payment + g_perc_delivery + g_perc_orderstatus))
    gen_order_status(thd_id);
  else if (x < (g_perc_payment + g_perc_delivery + g_perc_orderstatus +
                g_perc_stocklevel))
    gen_stock_level(thd_id);
  else {
    gen_new_order(thd_id);
  }
}

void tpcc_query::gen_payment(uint64_t thd_id) {
  type = TPCC_PAYMENT;
  if (FIRST_PART_LOCAL)
    w_id = thd_id % g_num_wh + 1;
  else
    w_id = URand(1, g_num_wh, thd_id % g_num_wh);
  d_w_id = w_id;
  uint64_t part_id = wh_to_part(w_id);
  part_to_access[0] = part_id;
  part_num = 1;

  d_id = URand(1, DIST_PER_WARE, w_id - 1);
  h_amount = URand(1, 5000, w_id - 1);
  int x = URand(1, 100, w_id - 1);
  int y = URand(1, 100, w_id - 1);

  if (x <= 85) {
    // home warehouse
    c_d_id = d_id;
    c_w_id = w_id;
  } else {
    // remote warehouse
    c_d_id = URand(1, DIST_PER_WARE, w_id - 1);
    if (g_num_wh > 1) {
      while ((c_w_id = URand(1, g_num_wh, w_id - 1)) == w_id) {
      }
      if (wh_to_part(w_id) != wh_to_part(c_w_id)) {
        part_to_access[1] = wh_to_part(c_w_id);
        part_num = 2;
      }
    } else
      c_w_id = w_id;
  }
  if (y <= 60) {
    // by last name
    by_last_name = true;
    Lastname(NURand(255, 0, 999, w_id - 1), c_last);
  } else {
    // by cust id
    by_last_name = false;
    c_id = NURand(1023, 1, g_cust_per_dist, w_id - 1);
  }

  // 初始化request,payment事务包含4个reuqest
  request_cnt = 4;
  requests = (tpcc_request *)mem_allocator.alloc(
      sizeof(tpcc_request) * request_cnt, thd_id);
  requests[0] = tpcc_request(WR, w_id, id, 0, m_wl->i_warehouse);
  requests[1] =
      tpcc_request(WR, distKey(d_id, d_w_id), id, 1, m_wl->i_district);
  if (by_last_name) {
    requests[2] = tpcc_request(WR, custNPKey(c_last, c_d_id, c_w_id), id, 2,
                               m_wl->i_customer_last);
  } else {
    requests[2] = tpcc_request(WR, custKey(c_id, c_d_id, c_w_id), id, 2,
                               m_wl->i_customer_id);
  }
  requests[3] = tpcc_request(IS, 0, id, 3, NULL);
}

void tpcc_query::gen_new_order(uint64_t thd_id) {
  type = TPCC_NEW_ORDER;
  if (FIRST_PART_LOCAL)
    w_id = thd_id % g_num_wh + 1;
  else
    w_id = URand(1, g_num_wh, thd_id % g_num_wh);
  d_id = URand(1, DIST_PER_WARE, w_id - 1);
  c_id = NURand(1023, 1, g_cust_per_dist, w_id - 1);
  rbk = URand(1, 100, w_id - 1);
  ol_cnt = URand(5, 15, w_id - 1);
  o_entry_d = 2013;
  items = (Item_no *)_mm_malloc(sizeof(Item_no) * ol_cnt, 64);
  remote = false;
  part_to_access[0] = wh_to_part(w_id);
  part_num = 1;

  for (UInt32 oid = 0; oid < ol_cnt; oid++) {
    items[oid].ol_i_id = NURand(8191, 1, g_max_items, w_id - 1);
    // #if TPCC_USER_ABORT
    //     // simulate user data entry errors and exercise the performance of
    //     // rolling back update transactions.
    //     // If this is the last item on the order and rbk = 1 (chosen from [1,
    //     // 100]), then the item number is set to an unused value.
    //     if ((oid == ol_cnt - 1) && (rbk == 1)) {
    //       items[oid].ol_i_id = 0;
    //     }
    // #endif
    UInt32 x = URand(1, 100, w_id - 1);
    if (x > 1 || g_num_wh == 1)
      items[oid].ol_supply_w_id = w_id;
    else {
      while ((items[oid].ol_supply_w_id = URand(1, g_num_wh, w_id - 1)) ==
             w_id) {
      }
      remote = true;
    }
    items[oid].ol_quantity = URand(1, 10, w_id - 1);
  }
  for (UInt32 i = 0; i < ol_cnt; i++) {
    for (UInt32 j = 0; j < i; j++) {
      if (items[i].ol_i_id == items[j].ol_i_id) {
        for (UInt32 k = i; k < ol_cnt - 1; k++)
          items[k] = items[k + 1];
        ol_cnt--;
        i--;
      }
    }
  }
  for (UInt32 i = 0; i < ol_cnt; i++)
    for (UInt32 j = 0; j < i; j++)
      assert(items[i].ol_i_id != items[j].ol_i_id);
  for (UInt32 i = 0; i < ol_cnt; i++) {
    UInt32 j;
    for (j = 0; j < part_num; j++)
      if (part_to_access[j] == wh_to_part(items[i].ol_supply_w_id))
        break;
    if (j == part_num)
      part_to_access[part_num++] = wh_to_part(items[i].ol_supply_w_id);
  }

  // 初始化request,neworder事务包含5 + 3 * ol_cnt个reuqest
  request_cnt = 5 + 3 * ol_cnt;
  requests = (tpcc_request *)mem_allocator.alloc(
      sizeof(tpcc_request) * request_cnt, thd_id);
  requests[0] = tpcc_request(RD, w_id, id, 0, m_wl->i_warehouse);
  requests[1] = tpcc_request(WR, distKey(d_id, w_id), id, 1, m_wl->i_district);
  requests[2] =
      tpcc_request(RD, custKey(c_id, d_id, w_id), id, 2, m_wl->i_customer_id);
  requests[3] = tpcc_request(IS, 0, id, 3, NULL);
  requests[4] = tpcc_request(IS, 0, id, 4, NULL);
  uint64_t rid = 5;
  for (; rid < 5 + ol_cnt; rid++) {
    requests[rid] =
        tpcc_request(RD, items[rid - 5].ol_i_id, id, rid, m_wl->i_item);
  }
  for (; rid < 5 + 2 * ol_cnt; rid++) {
    requests[rid] =
        tpcc_request(WR,
                     stockKey(items[rid - 5 - ol_cnt].ol_i_id,
                              items[rid - 5 - ol_cnt].ol_supply_w_id),
                     id, rid, m_wl->i_stock);
  }
  for (; rid < 5 + 3 * ol_cnt; rid++) {
    requests[rid] = tpcc_request(IS, 0, id, rid, NULL);
  }
}

void tpcc_query::gen_order_status(uint64_t thd_id) {
  type = TPCC_ORDER_STATUS;
  if (FIRST_PART_LOCAL)
    w_id = thd_id % g_num_wh + 1;
  else
    w_id = URand(1, g_num_wh, thd_id % g_num_wh);
  d_id = URand(1, DIST_PER_WARE, w_id - 1);
  c_w_id = w_id;
  c_d_id = d_id;
  int y = URand(1, 100, w_id - 1);
  if (y <= 60) {
    // by last name
    by_last_name = true;
    Lastname(NURand(255, 0, 999, w_id - 1), c_last);
  } else {
    // by cust id
    by_last_name = false;
    c_id = NURand(1023, 1, g_cust_per_dist, w_id - 1);
  }
}

void tpcc_query::gen_delivery(uint64_t thd_id) { type = TPCC_DELIVERY; }

void tpcc_query::gen_stock_level(uint64_t thd_id) { type = TPCC_STOCK_LEVEL; }
