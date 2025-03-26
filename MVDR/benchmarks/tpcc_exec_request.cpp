#include "tpcc_exec_request.h"
#include <cstdint>
#include <cstring>
#include <global.h>

#if WORKLOAD == TPCC
// tpcc两种事务的request执行操作

void exec_tpcc_request(uint64_t qid, uint64_t rid, row_t *row, tpcc_wl *wl) {
  auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                    ->queries[qid % query_cnt_per_thd];
  if (query->type == TPCC_PAYMENT) {
    switch (rid) {
    case 0:
      exec_tpcc_payment_request0(query, row);
      break;
    case 1:
      exec_tpcc_payment_request1(query, row);
      break;
    case 2:
      exec_tpcc_payment_request2(query, row);
      break;
    case 3:
      exec_tpcc_payment_request3(query, row, wl);
      break;
    default:
      assert(false);
    }
  } else if (query->type == TPCC_NEW_ORDER) {
    switch (rid) {
    case 0:
      exec_tpcc_neworder_request0(query, row);
      break;
    case 1:
      exec_tpcc_neworder_request1(query, row);
      break;
    case 2:
      exec_tpcc_neworder_request2(query, row);
      break;
    case 3:
      exec_tpcc_neworder_request3(query, row, wl);
      break;
    case 4:
      exec_tpcc_neworder_request4(query, row, wl);
      break;
    default:
      if (rid >= 5 && rid < 5 + query->ol_cnt) {
        exec_tpcc_neworder_request5(query, row, rid);
      } else if (rid >= 5 + query->ol_cnt && rid < 5 + 2 * query->ol_cnt) {
        exec_tpcc_neworder_request6(query, row, rid);
      } else if (rid >= 5 + 2 * query->ol_cnt && rid < 5 + 3 * query->ol_cnt) {
        exec_tpcc_neworder_request7(query, row, wl, rid);
      } else {
        assert(false);
      }
      break;
    }
  }
}

// payment事务，包含4个request
//  input:w_id,h_amount; output:w_name; wr; W表
void exec_tpcc_payment_request0(tpcc_query *query, row_t *row) {
  double tmp_value;
  char *tmp_str;
  row->get_value(W_YTD, tmp_value);
  row->set_value(W_YTD, tmp_value + query->h_amount);
  tmp_str = row->get_value(W_NAME);
  memcpy(query->w_name, tmp_str, 10);
  query->w_name[10] = '\0';
}

// input:d_w_id,d_id,h_amount; output:d_name; wr; D表
void exec_tpcc_payment_request1(tpcc_query *query, row_t *row) {
  double tmp_value;
  char *tmp_str;
  row->get_value(D_YTD, tmp_value);
  row->set_value(D_YTD, tmp_value + query->h_amount);
  tmp_str = row->get_value(D_NAME);
  memcpy(query->d_name, tmp_str, 10);
  query->d_name[10] = '\0';
}

// input:c_w_id,c_d_id,c_id,h_amount,by_last_name; wr; C表
void exec_tpcc_payment_request2(tpcc_query *query, row_t *row) {
  double c_balance, c_ytd_payment, c_payment_cnt;
  char *c_credit;
  row->get_value(C_BALANCE, c_balance);
  row->set_value(C_BALANCE, c_balance - query->h_amount);
  row->get_value(C_YTD_PAYMENT, c_ytd_payment);
  row->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
  row->get_value(C_PAYMENT_CNT, c_payment_cnt);
  row->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);
  c_credit = row->get_value(C_CREDIT);
  if (strstr(c_credit, "BC") && !TPCC_SMALL) {
    char c_new_data[501];
    sprintf(c_new_data, "| %zu %zu %zu %zu %zu $%7.2f %d", query->c_id,
            query->c_d_id, query->c_w_id, query->d_id, query->w_id,
            query->h_amount, '\0');
    row->set_value("C_DATA", c_new_data);
  }
}

// input:w_name,d_name; insert; H表; 依赖于r0，r1
void exec_tpcc_payment_request3(tpcc_query *query, row_t *row, tpcc_wl *wl) {
  uint64_t row_id;
  char h_data[25];
  strncpy(h_data, query->w_name, 10);
  int length = strlen(h_data);
  if (length > 10) {
    length = 10;
  }
  strcpy(&h_data[length], "    ");
  strncpy(&h_data[length + 4], query->d_name, 10);
  h_data[length + 14] = '\0';
  wl->t_history->get_new_row(row, 0, row_id);

  row->set_value(H_C_ID, query->c_id);
  row->set_value(H_C_D_ID, query->c_d_id);
  row->set_value(H_C_W_ID, query->c_w_id);
  row->set_value(H_D_ID, query->d_id);
  row->set_value(H_W_ID, query->w_id);
  int64_t date = 2013;
  row->set_value(H_DATE, date);
  row->set_value(H_AMOUNT, query->h_amount);
#if !TPCC_SMALL
  row->set_value(H_DATA, h_data);
#endif
}

// neworder事务，包含5+3*ol_cnt个request
// input:w_id; output:w_tax; rd; W表
void exec_tpcc_neworder_request0(tpcc_query *query, row_t *row) {
  row->get_value(W_TAX, query->w_tax);
}

// input:d_w_id,d_id; output:d_tax,o_id; wr; D表
void exec_tpcc_neworder_request1(tpcc_query *query, row_t *row) {
  int64_t o_id;
  row->get_value(D_TAX, query->d_tax);
  row->get_value(D_NEXT_O_ID, o_id);
  o_id++;
  row->set_value(D_NEXT_O_ID, o_id);
  query->o_id = o_id;
}

// input:c_id,c_d_id,c_w_id; output:c_discount; rd; C表
void exec_tpcc_neworder_request2(tpcc_query *query, row_t *row) {
  if (!TPCC_SMALL) {
    row->get_value(C_LAST);
    row->get_value(C_CREDIT);
  }
  row->get_value(C_DISCOUNT, query->c_discount);
}

// input:o_id,d_id,w_id; insert; NO表; 依赖于r1
void exec_tpcc_neworder_request3(tpcc_query *query, row_t *row, tpcc_wl *wl) {
  uint64_t row_id;
  wl->t_neworder->get_new_row(row, 0, row_id);
  row->set_value(NO_O_ID, query->o_id);
  row->set_value(NO_D_ID, query->d_id);
  row->set_value(NO_W_ID, query->w_id);
}

// input:o_id,d_id,w_id,c_id,ol_cnt,o_entry_d,remote; output:o_d_id; insert;
// O表; 依赖于r1
void exec_tpcc_neworder_request4(tpcc_query *query, row_t *row, tpcc_wl *wl) {
  int64_t all_local;
  uint64_t row_id;
  wl->t_order->get_new_row(row, 0, row_id);
  row->set_value(O_ID, query->o_id);
  row->set_value(O_C_ID, query->c_id);
  row->set_value(O_D_ID, query->d_id);
  row->set_value(O_W_ID, query->w_id);
  row->set_value(O_ENTRY_D, query->o_entry_d);
  row->set_value(O_OL_CNT, query->ol_cnt);
  query->o_d_id = query->d_id;
  all_local = (query->remote ? 0 : 1);
  row->set_value(O_ALL_LOCAL, all_local);
  // {
  //   lock_guard<mutex> lock(wl->t_order_mutex);
  //   wl->index_insert(wl->i_order, query->id * query->request_cnt + 3, row,
  //                    wh_to_part(query->w_id));
  // }
}

// input:ol_i_id; output:i_price; ol_cnt个rd操作； I表
void exec_tpcc_neworder_request5(tpcc_query *query, row_t *row, uint64_t rid) {
  row->get_value(I_PRICE, query->i_price);
  row->get_value(I_NAME);
  row->get_value(I_DATA);
  assert(row->data);
}

// input:ol_i_id,ol_supply_w_id,ol_quantity; output:s_dist_01-10;
// ol_cnt个wr操作; S表
void exec_tpcc_neworder_request6(tpcc_query *query, row_t *row, uint64_t rid) {
  uint64_t ol_quantity, quantity;
  int64_t s_ytd, s_order_cnt, s_remote_cnt, s_quantity;
  ol_quantity = query->items[rid - 5 - query->ol_cnt].ol_quantity;
  s_quantity = *(int64_t *)row->get_value(S_QUANTITY);
#if !TPCC_SMALL
  {
    lock_guard<mutex> lock(query->query_mutex);
    query->s_dist_01 = (char *)row->get_value(S_DIST_01);
    query->s_dist_02 = (char *)row->get_value(S_DIST_02);
    query->s_dist_03 = (char *)row->get_value(S_DIST_03);
    query->s_dist_04 = (char *)row->get_value(S_DIST_04);
    query->s_dist_05 = (char *)row->get_value(S_DIST_05);
    query->s_dist_06 = (char *)row->get_value(S_DIST_06);
    query->s_dist_07 = (char *)row->get_value(S_DIST_07);
    query->s_dist_08 = (char *)row->get_value(S_DIST_08);
    query->s_dist_09 = (char *)row->get_value(S_DIST_09);
    query->s_dist_10 = (char *)row->get_value(S_DIST_10);
  }
  row->get_value(S_YTD, s_ytd);
  row->set_value(S_YTD, s_ytd + ol_quantity);
  row->get_value(S_ORDER_CNT, s_order_cnt);
  row->set_value(S_ORDER_CNT, s_order_cnt + 1);
#endif
  if (query->remote) {
    s_remote_cnt = *(int64_t *)row->get_value(S_REMOTE_CNT);
    s_remote_cnt++;
    row->set_value(S_REMOTE_CNT, &s_remote_cnt);
  }
  if (s_quantity > ol_quantity + 10) {
    quantity = s_quantity - ol_quantity;
  } else {
    quantity = s_quantity - ol_quantity + 91;
  }
  row->set_value(S_QUANTITY, &quantity);
}

// input:ol_i_id,ol_supply_w_id,ol_quantity,o_id,o_d_id,i_price,w_tax,d_tax,c_discount;
// ol_cnt个insert操作; OL表; 依赖于r3,r4,r5,r6
void exec_tpcc_neworder_request7(tpcc_query *query, row_t *row, tpcc_wl *wl,
                                 uint64_t rid) {
  uint64_t ol_i_id, ol_supply_w_id, ol_quantity, row_id;
  int64_t ol_amount;
  ol_i_id = query->items[rid - 5 - 2 * query->ol_cnt].ol_i_id;
  ol_supply_w_id = query->items[rid - 5 - 2 * query->ol_cnt].ol_supply_w_id;
  ol_quantity = query->items[rid - 5 - 2 * query->ol_cnt].ol_quantity;
  wl->t_orderline->get_new_row(row, 0, row_id);
  row->set_value(OL_O_ID, &query->o_id);
  row->set_value(OL_D_ID, &query->d_id);
  row->set_value(OL_W_ID, &query->w_id);
  row->set_value(OL_NUMBER, &rid - 5 - 2 * query->ol_cnt);
  row->set_value(OL_I_ID, &ol_i_id);
  // deal with district
#if !TPCC_SMALL
  if (query->o_d_id == 1) {
    row->set_value(OL_DIST_INFO, &query->s_dist_01);
  } else if (query->o_d_id == 2) {
    row->set_value(OL_DIST_INFO, &query->s_dist_02);
  } else if (query->o_d_id == 3) {
    row->set_value(OL_DIST_INFO, &query->s_dist_03);
  } else if (query->o_d_id == 4) {
    row->set_value(OL_DIST_INFO, &query->s_dist_04);
  } else if (query->o_d_id == 5) {
    row->set_value(OL_DIST_INFO, &query->s_dist_05);
  } else if (query->o_d_id == 6) {
    row->set_value(OL_DIST_INFO, &query->s_dist_06);
  } else if (query->o_d_id == 7) {
    row->set_value(OL_DIST_INFO, &query->s_dist_07);
  } else if (query->o_d_id == 8) {
    row->set_value(OL_DIST_INFO, &query->s_dist_08);
  } else if (query->o_d_id == 9) {
    row->set_value(OL_DIST_INFO, &query->s_dist_09);
  } else if (query->o_d_id == 10) {
    row->set_value(OL_DIST_INFO, &query->s_dist_10);
  }
#endif
#if !TPCC_SMALL
  ol_amount = ol_quantity * query->i_price * (1 + query->w_tax + query->d_tax) *
              (1 - query->c_discount);
  row->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
  row->set_value(OL_QUANTITY, &ol_quantity);
  row->set_value(OL_AMOUNT, &ol_amount);
#endif
#if !TPCC_SMALL
  {
    lock_guard<mutex> lock(query->query_mutex);
    query->sum += ol_amount;
  }
#endif
  // {
  //   lock_guard<mutex> lock(wl->t_orderline_mutex);
  //   wl->index_insert(wl->i_orderline, query->id * query->request_cnt + rid,
  //   row,
  //                    wh_to_part(query->w_id));
  // }
}
#endif