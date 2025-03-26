#pragma once

#include "config.h"
#include "para.h"
#include "row.h"
#include "table.h"
#include "tpcc.h"
#include "tpcc_const.h"
#include "tpcc_helper.h"
#include "tpcc_query.h"

#if WORKLOAD == TPCC

// 用于tpcc两种事务的request执行
void exec_tpcc_request(uint64_t qid, uint64_t rid, row_t *row, tpcc_wl *wl);

// payment事务
void exec_tpcc_payment_request0(tpcc_query *query, row_t *row);
void exec_tpcc_payment_request1(tpcc_query *query, row_t *row);
void exec_tpcc_payment_request2(tpcc_query *query, row_t *row);
void exec_tpcc_payment_request3(tpcc_query *query, row_t *row, tpcc_wl *wl);

// neworder事务
void exec_tpcc_neworder_request0(tpcc_query *query, row_t *row);
void exec_tpcc_neworder_request1(tpcc_query *query, row_t *row);
void exec_tpcc_neworder_request2(tpcc_query *query, row_t *row);
void exec_tpcc_neworder_request3(tpcc_query *query, row_t *row, tpcc_wl *wl);
void exec_tpcc_neworder_request4(tpcc_query *query, row_t *row, tpcc_wl *wl);
void exec_tpcc_neworder_request5(tpcc_query *query, row_t *row, uint64_t rid);
void exec_tpcc_neworder_request6(tpcc_query *query, row_t *row, uint64_t rid);
void exec_tpcc_neworder_request7(tpcc_query *query, row_t *row, tpcc_wl *wl,
                                 uint64_t rid);

#endif