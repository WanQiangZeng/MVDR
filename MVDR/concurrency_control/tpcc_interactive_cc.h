#pragma once

///////////////////////////////////////////////////
// 注意，tpcc事务中应按照request的key区分，而非row的key，因为row的key可能重复

#include "config.h"
#include "my_list.h"
#include "para.h"
#include "row.h"
#include "tpcc.h"
#include "tpcc_exec_request.h"
#include "tpcc_helper.h"
#include "tpcc_query.h"
#include "txn.h"
#include <mutex>
#include <queue>

void *run_tpcc_2PL_interactive(void *args);
void *run_tpcc_no_wait_interactive(void *args);
void *run_tpcc_wait_die_interactive(void *args);
void *run_tpcc_wait_die_corou_interactive(void *args);
void *run_tpcc_wound_wait_interactive(void *args);
void *run_tpcc_wound_wait_corou_interactive(void *args);
void *run_tpcc_bamboo_interactive(void *args);
void *run_tpcc_plor_interactive(void *args);

void *run_tpcc_mvdr_interactive(void *args);
void *run_tpcc_mvdr_no_prio_interactive(void *args);
void *run_tpcc_mvdr_no_repair_interactive(void *args);
void *run_tpcc_mvdr_no_valid_interactive(void *args);

void *run_tpcc_silo_interactive(void *args);