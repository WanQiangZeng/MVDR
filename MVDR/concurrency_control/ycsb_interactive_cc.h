#pragma once

#include "config.h"
#include "my_list.h"
#include "para.h"
#include "row.h"
#include "txn.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include <algorithm>
#include <mutex>
#include <queue>

void *run_ycsb_2PL_interactive(void *args);
void *run_ycsb_no_wait_interactive(void *args);
void *run_ycsb_wait_die_interactive(void *args);
void *run_ycsb_wait_die_corou_interactive(void *args);
void *run_ycsb_wound_wait_interactive(void *args);
void *run_ycsb_wound_wait_corou_interactive(void *args);
void *run_ycsb_bamboo_interactive(void *args);
void *run_ycsb_plor_interactive(void *args);

void *run_ycsb_mvdr_interactive(void *args); // 包含四个组件的mvdr
void *run_ycsb_mvdr_no_prio_interactive(void *args);   // 无优先级的mvdr
void *run_ycsb_mvdr_no_repair_interactive(void *args); // 无valid的mvdr
void *run_ycsb_mvdr_no_valid_interactive(void *args);

void *run_ycsb_silo_interactive(void *args);