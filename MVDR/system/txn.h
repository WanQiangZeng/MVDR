#pragma once

#include "global.h"
#include "helper.h"
#include "index_hash.h"
#include "para.h"
#include "row.h"
#include "tpcc.h"
#include "tpcc_query.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include <mutex>
#include <tpcc_helper.h>

#if WORKLOAD == YCSB
// 适用于ycsb
inline void get_itemid(ycsb_wl *wl, uint64_t key, itemid_t *&item) {
  int part_id = wl->key_to_part(key);
  wl->the_index->index_read(key, item, part_id, 0);
}

inline void get_row(ycsb_wl *wl, uint64_t key, row_t *&row) {
  itemid_t *item = nullptr;
  get_itemid(wl, key, item);
  row = static_cast<row_t *>(item->location);
}

inline void read(ycsb_request *request, row_t *row) {
  char *data = row->get_data();
  for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
    __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
  }
}

inline void write(ycsb_request *request, row_t *row) { // 仅写入本地数据
  char *data = row->get_data();
  for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
    *(uint64_t *)(&data[fid * 10]) = 0;
  }
}

inline void read_mv_node(row_t *row, mv_data *mv_node) {
  char *data = mv_node->data;
  for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
    __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
  }
}

inline void write_mv_node(row_t *row, mv_data *mv_node) {
  char *data = mv_node->data;
  for (uint64_t fid = 0; fid < row->get_field_cnt(); fid++) {
    *(uint64_t *)(&data[fid * 10]) = 0;
  }
}

// 适用于tpcc
#elif WORKLOAD == TPCC

inline void get_itemid(INDEX *index, uint64_t key, uint64_t part_id,
                       itemid_t *&item) {
  index->index_read(key, item, part_id, 0);
}

inline void get_row(INDEX *index, uint64_t key, uint64_t part_id, row_t *&row) {
  itemid_t *item = nullptr;
  get_itemid(index, key, part_id, item);
  row = static_cast<row_t *>(item->location);
}
#endif

inline void repair(uint64_t qid, uint64_t rid) {
  auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                    ->queries[qid % query_cnt_per_thd];
  {
    lock_guard<mutex> lock(query->query_mutex);
    if (query->next_rid > rid && !query->rerun && query->request_cnt > rid) {
      // if (query->next_rid > rid) {
      query->rerun = true;
      query->next_rid = rid;
      query->num_repair++;
      if ((query->able_to_commit && !query->commited &&
           !query->in_commited_query)) {
        query->able_to_commit = false;
        {
          lock_guard<mutex> lock(req_queue_mutex);
          req_queue.push(make_pair(qid, query->next_rid));
        }
        query->next_rid++;
      }
      ATOM_ADD(repair_cnt, 1);
    }
  }
}
// 用于支持优先级的request插入
inline void insert_request_prio(uint64_t qid) {
  {
    lock_guard<mutex> lock(req_deque_mutex);
    auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                      ->queries[qid % query_cnt_per_thd];
    if (query->be_dep_cnt > g_be_dep_cnt) {
      auto it = req_deque.begin();
      for (auto it = req_deque.begin(); it != req_deque.end(); it++) {
        auto query_tmp =
            &query_queue->all_queries[it->first / query_cnt_per_thd]
                 ->queries[it->first % query_cnt_per_thd];
        if (query_tmp->be_dep_cnt < query->be_dep_cnt) {
          req_deque.insert(it, make_pair(qid, query->next_rid));
          break;
        }
      }
      if (it == req_deque.end()) {
        req_deque.push_back(make_pair(qid, query->next_rid));
      }
    } else {
      req_deque.push_back(make_pair(qid, query->next_rid));
    }
  }
}
// 用于mvdr的优先级
inline void repair_deque(uint64_t qid, uint64_t rid) {
  auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                    ->queries[qid % query_cnt_per_thd];
  {
    lock_guard<mutex> lock(query->query_mutex);
    if (query->next_rid > rid && !query->rerun && query->request_cnt > rid &&
        !query->in_commited_query) {
      // if (query->next_rid > rid) {
      query->rerun = true;
      query->next_rid = rid;
      query->num_repair++;
      if ((query->able_to_commit && !query->commited &&
           !query->in_commited_query)) {
        query->able_to_commit = false;
        insert_request_prio(qid);
        query->next_rid++;
      }
      ATOM_ADD(repair_cnt, 1);
    }
  }
}

inline void abort(uint64_t qid) {
  auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                    ->queries[qid % query_cnt_per_thd];
  {
    lock_guard<mutex> lock(query->query_mutex);
    if (query->next_rid > 0 && !query->rerun) {
      query->rerun = true;
      query->next_rid = 0;
      query->num_abort++;
      if ((query->able_to_commit && !query->commited &&
           !query->in_commited_query)) {
        query->able_to_commit = false;
        {
          lock_guard<mutex> lock(req_queue_mutex);
          req_queue.push(make_pair(qid, query->next_rid));
        }
        query->next_rid++;
      }
      query->able_to_commit = false;
      ATOM_ADD(abort_cnt, 1);
    }
  }
}

inline double get_mean_latency() {
  double sum = 0;
  for (uint64_t qid = 0; qid < finished_cnt; qid++) {
    auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                      ->queries[qid % query_cnt_per_thd];
    query->timespan += query->net_times / 2 * 0.05;
    sum += query->timespan;
  }
  return sum / finished_cnt;
}

inline double get_mean_commite_wait_time() {
  double sum = 0;
  for (uint64_t qid = 0; qid < finished_cnt; qid++) {
    auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                      ->queries[qid % query_cnt_per_thd];
    if (query->commit_wait_starttime != 0) {
      query->commit_wait_time =
          (double)(query->commit_wait_endtime - query->commit_wait_starttime) /
          1000000UL;
      sum += query->commit_wait_time;
    }
  }
  return sum / finished_cnt;
}

inline double get_mean_lock_wait_time() {
  double sum = 0;
  for (uint64_t qid = 0; qid < finished_cnt; qid++) {
    auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                      ->queries[qid % query_cnt_per_thd];
    sum += query->lock_wait_time;
  }
  return sum / finished_cnt;
}

inline double get_mean_net_time() {
  double sum = 0;
  for (uint64_t qid = 0; qid < finished_cnt; qid++) {
    auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                      ->queries[qid % query_cnt_per_thd];
    sum += (double)query->net_times * 0.05;
  }
  return sum / finished_cnt;
}

inline double get_mean_abort_rate() {
  return (double)(abort_cnt + repair_cnt) /
         (abort_cnt + repair_cnt + finished_cnt);
}

inline double get_mean_abort_time() {
  double sum = 0;
  for (uint64_t qid = 0; qid < finished_cnt; qid++) {
    auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                      ->queries[qid % query_cnt_per_thd];
    if (query->timespan > 0) {
      query->exec_time = query->timespan - query->commit_wait_time -
                         query->lock_wait_time -
                         (double)query->net_times * 0.05;
      // 用i/o次数net_times代替事务执行过的request次数，但在query_on_thread模型上这个数要减半
      // query->abort_time = query->exec_time *
      //                     (double)(query->net_times - query->request_cnt) /
      //                     (query->net_times);
      query->abort_time = query->exec_time *
                          (double)(query->net_times / 2 - query->request_cnt) /
                          (query->net_times / 2);
      query->useful_time = query->exec_time - query->abort_time;
    }
    sum += query->abort_time;
  }
  return sum / finished_cnt;
}

inline double get_mean_useful_time() {
  double sum = 0;
  for (uint64_t qid = 0; qid < finished_cnt; qid++) {
    auto query = &query_queue->all_queries[qid / query_cnt_per_thd]
                      ->queries[qid % query_cnt_per_thd];
    sum += (double)query->useful_time;
  }
  return sum / finished_cnt;
}