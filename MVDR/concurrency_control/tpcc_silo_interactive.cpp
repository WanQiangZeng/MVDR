#include "tpcc_interactive_cc.h"

#if WORKLOAD == TPCC

void *run_tpcc_silo_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  tpcc_wl *wl = (tpcc_wl *)task_args->tpccwl;

  while (finished_cnt < query_cnt * g_client_cnt) {
    tpcc_query *query = NULL;
    tpcc_request *request = NULL;
    pair<uint64_t, uint64_t> id;
    {
      lock_guard<mutex> lock(req_queue_mutex);
      if (req_queue.empty()) {
        for (auto i = 0; i < g_client_cnt; i++) {
          if (client_query[i].second &&
              client_query[i].first < query_cnt * g_client_cnt) {
            req_queue.push(make_pair(client_query[i].first, 0));
            client_query[i].second = false;
            // break;
          }
        }
      }
      if (!req_queue.empty()) {
        id = req_queue.front();
        req_queue.pop();
      } else {
        continue;
      }
    }

    usleep(50);

    query = &query_queue->all_queries[id.first / query_cnt_per_thd]
                 ->queries[id.first -
                           id.first / query_cnt_per_thd * query_cnt_per_thd];
    request = &query->requests[id.second];

    ++query->net_times;

    if (id.second == 0 && query->starttime == 0) {
      lock_guard<mutex> lock(query->query_mutex);
      query->starttime = get_clock();
    }

    row_t *row = NULL;
    uint64_t part_id = wh_to_part(query->w_id);
    uint64_t datasize;
    if (request->rtype == RD || request->rtype == WR) {
      get_row(request->index, request->key, part_id, row);
      datasize = row->datasize;
    }

    if (request->rtype == WR) {
      unique_lock<mutex> lock(row->lock_entry.entry_mutex);

      auto row_tmp = row;
      exec_tpcc_request(id.first, id.second, row_tmp, wl);
      request->data_local = (char *)_mm_malloc(sizeof(char) * datasize, 64);
      memcpy(request->data_local, row_tmp->data, row->datasize);

      request->last_version = row->version;
      query->write_set.push_back(id.second);
    } else if (request->rtype == RD) {
      unique_lock<mutex> lock(row->lock_entry.entry_mutex);
      exec_tpcc_request(id.first, id.second, row, wl);
      request->last_version = row->version;
      query->read_set.push_back(id.second);
    } else if (request->rtype == IS) {
      exec_tpcc_request(id.first, id.second, row, wl);
    }

    // usleep(50);
    query->net_times++;

    if (id.second < query->request_cnt - 1) {
      lock_guard<mutex> lock(req_queue_mutex);
      req_queue.push(make_pair(id.first, id.second + 1));
    }
    // 验证
    else if (id.second == query->request_cnt - 1) {
      // 对写集加锁
      for (auto rid : query->write_set) {
        auto v_request = &query->requests[rid];
        row_t *v_row = NULL;
        get_row(v_request->index, v_request->key, part_id, v_row);
        {
          lock_guard<mutex> lock(v_row->lock_entry.entry_mutex);
          // 若快照发生改变或锁已经被占有，则事务中止
          if (v_row->version != v_request->last_version ||
              v_row->writing_id != -1) {
            lock_guard<mutex> lock(query->query_mutex);
            query->rerun = true;
            query->num_abort++;
            break;
          }
          v_row->writing_id = query->id;
        }
      }
      // 若写集加锁验证通过，接下来验证读集
      if (!query->rerun) {
        // 验证读集
        for (auto rid : query->read_set) {
          auto v_request = &query->requests[rid];
          row_t *v_row = NULL;
          get_row(v_request->index, v_request->key, part_id, v_row);
          {
            lock_guard<mutex> lock(v_row->lock_entry.entry_mutex);
            if (v_row->version != v_request->last_version ||
                (v_row->writing_id != -1 && v_row->writing_id != query->id)) {
              lock_guard<mutex> lock(query->query_mutex);
              query->rerun = true;
              query->num_abort++;
              break;
            }
          }
        }
      }
      // 事务中止回滚
      if (query->rerun) {
        // 写集恢复request并解锁
        for (auto rid : query->write_set) {
          auto v_request = &query->requests[rid];
          // 初始化request
          v_request->last_version = -1;
          row_t *v_row = NULL;
          get_row(v_request->index, v_request->key, part_id, v_row);
          // 若request为写操作，两个解锁操作
          {
            lock_guard<mutex> lock(v_row->lock_entry.entry_mutex);
            if (v_row->writing_id == query->id) {
              v_row->writing_id = -1;
            }
          }
        }
        query->write_set.clear();
        // 读集恢复request
        for (auto rid : query->read_set) {
          auto v_request = &query->requests[rid];
          // 初始化request
          v_request->last_version = -1;
        }
        query->read_set.clear();
        // query重新执行
        {
          lock_guard<mutex> lock(query->query_mutex);
          query->rerun = false;
        }
        {
          lock_guard<mutex> lock(req_queue_mutex);
          req_queue.push(make_pair(id.first, 0));
        }
        ATOM_ADD(abort_cnt, 1);
      }
      // 事务提交,解锁并写入新数据
      else {
        for (auto rid : query->write_set) {
          auto v_request = &query->requests[rid];
          row_t *v_row = NULL;
          get_row(v_request->index, v_request->key, part_id, v_row);
          {
            lock_guard<mutex> lock(v_row->lock_entry.entry_mutex);
            memcpy(v_row->get_data(), v_request->data_local, v_row->datasize);
            v_row->version = query->id;
            v_row->writing_id = -1;
          }
        }
        {
          lock_guard<mutex> lock(query->query_mutex);
          query->endtime = get_clock();
          query->timespan =
              (double)(query->endtime - query->starttime) / 1000000UL;
        }
        client_query[id.first % g_client_cnt].first += g_client_cnt;
        client_query[id.first % g_client_cnt].second = true;
        ATOM_ADD(finished_cnt, 1);
        {
          lock_guard<mutex> lock(print);
          cout << "query: " << id.first << " finished" << endl;
        }
      }
    }
  }
  pthread_exit(NULL);
}
#endif