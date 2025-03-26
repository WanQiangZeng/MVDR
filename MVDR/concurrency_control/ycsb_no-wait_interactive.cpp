#include "ycsb_interactive_cc.h"

// 加入客户端的改变：1.while的终止条件 2.req_queue的加入 3.提交阶段
#if WORKLOAD == YCSB
void *run_ycsb_no_wait_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  ycsb_wl *wl = (ycsb_wl *)task_args->ycsbwl;

  while (finished_cnt < query_cnt * g_client_cnt) {
    ycsb_query *query = NULL;
    ycsb_request *request = NULL;
    pair<uint64_t, uint64_t> id;
    {
      lock_guard<mutex> lock(req_queue_mutex);
      if (req_queue.empty()) {
        for (uint64_t i = 0; i < g_client_cnt; i++) {
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

    row_t *row;
    get_row(wl, request->key, row);

    if (row->lock_entry.owner == -1) {
      lock_guard<mutex> lock(row->lock_entry.entry_mutex);
      row->lock_entry.owner = id.first;
      if (request->rtype == WR) {
        write(request, row);
      } else if (request->rtype == RD) {
        read(request, row);
      }
    } else {
      for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        row_t *row_tmp;
        get_row(wl, request_tmp->key, row_tmp);
        lock_guard<mutex> lock(row_tmp->lock_entry.entry_mutex);
        if (row_tmp->lock_entry.owner == id.first) {
          row_tmp->lock_entry.owner = -1;
        }
      }
      ATOM_ADD(abort_cnt, 1);
      {
        lock_guard<mutex> lock(query->query_mutex);
        query->num_abort++;
      }
      {
        lock_guard<mutex> lock(req_queue_mutex);
        req_queue.push(make_pair(id.first, 0));
      }
      continue;
    }

    if (id.second < query->request_cnt - 1) {
      lock_guard<mutex> lock(req_queue_mutex);
      req_queue.push(make_pair(id.first, id.second + 1));
    } else if (id.second == query->request_cnt - 1) {
      for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        row_t *row_tmp;
        get_row(wl, request_tmp->key, row_tmp);
        lock_guard<mutex> lock(row_tmp->lock_entry.entry_mutex);
        row_tmp->lock_entry.owner = -1;
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
  pthread_exit(NULL);
}
#endif