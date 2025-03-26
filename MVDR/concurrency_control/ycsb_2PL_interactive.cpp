#include "ycsb_interactive_cc.h"

#if WORKLOAD == YCSB
void *run_ycsb_2PL_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  ycsb_wl *wl = (ycsb_wl *)task_args->ycsbwl;

  while (finished_cnt < query_cnt * g_client_cnt) {
    ycsb_query *query = NULL;
    ycsb_request *request = NULL;
    pair<uint64_t, uint64_t> id;
    {
      lock_guard<mutex> lock(req_queue_mutex);
      if (req_queue.empty()) {
        for (auto i = 0; i < g_client_cnt; i++) {
          if (client_query[i].second &&
              client_query[i].first < query_cnt * g_client_cnt) {
            req_queue.push(make_pair(client_query[i].first, 0));
            client_query[i].second = false;
            break;
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

    query = &query_queue->all_queries[id.first / query_cnt_per_thd]
                 ->queries[id.first -
                           id.first / query_cnt_per_thd * query_cnt_per_thd];
    request = &query->requests[id.second];

    // pthread_mutex_lock(&print);
    // cout << "req_queue size: " << req_queue.size() << endl;
    // pthread_mutex_unlock(&print);

    if (request == NULL) {
      cout << "ERROR request is NULL" << endl;
      exit(0);
    }

    row_t *row;
    get_row(wl, request->key, row);

    pthread_mutex_lock(&row->lock_entry.row_mutex);
    if (request->rtype == WR) {
      write(request, row);
    } else if (request->rtype == RD) {
      read(request, row);
    }

    if (id.second < query->request_cnt - 1) {
      lock_guard<mutex> lock(req_queue_mutex);
      req_queue.push(make_pair(id.first, id.second + 1));
    } else if (id.second == query->request_cnt - 1) {
      for (auto rid = 0; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        row_t *row_tmp;
        get_row(wl, request_tmp->key, row_tmp);
        pthread_mutex_unlock(&row_tmp->lock_entry.row_mutex);
      }
      client_query[id.first % g_client_cnt].first += g_client_cnt;
      client_query[id.first % g_client_cnt].second = true;
      ATOM_ADD(finished_cnt, 1);
      // {
      //   lock_guard<mutex> lock(print);
      //   cout << "query: " << id.first << " finished" << endl;
      // }
    }
  }
  pthread_exit(NULL);
}
#endif