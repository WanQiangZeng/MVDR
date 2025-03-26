#include "ycsb_interactive_cc.h"

#if WORKLOAD == YCSB
void *run_ycsb_wait_die_corou_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  ycsb_wl *wl = (ycsb_wl *)task_args->ycsbwl;
  while (finished_cnt < query_cnt * g_client_cnt) {
    ycsb_query *query = NULL;
    ycsb_request *request = NULL;
    pair<uint64_t, uint64_t> id;
    bool is_empty = true;

    // 首先执行协程中的事务
    {
      unique_lock<mutex> lock(coroutine_to_exec_mutex);
      if (!coroutine_to_exec.empty()) {
        is_empty = false;
        auto it = coroutine_to_exec.begin();
        id.first = *it;
        it = coroutine_to_exec.erase(it);
        lock.unlock();
        {
          lock_guard<mutex> lock(coroutine_mutex);
          id.second = coroutine[id.first];
          coroutine.erase(id.first);
        }
      } else {
        is_empty = true;
      }
    }
    if (!is_empty) { // 若协程中有事务，则跳转执行
      goto exe_txn;
    }

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

    // 执行事务的部分
  exe_txn:
    query = &query_queue->all_queries[id.first / query_cnt_per_thd]
                 ->queries[id.first % query_cnt_per_thd];
    request = &query->requests[id.second];

    row_t *row = NULL;
    itemid_t *item = NULL;
    get_itemid(wl, request->key, item);
    row = static_cast<row_t *>(item->location);

    {
      std::unique_lock<std::mutex> lock(row->lock_entry.entry_mutex);
      if (row->lock_entry.owner == -1) {
        row->lock_entry.owner = id.first;
        if (request->rtype == WR) {
          write(request, row);
        } else if (request->rtype == RD) {
          read(request, row);
        }
      }
      // wait
      else if (row->lock_entry.owner > id.first) {
        item->waiter.insert(make_pair(id.first, request->rtype));
        lock.unlock();
        // 发现阻塞时将rid和qid存入coroutine，使用协程
        lock_guard<mutex> lock(coroutine_mutex);
        coroutine[id.first] = id.second;
        continue;
      }
      // die
      else if (row->lock_entry.owner < id.first) {
        lock.unlock();
        {
          std::lock_guard<mutex> lock(query->query_mutex);
          query->num_abort++;
          query->rerun = true;
        }
      }
    }

    // usleep(50);

    if (query->rerun) {
      for (auto rid = 0; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        row_t *row_tmp = NULL;
        itemid_t *item_tmp = NULL;
        get_itemid(wl, request_tmp->key, item_tmp);
        row_tmp = static_cast<row_t *>(item_tmp->location);
        {
          std::lock_guard<std::mutex> lock(row_tmp->lock_entry.entry_mutex);
          if (row_tmp->lock_entry.owner == id.first) {
            if (!item_tmp->waiter.empty()) {
              row_tmp->lock_entry.owner = prev(item_tmp->waiter.end())->first;
              item_tmp->waiter.erase(prev(item_tmp->waiter.end()));
              lock_guard<mutex> lock(coroutine_to_exec_mutex);
              coroutine_to_exec.push_back(row_tmp->lock_entry.owner);
            } else {
              row_tmp->lock_entry.owner = -1;
            }
          }
        }
      }
      {
        std::lock_guard<mutex> lock(query->query_mutex);
        query->rerun = false;
      }
      {
        lock_guard<mutex> lock(req_queue_mutex);
        req_queue.push(make_pair(id.first, 0));
      }
      ATOM_ADD(abort_cnt, 1);
      continue;
    }

    // 本request执行完毕，加入下一个request或提交
    if (id.second < query->request_cnt - 1) {
      lock_guard<mutex> lock(req_queue_mutex);
      req_queue.push(make_pair(id.first, id.second + 1));
    } else if (id.second == query->request_cnt - 1) {
      for (auto rid = 0; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        row_t *row_tmp = NULL;
        itemid_t *item_tmp = NULL;
        get_itemid(wl, request_tmp->key, item_tmp);
        row_tmp = static_cast<row_t *>(item_tmp->location);
        {
          std::lock_guard<std::mutex> lock(row_tmp->lock_entry.entry_mutex);
          if (row_tmp->lock_entry.owner == id.first) {
            if (!item_tmp->waiter.empty()) {
              row_tmp->lock_entry.owner = prev(item_tmp->waiter.end())->first;
              item_tmp->waiter.erase(prev(item_tmp->waiter.end()));
              lock_guard<mutex> lock(coroutine_to_exec_mutex);
              coroutine_to_exec.push_back(row_tmp->lock_entry.owner);
            } else {
              row_tmp->lock_entry.owner = -1;
            }
          }
        }
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