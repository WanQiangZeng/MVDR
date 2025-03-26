#include "ycsb_interactive_cc.h"

// 释放资源包括两个步骤：更改owner，释放锁

#if WORKLOAD == YCSB
void *run_ycsb_wound_wait_corou_interactive(void *args) {
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
      unique_lock<mutex> lock(req_queue_mutex);
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

  // 从此处开始事务的执行部分
  exe_txn:
    query = &query_queue->all_queries[id.first / query_cnt_per_thd]
                 ->queries[id.first % query_cnt_per_thd];
    request = &query->requests[id.second];

    ++query->net_times;

    if (id.second == 0 && query->starttime == 0) {
      query->starttime = get_clock();
    }

    row_t *row = NULL;
    itemid_t *item = NULL;
    // get_row(wl, request->key, row);
    get_itemid(wl, request->key, item);
    row = static_cast<row_t *>(item->location);

    {
      unique_lock<mutex> lock(row->lock_entry.entry_mutex);
      if (row->lock_entry.owner == -1 || row->lock_entry.owner == id.first) {
        row->lock_entry.owner = id.first;
        {
          unique_lock<mutex> lock(query->query_mutex);
          if (query->lock_wait_starttime != 0) {
            query->lock_wait_endtime = get_clock();
            query->lock_wait_time =
                query->lock_wait_time + ((double)(query->lock_wait_endtime -
                                                  query->lock_wait_starttime) /
                                         1000000UL);
            query->lock_wait_starttime = 0;
            query->lock_wait_endtime = 0;
          }
        }
        if (request->rtype == WR) {
          write(request, row);
        } else if (request->rtype == RD) {
          read(request, row);
        }
      }
      // wait
      else if (row->lock_entry.owner < id.first) {
        item->waiter.insert(make_pair(id.first, request->rtype));
        lock.unlock();
        query->lock_wait_starttime = get_clock();
        // 将rid和qid存入coroutine，使用协程
        lock_guard<mutex> lock(coroutine_mutex);
        coroutine[id.first] = id.second;
        continue;
      }
      // wound,首先更改owner并抢占锁，2种情况：被终止的事务request处于队列中等待，或者正在被其他线程执行
      // 无论是那种情况，等被终止的事务的在外面的request执行完之后，再释放资源并置于队列重新执行
      else if (row->lock_entry.owner > id.first) {
        auto abort_id = row->lock_entry.owner;
        auto abort_query =
            &query_queue->all_queries[abort_id / query_cnt_per_thd]
                 ->queries[abort_id % query_cnt_per_thd];
        row->lock_entry.owner = id.first;
        {
          lock_guard<mutex> lock(abort_query->query_mutex);
          abort_query->rerun = true;
          abort_query->num_abort++;
        }
        if (request->rtype == WR) {
          write(request, row);
        } else if (request->rtype == RD) {
          read(request, row);
        }
        lock.unlock();

        {
          unique_lock<mutex> lock(coroutine_mutex);
          if (coroutine.find(abort_id) != coroutine.end()) {
            lock.unlock();
            lock_guard<mutex> lock(coroutine_to_exec_mutex);
            coroutine_to_exec.push_back(abort_id);
          }
        }
      }
    }

    // usleep(50);
    query->net_times++;

    // 若本线程执行的request被其他事务中止，将其释放资源并重新加入队列
    if (query->rerun) {
      for (auto rid = 0; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        row_t *row_tmp = NULL;
        itemid_t *item_tmp = NULL;
        // get_row(wl, request_tmp->key, row_tmp);
        get_itemid(wl, request_tmp->key, item_tmp);
        row_tmp = static_cast<row_t *>(item_tmp->location);
        {
          std::unique_lock<std::mutex> lock(row_tmp->lock_entry.entry_mutex);
          if (row_tmp->lock_entry.owner == id.first) {
            if (!item_tmp->waiter.empty()) {
              row_tmp->lock_entry.owner = (*item_tmp->waiter.begin()).first;
              item_tmp->waiter.erase(item_tmp->waiter.begin());
              lock_guard<mutex> lock(coroutine_to_exec_mutex);
              coroutine_to_exec.push_back(row_tmp->lock_entry.owner);
            } else {
              row_tmp->lock_entry.owner = -1;
            }
          } else {
            item_tmp->waiter.erase(make_pair(id.first, request_tmp->rtype));
          }
        }
      }
      {
        std::lock_guard<mutex> lock(query->query_mutex);
        query->rerun = false;
        query->lock_wait_starttime = 0;
        query->lock_wait_endtime = 0;
      }
      {
        lock_guard<mutex> lock(req_queue_mutex);
        req_queue.push(make_pair(id.first, 0));
      }
      ATOM_ADD(abort_cnt, 1);
      continue;
    }

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
          std::unique_lock<std::mutex> lock(row_tmp->lock_entry.entry_mutex);
          if (!item_tmp->waiter.empty()) {
            row_tmp->lock_entry.owner = (*item_tmp->waiter.begin()).first;
            item_tmp->waiter.erase(item_tmp->waiter.begin());
            lock_guard<mutex> lock(coroutine_to_exec_mutex);
            coroutine_to_exec.push_back(row_tmp->lock_entry.owner);
          } else {
            row_tmp->lock_entry.owner = -1;
          }
        }
      }
      {
        // lock_guard<mutex> lock(query->query_mutex);
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
  pthread_exit(NULL);
}
#endif