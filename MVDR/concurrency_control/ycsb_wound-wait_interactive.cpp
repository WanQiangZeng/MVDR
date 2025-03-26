#include "ycsb_interactive_cc.h"
#include <cstdint>

// 释放资源包括两个步骤：更改owner，释放锁
// 加入了超时机制
#if WORKLOAD == YCSB

void *run_ycsb_wound_wait_interactive(void *args) {
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

    row_t *row = NULL;
    itemid_t *item = NULL;
    // get_row(wl, request->key, row);
    get_itemid(wl, request->key, item);
    row = static_cast<row_t *>(item->location);

    // 更改了三个地方，owner，reader，lock_type
    {
      unique_lock<mutex> lock(row->lock_entry.entry_mutex);
      if (row->lock_entry.owner == -1) {
        row->lock_entry.owner = id.first;
        if (request->rtype == WR) {
          row->lock_entry.lock_type = LOCK_EX;
          write(request, row);
        } else if (request->rtype == RD) {
          row->lock_entry.lock_type = LOCK_SH;
          read(request, row);
        }
      }
      // wait+超时机制
      else if (row->lock_entry.owner < id.first) {
        item->waiter.insert(make_pair(id.first, request->rtype));
        auto timeout = std::chrono::milliseconds(10); // 设置超时时间为1毫秒
        item->cv.wait_for(lock, timeout, [&]() {
          return ((row->lock_entry.owner == id.first) || query->rerun);
        });
        if (!query->rerun) {
          if (request->rtype == WR) {
            row->lock_entry.lock_type = LOCK_EX;
            write(request, row);
          } else if (request->rtype == RD) {
            row->lock_entry.lock_type = LOCK_SH;
            read(request, row);
          }
        }
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
          row->lock_entry.lock_type = LOCK_EX;
          write(request, row);
        } else if (request->rtype == RD) {
          row->lock_entry.lock_type = LOCK_SH;
          read(request, row);
        }
        item->cv.notify_all();
      }
    }

    usleep(50);

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
            } else {
              row_tmp->lock_entry.owner = -1;
            }
          } else {
            item_tmp->waiter.erase(make_pair(id.first, request_tmp->rtype));
          }
          item_tmp->cv.notify_all();
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

    if (id.second < query->request_cnt - 1) {
      lock_guard<mutex> lock(req_queue_mutex);
      req_queue.push(make_pair(id.first, id.second + 1));
    } else if (id.second == query->request_cnt - 1) {
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
            } else {
              row_tmp->lock_entry.owner = -1;
            }
          } else {
            item_tmp->waiter.erase(make_pair(id.first, request_tmp->rtype));
          }
          item_tmp->cv.notify_all();
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