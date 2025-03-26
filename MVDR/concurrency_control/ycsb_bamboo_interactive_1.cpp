#include "ycsb_interactive_cc.h"

// 可能存在事务同时处于中止和提交队列中，因此当事务进入提交队列即视为已提交
// 仍不稳定
// 用item中的waiter此set代替retired
#if WORKLOAD == YCSB
void *run_ycsb_bamboo_interactive(void *args) {
  TaskArgs *task_args = (TaskArgs *)args;
  ycsb_wl *wl = (ycsb_wl *)task_args->ycsbwl;
  while (finished_cnt < query_cnt * g_client_cnt) {

    // 再处理提交事务
    uint64_t commit_query_id;
    bool commit_is_empty;
    {
      lock_guard<mutex> lock(to_commit_query_mutex);
      commit_is_empty = to_commit_query.empty();
      if (!commit_is_empty) {
        commit_query_id = to_commit_query.front();
        to_commit_query.pop();
      }
    }
    if (!commit_is_empty) {
      auto query_tmp =
          &query_queue->all_queries[commit_query_id / query_cnt_per_thd]
               ->queries[commit_query_id % query_cnt_per_thd];
      query_tmp->commited = true;
      query_tmp->commit_wait_endtime = get_clock();
      for (uint64_t rid = 0; rid < query_tmp->request_cnt; rid++) {
        auto request_tmp = &query_tmp->requests[rid];
        row_t *row_tmp = NULL;
        itemid_t *item_tmp = NULL;
        get_itemid(wl, request_tmp->key, item_tmp);
        row_tmp = static_cast<row_t *>(item_tmp->location);
        {
          unique_lock<mutex> lock(row_tmp->lock_entry.entry_mutex);
          auto it_tmp = item_tmp->waiter.begin();
          for (; it_tmp != item_tmp->waiter.end() &&
                 it_tmp->first != commit_query_id;
               it_tmp++)
            ;
          if (it_tmp != item_tmp->waiter.end()) {
            it_tmp = item_tmp->waiter.erase(it_tmp);
          } else {
            lock_guard<mutex> lock(print);
            cout << "ERROR: during commiting query: " << commit_query_id
                 << " not in retired set" << endl;
            exit(0);
          }
          if (it_tmp == item_tmp->waiter.begin() &&
              it_tmp != item_tmp->waiter.end()) {
            auto next_id = (*it_tmp).first;
            auto next_query =
                &query_queue->all_queries[next_id / query_cnt_per_thd]
                     ->queries[next_id % query_cnt_per_thd];
            {
              lock_guard<mutex> lock(next_query->query_mutex);
              next_query->dep_cnt--;
              if (next_query->dep_cnt == 0 && next_query->able_to_commit &&
                  !next_query->commited) {
                lock_guard<mutex> lock(to_commit_query_mutex);
                to_commit_query.push(next_id);
              }
            }
          }
        }
      }
      {
        lock_guard<mutex> lock(query_tmp->query_mutex);
        query_tmp->endtime = get_clock();
        query_tmp->timespan =
            (double)(query_tmp->endtime - query_tmp->starttime) / 1000000UL;
      }
      client_query[commit_query_id % g_client_cnt].first += g_client_cnt;
      client_query[commit_query_id % g_client_cnt].second = true;
      ATOM_ADD(finished_cnt, 1);
      {
        lock_guard<mutex> lock(print);
        cout << "query: " << commit_query_id << " finished" << endl;
      }
    }

    ycsb_query *query = NULL;
    ycsb_request *request = NULL;
    pair<uint64_t, uint64_t> id;
    {
      lock_guard<mutex> lock(req_queue_mutex);
      if (req_queue.empty()) {
        for (uint64_t i = 0; i < g_client_cnt; i++) {
          if (client_query[i].second &&
              client_query[i].first < query_cnt * g_client_cnt) {
            auto id_tmp = client_query[i].first;
            auto query_tmp =
                &query_queue->all_queries[id_tmp / query_cnt_per_thd]
                     ->queries[id_tmp % query_cnt_per_thd];
            req_queue.push(
                make_pair(client_query[i].first, query_tmp->next_rid));
            query_tmp->next_rid++;
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
                 ->queries[id.first % query_cnt_per_thd];
    request = &query->requests[id.second];

    ++query->net_times;

    if (id.second == 0 && query->starttime == 0) {
      lock_guard<mutex> lock(query->query_mutex);
      query->starttime = get_clock();
    }

    row_t *row = NULL;
    itemid_t *item = NULL;
    get_itemid(wl, request->key, item);
    row = static_cast<row_t *>(item->location);
    uint64_t key = request->key;

    query->lock_wait_starttime = get_clock();
    {
      unique_lock<mutex> lock(row->lock_entry.entry_mutex);
      query->lock_wait_endtime = get_clock();
      query->lock_wait_time +=
          (double)(query->lock_wait_endtime - query->lock_wait_starttime) /
          1000000UL;

      // auto write_it = item->waiter.find(make_pair(id.first, request->rtype));
      auto write_it = item->waiter.begin();
      for (; write_it != item->waiter.end() && (*write_it).first != id.first;
           write_it++)
        ;
      if (write_it == item->waiter.end()) {
        write_it =
            item->waiter.insert(make_pair(id.first, request->rtype)).first;
        if (write_it != item->waiter.begin()) {
          lock_guard<mutex> lock(query->query_mutex);
          query->dep_cnt++;
        } else {
          if (next(write_it) != item->waiter.end()) {
            auto next_id = next(write_it)->first;
            auto next_query =
                &query_queue->all_queries[next_id / query_cnt_per_thd]
                     ->queries[next_id % query_cnt_per_thd];
            {
              lock_guard<mutex> lock(next_query->query_mutex);
              next_query->dep_cnt++;
            }
          }
        }
        write_it++;
        // 被中止的事务三种情况：1.在队列中等待执行 2.正在被其他线程执行 3.事务执行完成，等待提交
        if (request->rtype == RD) {
          for (; write_it != item->waiter.end() && write_it->second != WR;
               write_it++)
            ;
        }
        for (; write_it != item->waiter.end(); write_it++) {
          abort(write_it->first);
        }
      }
      // else if (write_it != item->waiter.end()) {
      //   write_it++;
      //   for (; write_it != item->waiter.end(); write_it++) {
      //     if (request->rtype == WR) {
      //       abort((*write_it).first);
      //     }
      //   }
      // }
      if (request->rtype == WR) {
        write(request, row);
      } else if (request->rtype == RD) {
        read(request, row);
      }
    }

    // 处理级联中止，但dep_cnt不变
    if (query->rerun) {
      for (uint64_t rid = 0; rid < query->request_cnt; rid++) {
        auto request_tmp = &query->requests[rid];
        row_t *row_tmp = NULL;
        itemid_t *item_tmp = NULL;
        get_itemid(wl, request_tmp->key, item_tmp);
        row_tmp = static_cast<row_t *>(item_tmp->location);
        {
          unique_lock<mutex> lock(row_tmp->lock_entry.entry_mutex);
          // auto it_tmp =
          //     item_tmp->waiter.find(make_pair(id.first, request_tmp->rtype));
          auto it_tmp = item_tmp->waiter.begin();
          for (;
               it_tmp != item_tmp->waiter.end() && (*it_tmp).first != id.first;
               it_tmp++)
            ;
          if (it_tmp != item_tmp->waiter.end()) {
            it_tmp++;
            if (request_tmp->rtype == WR) {
              for (; it_tmp != item_tmp->waiter.end(); it_tmp++) {
                abort((*it_tmp).first);
              }
            }
          }
        }
      }
    }

    // usleep(50);
    query->net_times++;

    {
      lock_guard<mutex> lock(query->query_mutex);
      if (id.second < query->request_cnt - 1) {
        {
          lock_guard<mutex> lock(req_queue_mutex);
          req_queue.push(make_pair(id.first, query->next_rid));
        }
        query->rerun = false;
        query->next_rid++;
      } else if (id.second == query->request_cnt - 1) {
        if (query->next_rid == query->request_cnt) {
          // 代表request都已经执行完，有资格提交
          query->able_to_commit = true;
          query->commit_wait_starttime = get_clock();
          // 无依赖项则可以提交，加入提交队列
          if (query->dep_cnt == 0) {
            query->in_commited_query = true;
            {
              lock_guard<mutex> lock(to_commit_query_mutex);
              to_commit_query.push(id.first);
            }
          } else if (query->dep_cnt < 0) {
            cout << "ERROR: query: " << id.first
                 << " has dep_cnt: " << query->dep_cnt << endl;
            exit(0);
          }
          // else if (query->dep_cnt > 0) {
          //   lock_guard<mutex> lock(print);
          //   cout << "query: " << id.first << " has dep_cnt: " <<
          //   query->dep_cnt
          //        << endl;
          // }
        } else if (query->next_rid < query->request_cnt) {
          {
            lock_guard<mutex> lock(req_queue_mutex);
            req_queue.push(make_pair(id.first, query->next_rid));
          }
          query->rerun = false;
          query->next_rid++;
        }
      }
    }
  }
  pthread_exit(NULL);
}
#endif